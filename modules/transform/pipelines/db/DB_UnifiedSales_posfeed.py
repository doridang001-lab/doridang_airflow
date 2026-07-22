"""
unified_sales - Posfeed 채널 전용 모듈.

입력:
- ANALYTICS_DB/posfeed_sales/brand=*/store=*/ym=YYYY-MM/posfeed_orders.csv
- ANALYTICS_DB/posfeed_sales_detail/brand=*/store=*/ym=YYYY-MM/posfeed_order_item.csv

출력:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet

참고 로직:
- 플랫폼별 포스피드_매출 계산 (땡겨요 보정 적용)
- 아이템 합계합 ≠ 포스피드_매출인 경우 비율 배분으로 total_price 산정
- 주문상태 '배달완료' → 정상, 그 외('취소') → 취소
"""

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    POSFEED_WHITELIST_CSV_PATH,
    existing_fin_product_csv_path,
)
from modules.transform.utility.store_normalize import (
    normalize as _normalize_store_names,
    strip_brand as _strip_brand,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    _apply_posfeed_blacklist,
    _load_posfeed_blacklist,
    _make_unified_pk,
    _normalize_item_key,
    _load_store_map,
    _lookup_store_meta,
    _normalize_time,
    _save_unified_daily,
    _to_int_series,
    _kst_today_str,
    iter_unified_sales_files,
)
from modules.transform.pipelines.db.DB_FinProduct import _safe_replace
from modules.transform.pipelines.db.DB_ItemIdAllocator import MANUAL_SOURCE_BASE, allocate_manual_item_ids, item_block_size

logger = logging.getLogger(__name__)

RAW_POSFEED_SALES = ANALYTICS_DB / "posfeed_sales"
RAW_POSFEED_DETAIL = ANALYTICS_DB / "posfeed_sales_detail"
POSFEED_SOURCE = "posfeed"
EXCLUSION_LOG_BASE = ANALYTICS_DB / "posfeed_exclusion_log"
# Posfeed 제외 항목 중 '도리' 포함 알림은 유지하고, 자동으로 Y로 보정합니다.
_ENABLE_POSFEED_DORI_EXCLUSION_ALERT = True
# 주문상태 → 정상 처리할 완료 상태
_COMPLETE_STATUSES = {"배달완료", "접수", "완료"}

# 주문경로 → platform 정규화 (배민1 → 배달의민족 통일)
_PLATFORM_MAP = {
    "배민1": "배달의민족",
    "배달의민족": "배달의민족",
    "쿠팡이츠": "쿠팡이츠",
    "땡겨요": "땡겨요",
    "요기요": "요기요",
    "배달특급": "배달특급",
    "네이버": "네이버주문",
    "기타": "기타",
}


def _derive_posfeed_revenue(df: pd.DataFrame) -> pd.Series:
    """플랫폼별 포스피드 실매출 계산.

    요구사항(2026-05 기준):
    - 배달의민족(배민1): 결제금액 - 배달비
    - 배달의민족(일반/포장): 결제금액
    - 쿠팡이츠:
        - 총 주문금액 == 할인 → 0
        - 그 외: 배달비 있으면 (총 주문금액 - 배달비), 없으면 총 주문금액
    - 요기요:
        - 주문자 구주소 == "요기요 요기배달" → 총 주문금액 - 배달비
        - 그 외(빈칸 포함) → 총 주문금액
    - 땡겨요 포장: 총주문금액 - 2000 (포장비 2000원 땡겨요가 별도 차감)
    - 땡겨요 배달: 총주문금액
    - 그 외: 결제금액 (플랫폼 수수료는 정산서 별도)
    """
    order_path = df["주문경로"].fillna("").astype(str).str.strip()
    order_type = df.get("주문타입", pd.Series("", index=df.index)).fillna("").astype(str)

    total_amount = df.get("총 주문금액", pd.Series(0, index=df.index))
    delivery_fee = df.get("배달비", pd.Series(0, index=df.index))
    discount_amount = df.get("할인", pd.Series(0, index=df.index))
    paid_amount = df.get("결제금액", pd.Series(0, index=df.index))

    revenue = pd.Series(paid_amount, index=df.index)

    # 땡겨요
    is_ddangyo = order_path.str.contains("땡겨요", regex=False)
    is_pack = order_type.str.contains("포장", regex=False)
    revenue = pd.Series(
        np.where(
            is_ddangyo & is_pack,
            total_amount - 2000,
            np.where(is_ddangyo & ~is_pack, total_amount, revenue),
        ),
        index=df.index,
    )

    # 배민1 (주문경로 원본 기준)
    is_baemin1 = order_path.eq("배민1")
    revenue = pd.Series(
        np.where(is_baemin1, paid_amount - delivery_fee, revenue),
        index=df.index,
    )

    # 쿠팡이츠
    is_coupang = order_path.eq("쿠팡이츠")
    coupang_revenue = np.where(
        total_amount.eq(discount_amount),
        0,
        np.where(delivery_fee.gt(0), total_amount - delivery_fee, total_amount),
    )
    revenue = pd.Series(np.where(is_coupang, coupang_revenue, revenue), index=df.index)

    # 요기요
    is_yogiyo = order_path.eq("요기요")
    if "주문자 구주소" in df.columns:
        old_addr = df["주문자 구주소"].fillna("").astype(str).str.strip()
        is_yogibae = old_addr.str.contains("요기요 요기배달", regex=False)
    else:
        is_yogibae = pd.Series(False, index=df.index)
    yogiyo_revenue = np.where(
        is_yogibae & delivery_fee.gt(0),
        total_amount - delivery_fee,
        total_amount,
    )
    revenue = pd.Series(np.where(is_yogiyo, yogiyo_revenue, revenue), index=df.index)

    return revenue.clip(lower=0).astype(int)


def _load_orders_for_ym(ym: str) -> pd.DataFrame:
    files = list(RAW_POSFEED_SALES.glob(f"brand=*/store=*/ym={ym}/posfeed_orders.csv"))
    if not files:
        return pd.DataFrame()
    parts = []
    for f in files:
        try:
            parts.append(pd.read_csv(f, encoding="utf-8-sig", dtype=str))
        except Exception as e:
            logger.warning("orders 로드 실패: %s | %s", f, e)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _load_items_for_ym(ym: str) -> pd.DataFrame:
    files = list(RAW_POSFEED_DETAIL.glob(f"brand=*/store=*/ym={ym}/posfeed_order_item.csv"))
    if not files:
        return pd.DataFrame()
    parts = []
    for f in files:
        try:
            parts.append(pd.read_csv(f, encoding="utf-8-sig", dtype=str))
        except Exception as e:
            logger.warning("items 로드 실패: %s | %s", f, e)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _transform_df(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    date_str: str,
    *,
    persist_item_ids: bool = True,
) -> pd.DataFrame:
    """posfeed orders + items → unified_sales 스키마 변환."""
    if orders.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    # 날짜 필터
    orders = orders[orders["등록날짜"].astype(str).str.strip() == date_str].copy()
    if orders.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    # 수치형 변환
    for col in ["총 주문금액", "배달비", "할인", "결제금액"]:
        if col in orders.columns:
            orders[col] = _to_int_series(orders[col])

    # 포스피드_매출 계산
    orders["포스피드_매출"] = _derive_posfeed_revenue(orders)

    # order_type: 주문타입 기반
    orders["order_type"] = orders["주문타입"].fillna("").astype(str).map(
        lambda v: "배달_포장" if "포장" in v else "배달"
    )

    # platform 정규화
    orders["platform"] = orders["주문경로"].fillna("").astype(str).str.strip().map(
        lambda v: _PLATFORM_MAP.get(v, v)
    )

    # sale_type
    orders["sale_type"] = orders["주문상태"].fillna("").astype(str).map(
        lambda v: "정상" if v in _COMPLETE_STATUSES else "취소"
    )

    # items join
    if not items.empty:
        items = items.copy()
        items["주문코드_key"] = pd.to_numeric(
            items["주문코드"].astype(str).str.strip(), errors="coerce"
        ).fillna(0).astype(int).astype(str)
        orders["주문코드_key"] = pd.to_numeric(
            orders["주문 코드"].astype(str).str.strip(), errors="coerce"
        ).fillna(0).astype(int).astype(str)

        for col in ["단품가격", "합계", "수량"]:
            if col in items.columns:
                items[col] = _to_int_series(items[col])

        dfs = pd.merge(
            orders,
            items[["주문코드_key", "상품명", "수량", "단품가격", "합계"]],
            on="주문코드_key",
            how="left",
        )
        null_orders = dfs[dfs["상품명"].isna()]["주문 코드"].unique()
        if len(null_orders) > 0:
            logger.warning(
                "items 매칭 누락 주문 %d건 — 상세 수집 미완료 또는 주문코드 불일치 가능성: %s",
                len(null_orders),
                list(null_orders[:10]),
            )
    else:
        dfs = orders.copy()
        for col in ["상품명", "수량", "단품가격", "합계"]:
            dfs[col] = "" if col == "상품명" else 0

    # 상세 items 와 조인된 주문만 적재한다.
    matched_mask = dfs["상품명"].notna() & (dfs["상품명"].astype(str).str.strip() != "")
    skipped_orders = dfs.loc[~matched_mask, "주문 코드"].dropna().astype(str).str.strip().unique()
    if len(skipped_orders) > 0:
        logger.warning(
            "items 미매칭 주문 %d건 스킵: %s",
            len(skipped_orders),
            list(skipped_orders[:10]),
        )
    dfs = dfs[matched_mask].copy()
    if dfs.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    dfs["합계"] = pd.to_numeric(dfs["합계"], errors="coerce").fillna(0).astype(int)
    dfs["단품가격"] = pd.to_numeric(dfs["단품가격"], errors="coerce").fillna(0).astype(int)
    dfs["수량"] = pd.to_numeric(dfs["수량"], errors="coerce").fillna(0).astype(int)

    # 숫자만으로 된 '+' 옵션(가격 캐리어)을 본품에 합산 후 제거.
    # 프로모션 메뉴는 본품 단가가 0이고 실제 가격이 "'+ 16900" 같은 옵션명에
    # 실려오는 경우가 있어, 그대로 두면 "16900"이 별도 상품으로 등록되고
    # 최고가라서 주문의 대표메뉴까지 가로챈다.
    _raw_name = dfs["상품명"].fillna("").astype(str).str.strip()
    _is_option = _raw_name.str.startswith("'+")
    _is_numeric_option = _is_option & _raw_name.str.lstrip("'+ ").str.match(r"^\d+$")
    if _is_numeric_option.any():
        _main_mask = ~_is_option & (_raw_name != "배달비")
        _main_rows = dfs[_main_mask].sort_values("단품가격", ascending=False)
        _main_idx_by_order = _main_rows.groupby("주문 코드").apply(lambda g: g.index[0])
        for opt_idx in dfs.index[_is_numeric_option]:
            order_code = dfs.at[opt_idx, "주문 코드"]
            if order_code in _main_idx_by_order.index:
                main_idx = _main_idx_by_order.loc[order_code]
                dfs.at[main_idx, "단품가격"] += dfs.at[opt_idx, "단품가격"]
                dfs.at[main_idx, "합계"] += dfs.at[opt_idx, "합계"]
            else:
                logger.warning(
                    "숫자 옵션 상품 본품 매칭 실패, 옵션 유지: 주문코드=%s 상품명=%s",
                    order_code, dfs.at[opt_idx, "상품명"],
                )
                _is_numeric_option.at[opt_idx] = False
        dfs = dfs[~_is_numeric_option].copy()

    # 아이템 비율 배분: sum(합계) != 포스피드_매출이면 비율로 total_price 산정
    order_total = dfs.groupby("주문 코드")["합계"].transform("sum")
    order_posfeed = dfs.groupby("주문 코드")["포스피드_매출"].transform("first").astype(int)
    is_zero = order_total == 0
    is_equal = order_posfeed == order_total

    dfs["total_price"] = np.where(
        is_zero,
        0,
        np.where(
            is_equal,
            dfs["합계"],
            (dfs["합계"] / order_total.where(order_total != 0, 1)) * order_posfeed,
        ),
    )
    dfs["total_price"] = dfs["total_price"].apply(lambda v: round(float(v))).astype(int)
    dfs["discount_amount"] = (dfs["합계"] - dfs["total_price"]).clip(lower=0)

    # 취소 주문은 total_price 음수
    is_cancel = dfs["sale_type"] == "취소"
    dfs.loc[is_cancel, "total_price"] = -dfs.loc[is_cancel, "total_price"].abs()

    # unified_sales 컬럼
    dfs["sale_date"] = date_str
    dfs["ym"] = date_str[:7]
    dfs["source"] = POSFEED_SOURCE

    # brand/store: merge 후 suffix 처리
    brand_col = "brand_x" if "brand_x" in dfs.columns else "brand"
    store_col = "store_x" if "store_x" in dfs.columns else "store"
    dfs["brand"] = dfs[brand_col].fillna("").astype(str).str.strip()
    dfs["store"] = dfs[store_col].fillna("").astype(str).str.strip()
    dfs["store"] = _normalize_store_names(dfs["store"])  # alias 통일 (서울역삼점 → 역삼점 등)
    dfs["store"] = _strip_brand(dfs["store"])            # 브랜드 접두어 제거 (나홀로/도리당)

    # store meta
    store_map = _load_store_map()
    dfs["_skey"] = dfs["store"].str.strip().str.split().str[-1]
    dfs["담당자"] = dfs["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    dfs["region"] = dfs["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    dfs["실오픈일"] = dfs["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    dfs = dfs.drop(columns=["_skey"])

    dfs["order_id"] = dfs["외부 주문 번호"].fillna("").astype(str).str.strip()
    # 외부 주문번호 없는 행: store+시각 조합으로 surrogate order_id 부여
    # (같은 매장·같은 시각 항목은 동일 주문으로 유지, 다른 그룹과 홀 오염 방지)
    _no_id = dfs["order_id"] == ""
    if _no_id.any():
        dfs.loc[_no_id, "order_id"] = (
            "tmp_"
            + dfs.loc[_no_id, "store"].astype(str).str.strip()
            + "_"
            + dfs.loc[_no_id, "주문등록 시각"].fillna("").astype(str).str.strip()
        )
    dfs["order_time"] = dfs["주문등록 시각"].apply(_normalize_time)
    dfs["item_name"] = (
        dfs["상품명"].fillna("").astype(str)
        .str.strip()
        .str.lstrip("'+ ")
    )
    dfs["qty"] = dfs["수량"].astype(str)
    dfs["unit_price"] = dfs["단품가격"]

    # item_seq: order 내 순번
    dfs["item_seq"] = dfs.groupby("order_id").cumcount().add(1).astype(int).astype(str)

    # item_id: source+brand+store+item_name 기준 매장별 숫자 코드
    dfs["item_id"] = allocate_manual_item_ids(
        dfs[["source", "brand", "store", "item_name", "unit_price"]],
        persist=persist_item_ids,
    )

    # "홀_" 접두어 아이템이 있는 주문: order_id 전체를 홀 테이블로 재분류
    # (배달비 등 부속 아이템도 같은 order_id에 묶여있어 item_name 단독 조건으로는 누락됨)
    _hall_order_ids = dfs.loc[
        dfs["item_name"].str.contains("홀_", regex=False),
        "order_id",
    ].unique()
    _hall_mask = dfs["order_id"].isin(_hall_order_ids)
    dfs.loc[_hall_mask, "platform"] = "홀"
    dfs.loc[_hall_mask, "order_type"] = "홀_테이블"

    # 블랙리스트 필터: grp exclude_check=Y 항목 제거 (brand·item_name 할당 후, menu_name 계산 전)
    _bl_before = dfs.copy()
    dfs = _apply_posfeed_blacklist(dfs)
    _bl_excluded = _bl_before.loc[~_bl_before.index.isin(dfs.index)]
    if dfs.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    # menu_name: 주문 내 최고가 아이템명 (없으면 orders 메뉴명 폴백)
    main_mask = dfs.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1)
    menu_map = dfs.loc[main_mask].set_index("order_id")["item_name"].to_dict()
    dfs["menu_name"] = dfs["order_id"].map(menu_map).fillna(
        dfs["메뉴명"].fillna("").astype(str)
    )

    # 제외 로그: menu_map 계산 후 저장 (menu_name 포함)
    if not _bl_excluded.empty:
        _save_exclusion_log(_bl_excluded, date_str, menu_map)

    # order_cnt: 대표행 1(정상) / -1(취소), 나머지 0
    dfs["order_cnt"] = 0
    dfs.loc[main_mask, "order_cnt"] = dfs.loc[main_mask, "sale_type"].map(
        lambda v: -1 if v == "취소" else 1
    )

    dfs["sale_type"] = dfs["sale_type"]
    dfs["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dfs["_pk"] = _make_unified_pk(dfs)

    return dfs.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def _save_exclusion_log(excluded: pd.DataFrame, date_str: str, menu_map: dict) -> None:
    """블랙리스트로 제외된 행을 월별 CSV에 누적 저장 (append+dedup)."""
    try:
        ym = date_str[:7]
        log_dir = EXCLUSION_LOG_BASE / f"ym={ym}"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "exclusion_log.csv"

        exc = excluded.copy()
        exc["menu_name"] = (
            exc["order_id"].map(menu_map)
            .fillna(exc["메뉴명"].fillna("").astype(str) if "메뉴명" in exc.columns else "")
        )

        cols = ["sale_date", "brand", "store", "menu_name", "item_name", "qty", "unit_price", "total_price"]
        save_df = exc[[c for c in cols if c in exc.columns]].copy()

        # 메뉴/상품명 개행이 누적 CSV 구조를 깨지 않도록 저장 직전에 정화한다.
        for col in ("menu_name", "item_name"):
            if col in save_df.columns:
                save_df[col] = save_df[col].astype(str).str.replace(r"[\r\n]+", " ", regex=True)

        if log_path.exists():
            try:
                existing = pd.read_csv(
                    str(log_path),
                    dtype=str,
                    encoding="utf-8-sig",
                    on_bad_lines="skip",
                )
                save_df = pd.concat([existing, save_df], ignore_index=True).drop_duplicates()
            except Exception as exc_read:
                logger.warning("exclusion_log 읽기 실패, 신규 행만 저장: %s | %s", log_path, exc_read)

        save_df.to_csv(str(log_path), index=False, encoding="utf-8-sig")
    except Exception as exc:
        logger.warning("exclusion_log 저장 실패, 메인 적재 계속: %s", exc)


def _auto_fix_dori_whitelist(doridori_df: pd.DataFrame) -> tuple[int, int]:
    """도리(item_name에 '도리' 포함) 항목을 grp에서 유지(N)로 자동 보정.

    - 기존 fin_product_grp_input.csv posfeed 행에서 일치하는 normalized 상품명의 exclude_check가 Y이면 N으로 변경
    - 항목이 존재하지 않으면 exclude_check=N posfeed 행을 추가
    """
    try:
        master = _read_fin_product_grp()
    except Exception as exc:
        logger.warning("도리 자동 보정용 grp 로드 실패: %s", exc)
        return 0, 0

    if not {"source", "상품명", "exclude_check"}.issubset(master.columns):
        logger.warning("도리 자동 보정 실패: grp CSV에 필수 컬럼 없음")
        return 0, 0

    master["_norm_item"] = master["상품명"].fillna("").astype(str).map(_normalize_item_key)

    dori_examples: dict[str, str] = {}
    for item_name in doridori_df["item_name"].fillna("").astype(str):
        norm = _normalize_item_key(item_name)
        if norm:
            dori_examples.setdefault(norm, item_name)

    if not dori_examples:
        return 0, 0

    posfeed_mask = _posfeed_master_mask(master)
    fixed_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for idx, row in master[posfeed_mask].iterrows():
        if row["_norm_item"] in dori_examples and str(row["exclude_check"]).strip().upper() == "Y":
            master.at[idx, "exclude_check"] = "N"
            master.at[idx, "updated_at"] = now
            if "llm_check" in master.columns:
                master.at[idx, "llm_check"] = "N"
            fixed_count += 1

    added_count = 0
    existing_norms = set(master.loc[posfeed_mask, "_norm_item"].dropna().astype(str))
    for norm, example_name in dori_examples.items():
        if norm not in existing_norms:
            logger.warning("도리 자동 보정 신규 행 스킵(매장별 item_id 없음): %s", example_name)

    if fixed_count or added_count:
        master = master.drop(columns=["_norm_item"], errors="ignore")
        _save_fin_product_grp(master)

    return fixed_count, added_count


def report_posfeed_exclusions(**context) -> str:
    """posfeed 블랙리스트 제외 내역을 ym별 상세·집계 CSV로 저장.

    저장 경로: ONEDRIVE_DB/posfeed_exclusion/
      - detail_{ym}.csv  : 원본 행 전체 (매장·상품명·금액·날짜)
      - summary_{ym}.csv : 매장·상품명 기준 집계 (건수·수량합계·금액합계·날짜목록)
    """
    from modules.transform.utility.paths import ONEDRIVE_DB

    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = conf.get("sale_date")

    if sale_date:
        yms = [sale_date[:7]]
    else:
        now = datetime.now()
        prev = (now.replace(day=1) - timedelta(days=1))
        yms = sorted({now.strftime("%Y-%m"), prev.strftime("%Y-%m")})

    out_dir = ONEDRIVE_DB / "posfeed_exclusion"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for ym in yms:
        log_path = EXCLUSION_LOG_BASE / f"ym={ym}" / "exclusion_log.csv"
        if not log_path.exists():
            logger.info("[exclusion report] ym=%s 제외 로그 없음", ym)
            continue

        try:
            all_df = pd.read_csv(str(log_path), dtype=str, encoding="utf-8-sig")
        except Exception as exc:
            logger.warning("[exclusion report] 파일 읽기 실패: %s | %s", log_path, exc)
            continue

        doridori_df = all_df[all_df["item_name"].str.contains("도리", na=False)]
        if not doridori_df.empty:
            fixed_count, added_count = _auto_fix_dori_whitelist(doridori_df)
            if fixed_count or added_count:
                logger.info(
                    "[exclusion fix] 도리 포함 항목 자동 보정: 업데이트 %d개, 신규 추가 %d개",
                    fixed_count,
                    added_count,
                )

            # grp 재조회 — 이미 exclude_check=N으로 수정된 항목은 알림에서 제외
            try:
                master = _read_fin_product_grp()
                master["_norm"] = master["상품명"].astype(str).map(_normalize_item_key)
                posfeed_mask = _posfeed_master_mask(master)
                exclude_mask = master["exclude_check"].fillna("").astype(str).str.strip().str.upper() == "Y"
                still_n = set(master.loc[posfeed_mask & exclude_mask, "_norm"])
                still_blocked_df = doridori_df[
                    doridori_df["item_name"].astype(str).map(_normalize_item_key).isin(still_n)
                ]
            except Exception:
                still_blocked_df = doridori_df

            if _ENABLE_POSFEED_DORI_EXCLUSION_ALERT and not still_blocked_df.empty:
                from modules.transform.utility.mailer import send_email
                rows_html = "".join(
                    f"<tr><td>{r.get('store','')}</td><td>{r.get('menu_name','')}</td>"
                    f"<td>{r.get('item_name','')}</td>"
                    f"<td>{r.get('sale_date','')}</td><td>{r.get('total_price','')}</td></tr>"
                    for _, r in still_blocked_df.iterrows()
                )
                html = f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:24px;">
<h2 style="color:#c0392b;">&#9888; Posfeed 제외 항목 중 "도리" 포함 ({ym})</h2>
<p>블랙리스트로 제외된 항목 중 <b>"도리"</b>가 포함된 상품명이 발견됐습니다. 검토가 필요합니다.</p>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;">
  <thead style="background:#f2f2f2;">
    <tr><th>매장</th><th>제외 메뉴명</th><th>상품명(블랙리스트)</th><th>날짜</th><th>금액</th></tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
</body></html>"""
                subject_suffix = ""
                if fixed_count or added_count:
                    subject_suffix = " (자동 Y 변환 적용)"
                try:
                    send_email(
                        subject=f"[도리당] Posfeed 제외 항목 검토 필요 - '도리' 포함 ({ym}, {len(still_blocked_df)}건){subject_suffix}",
                        html_content=html,
                        to_emails=MAIL_CMJ_PM,
                    )
                    logger.info("[exclusion alert] '도리' 포함 %d건 알림 발송 (ym=%s)%s", len(still_blocked_df), ym, subject_suffix)
                except Exception as exc:
                    logger.error("[exclusion alert] 알림 발송 실패: %s", exc)

        for col in ("unit_price", "total_price", "qty"):
            if col in all_df.columns:
                all_df[col] = pd.to_numeric(all_df[col], errors="coerce").fillna(0).astype(int)

        detail_path = out_dir / f"detail_{ym}.csv"
        all_df.sort_values(["store", "menu_name", "item_name", "sale_date"]).to_csv(
            str(detail_path), index=False, encoding="utf-8-sig"
        )

        grp_cols = [c for c in ("brand", "store", "menu_name", "item_name") if c in all_df.columns]
        summary = (
            all_df.groupby(grp_cols, as_index=False)
            .agg(
                제외건수=("qty", "count"),
                수량합계=("qty", "sum"),
                금액합계=("total_price", "sum"),
                날짜목록=("sale_date", lambda s: ",".join(sorted(s.dropna().unique().tolist()))),
            )
            .sort_values(grp_cols)
        )
        summary_path = out_dir / f"summary_{ym}.csv"
        summary.to_csv(str(summary_path), index=False, encoding="utf-8-sig")

        logger.info(
            "[exclusion report] ym=%s | 상세 %d행 → %s | 집계 %d항목 → %s",
            ym, len(all_df), detail_path.name, len(summary), summary_path.name,
        )
        results.append(f"ym={ym}: 상세 {len(all_df)}행 / 집계 {len(summary)}항목")

    return " | ".join(results) if results else "exclusion report: 제외 데이터 없음"


def run_posfeed(
    date_str: str,
    overwrite: bool = False,
    *,
    persist_item_ids: bool = True,
    stores: list[str] | None = None,
) -> str:
    ym = date_str[:7]
    orders = _load_orders_for_ym(ym)
    items = _load_items_for_ym(ym)
    if orders.empty:
        raise FileNotFoundError(f"posfeed orders 없음: {date_str}")
    df = _transform_df(orders, items, date_str, persist_item_ids=persist_item_ids)
    if df.empty:
        return f"SKIP: posfeed {date_str} 해당일 데이터 없음"
    store_scope = {str(store).strip() for store in (stores or []) if str(store).strip()}
    if store_scope:
        df = df[df["store"].fillna("").astype(str).str.strip().isin(store_scope)].copy()
        if df.empty:
            return f"SKIP: posfeed {date_str} 대상 매장 데이터 없음 | stores={sorted(store_scope)}"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite, replace_stores=stores)
    return f"posfeed {date_str}: {saved}행 저장"


def run_lookback_posfeed(days: int = 7) -> str:
    total = 0
    today = datetime.strptime(_kst_today_str(), "%Y-%m-%d")
    for i in range(1, days + 1):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            result = run_posfeed(d)
            logger.info(result)
            # 저장 행수 추출
            try:
                total += int(result.split(":")[1].strip().split("행")[0].strip())
            except Exception:
                pass
        except FileNotFoundError:
            logger.info("posfeed lookback skip (데이터 없음): %s", d)
        except Exception as e:
            logger.warning("posfeed lookback error: %s | %s", d, e)
    return f"posfeed lookback({days}일): {total}행 저장"


def _recalculate_order_meta(df: pd.DataFrame) -> pd.DataFrame:
    """order_id 단위로 menu_name / order_cnt / item_seq / _pk 재계산.

    블랙리스트 적용 후 일부 item이 제거된 order에만 호출한다.
    """
    if df.empty:
        return df
    df = df.copy()

    # item_seq: order 내 순번 재부여
    df["item_seq"] = df.groupby("order_id").cumcount().add(1).astype(int).astype(str)

    # menu_name: 주문 내 unit_price 최고 item_name
    unit_price_num = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
    main_mask = (
        unit_price_num.groupby(df["order_id"]).rank(method="first", ascending=False) == 1
    )
    menu_map = df.loc[main_mask].set_index("order_id")["item_name"].to_dict()
    df["menu_name"] = df["order_id"].map(menu_map).fillna(df["menu_name"])

    # order_cnt: 대표행 1(정상) / -1(취소), 나머지 0
    df["order_cnt"] = 0
    df.loc[main_mask, "order_cnt"] = df.loc[main_mask, "sale_type"].map(
        lambda v: -1 if str(v) == "취소" else 1
    )

    # _pk 재계산 (item_seq 변경됨)
    df["_pk"] = _make_unified_pk(df)
    return df


def _unified_paths_for_dates(date_strs: list[str]) -> list:
    paths = []
    for date_str in date_strs:
        try:
            ymd = datetime.strptime(str(date_str), "%Y-%m-%d").strftime("%y%m%d")
        except Exception:
            logger.warning("posfeed blacklist 날짜 형식 무시: %s", date_str)
            continue
        path = UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"
        if path.exists():
            paths.append(path)
        else:
            logger.info("posfeed blacklist 대상 파일 없음: %s", path)
    return sorted(set(paths))


def sync_posfeed_blacklist(target_dates: list[str] | None = None) -> str:
    """grp 블랙리스트(exclude_check=Y)를 기존 unified_sales_*.parquet에 소급 적용.

    처리 흐름:
    1. fin_product_grp_input.csv에서 source=posfeed & exclude_check=Y 항목 로드
    2. target_dates가 있으면 해당 일자만, 없으면 unified_sales_*.parquet 전체 스캔
    3. source=posfeed & item_name ∈ 블랙리스트인 행 제거
    4. 영향받은 order_id의 menu_name/order_cnt/item_seq/_pk 재계산
    5. 변경된 parquet만 덮어쓰기
    """
    blacklist = _load_posfeed_blacklist()
    if not blacklist:
        return "블랙리스트 없음 (fin_product_grp_input.csv posfeed exclude_check=Y 항목 없음) — 스킵"

    files = _unified_paths_for_dates(target_dates) if target_dates else iter_unified_sales_files()
    if not files:
        return "unified_sales parquet 없음 — 스킵"

    total_removed = 0
    total_updated = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            continue

        if df.empty or "source" not in df.columns:
            continue

        posfeed_mask = df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"
        if not posfeed_mask.any():
            continue

        posfeed_df = df[posfeed_mask].copy()
        other_df = df[~posfeed_mask]

        items = posfeed_df["item_name"].fillna("").astype(str).str.strip()
        stores_col = posfeed_df["store"].fillna("").astype(str).str.strip() if "store" in posfeed_df.columns else pd.Series("", index=posfeed_df.index)
        item_keys = items.map(_normalize_item_key)

        def _is_bl(key: str, store: str) -> bool:
            if key not in blacklist:
                return False
            r = blacklist[key]
            return r is None or store in r

        remove_mask = pd.Series(
            [_is_bl(k, s) for k, s in zip(item_keys, stores_col)],
            index=posfeed_df.index,
            dtype=bool,
        )

        if not remove_mask.any():
            continue

        removed_count = int(remove_mask.sum())
        logger.info(
            "%s: %d행 제거 | 항목: %s",
            path.name,
            removed_count,
            posfeed_df.loc[remove_mask, "item_name"].unique().tolist()[:10],
        )

        affected_orders = posfeed_df.loc[remove_mask, "order_id"].unique()
        posfeed_clean = posfeed_df[~remove_mask].copy()

        if not posfeed_clean.empty and len(affected_orders) > 0:
            affected_mask = posfeed_clean["order_id"].isin(affected_orders)
            if affected_mask.any():
                unchanged = posfeed_clean[~affected_mask]
                recalculated = _recalculate_order_meta(posfeed_clean[affected_mask])
                posfeed_clean = pd.concat([unchanged, recalculated], ignore_index=True)

        new_df = pd.concat([other_df, posfeed_clean], ignore_index=True)
        new_df = new_df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("unit_price", "total_price", "discount_amount"):
            if col in new_df.columns:
                new_df[col] = pd.to_numeric(new_df[col], errors="coerce").fillna(0).astype(int)
        new_df.to_parquet(path, index=False, engine="pyarrow")

        total_removed += removed_count
        total_updated += 1
        logger.info("저장 완료: %s | %d행", path.name, len(new_df))

    return (
        f"sync_posfeed_blacklist 완료 | "
        f"파일 {total_updated}/{len(files)}개 수정 | "
        f"총 {total_removed}행 제거"
    )


_BRAND_DESC = {
    "도리당": "한식 닭요리 전문점 (닭볶음탕, 닭갈비, 닭구이 등)",
    "나홀로": "도리당의 서브브랜드, 1인 닭도리탕 전문점 (1인 고객 타겟)",
}

_FIN_PRODUCT_REQUIRED_COLUMNS = [
    "source",
    "구분",
    "대메뉴",
    "중메뉴",
    "상품코드",
    "상품명",
    "판매단가",
    "수동분류",
    "is_main_candidate",
    "llm_check",
    "exclude_check",
    "updated_at",
    "is_latest",
]


def _read_fin_product_grp() -> pd.DataFrame:
    source_path = existing_fin_product_csv_path()
    if source_path.exists():
        df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    else:
        df = pd.DataFrame(columns=_FIN_PRODUCT_REQUIRED_COLUMNS)
    for col in _FIN_PRODUCT_REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _save_fin_product_grp(df: pd.DataFrame) -> None:
    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _posfeed_master_mask(df: pd.DataFrame) -> pd.Series:
    return df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"


def _make_posfeed_master_row(
    columns: list[str],
    item_name: str,
    exclude_check: str,
    updated_at: str,
    unit_price: int | str = "",
) -> dict[str, str]:
    row = {col: "" for col in columns}
    row.update({
        "source": "posfeed",
        "구분": "배달",
        "상품코드": "",
        "상품명": item_name,
        "판매단가": unit_price,
        "llm_check": "N",
        "exclude_check": exclude_check,
        "updated_at": updated_at,
    })
    if "is_main_candidate" in row:
        row["is_main_candidate"] = "N"
    if "is_latest" in row:
        row["is_latest"] = "Y"
    if "중복_수동분류" in row:
        row["중복_수동분류"] = "N"
    return row


def _build_posfeed_latest_price_map() -> dict[str, int]:
    """posfeed 상세 원천에서 상품명별 최신 관측 단품가격 맵을 만든다."""
    files = sorted(RAW_POSFEED_DETAIL.glob("brand=*/store=*/ym=*/posfeed_order_item.csv"))
    if not files:
        return {}

    parts = []
    for file_pos, path in enumerate(files):
        try:
            header = pd.read_csv(path, encoding="utf-8-sig", dtype=str, nrows=0)
            usecols = [c for c in ("상품명", "단품가격", "order_date", "ym") if c in header.columns]
            if "상품명" not in usecols or "단품가격" not in usecols:
                continue
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, usecols=usecols).fillna("")
        except Exception as e:
            logger.warning("posfeed 단가 원천 로드 실패, 스킵: %s | %s", path, e)
            continue

        df["_item_key"] = df["상품명"].astype(str).map(lambda v: _normalize_item_key(v.strip()))
        df["_unit_price"] = _to_int_series(df["단품가격"])
        df["_file_pos"] = file_pos
        df["_row_pos"] = range(len(df))
        if "order_date" not in df.columns:
            df["order_date"] = ""
        if "ym" not in df.columns:
            df["ym"] = ""
        df["_observed_at"] = pd.to_datetime(
            df["order_date"].astype(str).str.strip().where(
                df["order_date"].astype(str).str.strip() != "",
                df["ym"].astype(str).str.strip() + "-01",
            ),
            errors="coerce",
        )
        df = df[df["_item_key"] != ""].copy()
        if not df.empty:
            parts.append(df[["_item_key", "_unit_price", "_observed_at", "_file_pos", "_row_pos"]])

    if not parts:
        return {}

    all_prices = pd.concat(parts, ignore_index=True)
    latest = (
        all_prices.sort_values(["_observed_at", "_file_pos", "_row_pos"], na_position="first")
        .groupby("_item_key", as_index=False)
        .last()
    )
    return latest.set_index("_item_key")["_unit_price"].astype(int).to_dict()


def _fill_posfeed_master_prices(
    master: pd.DataFrame,
    price_map: dict[str, int],
    *,
    blank_only: bool = True,
) -> tuple[pd.DataFrame, int]:
    """fin_product_grp의 posfeed 판매단가를 상품명 정규화키 기준으로 채운다."""
    if master.empty or not price_map:
        return master, 0
    if "판매단가" not in master.columns:
        master["판매단가"] = ""
    if "상품명" not in master.columns or "source" not in master.columns:
        return master, 0

    out = master.copy()
    posfeed_mask = _posfeed_master_mask(out)
    item_key = out["상품명"].fillna("").astype(str).map(lambda v: _normalize_item_key(v.strip()))
    mapped = item_key.map(price_map)
    target_mask = posfeed_mask & mapped.notna()
    if blank_only:
        current = out["판매단가"].fillna("").astype(str).str.strip()
        target_mask &= current.eq("")

    update_count = int(target_mask.sum())
    if update_count:
        out.loc[target_mask, "판매단가"] = mapped.loc[target_mask].astype(int).astype(str)
    return out, update_count


def backfill_posfeed_product_prices(dry_run: bool = False) -> str:
    """기존 fin_product_grp_input.csv posfeed 행의 빈 판매단가를 최신 관측 단가로 채운다."""
    master = _read_fin_product_grp()
    price_map = _build_posfeed_latest_price_map()
    if not price_map:
        return "posfeed 단가 원천 없음 - 스킵"

    out, update_count = _fill_posfeed_master_prices(master, price_map, blank_only=True)
    if update_count == 0:
        return "posfeed 판매단가 백필 대상 없음"
    if dry_run:
        return f"dry_run: posfeed 판매단가 {update_count}행 백필 예정"

    _save_fin_product_grp(out)
    return f"posfeed 판매단가 {update_count}행 백필"


def _posfeed_llm_examples(master: pd.DataFrame) -> list[dict]:
    pf = master[_posfeed_master_mask(master)].copy()
    examples = []
    for _, row in pf.iterrows():
        item_name = str(row.get("상품명", "")).strip()
        if not item_name:
            continue
        is_valid = "N" if str(row.get("exclude_check", "")).strip().upper() == "Y" else "Y"
        examples.append({"item_name": item_name, "is_valid": is_valid})
    return examples


def _classify_item_with_llm(
    item_name: str,
    brands: set[str],
    examples: list[dict] | None = None,
) -> str:
    """Ollama로 item_name이 브랜드에 적절한지 Y/N 판정. 실패 시 N 반환."""
    brand_context = "\n".join(
        f"- {b}: {_BRAND_DESC.get(b, '외식 브랜드')}"
        for b in sorted(brands)
    ) if brands else "- 알 수 없음: 외식 브랜드"

    example_block = ""
    if examples:
        y_ex = [e["item_name"] for e in examples if str(e.get("is_valid", "")).upper() == "Y"][:10]
        n_ex = [e["item_name"] for e in examples if str(e.get("is_valid", "")).upper() == "N"][:10]
        ex_lines = [f"- {n} → Y" for n in y_ex] + [f"- {n} → N" for n in n_ex]
        if ex_lines:
            example_block = "기존 분류 예시:\n" + "\n".join(ex_lines) + "\n\n"

    prompt = (
        f"다음 상품이 해당 외식 브랜드의 정상 메뉴인지 판단하세요.\n\n"
        f"브랜드:\n{brand_context}\n\n"
        f"{example_block}"
        f"상품명: {item_name}\n\n"
        "정상 메뉴이면 Y, 브랜드와 전혀 맞지 않는 잘못된 주문이면 N.\n"
        "Y 또는 N 한 글자만 답하세요."
    )
    try:
        from modules.transform.utility.qwen_client import query_qwen
        result = query_qwen(prompt).strip().upper()
        return "Y" if result.startswith("Y") else "N"
    except Exception as e:
        logger.warning("LLM 분류 실패, N으로 처리: %s | %s", item_name, e)
        return "N"


def _load_legacy_posfeed_decisions() -> dict[str, str]:
    """구 whitelist의 item_key별 최종 판단을 exclude_check 값으로 변환한다."""
    if not POSFEED_WHITELIST_CSV_PATH.exists():
        return {}
    try:
        df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("legacy posfeed whitelist 로드 실패: %s", exc)
        return {}
    if not {"item_name", "is_valid"}.issubset(df.columns):
        logger.warning("legacy posfeed whitelist 컬럼 오류 (필요: item_name, is_valid)")
        return {}

    decisions: dict[str, str] = {}
    # N을 우선한다. 같은 item_key가 Y/N 모두 있으면 제외를 유지한다.
    for _, row in df.iterrows():
        key = _normalize_item_key(str(row.get("item_name", "")).strip())
        if not key:
            continue
        is_valid = str(row.get("is_valid", "")).strip().upper()
        if is_valid == "N":
            decisions[key] = "Y"
        elif is_valid == "Y" and key not in decisions:
            decisions[key] = "N"
    return decisions


def generate_posfeed_whitelist_draft(
    enable_llm: bool = False,
    max_llm_candidates: int = 50,
) -> str:
    """기존 unified_sales parquet에서 신규 posfeed item_name을 찾아 fin_product_grp_input.csv에 추가."""
    if not enable_llm:
        return "posfeed whitelist LLM 비활성화 — 스킵"

    files = _unified_paths_for_dates(target_dates) if target_dates else iter_unified_sales_files()
    if not files:
        return "unified_sales parquet 없음 — 스킵"

    # parquet 스캔: item_key 판단은 전역으로, item_id는 매장별로 유지한다.
    norm_items: set[tuple[str, str, str]] = set()
    item_brands: dict[tuple[str, str, str], set[str]] = {}
    global_item_brands: dict[str, set[str]] = {}
    for path in files:
        try:
            df = pd.read_parquet(path, columns=["source", "item_name", "brand", "store"])
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            continue
        pf = df[df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"]
        for _, row in pf[["item_name", "brand", "store"]].dropna(subset=["item_name"]).iterrows():
            norm = _normalize_item_key(str(row["item_name"]).strip())
            if not norm:
                continue
            brand = str(row.get("brand", "")).strip()
            store = str(row.get("store", "")).strip()
            key = (brand, store, norm)
            norm_items.add(key)
            if brand:
                item_brands.setdefault(key, set()).add(brand)
                global_item_brands.setdefault(norm, set()).add(brand)

    if not norm_items:
        return "posfeed 데이터 없음"

    try:
        master = _read_fin_product_grp()
    except Exception as e:
        logger.warning("fin_product_grp_input.csv 로드 실패: %s", e)
        return f"fin_product_grp_input.csv 로드 실패 — {e}"

    posfeed_mask = _posfeed_master_mask(master)
    for col in ("brand", "store", "item_key"):
        if col not in master.columns:
            master[col] = ""
    legacy_decisions = _load_legacy_posfeed_decisions()
    existing_items = set()
    master_decisions: dict[str, str] = {}
    for _, row in master.loc[posfeed_mask].iterrows():
        norm = str(row.get("item_key", "")).strip() or _normalize_item_key(str(row.get("상품명", "")).strip())
        if norm:
            existing_items.add((str(row.get("brand", "")).strip(), str(row.get("store", "")).strip(), norm))
            exclude_check = str(row.get("exclude_check", "")).strip().upper()
            llm_check = str(row.get("llm_check", "")).strip().upper()
            if exclude_check == "Y":
                master_decisions[norm] = "Y"
            elif exclude_check == "N" and norm not in master_decisions:
                if legacy_decisions.get(norm) == "Y" and llm_check == "Y":
                    continue
                master_decisions[norm] = "N"
    known_decisions = {**legacy_decisions, **master_decisions}
    examples = _posfeed_llm_examples(master)
    price_map = _build_posfeed_latest_price_map()

    candidates = [
        (brand, store, norm)
        for brand, store, norm in sorted(norm_items)
        if (brand, store, norm) not in existing_items
    ]
    candidate_norms = sorted({norm for _, _, norm in candidates})
    unknown_norms = [norm for norm in candidate_norms if norm not in known_decisions]
    logger.info(
        "posfeed whitelist 후보 | parquet_keys=%d store_item_candidates=%d known=%d unknown_llm=%d",
        len(norm_items),
        len(candidates),
        len(candidate_norms) - len(unknown_norms),
        len(unknown_norms),
    )
    if len(unknown_norms) > max_llm_candidates:
        return (
            "posfeed whitelist LLM 후보 과다 - 스킵 | "
            f"unknown={len(unknown_norms)} max={max_llm_candidates} "
            "conf['posfeed_whitelist_llm'] 실행 전 상품테이블/구 whitelist 병합 필요"
        )

    llm_decisions: dict[str, str] = {}
    for norm in unknown_norms:
        brands = global_item_brands.get(norm, set())
        is_valid = _classify_item_with_llm(norm, brands, examples=examples)
        llm_decisions[norm] = "Y" if is_valid == "N" else "N"
        logger.info("LLM 분류: %s → %s (브랜드: %s)", norm, is_valid, brands)

    decisions = {**known_decisions, **llm_decisions}

    # 신규 매장별 item_id 행 생성. 분류 판단은 전역 item_key 결정을 재사용한다.
    new_rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    code_series = allocate_manual_item_ids(pd.DataFrame([
        {
            "source": "posfeed",
            "brand": brand,
            "store": store,
            "item_name": norm,
            "unit_price": price_map.get(norm, ""),
        }
        for brand, store, norm in candidates
    ]), persist=False) if candidates else pd.Series(dtype=str)

    for pos, (brand, store, norm) in enumerate(candidates):
        exclude_check = decisions.get(norm, "Y")
        unit_price = price_map.get(norm, "")
        code = str(code_series.iloc[pos])
        row = _make_posfeed_master_row(list(master.columns), norm, exclude_check, now, unit_price)
        offset = int(code) - MANUAL_SOURCE_BASE["posfeed"]
        block_size = item_block_size("posfeed")
        row.update({
            "brand": brand,
            "store": store,
            "item_key": norm,
            "store_seq": str(offset // block_size),
            "item_seq": str(offset % block_size),
            "상품코드": code,
        })
        new_rows.append(row)
        logger.info(
            "posfeed master 신규: %s/%s/%s → exclude_check=%s",
            brand,
            store,
            norm,
            exclude_check,
        )

    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=master.columns)
        combined = pd.concat([master, new_df], ignore_index=True)
    else:
        combined = master.copy()
    combined, price_update_count = _fill_posfeed_master_prices(combined, price_map, blank_only=True)
    if not new_rows and price_update_count == 0:
        return f"신규 항목 없음 (기존 grp에 모두 포함됨) | {FIN_PRODUCT_CSV_PATH}"
    _save_fin_product_grp(combined)

    if not new_rows:
        return f"신규 항목 없음, posfeed 판매단가 {price_update_count}행 백필 | {FIN_PRODUCT_CSV_PATH}"

    # 이메일: LLM 분류 결과 표 (Y=초록, N=빨강)
    try:
        from modules.transform.utility.mailer import send_email

        table_rows = "".join(
            "<tr>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;'>{r['상품명']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:center;"
            f"color:{'#27ae60' if r['exclude_check']=='N' else '#c0392b'};font-weight:bold;'>"
            f"{'Y' if r['exclude_check']=='N' else 'N'}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;color:#888;font-size:12px;'>"
            f"{r.get('brand', '')}/{r.get('store', '')}</td>"
            "</tr>"
            for r in new_rows
        )
        html = f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:24px;background:#f4f6f8;">
<div style="max-width:800px;margin:auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <h2 style="margin-top:0;color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:8px;">[도리당] Posfeed 신규 상품 LLM 자동 분류</h2>
  <p style="color:#555;">신규 item_name <b>{len(new_rows)}개</b>를 LLM이 자동 분류해 fin_product_grp_input.csv에 추가했습니다.</p>
  <table style="border-collapse:collapse;width:100%;">
    <thead><tr>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:left;">item_name</th>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:center;">is_valid</th>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:left;">브랜드</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <p style="margin-top:16px;">
    <a href="{FIN_PRODUCT_CSV_PATH.as_uri()}"
       style="display:inline-block;padding:8px 16px;background:#2c3e50;color:#fff;border-radius:4px;text-decoration:none;font-size:13px;">
      📂 CSV 파일 열기
    </a>
    <span style="color:#999;font-size:11px;margin-left:10px;">{FIN_PRODUCT_CSV_PATH}</span>
  </p>
</div></body></html>"""

        send_email(
            subject=f"[도리당] Posfeed 신규 상품 {len(new_rows)}개 LLM 자동 분류 완료",
            html_content=html,
            to_emails=MAIL_CMJ_PM,
        )
        logger.info("신규 상품 알림 발송 완료: %d개", len(new_rows))
    except Exception as e:
        logger.warning("신규 상품 알림 발송 실패: %s", e)

    approved = sum(1 for r in new_rows if r["exclude_check"] == "N")
    rejected = len(new_rows) - approved
    return (
        f"draft 생성 완료 | 신규 {len(new_rows)}개 (LLM: Y={approved}/N={rejected}) | "
        f"총 {len(combined)}개 항목 | {FIN_PRODUCT_CSV_PATH}"
    )


def backfill_posfeed() -> str:
    """전체 posfeed 데이터 backfill (등록날짜 기준 전체 처리)."""
    files = sorted(RAW_POSFEED_SALES.glob("brand=*/store=*/ym=*/posfeed_orders.csv"))
    if not files:
        return "SKIP: posfeed orders 파일 없음"

    dates: set[str] = set()
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig", usecols=["등록날짜"], dtype=str)
            for d in df["등록날짜"].dropna().unique():
                d = str(d).strip()
                if d and d.lower() != "nan":
                    dates.add(d)
        except Exception as e:
            logger.warning("backfill 날짜 수집 실패: %s | %s", f, e)

    if not dates:
        return "SKIP: 유효한 등록날짜 없음"

    total = 0
    for date_str in sorted(dates):
        try:
            result = run_posfeed(date_str, overwrite=True)
            logger.info(result)
            try:
                total += int(result.split(":")[1].strip().split("행")[0].strip())
            except Exception:
                pass
        except Exception as e:
            logger.warning("backfill 실패: %s | %s", date_str, e)

    return f"posfeed backfill 완료: {len(dates)}일 / {total}행 저장"
