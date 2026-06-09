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

import hashlib
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB
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
)

logger = logging.getLogger(__name__)

RAW_POSFEED_SALES = ANALYTICS_DB / "posfeed_sales"
RAW_POSFEED_DETAIL = ANALYTICS_DB / "posfeed_sales_detail"
POSFEED_SOURCE = "posfeed"
EXCLUSION_LOG_BASE = ANALYTICS_DB / "posfeed_exclusion_log"
# Posfeed 제외 항목 중 '도리' 포함 알림은 유지하고, 자동으로 Y로 보정합니다.
_ENABLE_POSFEED_DORI_EXCLUSION_ALERT = True
# 주문상태 → 정상 처리할 완료 상태 (포장주문도 '배달완료'로 기록됨)
_COMPLETE_STATUSES = {"배달완료"}

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


def _transform_df(orders: pd.DataFrame, items: pd.DataFrame, date_str: str) -> pd.DataFrame:
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

    # item_id: 상품명 md5 해시
    dfs["item_id"] = dfs["item_name"].map(
        lambda s: hashlib.md5(s.encode()).hexdigest() if s else ""
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

    # 블랙리스트 필터: is_valid=N 항목 제거 (brand·item_name 할당 후, menu_name 계산 전)
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

    if log_path.exists():
        existing = pd.read_csv(str(log_path), dtype=str, encoding="utf-8-sig")
        save_df = pd.concat([existing, save_df], ignore_index=True).drop_duplicates()

    save_df.to_csv(str(log_path), index=False, encoding="utf-8-sig")


def _auto_fix_dori_whitelist(doridori_df: pd.DataFrame) -> tuple[int, int]:
    """도리(item_name에 '도리' 포함) 항목을 whitelist에서 Y로 자동 보정.

    - 기존 fin_product_posfeed_whitelist.csv에서 일치하는 normalized item_name이 N이면 Y로 변경
    - 항목이 존재하지 않으면 새 Y 행을 추가
    - review_needed는 자동 보정된 항목에 대해 N으로 설정
    """
    from modules.transform.utility.paths import POSFEED_WHITELIST_CSV_PATH

    if POSFEED_WHITELIST_CSV_PATH.exists():
        try:
            wl_df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str).fillna("")
        except Exception as exc:
            logger.warning("도리 자동 보정용 whitelist 로드 실패: %s", exc)
            return 0, 0
    else:
        wl_df = pd.DataFrame(columns=_WHITELIST_COLUMNS)

    if not {"item_name", "is_valid"}.issubset(wl_df.columns):
        logger.warning("도리 자동 보정 실패: whitelist CSV에 필수 컬럼 없음")
        return 0, 0

    wl_df = wl_df.reindex(columns=_WHITELIST_COLUMNS, fill_value="")
    wl_df["_norm_item"] = wl_df["item_name"].fillna("").astype(str).map(_normalize_item_key)

    dori_examples: dict[str, str] = {}
    for item_name in doridori_df["item_name"].fillna("").astype(str):
        norm = _normalize_item_key(item_name)
        if norm:
            dori_examples.setdefault(norm, item_name)

    if not dori_examples:
        return 0, 0

    fixed_count = 0
    for idx, row in wl_df.iterrows():
        if row["_norm_item"] in dori_examples and row["is_valid"].strip().upper() == "N":
            wl_df.at[idx, "is_valid"] = "Y"
            wl_df.at[idx, "review_needed"] = "N"
            wl_df.at[idx, "classified_by"] = "auto_dori_fix"
            fixed_count += 1

    added_count = 0
    existing_norms = set(wl_df["_norm_item"].dropna().astype(str))
    for norm, example_name in dori_examples.items():
        if norm not in existing_norms:
            wl_df = pd.concat(
                [
                    wl_df,
                    pd.DataFrame([
                        {
                            "item_name": example_name,
                            "is_valid": "Y",
                            "store": "",
                            "review_needed": "N",
                            "classified_by": "auto_dori_fix",
                            "_norm_item": norm,
                        }
                    ])
                ],
                ignore_index=True,
            )
            added_count += 1

    if fixed_count or added_count:
        wl_df = wl_df.drop(columns=["_norm_item"])
        wl_df = wl_df.drop_duplicates(subset=["item_name", "store"], keep="first")
        wl_df.to_csv(POSFEED_WHITELIST_CSV_PATH, index=False, encoding="utf-8-sig")

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

            if _ENABLE_POSFEED_DORI_EXCLUSION_ALERT:
                from modules.transform.utility.mailer import send_email
                rows_html = "".join(
                    f"<tr><td>{r.get('store','')}</td><td>{r.get('menu_name','')}</td>"
                    f"<td>{r.get('item_name','')}</td>"
                    f"<td>{r.get('sale_date','')}</td><td>{r.get('total_price','')}</td></tr>"
                    for _, r in doridori_df.iterrows()
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
                        subject=f"[도리당] Posfeed 제외 항목 검토 필요 - '도리' 포함 ({ym}, {len(doridori_df)}건){subject_suffix}",
                        html_content=html,
                        to_emails="a17019@kakao.com",
                    )
                    logger.info("[exclusion alert] '도리' 포함 %d건 알림 발송 (ym=%s)%s", len(doridori_df), ym, subject_suffix)
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


def run_posfeed(date_str: str, overwrite: bool = False) -> str:
    ym = date_str[:7]
    orders = _load_orders_for_ym(ym)
    items = _load_items_for_ym(ym)
    if orders.empty:
        raise FileNotFoundError(f"posfeed orders 없음: {date_str}")
    df = _transform_df(orders, items, date_str)
    if df.empty:
        return f"SKIP: posfeed {date_str} 해당일 데이터 없음"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    return f"posfeed {date_str}: {saved}행 저장"


def run_lookback_posfeed(days: int = 7) -> str:
    total = 0
    today = datetime.now()
    for i in range(days):
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


def sync_posfeed_blacklist() -> str:
    """블랙리스트(is_valid=N)를 기존 unified_sales_*.parquet에 소급 적용.

    처리 흐름:
    1. fin_product_posfeed_whitelist.csv에서 N 항목 로드
    2. unified_sales_*.parquet 전체 스캔
    3. source=posfeed & item_name ∈ 블랙리스트인 행 제거
    4. 영향받은 order_id의 menu_name/order_cnt/item_seq/_pk 재계산
    5. 변경된 parquet만 덮어쓰기
    """
    blacklist = _load_posfeed_blacklist()
    if not blacklist:
        return "블랙리스트 없음 (fin_product_posfeed_whitelist.csv 미존재 또는 N 항목 없음) — 스킵"

    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
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

_WHITELIST_COLUMNS = ["item_name", "is_valid", "store", "review_needed", "classified_by"]


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


def generate_posfeed_whitelist_draft() -> str:
    """기존 unified_sales parquet에서 posfeed item_name 목록을 추출해 draft CSV 생성.

    - 신규 item_name: Ollama로 브랜드 맥락 자동 분류(Y/N), review_needed=Y
    - 기존 항목: is_valid 유지, review_needed/classified_by 컬럼 마이그레이션
    - 구버전 CSV(brand/store_name 컬럼) 자동 마이그레이션
    """
    from modules.transform.utility.paths import POSFEED_WHITELIST_CSV_PATH

    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        return "unified_sales parquet 없음 — 스킵"

    # parquet 스캔: item_name + brand 수집
    norm_items: set[str] = set()
    item_brands: dict[str, set[str]] = {}
    for path in files:
        try:
            df = pd.read_parquet(path, columns=["source", "item_name", "brand"])
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            continue
        pf = df[df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"]
        for _, row in pf[["item_name", "brand"]].dropna(subset=["item_name"]).iterrows():
            norm = _normalize_item_key(str(row["item_name"]).strip())
            if not norm:
                continue
            norm_items.add(norm)
            brand = str(row.get("brand", "")).strip()
            if brand:
                item_brands.setdefault(norm, set()).add(brand)

    if not norm_items:
        return "posfeed 데이터 없음"

    # 기존 CSV 로드
    existing_df: pd.DataFrame | None = None
    existing_items: set[str] = set()
    POSFEED_WHITELIST_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    if POSFEED_WHITELIST_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str).fillna("")
            # 구버전 CSV(brand/store_name 컬럼) 마이그레이션
            if "brand" in existing_df.columns or "store_name" in existing_df.columns:
                logger.info("구버전 whitelist CSV 감지 → 마이그레이션")
                existing_df["item_name"] = existing_df["item_name"].fillna("").astype(str).str.strip().map(_normalize_item_key)
                existing_df["_sort"] = existing_df["is_valid"].str.strip().str.upper().map({"N": 0, "Y": 1}).fillna(1)
                existing_df = (
                    existing_df.sort_values("_sort")
                    .drop_duplicates(subset=["item_name"], keep="first")
                    .drop(columns=[c for c in ("brand", "store_name", "_sort") if c in existing_df.columns])
                )
                if "store" not in existing_df.columns:
                    existing_df["store"] = existing_df.pop("note") if "note" in existing_df.columns else ""
                existing_df = (
                    existing_df[existing_df["item_name"].str.strip() != ""]
                    .reset_index(drop=True)
                )
                logger.info("구버전 마이그레이션 완료: %d행", len(existing_df))
            # note→store 마이그레이션
            elif "note" in existing_df.columns and "store" not in existing_df.columns:
                logger.info("whitelist CSV note→store 마이그레이션")
                existing_df = existing_df.rename(columns={"note": "store"})

            # review_needed / classified_by 컬럼 마이그레이션
            if "review_needed" not in existing_df.columns:
                existing_df["review_needed"] = "N"
            if "classified_by" not in existing_df.columns:
                existing_df["classified_by"] = "human"

            existing_df = existing_df.reindex(columns=_WHITELIST_COLUMNS, fill_value="")
            existing_df.to_csv(POSFEED_WHITELIST_CSV_PATH, index=False, encoding="utf-8-sig")

            existing_items = {
                str(r["item_name"]).strip()
                for _, r in existing_df.iterrows()
                if str(r["item_name"]).strip()
            }
        except Exception as e:
            logger.warning("기존 CSV 로드 실패, 신규 생성으로 진행: %s", e)
            existing_df = None

    # 신규 아이템: LLM 자동 분류
    new_rows = []
    for norm in sorted(norm_items):
        if norm in existing_items:
            continue
        brands = item_brands.get(norm, set())
        ex = existing_df.to_dict("records") if existing_df is not None else None
        is_valid = _classify_item_with_llm(norm, brands, examples=ex)
        classified_by = "auto_llm"
        new_rows.append({
            "item_name": norm,
            "is_valid": is_valid,
            "store": "",
            "review_needed": "Y",
            "classified_by": classified_by,
        })
        logger.info("LLM 분류: %s → %s (브랜드: %s)", norm, is_valid, brands)

    if not new_rows:
        return f"신규 항목 없음 (기존 CSV에 모두 포함됨) | {POSFEED_WHITELIST_CSV_PATH}"

    new_df = pd.DataFrame(new_rows, columns=_WHITELIST_COLUMNS)

    combined = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
    combined["_sort"] = combined["is_valid"].str.strip().str.upper().map({"N": 0, "Y": 1}).fillna(1)
    combined = (
        combined.sort_values("_sort")
        .drop_duplicates(subset=["item_name", "store"], keep="first")
        .drop(columns=["_sort"])
        .sort_values(["item_name", "store"])
        .reset_index(drop=True)
    )
    combined.to_csv(POSFEED_WHITELIST_CSV_PATH, index=False, encoding="utf-8-sig")

    # 이메일: LLM 분류 결과 표 (Y=초록, N=빨강)
    try:
        from modules.transform.utility.mailer import send_email

        table_rows = "".join(
            "<tr>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;'>{r['item_name']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;text-align:center;"
            f"color:{'#27ae60' if r['is_valid']=='Y' else '#c0392b'};font-weight:bold;'>{r['is_valid']}</td>"
            f"<td style='padding:6px 12px;border-bottom:1px solid #eee;color:#888;font-size:12px;'>{','.join(sorted(item_brands.get(r['item_name'],set())))}</td>"
            "</tr>"
            for r in new_rows
        )
        html = f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:24px;background:#f4f6f8;">
<div style="max-width:800px;margin:auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <h2 style="margin-top:0;color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:8px;">[도리당] Posfeed 신규 상품 LLM 자동 분류</h2>
  <p style="color:#555;">신규 item_name <b>{len(new_rows)}개</b>를 LLM이 자동 분류했습니다. <b>review_needed=Y</b> 항목을 검토해 주세요.</p>
  <table style="border-collapse:collapse;width:100%;">
    <thead><tr>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:left;">item_name</th>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:center;">is_valid</th>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:left;">브랜드</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <p style="margin-top:16px;">
    <a href="{POSFEED_WHITELIST_CSV_PATH.as_uri()}"
       style="display:inline-block;padding:8px 16px;background:#2c3e50;color:#fff;border-radius:4px;text-decoration:none;font-size:13px;">
      📂 CSV 파일 열기
    </a>
    <span style="color:#999;font-size:11px;margin-left:10px;">{POSFEED_WHITELIST_CSV_PATH}</span>
  </p>
</div></body></html>"""

        send_email(
            subject=f"[도리당] Posfeed 신규 상품 {len(new_rows)}개 LLM 자동 분류 완료",
            html_content=html,
            to_emails="a17019@kakao.com",
        )
        logger.info("신규 상품 알림 발송 완료: %d개", len(new_rows))
    except Exception as e:
        logger.warning("신규 상품 알림 발송 실패: %s", e)

    approved = sum(1 for r in new_rows if r["is_valid"] == "Y")
    rejected = len(new_rows) - approved
    return (
        f"draft 생성 완료 | 신규 {len(new_rows)}개 (LLM: Y={approved}/N={rejected}) | "
        f"총 {len(combined)}개 항목 | {POSFEED_WHITELIST_CSV_PATH}"
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
