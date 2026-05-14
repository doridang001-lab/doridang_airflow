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

    dfs["order_id"] = dfs["주문 코드"].fillna("").astype(str).str.strip()
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

    # 법흥리점: item_name "홀_" 포함 → 홀 테이블 주문 재분류
    _hall_mask = (
        dfs["store"].str.strip().eq("법흥리점") &
        dfs["item_name"].str.contains("홀_", regex=False)
    )
    dfs.loc[_hall_mask, "platform"] = "홀"
    dfs.loc[_hall_mask, "order_type"] = "홀_테이블"

    # 블랙리스트 필터: is_valid=N 항목 제거 (brand·item_name 할당 후, menu_name 계산 전)
    dfs = _apply_posfeed_blacklist(dfs)
    if dfs.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    # menu_name: 주문 내 최고가 아이템명 (없으면 orders 메뉴명 폴백)
    main_mask = dfs.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1)
    menu_map = dfs.loc[main_mask].set_index("order_id")["item_name"].to_dict()
    dfs["menu_name"] = dfs["order_id"].map(menu_map).fillna(
        dfs["메뉴명"].fillna("").astype(str)
    )

    # order_cnt: 대표행 1(정상) / -1(취소), 나머지 0
    dfs["order_cnt"] = 0
    dfs.loc[main_mask, "order_cnt"] = dfs.loc[main_mask, "sale_type"].map(
        lambda v: -1 if v == "취소" else 1
    )

    dfs["sale_type"] = dfs["sale_type"]
    dfs["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dfs["_pk"] = _make_unified_pk(dfs)

    return dfs.reindex(columns=UNIFIED_COLUMNS, fill_value="")


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
        remove_mask = items.map(_normalize_item_key).isin(blacklist)

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


def generate_posfeed_whitelist_draft() -> str:
    """기존 unified_sales parquet에서 posfeed item_name 목록을 추출해 draft CSV 생성.

    - CSV 포맷: item_name, is_valid, note (brand/store_name 제거)
    - item_name은 정규화된 값으로 저장 (표기 변형 통합)
    - 구버전 CSV(brand/store_name 컬럼 있음) 자동 마이그레이션
    - 기본값 is_valid=Y (블랙리스트 방식: 명시적 N만 차단)
    """
    from modules.transform.utility.paths import POSFEED_WHITELIST_CSV_PATH

    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        return "unified_sales parquet 없음 — 스킵"

    # parquet item_name → 정규화 후 수집
    norm_items: set[str] = set()
    for path in files:
        try:
            df = pd.read_parquet(path, columns=["source", "item_name"])
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            continue
        pf = df[df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"]
        for item in pf["item_name"].dropna().unique():
            norm = _normalize_item_key(str(item).strip())
            if norm:
                norm_items.add(norm)

    if not norm_items:
        return "posfeed 데이터 없음"

    # 기존 CSV 로드 (없으면 빈 DataFrame)
    existing_df: pd.DataFrame | None = None
    existing_items: set[str] = set()
    POSFEED_WHITELIST_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    if POSFEED_WHITELIST_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str).fillna("")
            # 구버전 CSV(brand/store_name 컬럼 있음) → 자동 마이그레이션: item_name 정규화 후 dedup
            if "brand" in existing_df.columns or "store_name" in existing_df.columns:
                logger.info("구버전 whitelist CSV 감지 → item_name 정규화 후 마이그레이션")
                existing_df["item_name"] = existing_df["item_name"].fillna("").astype(str).str.strip().map(_normalize_item_key)
                existing_df["_sort"] = existing_df["is_valid"].str.strip().str.upper().map({"N": 0, "Y": 1}).fillna(1)
                existing_df = (
                    existing_df.sort_values("_sort")
                    .drop_duplicates(subset=["item_name"], keep="first")
                    .drop(columns=[c for c in ("brand", "store_name", "_sort") if c in existing_df.columns])
                )
                if "note" not in existing_df.columns:
                    existing_df["note"] = ""
                existing_df = (
                    existing_df[existing_df["item_name"].str.strip() != ""]
                    .reindex(columns=["item_name", "is_valid", "note"], fill_value="")
                    .reset_index(drop=True)
                )
                existing_df.to_csv(POSFEED_WHITELIST_CSV_PATH, index=False, encoding="utf-8-sig")
                logger.info("마이그레이션 완료: %d행", len(existing_df))
            # 기존 CSV item_name 집합 (이미 정규화된 값)
            existing_items = {
                str(r["item_name"]).strip()
                for _, r in existing_df.iterrows()
                if str(r["item_name"]).strip()
            }
        except Exception as e:
            logger.warning("기존 CSV 로드 실패, 신규 생성으로 진행: %s", e)
            existing_df = None

    new_rows = [
        {"item_name": norm, "is_valid": "Y", "note": ""}
        for norm in sorted(norm_items)
        if norm not in existing_items
    ]

    if not new_rows:
        return f"신규 항목 없음 (기존 CSV에 모두 포함됨) | {POSFEED_WHITELIST_CSV_PATH}"

    new_df = pd.DataFrame(new_rows, columns=["item_name", "is_valid", "note"])

    combined = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
    combined["_sort"] = combined["is_valid"].str.strip().str.upper().map({"N": 0, "Y": 1}).fillna(1)
    combined = (
        combined.sort_values("_sort")
        .drop_duplicates(subset=["item_name"], keep="first")
        .drop(columns=["_sort"])
        .sort_values("item_name")
        .reset_index(drop=True)
    )
    combined.to_csv(POSFEED_WHITELIST_CSV_PATH, index=False, encoding="utf-8-sig")

    # 신규 항목 이메일 알림
    try:
        from modules.transform.utility.mailer import send_email

        table_rows = "".join(
            f"<tr><td style='padding:6px 12px;border-bottom:1px solid #eee;'>{r['item_name']}</td></tr>"
            for r in new_rows
        )
        html = f"""<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:24px;background:#f4f6f8;">
<div style="max-width:800px;margin:auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <h2 style="margin-top:0;color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:8px;">[도리당] Posfeed 신규 상품 등록 알림</h2>
  <p style="color:#555;">신규 item_name <b>{len(new_rows)}개</b>가 whitelist에 추가되었습니다. is_valid를 확인해 주세요.</p>
  <table style="border-collapse:collapse;width:100%;">
    <thead><tr>
      <th style="background:#2c3e50;color:#fff;padding:8px 12px;text-align:left;">item_name</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
  <p style="color:#999;font-size:12px;margin-top:16px;">파일: {POSFEED_WHITELIST_CSV_PATH}</p>
</div></body></html>"""

        send_email(
            subject=f"[도리당] Posfeed 신규 상품 {len(new_rows)}개 등록",
            html_content=html,
            to_emails="a17019@kakao.com",
        )
        logger.info("신규 상품 알림 발송 완료: %d개", len(new_rows))
    except Exception as e:
        logger.warning("신규 상품 알림 발송 실패: %s", e)

    return (
        f"draft 생성 완료 | 신규 {len(new_rows)}개 추가 | "
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
