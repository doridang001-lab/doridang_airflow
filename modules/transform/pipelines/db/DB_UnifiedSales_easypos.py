"""
unified_sales - EasyPOS 채널 전용 모듈.

입력:
- ANALYTICS_DB/easypos_sales_raw/ym=YYYY-MM/receipts.csv (DB_EasyPOS_Sales 파이프라인 산출물)

출력:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet (공통 _save_unified_daily 사용)
"""

import logging
import re
from datetime import datetime

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, FIN_PRODUCT_CSV_PATH, existing_fin_product_csv_path
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    _apply_fin_item_name,
    _make_unified_pk,
    _load_store_map,
    _lookup_store_meta,
    _normalize_time,
    _save_unified_daily,
    _strip_and_coalesce_columns,
    _to_int_series,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import allocate_manual_item_ids

logger = logging.getLogger(__name__)

EASYPOS_ROOT = ANALYTICS_DB / "easypos_sales_raw"
EASYPOS_BRAND = "도리당"
EASYPOS_STORE = "송파점"

_ITEM_ID_CLEAN_RE = re.compile(r"[^0-9A-Za-z가-힣]+")

# 월 단위 캐시 (DAG 실행 단위로 재사용)
_EASYPOS_MONTH_CACHE: dict[str, pd.DataFrame] = {}

_EASYPOS_PRODUCT_NAME_TO_CODE: dict[str, str] | None = None


def _normalize_easypos_product_name(name: str) -> str:
    s = "" if name is None else str(name)
    s = s.strip()
    if not s or s.lower() == "nan":
        return ""
    # EasyPOS 영수증 item_name에 붙는 트리 표시(prefix) 제거만 적용
    # 예: "└▶ 낙곱새 전골" -> "낙곱새 전골"
    s = re.sub(r"^(?:└\s*▶|└▶)\s*", "", s).strip()
    return s


def _load_easypos_product_name_map() -> dict[str, str]:
    """fin_product_grp_input.csv에서 easypos 상품명→상품코드 매핑을 만든다.

    EasyPOS 영수증에는 상품코드가 없어서 item_id를 상품명 기반으로 '역매핑'해야 함.
    - updated_at 컬럼이 있으면 최신 기준으로 dedupe
    - 동일 상품명에 여러 코드가 매핑되는 케이스는 최신 값을 우선
    """
    global _EASYPOS_PRODUCT_NAME_TO_CODE
    if _EASYPOS_PRODUCT_NAME_TO_CODE is not None:
        return _EASYPOS_PRODUCT_NAME_TO_CODE

    source_path = existing_fin_product_csv_path()
    if not source_path.exists():
        _EASYPOS_PRODUCT_NAME_TO_CODE = {}
        return _EASYPOS_PRODUCT_NAME_TO_CODE

    try:
        df = pd.read_csv(source_path, dtype=str).fillna("")
    except Exception:
        _EASYPOS_PRODUCT_NAME_TO_CODE = {}
        return _EASYPOS_PRODUCT_NAME_TO_CODE

    for c in ("source", "상품코드", "상품명"):
        if c not in df.columns:
            _EASYPOS_PRODUCT_NAME_TO_CODE = {}
            return _EASYPOS_PRODUCT_NAME_TO_CODE

    df = df[df["source"].fillna("").astype(str).str.strip().str.lower() == "easypos"].copy()
    if df.empty:
        _EASYPOS_PRODUCT_NAME_TO_CODE = {}
        return _EASYPOS_PRODUCT_NAME_TO_CODE

    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["상품명"] = df["상품명"].fillna("").astype(str).map(_normalize_easypos_product_name)

    df = df[(df["상품코드"] != "") & (df["상품명"] != "")].copy()
    if df.empty:
        _EASYPOS_PRODUCT_NAME_TO_CODE = {}
        return _EASYPOS_PRODUCT_NAME_TO_CODE

    if "updated_at" in df.columns:
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(["상품명", "_updated_at_ts"], na_position="last").groupby("상품명", as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(["상품명"]).groupby("상품명", as_index=False).last()

    _EASYPOS_PRODUCT_NAME_TO_CODE = df.set_index("상품명")["상품코드"].to_dict()
    return _EASYPOS_PRODUCT_NAME_TO_CODE


def _load_easypos_month(ym: str) -> pd.DataFrame:
    global _EASYPOS_MONTH_CACHE
    if ym in _EASYPOS_MONTH_CACHE:
        return _EASYPOS_MONTH_CACHE[ym]

    path = EASYPOS_ROOT / f"ym={ym}" / "receipts.csv"
    if not path.exists():
        raise FileNotFoundError(f"easypos receipts.csv 없음 | ym={ym} | {path}")

    df = pd.read_csv(path, dtype=str)
    df = _strip_and_coalesce_columns(df)
    logger.info("EasyPOS 월 로드: %s | %d행", path, len(df))
    _EASYPOS_MONTH_CACHE[ym] = df
    return df


def _load_easypos_by_date(date_str: str) -> pd.DataFrame:
    ym = date_str[:7]
    df = _load_easypos_month(ym)
    if df.empty:
        raise FileNotFoundError(f"easypos receipts.csv 비어있음 | ym={ym}")

    if "sale_date" not in df.columns:
        raise ValueError(f"easypos receipts.csv에 sale_date 컬럼이 없습니다 | {EASYPOS_ROOT}/ym={ym}/receipts.csv")

    out = df[df["sale_date"].astype(str).str.strip() == date_str].reset_index(drop=True)
    if out.empty:
        raise FileNotFoundError(f"easypos receipts 데이터 없음 | {date_str}")
    return out


def _transform_easypos_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df = _strip_and_coalesce_columns(df)

    # item_name 통일
    if "menu_name" in df.columns and "item_name" not in df.columns:
        df = df.rename(columns={"menu_name": "item_name"})

    for col in ("sale_date", "receipt_no", "order_time", "sale_type", "receipt_type", "shop_name", "item_name"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    if "qty" not in df.columns:
        df["qty"] = "0"
    if "line_amount" not in df.columns:
        df["line_amount"] = "0"

    df["ym"] = df["sale_date"].str[:7]
    df["source"] = "easypos"

    # EasyPOS 채널은 unified_sales에서 고정 brand/store로 관리
    # (예: 안산중앙점/연신내점/용인동천점 등으로 수집되더라도 도리당/송파점으로 통일)
    df["brand"] = EASYPOS_BRAND
    df["store"] = EASYPOS_STORE

    # store meta
    store_map = _load_store_map()
    df["_skey"] = df["store"].str.strip().str.split().str[-1]
    df["담당자"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    df["region"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    df["실오픈일"] = df["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    df = df.drop(columns=["_skey"])

    # order_time normalize
    df["order_time"] = df["order_time"].map(_normalize_time)

    # platform / order_type (포장 여부)
    is_pack = df["receipt_type"].astype(str).str.contains("포장", na=False)
    df["platform"] = "홀"
    df["order_type"] = is_pack.map(lambda v: "홀_포장" if v else "홀_테이블")

    # 금액/수량
    df["unit_price"] = _to_int_series(df["line_amount"])
    # easypos는 unit_price가 이미 라인 총액
    df["total_price"] = df["unit_price"]
    df["discount_amount"] = 0
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int).astype(str)

    # sale_type 정규화
    st = df["sale_type"].fillna("").astype(str).str.strip()
    df["sale_type"] = st.map({"매출": "정상", "반품": "취소"}).fillna("정상")

    # 취소 라인은 total_price를 음수로 저장(집계 시 CASE 불필요)
    refund_mask = df["sale_type"] == "취소"
    df.loc[refund_mask, "total_price"] = -df.loc[refund_mask, "total_price"].abs()
    df.loc[~refund_mask, "total_price"] = df.loc[~refund_mask, "total_price"].abs()

    # order_id (receipt_no + sale_date + order_time)
    df["order_id"] = df["receipt_no"].astype(str) + "_" + df["sale_date"].astype(str) + "_" + df["order_time"].astype(str)
    _empty_rcpt = (df["receipt_no"].astype(str).str.strip() == "").sum()
    if _empty_rcpt:
        logger.warning("easypos: receipt_no 빈값 %d행 — order_id 중복 위험", _empty_rcpt)

    # item_seq: order_id 내 순번(1부터)
    df["item_seq"] = df.groupby("order_id").cumcount().add(1).astype(int).astype(str)

    # item_id: fin_product_grp_input.csv(easypos 상품조회)에서 상품명→상품코드 역매핑
    # 매핑 실패 시 숫자 item_id 원칙이 깨지므로 저장하지 않고 실패시킨다.
    name_to_code = _load_easypos_product_name_map()
    item_name_norm = (
        df["item_name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .map(_normalize_easypos_product_name)
    )
    mapped_code = item_name_norm.map(lambda s: name_to_code.get(s, "") if s else "").fillna("").astype(str).str.strip()
    missing_code = (item_name_norm != "") & (mapped_code == "")
    if missing_code.any():
        samples = item_name_norm[missing_code].drop_duplicates().head(20).tolist()
        raise ValueError(f"easypos 상품코드 매핑 실패 {int(missing_code.sum())}행: {samples}")
    df["item_id"] = mapped_code
    df["item_id"] = allocate_manual_item_ids(
        df[["source", "brand", "store", "item_id", "item_name", "unit_price"]],
        persist=True,
    )

    # 주문서별 대표상품(menu_name) 및 주문카운트(order_cnt)
    item_cnt = df.groupby("order_id", as_index=False).size().rename(columns={"size": "item_cnt"})
    main_row = df.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1)
    main = df.loc[main_row, ["order_id", "item_name"]].rename(columns={"item_name": "menu_name"})
    summary = main.merge(item_cnt, on="order_id", how="left")
    df = df.merge(summary[["order_id", "menu_name", "item_cnt"]], on="order_id", how="left")

    df["order_cnt"] = df.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1).astype(int)
    # 반품 주문의 order_cnt는 -1 (집계 시 주문건수 차감)
    df.loc[df["sale_type"] == "취소", "order_cnt"] = -1

    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["_pk"] = _make_unified_pk(df)

    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    # scoped item_id 기준으로 item_name 정합
    df = _apply_fin_item_name(df)
    return df


def run_easypos(date_str: str, overwrite: bool = False, stores: list[str] | None = None) -> str:
    """특정 일자의 easypos receipts.csv를 unified_sales로 append/overwrite."""
    try:
        raw = _load_easypos_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (easypos CSV 없음 | {date_str})"

    df = _transform_easypos_df(raw)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"

    store_scope = {str(store).strip() for store in (stores or []) if str(store).strip()}
    if store_scope:
        df = df[df["store"].fillna("").astype(str).str.strip().isin(store_scope)].copy()
        if df.empty:
            return f"스킵 (easypos 대상 매장 데이터 없음 | {date_str} | stores={sorted(store_scope)})"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite, replace_stores=stores)
    result = f"unified_sales(easypos) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result


def run_lookback_easypos(days: int = 7) -> str:
    """최근 N일 easypos 데이터를 append-only로 적재."""
    try:
        import pendulum  # type: ignore
    except ModuleNotFoundError:
        from datetime import timedelta
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("Asia/Seoul"))
        date_iter = ((now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1))
    else:
        kst = pendulum.timezone("Asia/Seoul")
        date_iter = (pendulum.now(kst).subtract(days=i).format("YYYY-MM-DD") for i in range(1, days + 1))

    total_saved = 0
    for date_str in date_iter:
        try:
            raw = _load_easypos_by_date(date_str)
        except FileNotFoundError:
            logger.info("easypos CSV 없음, 스킵: %s", date_str)
            continue
        df = _transform_easypos_df(raw)
        if df.empty:
            continue
        saved = _save_unified_daily(df, date_str)
        total_saved += saved
        if saved:
            logger.info("lookback easypos %s | 추가 %d행", date_str, saved)

    result = f"unified_sales(easypos) lookback({days}일) | 추가 {total_saved}행"
    logger.info(result)
    return result


def backfill_easypos() -> str:
    """easypos_sales_raw 전체 receipts.csv를 훑어서 일자별로 unified_sales로 적재."""
    paths = sorted(EASYPOS_ROOT.glob("ym=*/receipts.csv"))
    if not paths:
        raise FileNotFoundError(f"easypos receipts.csv 없음 | {EASYPOS_ROOT}")

    date_set: set[str] = set()
    for p in paths:
        try:
            tmp = pd.read_csv(p, dtype=str, usecols=["sale_date"])
            date_set.update(tmp["sale_date"].astype(str).str.strip().dropna().unique())
        except Exception as e:
            logger.warning("sale_date 스캔 실패: %s | %s", p, e)

    total_targets = 0
    for date_str in sorted(date_set):
        if not date_str or date_str.lower() == "nan":
            continue
        try:
            run_easypos(date_str)
            total_targets += 1
        except Exception as e:
            logger.warning("run_easypos 실패: %s | %s", date_str, e)

    result = f"unified_sales(easypos) backfill 완료 | {total_targets}일"
    logger.info(result)
    return result
