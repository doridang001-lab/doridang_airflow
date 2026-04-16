"""
투오더/OKPOS 매출 → mart 일별 unified_sales_YYMMDD.parquet 생성 파이프라인

입력(toorder): ANALYTICS_DB/toorder_daily_sales/toorder_daily_sales_YYYYMMDD.csv
              컬럼: 수집채널, 주문일자, 매장명, 플랫폼명, 매출액

입력(okpos):  RAW_OKPOS_SALES/brand=도리당/store=*/ym=YYYY-MM/okpos_order.csv
             RAW_OKPOS_SALES/brand=도리당/store=*/ym=YYYY-MM/okpos_order_item.csv

출력: MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet  (일자별 파일, _pk dedup 병합)
     grain='daily' (toorder) | grain='item' (okpos)

공개 API:
    run(date_str)        — 특정 날짜 toorder 실행 (YYYY-MM-DD)
    backfill()           — 전체 toorder CSV 일괄 처리
    run_okpos(date_str)  — 특정 날짜 okpos 실행 (YYYY-MM-DD)
"""

import hashlib
import logging
from datetime import datetime

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, MART_DB, ONEDRIVE_DB, RAW_OKPOS_SALES

logger = logging.getLogger(__name__)

TOORDER_SALES_ROOT = ANALYTICS_DB / "toorder_daily_sales"
OKPOS_BRAND_ROOT   = RAW_OKPOS_SALES / "brand=도리당"
UNIFIED_ROOT       = MART_DB / "unified_sales_grp"

UNIFIED_COLUMNS = [
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "region",
    "담당자",
    "실오픈일",
    "platform",
    "order_type",
    "order_id",
    "order_time",        # HH:MM:SS 통일
    "menu_name",
    "item_seq",
    "item_id",
    "item_name",
    "qty",
    "unit_price",        # 매출금액 통합 (toorder=일별합계, okpos=아이템별)
    "grain",
    "_pk",
    "collected_at",
]


# ============================================================
# 공통 내부 유틸
# ============================================================

def _load_store_map() -> dict[str, dict]:
    """sales_employee.csv → {지점명: {담당자, region, 실오픈일}} 맵.
    지점명 = 매장명 마지막 단어  ex) '도리당 백석점' → '백석점'
    region  = 상세주소 앞 2자    ex) '경기도 고양시...' → '경기'
    """
    csv_path = ONEDRIVE_DB / "sales_employee.csv"
    try:
        try:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소", "실오픈일"])
        except ValueError:
            # 실오픈일 컬럼 없는 구버전 파일 fallback
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소"])
            df["실오픈일"] = ""
    except FileNotFoundError:
        logger.warning("sales_employee.csv 없음: %s", csv_path)
        return {}

    df["_key"] = df["매장명"].str.strip().str.split().str[-1]
    df = df.drop_duplicates(subset=["_key"], keep="first")
    return {
        row["_key"]: {
            "담당자":       str(row.get("담당자", "")).strip(),
            "region":       str(row.get("상세주소", "")).strip()[:2],
            "실오픈일":     str(row.get("실오픈일", "")).strip(),
        }
        for _, row in df.iterrows()
    }


def _lookup_store_meta(store_map: dict[str, dict], key, field: str) -> str:
    """매장 키가 비어있거나 NaN이어도 안전하게 메타값을 반환한다."""
    if pd.isna(key):
        return ""
    return str(store_map.get(str(key), {}).get(field, "")).strip()


def _unified_daily_path(date_str: str):
    """'YYYY-MM-DD' → unified_sales_YYMMDD.parquet 경로"""
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"


def _save_unified_daily(df: pd.DataFrame, date_str: str) -> int:
    """일별 unified_sales_YYMMDD.parquet 저장(기존 파일 병합 + _pk dedup). 신규 행 수 반환."""
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date_str)

    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")

    if daily_path.exists():
        existing       = pd.read_parquet(daily_path)
        existing_count = len(existing)
        existing       = existing.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        merged         = pd.concat([existing, df], ignore_index=True)
        merged         = merged.drop_duplicates(subset=["_pk"], keep="last")
        merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
    else:
        existing_count = 0
        merged         = df.copy()

    merged.to_parquet(daily_path, index=False, engine="pyarrow")
    new_count = len(merged) - existing_count
    logger.info("저장(일별): %s | 전체 %d행 (신규 %d행)", daily_path, len(merged), new_count)
    return new_count


# ============================================================
# 공통 시각 정규화
# ============================================================

def _normalize_time(raw) -> str:
    """시각 문자열 → HH:MM:SS 정규화.

    ex) ':mm:ss11:58:33' → '11:58:33'   (OKPOS raw 포맷)
    ex) '15:47:31'       → '15:47:31'   (이미 정상)
    ex) ''  / NaN        → ''
    """
    s = str(raw).strip()
    if not s or s == "nan":
        return ""
    # 뒤 8자가 HH:MM:SS 패턴이면 그대로 사용
    tail = s[-8:]
    if len(tail) == 8 and tail[2] == ":" and tail[5] == ":":
        return tail
    return s


# ============================================================
# ToOrder 내부 유틸
# ============================================================

def _derive_order_type(platform: str) -> str:
    """플랫폼명에서 주문 유형 파생.
    '포장' 포함 → '포장', '홀' 포함 → '홀', 그 외 → '배달'
    """
    p = str(platform)
    if "포장" in p:
        return "포장"
    if "홀" in p:
        return "홀"
    return "배달"


def _make_toorder_pk(sale_date: str, store: str, platform: str) -> str:
    """(sale_date, toorder, store, platform) → MD5 PK"""
    key = f"{sale_date}|toorder|{store}|{platform}"
    return hashlib.md5(key.encode()).hexdigest()


def _transform_df(raw: pd.DataFrame) -> pd.DataFrame:
    """toorder_daily_sales CSV → unified_sales 스키마 DataFrame 변환"""
    df = raw.copy()

    df = df.rename(columns={
        "주문일자": "sale_date",
        "매장명":   "store",
        "플랫폼명": "platform",
        "매출액":   "unit_price",
    })

    df["sale_date"]   = df["sale_date"].astype(str).str.strip()
    df["ym"]          = df["sale_date"].str[:7]
    df["source"]      = "toorder"
    df["brand"]       = "총브랜드합"
    df["order_type"]  = df["platform"].apply(_derive_order_type)
    df["unit_price"]  = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0).astype(int)

    store_map        = _load_store_map()
    df["_key"]       = df["store"].str.strip().str.split().str[-1]
    df["담당자"]       = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    df["region"]       = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    df["실오픈일"]     = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    df = df.drop(columns=["_key"])

    for col in ("order_id", "order_time", "menu_name",
                "item_seq", "item_id", "item_name", "qty"):
        df[col] = ""

    df["grain"]        = "daily"
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["_pk"]          = df.apply(
        lambda r: _make_toorder_pk(r["sale_date"], r["store"], r["platform"]), axis=1
    )

    return df[UNIFIED_COLUMNS]


def _load_from_paths(file_paths: list[str]) -> pd.DataFrame:
    """파일 경로 리스트 → DataFrame 로드"""
    parts = []
    for p in file_paths:
        try:
            parts.append(pd.read_csv(p, dtype=str))
        except Exception as e:
            logger.warning("파일 로드 실패: %s | %s", p, e)
    if not parts:
        raise FileNotFoundError(f"로드 가능한 파일 없음: {file_paths}")
    df = pd.concat(parts, ignore_index=True)
    logger.info("toorder CSV 로드: %d개 파일, %d행", len(parts), len(df))
    return df


def _load_by_date(date_str: str) -> pd.DataFrame:
    """YYYY-MM-DD → 해당 날짜 CSV 로드"""
    ymd   = date_str.replace("-", "")
    files = list(TOORDER_SALES_ROOT.glob(f"toorder_daily_sales_{ymd}.csv"))
    if not files:
        raise FileNotFoundError(f"toorder CSV 없음 | {date_str}")
    return _load_from_paths([str(f) for f in files])


def _load_all() -> pd.DataFrame:
    """전체 CSV 로드 (backfill용)"""
    files = sorted(TOORDER_SALES_ROOT.glob("toorder_daily_sales_*.csv"))
    if not files:
        raise FileNotFoundError(f"toorder CSV 없음 | {TOORDER_SALES_ROOT}")
    return _load_from_paths([str(f) for f in files])


# ============================================================
# OKPOS 내부 유틸
# ============================================================

def _make_okpos_pk(sale_date: str, store: str, platform: str, order_id: str, item_seq: int) -> str:
    """OKPOS 아이템 레벨 PK: (sale_date, okpos, store, platform, order_id, item_seq) → MD5"""
    key = f"{sale_date}|okpos|{store}|{platform}|{order_id}|{item_seq}"
    return hashlib.md5(key.encode()).hexdigest()


def _load_okpos_by_date(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """지정 날짜의 okpos_order + okpos_order_item CSV 로드 (모든 store).
    sale_date 컬럼으로 해당 날짜만 필터링.
    """
    ym         = date_str[:7]
    order_dfs  = []
    item_dfs   = []

    for order_path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order.csv")):
        try:
            df = pd.read_csv(order_path, dtype=str)
            df = df[df["sale_date"].str.strip() == date_str]
            if not df.empty:
                order_dfs.append(df)
        except Exception as e:
            logger.warning("okpos_order 로드 실패: %s | %s", order_path, e)

    for item_path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order_item.csv")):
        try:
            df = pd.read_csv(item_path, dtype=str)
            df = df[df["sale_date"].str.strip() == date_str]
            if not df.empty:
                item_dfs.append(df)
        except Exception as e:
            logger.warning("okpos_order_item 로드 실패: %s | %s", item_path, e)

    if not order_dfs:
        raise FileNotFoundError(f"okpos_order CSV 없음 | {date_str}")
    if not item_dfs:
        raise FileNotFoundError(f"okpos_order_item CSV 없음 | {date_str}")

    order_df = pd.concat(order_dfs, ignore_index=True)
    item_df  = pd.concat(item_dfs, ignore_index=True)
    logger.info("OKPOS 로드: order %d행, item %d행 | %s", len(order_df), len(item_df), date_str)
    return order_df, item_df


def _transform_okpos_df(order_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    """OKPOS order + order_item → unified_sales 스키마 DataFrame.

    notebooks/003_okpos_사전수정.ipynb 로직 이식.
    """
    order = order_df.copy()
    item  = item_df.copy()

    # 숫자 컬럼 변환
    for col in ["총매출액", "실매출액"]:
        if col in order.columns:
            order[col] = pd.to_numeric(order[col], errors="coerce").fillna(0).astype(int)
    for col in ["총매출액", "실매출액"]:
        if col in item.columns:
            item[col] = pd.to_numeric(item[col], errors="coerce").fillna(0).astype(int)

    # 병합: order(헤더) LEFT JOIN order_item(품목)
    merged = pd.merge(
        order, item,
        left_on=["영수번호", "sale_date"],
        right_on=["영수증번호", "sale_date"],
        how="left",
    )

    # 채널별 아이템 대표금액 결정 (쿠팡이츠 → 총매출액_y, 나머지 → 실매출액_y)
    channel_col = "주문채널" if "주문채널" in merged.columns else None
    actual_source = merged["실매출액_y"] if "실매출액_y" in merged.columns else pd.Series(0, index=merged.index)
    total_source  = merged["총매출액_y"] if "총매출액_y" in merged.columns else pd.Series(0, index=merged.index)
    actual_y      = pd.to_numeric(actual_source, errors="coerce").fillna(0).astype(int)
    total_y       = pd.to_numeric(total_source, errors="coerce").fillna(0).astype(int)

    if channel_col and "총매출액_y" in merged.columns:
        is_coupang         = merged[channel_col].str.strip() == "쿠팡이츠"
        merged["_item_amt"] = total_y.where(is_coupang, actual_y)
    else:
        merged["_item_amt"] = actual_y

    # 주문별 품목 수
    cnt_map            = merged.groupby("_pk_x").size().rename("_item_cnt")
    merged             = merged.join(cnt_map, on="_pk_x")

    # 주문별 대표 품목 (금액 최대 아이템의 상품명)
    rep_idx = merged.groupby("_pk_x")["_item_amt"].idxmax()
    rep_map = merged.loc[rep_idx, ["_pk_x", "상품명"]].set_index("_pk_x")["상품명"]
    merged["_rep_name"] = merged["_pk_x"].map(rep_map)
    merged["menu_name"] = merged.apply(
        lambda r: str(r["_rep_name"])
        if r.get("_item_cnt", 1) <= 1
        else f"{r['_rep_name']} 외 {r['_item_cnt'] - 1}건",
        axis=1,
    )

    # item_seq: 주문 내 순번 (1-based)
    merged["item_seq"] = (merged.groupby("_pk_x").cumcount() + 1).astype(str)

    # brand / store 파생
    merged["brand"] = merged["매장명_x"].str.split(" ").str[0].fillna("")
    merged["store"] = merged["매장명_x"].str.split(" ").str[1].fillna("")

    # store_map 매핑 (담당자, region, 실오픈일)
    store_map          = _load_store_map()
    merged["_skey"]    = merged["store"].str.strip().str.split().str[-1]
    merged["담당자"]       = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    merged["region"]       = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    merged["실오픈일"]     = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))

    # 나머지 컬럼 정리
    merged["source"]    = "okpos"
    merged["ym"]        = merged["sale_date"].str[:7]
    merged["platform"]  = (merged[channel_col].fillna("").astype(str).str.strip()
                           if channel_col else "")
    # order_type: okpos_order의 주문유형 컬럼 사용 (merge 후 _x 접미사 가능)
    ot_col = ("주문유형_x" if "주문유형_x" in merged.columns
              else "주문유형" if "주문유형" in merged.columns else None)
    merged["order_type"] = (merged[ot_col].fillna("").astype(str).str.strip()
                            if ot_col else "")

    merged["order_id"]   = merged["_pk_x"].fillna("").astype(str)
    merged["order_time"] = merged["결제시각_x"].apply(_normalize_time)
    merged["item_id"]    = merged["상품코드"].fillna("").astype(str)
    merged["item_name"]  = merged["상품명"].fillna("").astype(str)

    qty_col       = "수량_y" if "수량_y" in merged.columns else "수량"
    qty_source    = merged[qty_col] if qty_col in merged.columns else pd.Series(0, index=merged.index)
    merged["qty"] = pd.to_numeric(qty_source, errors="coerce").fillna(0).astype(int).astype(str)

    merged["unit_price"] = merged["_item_amt"]

    merged["grain"]        = "item"
    merged["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged["_pk"]          = merged.apply(
        lambda r: _make_okpos_pk(
            r["sale_date"], r["store"], r["platform"], r["order_id"], r["item_seq"]
        ),
        axis=1,
    )

    return merged[UNIFIED_COLUMNS]


# ============================================================
# Public API
# ============================================================

def run(date_str: str) -> str:
    """특정 날짜 toorder CSV → unified_sales 저장.

    Args:
        date_str: 'YYYY-MM-DD'
    """
    raw = _load_by_date(date_str)
    df  = _transform_df(raw)

    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"

    saved  = _save_unified_daily(df, date_str)
    result = f"unified_sales(toorder) 일별 저장 완료 | {date_str} | {saved}행"
    logger.info(result)
    return result


def backfill() -> str:
    """전체 toorder_daily_sales CSV → unified_sales 일괄 변환."""
    raw = _load_all()
    df  = _transform_df(raw)

    if df.empty:
        return "스킵 (데이터 없음)"

    total_saved = 0
    for sale_date, part in df.groupby("sale_date", dropna=False):
        sale_date = str(sale_date).strip()
        if not sale_date or sale_date.lower() == "nan":
            continue
        total_saved += _save_unified_daily(part, sale_date)
    result = f"unified_sales(toorder) backfill(일별) 완료 | {total_saved}행"
    logger.info(result)
    return result


def run_okpos(date_str: str) -> str:
    """특정 날짜 OKPOS order+order_item → unified_sales 저장.

    Args:
        date_str: 'YYYY-MM-DD'
    """
    order_df, item_df = _load_okpos_by_date(date_str)
    df                = _transform_okpos_df(order_df, item_df)

    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"

    saved  = _save_unified_daily(df, date_str)
    result = f"unified_sales(okpos) 일별 저장 완료 | {date_str} | {saved}행"
    logger.info(result)
    return result
