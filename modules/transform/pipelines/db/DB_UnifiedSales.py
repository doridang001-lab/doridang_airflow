"""
투오더/OKPOS 매출 → mart 일별 unified_sales_YYMMDD.parquet 생성 파이프라인

입력(toorder): ANALYTICS_DB/toorder_daily_sales/toorder_daily_sales_YYYYMMDD.csv
              컬럼: 수집채널, 주문일자, 매장명, 플랫폼명, 매출액

입력(okpos):  RAW_OKPOS_SALES/brand=도리당/store=*/ym=YYYY-MM/okpos_order.csv
             RAW_OKPOS_SALES/brand=도리당/store=*/ym=YYYY-MM/okpos_order_item.csv

출력: MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet  (일자별 파일, append-only)
     grain='daily' (toorder) | grain='item' (okpos)

공개 API:
    run(date_str, overwrite)         — 특정 날짜 toorder (YYYY-MM-DD, 정정용 overwrite)
    run_okpos(date_str, overwrite)   — 특정 날짜 okpos
    run_lookback_toorder(days)       — 최근 N일 toorder 누락분 보충 (기본 7일)
    run_lookback_okpos(days)         — 최근 N일 okpos 누락분 보충 (기본 7일, 월별 캐시)
    backfill()                       — 전체 toorder CSV 일괄 처리
    backfill_okpos()                 — 전체 OKPOS order CSV 일괄 처리
    resave_existing_unified_sales()  — 기존 unified_sales parquet 전체 재저장
"""

import hashlib
import logging
import re
from datetime import datetime

import pandas as pd
import pendulum

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
    "order_time",
    "menu_name",
    "item_seq",
    "item_id",
    "item_name",
    "qty",
    "unit_price",
    "구분",
    "grain",
    "_pk",
    "collected_at",
]

# 모듈 레벨 캐시 (DAG 실행 단위로 재사용)
_STORE_MAP_CACHE: dict | None = None
_OKPOS_MONTH_CACHE: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}


# ============================================================
# 공통 내부 유틸
# ============================================================

def _load_store_map() -> dict[str, dict]:
    """sales_employee.csv → {지점명: {담당자, region, 실오픈일}} 맵 (캐시)."""
    global _STORE_MAP_CACHE
    if _STORE_MAP_CACHE is not None:
        return _STORE_MAP_CACHE

    csv_path = ONEDRIVE_DB / "sales_employee.csv"
    try:
        try:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소", "실오픈일"])
        except ValueError:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소"])
            df["실오픈일"] = ""
    except FileNotFoundError:
        logger.warning("sales_employee.csv 없음: %s", csv_path)
        _STORE_MAP_CACHE = {}
        return _STORE_MAP_CACHE

    df["_key"] = df["매장명"].str.strip().str.split().str[-1]
    df = df.drop_duplicates(subset=["_key"], keep="first")
    _STORE_MAP_CACHE = {
        row["_key"]: {
            "담당자":   str(row.get("담당자", "")).strip(),
            "region":   str(row.get("상세주소", "")).strip()[:2],
            "실오픈일": str(row.get("실오픈일", "")).strip(),
        }
        for _, row in df.iterrows()
    }
    return _STORE_MAP_CACHE


def _lookup_store_meta(store_map: dict[str, dict], key, field: str) -> str:
    if pd.isna(key):
        return ""
    return str(store_map.get(str(key), {}).get(field, "")).strip()


def _unified_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"


def _save_unified_daily(df: pd.DataFrame, date_str: str, overwrite: bool = False) -> int:
    """일별 unified_sales 저장.

    overwrite=False(기본): 누락 PK만 append.
    overwrite=True: 기존 파일 전체 교체 (정정용).
    """
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date_str)
    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")

    if daily_path.exists() and not overwrite:
        existing  = pd.read_parquet(daily_path)
        existing  = existing.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        new_rows  = df[~df["_pk"].isin(existing["_pk"])]
        if new_rows.empty:
            logger.info("신규 행 없음, 스킵: %s", daily_path)
            return 0
        merged    = pd.concat([existing, new_rows], ignore_index=True)
        new_count = len(new_rows)
    else:
        merged    = df.copy()
        new_count = len(merged)

    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
    merged.to_parquet(daily_path, index=False, engine="pyarrow")
    logger.info("저장(일별): %s | 전체 %d행 (신규 %d행)", daily_path, len(merged), new_count)
    return new_count


# ============================================================
# 공통 시각 정규화
# ============================================================

def _normalize_time(raw) -> str:
    s = str(raw).strip()
    if not s or s == "nan":
        return ""
    tail = s[-8:]
    if len(tail) == 8 and tail[2] == ":" and tail[5] == ":":
        return tail
    return s


# ============================================================
# ToOrder 내부 유틸
# ============================================================

def _derive_order_type(platform: str) -> str:
    p = str(platform)
    if "홀" in p and "포장" in p:
        return "홀_포장"
    if "홀" in p:
        return "홀_테이블"
    if "포장" in p:
        return "플랫폼_포장"
    return "배달"


def _transform_df(raw: pd.DataFrame) -> pd.DataFrame:
    """toorder_daily_sales CSV → unified_sales 스키마 DataFrame 변환"""
    df = raw.copy()

    df = df.rename(columns={
        "주문일자": "sale_date",
        "매장명":   "store",
        "플랫폼명": "platform",
        "매출액":   "unit_price",
    })

    df["sale_date"]  = df["sale_date"].astype(str).str.strip()
    df["ym"]         = df["sale_date"].str[:7]
    df["source"]     = "toorder"
    df["brand"]      = "총브랜드합"
    df["order_type"] = df["platform"].apply(_derive_order_type)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0).astype(int)

    store_map    = _load_store_map()
    df["_key"]   = df["store"].str.strip().str.split().str[-1]
    df["담당자"]   = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    df["region"]   = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    df["실오픈일"] = df["_key"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    df = df.drop(columns=["_key"])

    for col in ("order_id", "order_time", "menu_name", "item_seq", "item_id", "item_name", "qty"):
        df[col] = ""

    df["구분"]        = "정상"
    df["grain"]      = "daily"
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # vectorized PK 생성
    df["_pk"] = (
        df["sale_date"] + "|toorder|" + df["store"] + "|" + df["platform"]
    ).map(lambda s: hashlib.md5(s.encode()).hexdigest())

    return df[UNIFIED_COLUMNS]


def _load_by_date(date_str: str) -> pd.DataFrame:
    ymd   = date_str.replace("-", "")
    files = list(TOORDER_SALES_ROOT.glob(f"toorder_daily_sales_{ymd}.csv"))
    if not files:
        raise FileNotFoundError(f"toorder CSV 없음 | {date_str}")
    parts = []
    for p in files:
        try:
            parts.append(pd.read_csv(p, dtype=str))
        except Exception as e:
            logger.warning("파일 로드 실패: %s | %s", p, e)
    if not parts:
        raise FileNotFoundError(f"toorder CSV 로드 실패 | {date_str}")
    return pd.concat(parts, ignore_index=True)


# ============================================================
# OKPOS 내부 유틸
# ============================================================

def _load_okpos_month(ym: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ym='YYYY-MM' 전체 월 데이터 로드 (모듈 레벨 캐시).
    같은 ym을 여러 날짜에서 호출해도 1회만 읽음.
    """
    global _OKPOS_MONTH_CACHE
    if ym in _OKPOS_MONTH_CACHE:
        return _OKPOS_MONTH_CACHE[ym]

    order_dfs = []
    item_dfs  = []

    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order.csv")):
        try:
            order_dfs.append(pd.read_csv(path, dtype=str))
        except Exception as e:
            logger.warning("okpos_order 로드 실패: %s | %s", path, e)

    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order_item.csv")):
        try:
            item_dfs.append(pd.read_csv(path, dtype=str))
        except Exception as e:
            logger.warning("okpos_order_item 로드 실패: %s | %s", path, e)

    order_df = pd.concat(order_dfs, ignore_index=True) if order_dfs else pd.DataFrame()
    item_df  = pd.concat(item_dfs, ignore_index=True) if item_dfs else pd.DataFrame()

    logger.info("OKPOS 월 로드: order %d행, item %d행 | ym=%s", len(order_df), len(item_df), ym)
    _OKPOS_MONTH_CACHE[ym] = (order_df, item_df)
    return order_df, item_df


def _load_okpos_by_date(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """지정 날짜의 okpos_order + okpos_order_item (월별 캐시에서 날짜 필터)."""
    ym       = date_str[:7]
    order_df, item_df = _load_okpos_month(ym)

    if order_df.empty:
        raise FileNotFoundError(f"okpos_order CSV 없음 | ym={ym}")
    if item_df.empty:
        raise FileNotFoundError(f"okpos_order_item CSV 없음 | ym={ym}")

    order_f = order_df[order_df["sale_date"].str.strip() == date_str].reset_index(drop=True)
    item_f  = item_df[item_df["sale_date"].str.strip() == date_str].reset_index(drop=True)

    if order_f.empty:
        raise FileNotFoundError(f"okpos_order 데이터 없음 | {date_str}")
    if item_f.empty:
        raise FileNotFoundError(f"okpos_order_item 데이터 없음 | {date_str}")

    logger.info("OKPOS 필터: order %d행, item %d행 | %s", len(order_f), len(item_f), date_str)
    return order_f, item_f


def _make_okpos_pk(sale_date: str, store: str, platform: str, order_id: str, item_seq: str) -> str:
    key = f"{sale_date}|okpos|{store}|{platform}|{order_id}|{item_seq}"
    return hashlib.md5(key.encode()).hexdigest()


def _transform_okpos_df(order_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    """OKPOS order + order_item → unified_sales 스키마 DataFrame."""
    order = order_df.copy()
    item  = item_df.copy()

    for col in ["총매출액", "실매출액"]:
        if col in order.columns:
            order[col] = pd.to_numeric(order[col], errors="coerce").fillna(0).astype(int)
    for col in ["총매출액", "실매출액"]:
        if col in item.columns:
            item[col] = pd.to_numeric(item[col], errors="coerce").fillna(0).astype(int)

    # OKPOS는 "영수번호"(order) vs "영수증번호"(item)로 컬럼명이 달라짐.
    # 영수번호/포스번호는 매장 간에도 재사용될 수 있어 "매장명"까지 포함해서 join 하는 것이 안전.
    if (
        {"매장명", "포스번호", "영수번호", "sale_date"}.issubset(order.columns)
        and {"매장명", "포스번호", "영수증번호", "sale_date"}.issubset(item.columns)
    ):
        merged = pd.merge(
            order, item,
            left_on=["매장명", "포스번호", "영수번호", "sale_date"],
            right_on=["매장명", "포스번호", "영수증번호", "sale_date"],
            how="left",
        )
    elif (
        {"포스번호", "영수번호", "sale_date"}.issubset(order.columns)
        and {"포스번호", "영수증번호", "sale_date"}.issubset(item.columns)
    ):
        merged = pd.merge(
            order, item,
            left_on=["포스번호", "영수번호", "sale_date"],
            right_on=["포스번호", "영수증번호", "sale_date"],
            how="left",
        )
    else:
        merged = pd.merge(
            order, item,
            left_on=["영수번호", "sale_date"],
            right_on=["영수증번호", "sale_date"],
            how="left",
        )

    channel_col   = "주문채널" if "주문채널" in merged.columns else None
    actual_source = merged["실매출액_y"] if "실매출액_y" in merged.columns else pd.Series(0, index=merged.index)
    total_source  = merged["총매출액_y"] if "총매출액_y" in merged.columns else pd.Series(0, index=merged.index)
    actual_y      = pd.to_numeric(actual_source, errors="coerce").fillna(0).astype(int)
    total_y       = pd.to_numeric(total_source, errors="coerce").fillna(0).astype(int)

    if channel_col and "총매출액_y" in merged.columns:
        is_coupang          = merged[channel_col].str.strip() == "쿠팡이츠"
        merged["_item_amt"] = total_y.where(is_coupang, actual_y)
    else:
        merged["_item_amt"] = actual_y

    # 영수증 단위로 grouping 하기 위한 안정 키(매장/포스/영수번호 기반).
    # 기존 _pk_x(행 hash)는 다운로드/정렬/표기 변화에 취약할 수 있어 가능하면 사용하지 않음.
    merged["_order_key"] = merged["_pk_x"].fillna("").astype(str)
    pos_col = "포스번호_x" if "포스번호_x" in merged.columns else "포스번호" if "포스번호" in merged.columns else None
    rcpt_col = "영수번호" if "영수번호" in merged.columns else "영수증번호" if "영수증번호" in merged.columns else None
    store_name_col = "매장명_x" if "매장명_x" in merged.columns else "매장명" if "매장명" in merged.columns else None
    if pos_col and rcpt_col and store_name_col and "sale_date" in merged.columns:
        merged["_order_key"] = (
            merged["sale_date"].fillna("").astype(str).str.strip()
            + "|" + merged[store_name_col].fillna("").astype(str).str.strip()
            + "|" + merged[pos_col].fillna("").astype(str).str.strip()
            + "|" + merged[rcpt_col].fillna("").astype(str).str.strip()
        )

    cnt_map  = merged.groupby("_order_key").size().rename("_item_cnt")
    merged   = merged.join(cnt_map, on="_order_key")

    rep_idx          = merged.groupby("_order_key")["_item_amt"].idxmax()
    rep_map          = merged.loc[rep_idx, ["_order_key", "상품명"]].set_index("_order_key")["상품명"]
    merged["_rep_name"] = merged["_order_key"].map(rep_map)
    merged["menu_name"] = merged.apply(
        lambda r: str(r["_rep_name"])
        if r.get("_item_cnt", 1) <= 1
        else f"{r['_rep_name']} 외 {r['_item_cnt'] - 1}건",
        axis=1,
    )

    merged["item_seq"] = (merged.groupby("_order_key").cumcount() + 1).astype(str)

    if store_name_col:
        merged["brand"] = merged[store_name_col].str.split(" ").str[0].fillna("")
        merged["store"] = merged[store_name_col].str.split(" ").str[1].fillna("")
    else:
        merged["brand"] = ""
        merged["store"] = ""

    store_map       = _load_store_map()
    merged["_skey"] = merged["store"].str.strip().str.split().str[-1]
    merged["담당자"]   = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    merged["region"]   = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    merged["실오픈일"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))

    merged["source"]   = "okpos"
    merged["ym"]       = merged["sale_date"].str[:7]
    merged["platform"] = (merged[channel_col].fillna("").astype(str).str.strip()
                          if channel_col else "")

    ot_col = ("주문유형_x" if "주문유형_x" in merged.columns
              else "주문유형" if "주문유형" in merged.columns else None)
    merged["order_type"] = (merged[ot_col].fillna("").astype(str).str.strip()
                            if ot_col else "")

    # order_id: 영수증 기준으로 사람이 읽기 쉬운 값(포스번호-영수번호) 우선.
    # (unified PK에는 sale_date/store가 포함되므로 order_id 자체는 매장 간 유일성까지 요구하지 않음)
    if pos_col and rcpt_col:
        merged["order_id"] = (
            merged[pos_col].fillna("").astype(str).str.strip()
            + "-" + merged[rcpt_col].fillna("").astype(str).str.strip()
        )
    else:
        merged["order_id"] = merged["_order_key"].fillna("").astype(str)

    # 테이블명 기반 보정: OKPOS의 주문유형이 "홀(매장)"으로 고정되는 경우가 있어 포장 여부를 추가 반영
    table_col = ("테이블명_x" if "테이블명_x" in merged.columns
                 else "테이블명" if "테이블명" in merged.columns else None)
    if table_col:
        table_v = merged[table_col].fillna("").astype(str).str.strip()
        is_pack = table_v.str.contains("포장", regex=False)
        is_num  = table_v.str.fullmatch(r"\d+").fillna(False)

        # 테이블명 규칙: "포스"/"제휴사주문" 주문에만 적용 (배달 채널 보호)
        # - 테이블명에 "포장" 포함 → order_type="홀_포장",   platform="홀 포장"
        # - 테이블명이 순수 숫자    → order_type="홀_테이블", platform="홀"
        platform_v = merged["platform"].fillna("").astype(str).str.strip() if "platform" in merged.columns else ""
        is_hall_platform = platform_v.isin(["포스", "제휴사주문"]) if hasattr(platform_v, "isin") else False
        cond = is_hall_platform if hasattr(is_hall_platform, "__and__") else None

        is_delivery_table = table_v.str.contains("배달", regex=False)

        if cond is not None:
            merged.loc[cond & is_pack, "order_type"] = "홀_포장"
            merged.loc[cond & is_pack, "platform"]   = "홀 포장"
            merged.loc[cond & (~is_pack) & is_num, "order_type"] = "홀_테이블"
            merged.loc[cond & (~is_pack) & is_num, "platform"]   = "홀"
            # 테이블명에 "배달" 포함 (배달1, 배달-01 등) → 배달 주문
            merged.loc[cond & (~is_pack) & (~is_num) & is_delivery_table, "order_type"] = "배달"
        else:
            merged.loc[is_pack, "order_type"] = "홀_포장"
            merged.loc[is_pack, "platform"]   = "홀 포장"
            merged.loc[(~is_pack) & is_num, "order_type"] = "홀_테이블"
            merged.loc[(~is_pack) & is_num, "platform"]   = "홀"
            merged.loc[(~is_pack) & (~is_num) & is_delivery_table, "order_type"] = "배달"

    # 최종 order_type 정규화: 배달 채널 포장 주문 및 미분류 값 처리
    _HALL_CH = {"포스", "제휴사주문", "홀", "홀 포장"}
    _VALID_OT = {"홀_테이블", "홀_포장", "플랫폼_포장", "배달"}
    pf_v = merged["platform"].fillna("").astype(str).str.strip()
    ot_v = merged["order_type"].fillna("").astype(str).str.strip()
    # 포스전화(CID) 포장 → 홀_포장 (전화 픽업은 홀 포장으로 처리)
    merged.loc[(pf_v == "포스전화(CID)") & (ot_v == "포장"), "order_type"] = "홀_포장"
    ot_v = merged["order_type"].fillna("").astype(str).str.strip()
    # 배달 채널(배달의민족/쿠팡이츠 등) 포장 주문 → 플랫폼_포장
    merged.loc[~pf_v.isin(_HALL_CH) & (ot_v == "포장"), "order_type"] = "플랫폼_포장"
    # 나머지 미분류(홀(매장) 등) → 홀_테이블
    ot_v2 = merged["order_type"].fillna("").astype(str).str.strip()
    merged.loc[~ot_v2.isin(_VALID_OT), "order_type"] = "홀_테이블"

    # order_time: OKPOS는 "최초주문"이 결제시각보다 더 자연스러운 주문시각인 경우가 많아 우선 사용.
    # (없으면 결제시각으로 fallback)
    time_col = next(
        (c for c in ("최초주문", "최초주문시각", "결제시각_y", "결제시각_x", "결제시각") if c in merged.columns),
        None,
    )
    merged["order_time"] = merged[time_col].apply(_normalize_time) if time_col else ""
    merged["item_id"]    = merged["상품코드"].fillna("").astype(str)
    merged["item_name"]  = merged["상품명"].fillna("").astype(str)

    qty_col       = "수량_y" if "수량_y" in merged.columns else "수량"
    qty_source    = merged[qty_col] if qty_col in merged.columns else pd.Series(0, index=merged.index)
    merged["qty"] = pd.to_numeric(qty_source, errors="coerce").fillna(0).astype(int).astype(str)

    merged["unit_price"] = merged["_item_amt"]

    gbn_col = ("구분_x" if "구분_x" in merged.columns
               else "구분" if "구분" in merged.columns else None)
    if gbn_col:
        merged["구분"] = (merged[gbn_col].str.strip()
                          .map({"매출": "정상", "반품": "반품"}).fillna("정상"))
    else:
        merged["구분"] = "정상"

    merged["grain"]      = "item"
    merged["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # vectorized PK 생성
    merged["_pk"] = (
        merged["sale_date"] + "|okpos|" + merged["store"] + "|" +
        merged["platform"] + "|" + merged["order_id"] + "|" + merged["item_seq"]
    ).map(lambda s: hashlib.md5(s.encode()).hexdigest())

    return merged[UNIFIED_COLUMNS]


# ============================================================
# Public API
# ============================================================

def run(date_str: str, overwrite: bool = False) -> str:
    """특정 날짜 toorder CSV → unified_sales 저장.

    overwrite=True: 기존 데이터 교체 (정정용).
    """
    try:
        raw = _load_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (toorder CSV 없음 | {date_str})"
    df = _transform_df(raw)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"
    saved  = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"unified_sales(toorder) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result


def run_okpos(date_str: str, overwrite: bool = False) -> str:
    """특정 날짜 OKPOS order+order_item → unified_sales 저장.

    overwrite=True: 기존 데이터 교체 (정정용).
    """
    try:
        order_df, item_df = _load_okpos_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (okpos CSV 없음 | {date_str})"
    df = _transform_okpos_df(order_df, item_df)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"
    saved  = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"unified_sales(okpos) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result


def run_lookback_toorder(days: int = 7) -> str:
    """최근 N일 toorder 누락분 보충 (append-only).

    기존 parquet에 없는 PK만 추가. 매일 실행해도 안전.
    """
    kst         = pendulum.timezone("Asia/Seoul")
    total_saved = 0
    for i in range(1, days + 1):
        date_str = pendulum.now(kst).subtract(days=i).format("YYYY-MM-DD")
        try:
            raw = _load_by_date(date_str)
        except FileNotFoundError:
            logger.info("toorder CSV 없음, 스킵: %s", date_str)
            continue
        df = _transform_df(raw)
        if df.empty:
            continue
        saved        = _save_unified_daily(df, date_str)
        total_saved += saved
        if saved:
            logger.info("lookback toorder %s | 신규 %d행", date_str, saved)
    result = f"unified_sales(toorder) lookback({days}일) | 신규 {total_saved}행"
    logger.info(result)
    return result


def run_lookback_okpos(days: int = 7) -> str:
    """최근 N일 okpos 누락분 보충 (append-only, 월별 캐시).

    같은 달 날짜를 반복 처리해도 월간 CSV는 1회만 로드.
    """
    kst         = pendulum.timezone("Asia/Seoul")
    total_saved = 0
    for i in range(1, days + 1):
        date_str = pendulum.now(kst).subtract(days=i).format("YYYY-MM-DD")
        try:
            order_df, item_df = _load_okpos_by_date(date_str)
        except FileNotFoundError:
            logger.info("okpos CSV 없음, 스킵: %s", date_str)
            continue
        df = _transform_okpos_df(order_df, item_df)
        if df.empty:
            continue
        saved        = _save_unified_daily(df, date_str)
        total_saved += saved
        if saved:
            logger.info("lookback okpos %s | 신규 %d행", date_str, saved)
    result = f"unified_sales(okpos) lookback({days}일) | 신규 {total_saved}행"
    logger.info(result)
    return result


def backfill() -> str:
    """전체 toorder_daily_sales CSV → unified_sales 일괄 변환 (파일 단위, OOM 방지)."""
    files = sorted(TOORDER_SALES_ROOT.glob("toorder_daily_sales_*.csv"))
    if not files:
        raise FileNotFoundError(f"toorder CSV 없음 | {TOORDER_SALES_ROOT}")

    total_saved = 0
    for csv_file in files:
        try:
            raw = pd.read_csv(csv_file, dtype=str)
            df  = _transform_df(raw)
            if df.empty:
                continue
            for sale_date, part in df.groupby("sale_date", dropna=False):
                sale_date = str(sale_date).strip()
                if not sale_date or sale_date.lower() == "nan":
                    continue
                total_saved += _save_unified_daily(part, sale_date)
            del raw, df
        except Exception as e:
            logger.warning("backfill 실패: %s | %s", csv_file, e)

    result = f"unified_sales(toorder) backfill 완료 | {total_saved}행"
    logger.info(result)
    return result


def backfill_okpos() -> str:
    """전체 OKPOS order CSV → unified_sales 일별 일괄 변환."""
    date_set: set[str] = set()
    for order_path in sorted(OKPOS_BRAND_ROOT.glob("store=*/ym=*/okpos_order.csv")):
        try:
            df = pd.read_csv(order_path, dtype=str, usecols=["sale_date"])
            date_set.update(df["sale_date"].str.strip().dropna().unique())
        except Exception as e:
            logger.warning("날짜 스캔 실패: %s | %s", order_path, e)

    total = 0
    for date_str in sorted(date_set):
        if not date_str or date_str.lower() == "nan":
            continue
        try:
            run_okpos(date_str)
            total += 1
        except Exception as e:
            logger.warning("run_okpos 실패: %s | %s", date_str, e)

    result = f"unified_sales(okpos) backfill 완료 | {total}일"
    logger.info(result)
    return result


def resave_existing_unified_sales() -> str:
    """현재 저장된 unified_sales parquet을 전부 읽어서 overwrite로 다시 저장.

    스키마/타입 정규화가 필요할 때(예: 컬럼 추가/정렬 변경) 안전하게 재저장하는 용도.
    """
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 없음 | {UNIFIED_ROOT}")

    total_files = 0
    total_rows = 0
    total_targets = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            continue

        # 저장 기준 날짜 우선순위:
        # 1) collected_at 날짜(수집 원데이트) — 요구사항
        # 2) 파일명 YYMMDD
        # 3) sale_date 단일값
        target_dates: list[str] = []

        if "collected_at" in df.columns:
            collect_dates = (
                pd.to_datetime(df["collected_at"], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .dropna()
                .unique()
                .tolist()
            )
            collect_dates = sorted({str(d).strip() for d in collect_dates if str(d).strip() and str(d).lower() != "nan"})
            if collect_dates:
                target_dates = collect_dates

        if not target_dates:
            m = re.search(r"unified_sales_(\\d{6})\\.parquet$", path.name)
            if m:
                yymmdd = m.group(1)
                yy = int(yymmdd[:2])
                mm = int(yymmdd[2:4])
                dd = int(yymmdd[4:6])
                target_dates = [f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"]
            elif "sale_date" in df.columns:
                uniq = df["sale_date"].dropna().astype(str).str.strip().unique().tolist()
                uniq = [u for u in uniq if u and u.lower() != "nan"]
                if len(uniq) == 1:
                    target_dates = [uniq[0]]

        if not target_dates:
            logger.warning("저장 기준 날짜 추출 실패, 스킵: %s", path)
            continue

        if len(target_dates) == 1:
            _save_unified_daily(df, target_dates[0], overwrite=True)
            total_targets += 1
        else:
            # 하나의 parquet에 여러 collected_at 날짜가 섞여 있으면 날짜별로 분리 저장
            parsed = pd.to_datetime(df["collected_at"], errors="coerce")
            df = df.assign(_collect_date=parsed.dt.strftime("%Y-%m-%d"))
            for collect_date, part in df.groupby("_collect_date", dropna=True):
                collect_date = str(collect_date).strip()
                if not collect_date or collect_date.lower() == "nan":
                    continue
                _save_unified_daily(part.drop(columns=["_collect_date"]), collect_date, overwrite=True)
                total_targets += 1

        total_files += 1
        total_rows += len(df)

    result = f"unified_sales 재저장 완료 | 소스파일 {total_files}개 | 저장 타겟 {total_targets}개 | 행 {total_rows}행"
    logger.info(result)
    return result


def reclassify_hall_platform(date_str: str, overwrite: bool = True) -> str:
    """unified_sales parquet의 포스/제휴사주문 행을 테이블명 기반으로 재분류."""
    path = _unified_daily_path(date_str)
    if not path.exists():
        return f"파일 없음: {path}"

    df = pd.read_parquet(path)
    if "테이블명" not in df.columns:
        return f"{date_str}: 테이블명 컬럼 없음, 스킵"

    target   = df["platform"].isin(["포스", "제휴사주문"])
    table_v  = df.loc[target, "테이블명"].fillna("").astype(str).str.strip()
    is_pack  = table_v.str.contains("포장", regex=False).reindex(df.index, fill_value=False)
    is_num   = table_v.str.fullmatch(r"\d+").fillna(False).reindex(df.index, fill_value=False)

    df.loc[target & is_pack, "order_type"] = "홀_포장"
    df.loc[target & is_pack, "platform"]   = "홀 포장"
    df.loc[target & is_num,  "order_type"] = "홀_테이블"
    df.loc[target & is_num,  "platform"]   = "홀"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"{date_str}: {saved}행 재분류 저장"
    logger.info(result)
    return result
