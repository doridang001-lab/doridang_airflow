from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_ROOT,
    _unified_daily_path,
    iter_unified_sales_files,
)
from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_JOIN_CSV_PATH,
    ORDER_CROSS_DIR,
    existing_fin_product_csv_path,
)

logger = logging.getLogger(__name__)

CROSS_ROOT = ORDER_CROSS_DIR

CROSS_COLUMNS = [
    "sale_date",
    "ym",
    "brand",
    "store",
    "order_type",
    "main_item_id",
    "main_name",
    "main_standard_menu_name",
    "main_source",
    "pair_type",
    "pair_item_id",
    "pair_name",
    "pair_standard_menu_name",
    "co_order_cnt",
    "co_qty",
    "co_amount",
    "is_multi_main",
    "updated_at",
]

PAIR_WORK_COLUMNS = [
    "sale_date",
    "ym",
    "brand",
    "store",
    "order_type",
    "main_item_id",
    "main_name",
    "main_standard_menu_name",
    "main_source",
    "pair_type",
    "pair_item_id",
    "pair_name",
    "pair_standard_menu_name",
    "order_id",
    "pair_qty",
    "pair_amount",
    "is_multi_main",
]

REQUIRED_SALES_COLUMNS = [
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "order_type",
    "order_id",
    "menu_name",
    "item_seq",
    "item_id",
    "item_name",
    "qty",
    "total_price",
    "sale_type",
]

FEE_KEYWORDS = ("배달비", "할인", "수수료", "배달팁", "포장비")
SYNTHETIC_MAIN_SOURCES = {"posfeed", "배민수동", "쿠팡수동"}
ALLOWED_CATEGORIES = {"메인", "사이드", "옵션", "토핑", "음료", "주류", "세트", "기타"}

_CATEGORY_MAP_CACHE: dict[str, dict] | None = None
_CATEGORY_MAP_CACHE_MTIME: float | None = None
_OVERLAY_CACHE: dict[str, dict] | None = None
_OVERLAY_CACHE_MTIME: float | None = None


def _read_csv_utf8(path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str).fillna("")


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _compact_text(value) -> str:
    return _normalize_text(value).replace("[홀]", "").replace(" ", "")


def _normalize_category(value) -> str:
    text = _compact_text(value)
    if not text:
        return "기타"
    if "토핑" in text:
        return "토핑"
    if "사이드" in text or "곁들임" in text:
        return "사이드"
    if "옵션" in text or "반반" in text:
        return "옵션"
    if "주류" in text:
        return "주류"
    if "음료" in text:
        return "음료"
    if "세트" in text:
        return "세트"
    if any(token in text for token in ("메인", "점심", "저녁", "단품")):
        return "메인"
    return "기타"


def _coerce_category(value) -> str:
    category = _normalize_text(value)
    if category == "1인":
        return "메인"
    if category in ALLOWED_CATEGORIES:
        return category
    return _normalize_category(category)


def _infer_category_from_name(name: str) -> str:
    text = _compact_text(name)
    if not text:
        return "기타"
    if any(token in text for token in FEE_KEYWORDS):
        return "기타"
    if any(token in text for token in ("소주", "맥주", "막걸리", "청하", "하이볼", "참이슬", "처음처럼", "새로", "카스", "테라")):
        return "주류"
    if any(token in text for token in ("콜라", "사이다", "스프라이트", "환타", "제로", "음료", "탄산", "생수")):
        return "음료"
    if any(token in text for token in ("공기밥", "주먹밥", "계란찜", "치킨무", "파김치")):
        return "사이드"
    if any(token in text for token in ("추가", "토핑", "사리", "당면", "분모자", "떡", "오뎅", "대파", "감자", "버섯", "메추리알", "계란", "순살", "대창")):
        return "토핑"
    if any(token in text for token in ("기본맛", "매운맛", "순한맛", "맛", "뼈", "괜찮습니다", "없음", "빼주세요", "많이", "적게", "중간")):
        return "옵션"
    if "세트" in text:
        return "세트"
    return "기타"


def _is_fee_like(name: str) -> bool:
    text = _compact_text(name)
    return any(token in text for token in FEE_KEYWORDS)


def _load_fin_product_category_map() -> dict[str, dict]:
    global _CATEGORY_MAP_CACHE, _CATEGORY_MAP_CACHE_MTIME
    try:
        source_path = existing_fin_product_csv_path()
        mtime = source_path.stat().st_mtime
    except FileNotFoundError:
        _CATEGORY_MAP_CACHE = {}
        _CATEGORY_MAP_CACHE_MTIME = None
        return _CATEGORY_MAP_CACHE

    if _CATEGORY_MAP_CACHE is not None and _CATEGORY_MAP_CACHE_MTIME == mtime:
        return _CATEGORY_MAP_CACHE

    try:
        df = _read_csv_utf8(source_path)
    except Exception as exc:
        logger.warning("fin_product 로드 실패: %s | %s", source_path, exc)
        _CATEGORY_MAP_CACHE = {}
        _CATEGORY_MAP_CACHE_MTIME = mtime
        return _CATEGORY_MAP_CACHE

    for col in ("source", "brand", "store", "상품코드", "상품명", "중메뉴", "is_main_candidate", "is_latest", "updated_at"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    df = df[df["상품코드"].ne("")].copy()
    if df.empty:
        _CATEGORY_MAP_CACHE = {}
        _CATEGORY_MAP_CACHE_MTIME = mtime
        return _CATEGORY_MAP_CACHE

    df["_is_latest_rank"] = df["is_latest"].eq("Y").astype(int)
    if df["updated_at"].ne("").any():
        df["_updated_at_ts"] = pd.to_datetime(df["updated_at"], errors="coerce").fillna(pd.Timestamp.min)
        df = (
            df.sort_values(["source", "brand", "store", "상품코드", "_is_latest_rank", "_updated_at_ts"])
            .groupby(["source", "brand", "store", "상품코드"], as_index=False)
            .last()
        )
    else:
        df = (
            df.sort_values(["source", "brand", "store", "상품코드", "_is_latest_rank"])
            .groupby(["source", "brand", "store", "상품코드"], as_index=False)
            .last()
        )

    result: dict[str, dict] = {}
    for row in df.to_dict("records"):
        item_id = _normalize_text(row.get("상품코드"))
        key = (
            _normalize_text(row.get("source")),
            _normalize_text(row.get("brand")),
            _normalize_text(row.get("store")),
            item_id,
        )
        is_main = _normalize_text(row.get("is_main_candidate")) == "Y"
        category = "메인" if is_main else _normalize_category(row.get("중메뉴"))
        result[key] = {
            "category": category,
            "is_main": is_main or category == "메인",
            "name": _normalize_text(row.get("상품명")),
        }

    _CATEGORY_MAP_CACHE = result
    _CATEGORY_MAP_CACHE_MTIME = mtime
    return _CATEGORY_MAP_CACHE


def _load_map_join_overlay() -> dict[str, dict]:
    global _OVERLAY_CACHE, _OVERLAY_CACHE_MTIME
    try:
        mtime = FIN_PRODUCT_MAP_JOIN_CSV_PATH.stat().st_mtime
    except FileNotFoundError:
        _OVERLAY_CACHE = {}
        _OVERLAY_CACHE_MTIME = None
        return _OVERLAY_CACHE

    if _OVERLAY_CACHE is not None and _OVERLAY_CACHE_MTIME == mtime:
        return _OVERLAY_CACHE

    try:
        df = _read_csv_utf8(FIN_PRODUCT_MAP_JOIN_CSV_PATH)
    except Exception as exc:
        logger.warning("fin_product_map_join 로드 실패: %s | %s", FIN_PRODUCT_MAP_JOIN_CSV_PATH, exc)
        _OVERLAY_CACHE = {}
        _OVERLAY_CACHE_MTIME = mtime
        return _OVERLAY_CACHE

    for col in ("item_id", "store", "source", "brand", "category", "standard_menu_name"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    result: dict[str, dict] = {}
    for row in df[df["item_id"].ne("")].to_dict("records"):
        item_id = _normalize_text(row.get("item_id"))
        key = (
            _normalize_text(row.get("source")),
            _normalize_text(row.get("brand")),
            _normalize_text(row.get("store")),
            item_id,
        )
        category = _normalize_text(row.get("category"))
        name = _normalize_text(row.get("standard_menu_name"))
        result[key] = {"category": category, "name": name}

    _OVERLAY_CACHE = result
    _OVERLAY_CACHE_MTIME = mtime
    return _OVERLAY_CACHE


def _load_map_join_scopes() -> set[tuple[str, str, str]]:
    overlay = _load_map_join_overlay()
    return {(source, brand, store) for source, brand, store, _ in overlay.keys()}


def _classify_categories(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    category_map = _load_fin_product_category_map()
    overlay = _load_map_join_overlay()
    out = df.copy()
    out["item_id"] = out["item_id"].fillna("").astype(str).str.strip()
    out["item_name"] = out["item_name"].fillna("").astype(str).str.strip()
    for col in ("source", "brand", "store"):
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()

    def classify(source: str, brand: str, store: str, item_id: str, item_name: str) -> dict:
        key = (source, brand, store, item_id)
        base = dict(category_map.get(key, {}))
        over = overlay.get(key, {})
        category = _coerce_category(over.get("category") or base.get("category") or "기타")
        standard_name = _normalize_text(over.get("name")) or _normalize_text(base.get("name"))
        name = standard_name or item_name
        if category == "기타":
            category = _infer_category_from_name(name)
        is_main = bool(base.get("is_main")) or category == "메인"
        return {
            "category": category,
            "is_main": is_main,
            "disp_name": name,
            "standard_menu_name": standard_name,
        }

    classified = [
        classify(source, brand, store, item_id, item_name)
        for source, brand, store, item_id, item_name in zip(
            out["source"],
            out["brand"],
            out["store"],
            out["item_id"],
            out["item_name"],
        )
    ]
    out["category"] = [row["category"] for row in classified]
    out["is_main"] = [row["is_main"] for row in classified]
    out["disp_name"] = [row["disp_name"] for row in classified]
    out["standard_menu_name"] = [row["standard_menu_name"] for row in classified]
    return out


def _synthetic_main_id(source: str, store: str, menu_name: str) -> str:
    key = f"{source}|{store}|{menu_name}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()[:12]
    return f"MENU::{digest}"


def _empty_pairs() -> pd.DataFrame:
    return pd.DataFrame(columns=PAIR_WORK_COLUMNS)


def _order_pairs(group: pd.DataFrame) -> list[pd.DataFrame]:
    mains = group[group["is_main"]].copy()
    synthetic = False

    if mains.empty:
        menu_names = group["menu_name"].fillna("").astype(str).str.strip()
        menu_names = menu_names[menu_names.ne("")]
        if menu_names.empty:
            return []
        first = group.iloc[0]
        main_name = menu_names.iloc[0]
        mains = pd.DataFrame(
            [
                {
                    "_row_id": "__synthetic_main__",
                    "item_id": _synthetic_main_id(first["source"], first["store"], main_name),
                    "disp_name": main_name,
                    "standard_menu_name": "",
                    "main_source": "menu_name",
                }
            ]
        )
        synthetic = True
    else:
        mains["main_source"] = "item_id"

    multi_main = mains["item_id"].nunique() >= 2
    parts: list[pd.DataFrame] = []
    min_seq = group["item_seq_num"].min()

    for _, main in mains.iterrows():
        pairs = group.copy()
        if synthetic:
            pairs = pairs[pairs["item_seq_num"].ne(min_seq)]
            pairs = pairs[pairs["disp_name"].map(_compact_text).ne(_compact_text(main["disp_name"]))]
        else:
            pairs = pairs[pairs["_row_id"].ne(main["_row_id"])]
            pairs = pairs[pairs["item_id"].ne(main["item_id"])]

        pairs = pairs[pairs["category"].ne("기타")]
        pairs = pairs[~pairs["disp_name"].map(_is_fee_like)]
        if pairs.empty:
            continue

        part = pd.DataFrame(
            {
                "sale_date": pairs["sale_date"].values,
                "ym": pairs["ym"].values,
                "brand": pairs["brand"].values,
                "store": pairs["store"].values,
                "order_type": pairs["order_type"].values,
                "main_item_id": main["item_id"],
                "main_name": main["disp_name"],
                "main_standard_menu_name": main["standard_menu_name"],
                "main_source": main["main_source"],
                "pair_type": pairs["category"].values,
                "pair_item_id": pairs["item_id"].values,
                "pair_name": pairs["disp_name"].values,
                "pair_standard_menu_name": pairs["standard_menu_name"].values,
                "order_id": pairs["order_id"].values,
                "pair_qty": pairs["qty_num"].values,
                "pair_amount": pairs["total_price_num"].values,
                "is_multi_main": multi_main,
            }
        )
        parts.append(part)
    return parts


def _build_pairs_one_day(date_str: str) -> pd.DataFrame:
    path = _unified_daily_path(date_str)
    if not path.exists():
        logger.warning("unified_sales parquet 없음: %s", path)
        return _empty_pairs()

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.warning("unified_sales parquet 로드 실패, 스킵: %s | %s", path, exc)
        return _empty_pairs()

    if df.empty:
        return _empty_pairs()

    for col in REQUIRED_SALES_COLUMNS:
        if col not in df.columns:
            logger.warning("필수 컬럼 누락(%s), 스킵: %s", col, path)
            return _empty_pairs()

    out = df.copy()
    for col in (
        "sale_date",
        "ym",
        "source",
        "brand",
        "store",
        "order_type",
        "order_id",
        "menu_name",
        "item_seq",
        "item_id",
        "item_name",
        "sale_type",
    ):
        out[col] = out[col].fillna("").astype(str).str.strip()

    out = out[out["sale_type"].ne("취소")].copy()
    out = out[out["order_id"].ne("") & out["item_id"].ne("")].copy()
    if out.empty:
        return _empty_pairs()

    out["qty_num"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["total_price_num"] = pd.to_numeric(out["total_price"], errors="coerce").fillna(0)
    out = out[out["total_price_num"].ge(0)].copy()
    if out.empty:
        return _empty_pairs()

    scopes = _load_map_join_scopes()
    if scopes:
        scope_key = list(zip(out["source"], out["brand"], out["store"]))
        out = out[[key in scopes for key in scope_key]].copy()
        if out.empty:
            return _empty_pairs()

    out = _classify_categories(out)
    out = out[~out["disp_name"].map(_is_fee_like)].copy()
    out["_row_id"] = out.index.astype(str)
    out["item_seq_num"] = pd.to_numeric(out["item_seq"], errors="coerce")
    out["item_seq_num"] = out["item_seq_num"].fillna(out.groupby(["source", "brand", "store", "order_id"]).cumcount() + 1)

    parts: list[pd.DataFrame] = []
    group_cols = ["source", "brand", "store", "order_id"]
    for _, group in out.groupby(group_cols, dropna=False):
        parts.extend(_order_pairs(group))

    if not parts:
        return _empty_pairs()
    return pd.concat(parts, ignore_index=True).reindex(columns=PAIR_WORK_COLUMNS)


def _aggregate_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs is None or pairs.empty:
        return pd.DataFrame(columns=CROSS_COLUMNS)

    key_cols = [
        "sale_date",
        "ym",
        "brand",
        "store",
        "order_type",
        "main_item_id",
        "main_name",
        "main_standard_menu_name",
        "main_source",
        "pair_type",
        "pair_item_id",
        "pair_name",
        "pair_standard_menu_name",
    ]
    agg = (
        pairs.groupby(key_cols, dropna=False)
        .agg(
            co_order_cnt=("order_id", "nunique"),
            co_qty=("pair_qty", "sum"),
            co_amount=("pair_amount", "sum"),
            is_multi_main=("is_multi_main", "any"),
        )
        .reset_index()
    )
    agg["co_order_cnt"] = pd.to_numeric(agg["co_order_cnt"], errors="coerce").fillna(0).astype(int)
    agg["co_qty"] = pd.to_numeric(agg["co_qty"], errors="coerce").fillna(0).round().astype(int)
    agg["co_amount"] = pd.to_numeric(agg["co_amount"], errors="coerce").fillna(0).round().astype(int)
    agg["is_multi_main"] = agg["is_multi_main"].astype(bool)
    agg["updated_at"] = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    return agg.reindex(columns=CROSS_COLUMNS).sort_values(
        ["sale_date", "brand", "store", "order_type", "main_name", "co_order_cnt", "pair_type", "pair_name"],
        ascending=[True, True, True, True, True, False, True, True],
    ).reset_index(drop=True)


def _cross_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return CROSS_ROOT / f"order_cross_{ymd}.parquet"


def _save_cross_daily(df: pd.DataFrame, date_str: str) -> int:
    CROSS_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _cross_daily_path(date_str)
    out = (df if df is not None else pd.DataFrame()).reindex(columns=CROSS_COLUMNS)
    out.to_parquet(daily_path, index=False, engine="pyarrow")
    logger.info("order_cross 저장 완료: %s | rows=%d", daily_path, len(out))
    return len(out)


def _process_order_cross(date_str: str, overwrite: bool = True) -> dict:
    pairs = _build_pairs_one_day(date_str)
    agg = _aggregate_pairs(pairs)
    rows = _save_cross_daily(agg, date_str)
    return {
        "date": date_str,
        "rows": rows,
        "pair_rows": len(pairs),
        "path": str(_cross_daily_path(date_str)),
        "overwrite": overwrite,
    }


def run_order_cross_analysis(date_str: str, overwrite: bool = True) -> str:
    result = _process_order_cross(date_str, overwrite=overwrite)
    msg = f"order_cross overwrite | {date_str} | rows={result['rows']} pair_rows={result['pair_rows']}"
    logger.info(msg)
    return msg


def run_lookback_order_cross_analysis(days: int = 3) -> str:
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    total = 0
    for i in range(1, days + 1):
        date_str = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        total += _process_order_cross(date_str, overwrite=True)["rows"]
    msg = f"order_cross lookback({days}d) | overwrite rows={total}"
    logger.info(msg)
    return msg


def backfill_order_cross_analysis() -> str:
    files = iter_unified_sales_files()
    if not files:
        msg = f"order_cross backfill 스킵 (unified_sales parquet 없음) | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    total = 0
    dates = 0
    for path in files:
        try:
            ymd = path.name.split("unified_sales_", 1)[1].split(".", 1)[0]
            date_str = f"{2000 + int(ymd[:2])}-{ymd[2:4]}-{ymd[4:6]}"
        except Exception:
            logger.warning("unified_sales 파일명 파싱 실패, 스킵: %s", path)
            continue
        total += _process_order_cross(date_str, overwrite=True)["rows"]
        dates += 1

    msg = f"order_cross backfill 완료 | dates={dates} rows={total}"
    logger.info(msg)
    return msg


def validate_order_cross(date_str: str) -> str:
    path = _cross_daily_path(date_str)
    if not path.exists():
        raise FileNotFoundError(f"order_cross 결과 없음: {path}")
    df = pd.read_parquet(path)
    missing = [col for col in CROSS_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"order_cross 컬럼 누락: {missing}")
    key_cols = ["sale_date", "brand", "store", "order_type", "main_item_id", "pair_type", "pair_item_id"]
    dup_cnt = int(df.duplicated(key_cols, keep=False).sum()) if not df.empty else 0
    if dup_cnt:
        raise ValueError(f"order_cross unique key 중복: {dup_cnt} rows")
    msg = f"order_cross validate | {date_str} | rows={len(df)}"
    logger.info(msg)
    return msg


def backup_order_cross_to_onedrive(date_str: str | None = None) -> str:
    # 산출 루트가 이미 MART_DB(OneDrive)라 별도 백업은 DAG 승인 게이트의 명시 로그만 남긴다.
    if date_str:
        path = _cross_daily_path(date_str)
        return f"order_cross onedrive backup 스킵(이미 MART_DB 저장): {path}"
    return f"order_cross onedrive backup 스킵(이미 MART_DB 저장): {CROSS_ROOT}"
