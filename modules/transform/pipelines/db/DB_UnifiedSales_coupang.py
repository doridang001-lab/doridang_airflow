"""
쿠팡이츠 직수집(coupang_macro/orders) -> unified_sales 교정 파이프라인.

TEST_STORES에 한해 쿠팡이츠 행을 제거하고 직수집 데이터로 대체한다.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import timedelta
from glob import glob

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _to_int_series,
    _unified_daily_path,
)
from modules.transform.utility.paths import ANALYTICS_DB

logger = logging.getLogger(__name__)

COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"
COUPANG_REPLACED_PLATFORMS = {COUPANG_PLATFORM, "쿠팡 포장"}

_DEDUP_COLUMNS = [
    "store_id",
    "order_id",
    "order_date",
    "menu_name",
    "menu_qty",
    "menu_price",
    "menu_options",
    "total_price",
    "is_cancelled",
]


def reconcile_coupang_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 행을 coupang_macro 직수집 기준으로 교정."""
    dates = _resolve_coupang_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 교정 스킵 | 대상 날짜 없음"
    ym_list = sorted({date[:7] for date in dates})

    total_added = 0
    total_removed = 0
    store_map = _load_store_map()

    for store in stores:
        for ym in ym_list:
            coupang_files = _find_coupang_files(store, ym)
            if not coupang_files:
                logger.warning("쿠팡 수집 파일 없음, 기존 쿠팡이츠 행 유지: store=%s ym=%s", store, ym)
                continue

            opt_map = _find_options_map(store, ym)
            df_raw = _deduplicate_raw_orders(_read_coupang_files(coupang_files))
            if df_raw.empty:
                logger.warning("쿠팡 수집 데이터 없음, 기존 쿠팡이츠 행 유지: store=%s ym=%s", store, ym)
                continue

            _assert_required_columns(df_raw, store, ym)
            brand = _parse_brand_from_path(coupang_files[0])
            parsed = df_raw["order_date"].map(_parse_coupang_datetime)
            df_raw = df_raw.copy()
            df_raw["sale_date"] = parsed.map(lambda value: value[0])
            df_raw["order_time"] = parsed.map(lambda value: value[1])

            for date in dates:
                if date[:7] != ym:
                    continue
                df_day = df_raw[df_raw["sale_date"] == date].copy()
                if df_day.empty:
                    logger.info("쿠팡 데이터 없음, 기존 쿠팡이츠 행 유지: store=%s date=%s", store, date)
                    continue

                df_unified = _transform_to_unified(df_day, store, brand, store_map, opt_map)
                df_unified = _recalculate_order_fields(df_unified)
                removed, added = _upsert_daily(df_unified, date, store)
                total_removed += removed
                total_added += added
                logger.info("쿠팡 교정 완료: store=%s date=%s | 제거=%d 추가=%d", store, date, removed, added)

    return f"쿠팡수동 교정 완료 | 제거={total_removed}행 추가={total_added}행"


def enforce_coupang_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 최종 방어막."""
    if not stores:
        return "TEST_STORES 없음 - 쿠팡수동 최종 정리 스킵"

    dates = _resolve_coupang_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 최종 정리 스킵 | 대상 날짜 없음"

    store_set = {str(store).strip() for store in stores if str(store).strip()}
    total_removed = 0
    changed_files = 0

    for date in dates:
        daily_path = _unified_daily_path(date)
        if not daily_path.exists():
            logger.info("쿠팡수동 최종 정리 스킵, 파일 없음: %s", daily_path)
            continue

        df = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        remove_mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().isin(COUPANG_REPLACED_PLATFORMS)
            & ~df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE)
        )
        removed = int(remove_mask.sum())
        if removed == 0:
            logger.info("쿠팡수동 최종 정리 변경 없음: %s", daily_path.name)
            continue

        df_out = df[~remove_mask].reset_index(drop=True)
        _write_unified_daily(df_out, daily_path)
        total_removed += removed
        changed_files += 1
        logger.warning(
            "쿠팡수동 최종 정리: %s | 제거=%d | stores=%s",
            daily_path.name,
            removed,
            sorted(df.loc[remove_mask, "store"].dropna().astype(str).unique().tolist()),
        )

    return f"쿠팡수동 최종 정리 완료 | 파일={changed_files} 제거={total_removed}행"


def _resolve_coupang_target_dates(
    stores: list[str],
    sale_date: str | None,
    lookback_days: int | None,
) -> list[str]:
    """교정 대상 날짜 결정.

    - sale_date 지정: 단일 날짜
    - lookback_days 정수: 최근 N일
    - lookback_days None: 쿠팡수동 원천과 기존 unified 쿠팡수동/중복 날짜 전체
    """
    if sale_date:
        return [str(sale_date)]

    if lookback_days is not None:
        kst_now = pendulum.now("Asia/Seoul")
        return [
            (kst_now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(lookback_days)
        ]

    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_coupang_source_dates(store))
    dates.update(_collect_existing_unified_coupang_manual_dates(stores))
    dates.update(_collect_existing_coupang_duplicate_dates(stores))
    return sorted(d for d in dates if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d))


def _find_all_coupang_files(store: str) -> list[str]:
    base = ANALYTICS_DB / "coupang_macro" / "orders"
    pattern = str(base / "brand=*" / f"store={store}" / "ym=*" / "orders_*.parquet")
    return sorted(glob(pattern))


def _collect_coupang_source_dates(store: str) -> set[str]:
    dates: set[str] = set()
    for file_path in _find_all_coupang_files(store):
        try:
            df = pd.read_parquet(file_path, columns=["order_date"])
        except Exception as exc:
            logger.warning("쿠팡 날짜 수집 실패: %s | %s", file_path, exc)
            continue
        parsed = df["order_date"].map(_parse_coupang_datetime)
        dates.update(date for date, _ in parsed if date)
    return dates


def _collect_existing_unified_coupang_manual_dates(stores: list[str]) -> set[str]:
    store_set = {str(store).strip() for store in stores if str(store).strip()}
    if not store_set or not UNIFIED_ROOT.exists():
        return set()

    dates: set[str] = set()
    for path in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "source"])
        except Exception as exc:
            logger.warning("unified 쿠팡수동 날짜 수집 실패: %s | %s", path, exc)
            continue

        mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().isin(COUPANG_REPLACED_PLATFORMS)
            & df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE)
        )
        if mask.any():
            dates.update(df.loc[mask, "sale_date"].fillna("").astype(str).str.strip().tolist())
    return dates


def _collect_existing_coupang_duplicate_dates(stores: list[str]) -> set[str]:
    store_set = {str(store).strip() for store in stores if str(store).strip()}
    if not store_set or not UNIFIED_ROOT.exists():
        return set()

    dates: set[str] = set()
    for path in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "source"])
        except Exception as exc:
            logger.warning("unified 쿠팡 중복 날짜 수집 실패: %s | %s", path, exc)
            continue

        df = df.copy()
        df["sale_date"] = df["sale_date"].fillna("").astype(str).str.strip()
        df["store"] = df["store"].fillna("").astype(str).str.strip()
        df["platform"] = df["platform"].fillna("").astype(str).str.strip()
        df["source"] = df["source"].fillna("").astype(str).str.strip()
        sub = df[
            df["store"].isin(store_set)
            & df["platform"].isin(COUPANG_REPLACED_PLATFORMS)
        ]
        if sub.empty:
            continue

        source_sets = sub.groupby(["sale_date", "store", "platform"])["source"].agg(set)
        for (sale_date, _, _), sources in source_sets.items():
            if COUPANG_SOURCE in sources and any(source != COUPANG_SOURCE for source in sources):
                dates.add(str(sale_date).strip())
    return dates


def _find_coupang_files(store: str, ym: str) -> list[str]:
    base = ANALYTICS_DB / "coupang_macro" / "orders"
    pattern = str(base / "brand=*" / f"store={store}" / f"ym={ym}" / f"orders_{ym}.parquet")
    return sorted(glob(pattern))


def _find_options_map(store: str, ym: str) -> dict[str, int]:
    base = ANALYTICS_DB / "coupang_macro" / "options"
    pattern = str(base / "brand=*" / f"store={store}" / f"ym={ym}" / "options.csv")
    files = sorted(glob(pattern))
    if not files:
        logger.warning("쿠팡 options.csv 없음: store=%s ym=%s", store, ym)
        return {}

    try:
        df = pd.read_csv(files[0], dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("options.csv 로드 실패: %s | %s", files[0], exc)
        return {}

    if not {"옵션명", "옵션가격"}.issubset(df.columns):
        logger.warning("options.csv 컬럼 누락: %s", files[0])
        return {}

    df["option_price"] = pd.to_numeric(df["옵션가격"], errors="coerce").fillna(0).astype(int)
    price_counts = df.groupby("옵션명")["option_price"].nunique()
    ambiguous = set(price_counts[price_counts > 1].index.astype(str))
    if ambiguous:
        logger.warning("가격 충돌 옵션은 0원 처리: %s", sorted(ambiguous)[:20])

    result: dict[str, int] = {}
    for _, row in df.iterrows():
        name = str(row["옵션명"]).strip()
        if not name:
            continue
        result[name] = 0 if name in ambiguous else int(row["option_price"])
    return result


def _read_coupang_files(files: list[str]) -> pd.DataFrame:
    dfs = []
    for file_path in files:
        try:
            df = pd.read_parquet(file_path)
            df["_src_path"] = file_path
            dfs.append(df)
        except Exception as exc:
            logger.warning("쿠팡 파일 읽기 실패: %s | %s", file_path, exc)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _canonical_order_keys(df: pd.DataFrame) -> pd.DataFrame:
    keys = pd.DataFrame(index=df.index)
    numeric_cols = {"menu_qty", "menu_price", "total_price", "매출액", "취소금액"}
    for col in _DEDUP_COLUMNS:
        if col not in df.columns:
            keys[col] = ""
            continue
        if col in numeric_cols:
            values = pd.to_numeric(df[col], errors="coerce")
            keys[col] = values.round(6).astype("string").fillna("")
        else:
            keys[col] = df[col].fillna("").astype(str).str.strip()
    return keys


def _deduplicate_raw_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    work = df.copy()
    if "collected_at" in work.columns:
        work["_collected_at_sort"] = pd.to_datetime(work["collected_at"], errors="coerce", utc=True)
        work = work.sort_values("_collected_at_sort", ascending=False, na_position="last", kind="mergesort")

    keys = _canonical_order_keys(work)
    before = len(work)
    work = work.loc[~keys.duplicated(keep="first")].drop(columns=["_collected_at_sort"], errors="ignore")
    removed = before - len(work)
    if removed:
        logger.warning("쿠팡 원천 중복 방어 제거: %d행", removed)
    return work.reset_index(drop=True)


def _assert_required_columns(df: pd.DataFrame, store: str, ym: str) -> None:
    required = {"order_date", "order_id", "menu_name", "menu_qty", "menu_price", "menu_options", "total_price"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"쿠팡 수집 컬럼 누락: store={store} ym={ym} missing={missing}")


def _parse_brand_from_path(file_path: str) -> str:
    match = re.search(r"brand=([^/\\]+)", file_path)
    return match.group(1) if match else ""


def _parse_coupang_datetime(value) -> tuple[str, str]:
    if not isinstance(value, str):
        return "", ""
    match = re.match(r"\s*(\d{4})\.(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})", str(value).strip())
    if not match:
        return "", ""
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    hour, minute = int(match.group(4)), int(match.group(5))
    return f"{year:04d}-{month:02d}-{day:02d}", f"{hour:02d}:{minute:02d}:00"


def _order_amount(order_df: pd.DataFrame) -> int:
    sales = pd.to_numeric(order_df.get("매출액", pd.Series(index=order_df.index)), errors="coerce").fillna(0)
    positive_sales = sales[sales > 0]
    if not positive_sales.empty:
        return int(round(float(positive_sales.iloc[0])))

    totals = pd.to_numeric(order_df.get("total_price", pd.Series(index=order_df.index)), errors="coerce").fillna(0)
    positive_totals = totals[totals > 0]
    if not positive_totals.empty:
        return int(round(float(positive_totals.max())))
    return 0


def _is_cancelled_order(order_df: pd.DataFrame) -> bool:
    if "is_cancelled" not in order_df.columns:
        return False
    return order_df["is_cancelled"].fillna("").astype(str).str.strip().str.upper().eq("Y").any()


def _group_ids(order_df: pd.DataFrame) -> pd.Series:
    prices = pd.to_numeric(order_df["menu_price"], errors="coerce")
    group_ids = []
    current = -1
    for has_price in prices.notna().tolist():
        if has_price or current < 0:
            current += 1
        group_ids.append(current)
    return pd.Series(group_ids, index=order_df.index)


def _calc_order_total_price(order_df: pd.DataFrame, amount: int, opt_map: dict[str, int]) -> pd.Series:
    result = pd.Series(0, index=order_df.index, dtype=int)
    if order_df.empty or amount <= 0:
        return result

    group_ids = _group_ids(order_df)
    groups = [(gid, order_df.loc[group_ids == gid]) for gid in group_ids.drop_duplicates().tolist()]
    weights: list[float] = []
    main_indexes: list[int] = []
    for _, group in groups:
        prices = pd.to_numeric(group["menu_price"], errors="coerce")
        main_idx = prices[prices.notna()].index[0] if prices.notna().any() else group.index[0]
        qty = pd.to_numeric(group.loc[main_idx, "menu_qty"], errors="coerce")
        qty_value = float(qty) if pd.notna(qty) and float(qty) > 0 else 1.0
        price_value = float(prices.loc[main_idx]) if pd.notna(prices.loc[main_idx]) else 0.0
        weights.append(price_value * qty_value)
        main_indexes.append(main_idx)

    total_weight = sum(weights)
    remaining_amount = int(amount)
    for pos, ((_, group), weight, main_idx) in enumerate(zip(groups, weights, main_indexes)):
        if pos < len(groups) - 1 and total_weight > 0:
            group_amount = int(round(amount * weight / total_weight))
        elif total_weight <= 0 and pos < len(groups) - 1:
            group_amount = 0
        else:
            group_amount = remaining_amount
        remaining_amount -= group_amount

        opt_rows = [idx for idx in group.index if idx != main_idx]
        opt_prices = pd.Series(
            {
                idx: opt_map.get(str(group.loc[idx, "menu_options"]).strip(), 0)
                for idx in opt_rows
            },
            dtype=int,
        )
        paid_opts = opt_prices[opt_prices > 0]
        opt_alloc = pd.Series(0, index=group.index, dtype=int)
        if not paid_opts.empty:
            opt_remaining = group_amount
            opt_total = int(paid_opts.sum())
            for opt_pos, idx in enumerate(paid_opts.index):
                if opt_pos < len(paid_opts) - 1 and opt_total > 0:
                    value = int(round(group_amount * int(paid_opts.loc[idx]) / opt_total))
                else:
                    value = opt_remaining
                opt_alloc.loc[idx] = value
                opt_remaining -= value
        opt_alloc.loc[main_idx] = group_amount - int(opt_alloc.sum())
        result.loc[group.index] = opt_alloc.loc[group.index].astype(int)

    diff = int(amount) - int(result.sum())
    if diff and len(result) > 0:
        result.loc[main_indexes[-1]] = int(result.loc[main_indexes[-1]]) + diff
    return result.astype(int)


def _transform_to_unified(
    df: pd.DataFrame,
    store: str,
    brand: str,
    store_map: dict,
    opt_map: dict[str, int],
) -> pd.DataFrame:
    active_parts = []
    amount_by_order: dict[str, int] = {}
    for order_id, group in df.groupby("order_id", sort=False):
        amount = _order_amount(group)
        if _is_cancelled_order(group) or amount <= 0:
            continue
        active_parts.append(group)
        amount_by_order[str(order_id).strip()] = amount

    if not active_parts:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    df = pd.concat(active_parts, ignore_index=False).copy()
    out = pd.DataFrame(index=df.index)
    out["sale_date"] = df["sale_date"]
    out["ym"] = df["sale_date"].str[:7]
    out["source"] = COUPANG_SOURCE
    out["brand"] = brand
    out["store"] = store
    out["platform"] = COUPANG_PLATFORM
    out["order_id"] = df["order_id"].fillna("").astype(str).str.strip()
    out["order_time"] = df["order_time"]
    out["sale_type"] = "정상"
    out["order_type"] = df["delivery_type"].map(lambda value: "배달_포장" if str(value).strip() == "포장" else "배달") if "delivery_type" in df.columns else "배달"
    out["menu_name"] = df["menu_name"].fillna("").astype(str).str.strip()
    out["item_name"] = df["menu_options"].fillna("").astype(str).str.strip()
    out["qty"] = _to_int_series(df["menu_qty"]).replace(0, 1)
    out["discount_amount"] = 0

    total_price = pd.Series(0, index=df.index, dtype=int)
    for order_id, group in df.groupby("order_id", sort=False):
        key = str(order_id).strip()
        total_price.loc[group.index] = _calc_order_total_price(group, amount_by_order.get(key, 0), opt_map)
    out["total_price"] = total_price
    out["unit_price"] = out["total_price"]

    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    out["item_id"] = out["item_name"].map(lambda value: hashlib.md5(value.encode()).hexdigest() if value else "")
    out["담당자"] = _lookup_store_meta(store_map, store, "담당자")
    out["region"] = _lookup_store_meta(store_map, store, "region")
    out["실오픈일"] = _lookup_store_meta(store_map, store, "실오픈일")
    out["collected_at"] = pendulum.now("Asia/Seoul").isoformat()
    return out.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def _recalculate_order_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["order_cnt"] = 0
    representative_indexes = []
    for _, group in df.groupby("order_id", sort=False):
        menu_match = group["item_name"].fillna("").astype(str).str.strip().eq(
            group["menu_name"].fillna("").astype(str).str.strip()
        )
        if menu_match.any():
            representative_indexes.append(group[menu_match].index[0])
            continue
        unit_price = pd.to_numeric(group["unit_price"], errors="coerce").fillna(0)
        representative_indexes.append(unit_price.idxmax())

    if representative_indexes:
        df.loc[representative_indexes, "order_cnt"] = df.loc[representative_indexes, "sale_type"].map(
            lambda value: 0 if str(value) == "취소" else 1
        )
    df["_pk"] = _make_unified_pk(df)
    return df


def _upsert_daily(df_new: pd.DataFrame, date: str, store: str) -> tuple[int, int]:
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date)

    if daily_path.exists():
        df_existing = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        remove_mask = (
            df_existing["store"].fillna("").astype(str).str.strip().eq(store)
            & df_existing["platform"].fillna("").astype(str).str.strip().isin(COUPANG_REPLACED_PLATFORMS)
        )
        removed_count = int(remove_mask.sum())
        df_existing = df_existing[~remove_mask]
    else:
        df_existing = pd.DataFrame(columns=UNIFIED_COLUMNS)
        removed_count = 0

    df_out = pd.concat([df_existing, df_new], ignore_index=True)
    if "_pk" in df_out.columns:
        before = len(df_out)
        df_out = df_out.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        dropped = before - len(df_out)
        if dropped:
            logger.warning("쿠팡 교정 저장 중 _pk 중복 %d행 제거", dropped)

    _write_unified_daily(df_out, daily_path)
    return removed_count, len(df_new)


def _write_unified_daily(df: pd.DataFrame, daily_path) -> None:
    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df.to_parquet(daily_path, index=False, engine="pyarrow")
