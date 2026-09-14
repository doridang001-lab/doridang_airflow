"""쿠팡 macro orders -> unified_sales 교정 파이프라인."""

from __future__ import annotations

from modules.transform.utility.process_lock import unified_writer
import logging
import re
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from modules.transform.utility.paths import COUPANG_ORDERS_DB, COUPANG_ORDERS_DETAIL_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES,
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    clear_manual_fallback_marker,
    clear_manual_partial_marker,
    clear_manual_reingest_marker,
    detect_manual_partial_collection,
    delivery_baseline_summary,
    fill_missing_manual_item_name,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _unified_daily_path,
    iter_unified_sales_files,
    list_manual_reingest_dates,
    notify_manual_fallback,
    notify_manual_missing_all,
    notify_manual_partial,
    record_manual_fallback_marker,
    record_manual_partial_marker,
    save_unified_parquet,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import allocate_manual_item_ids

logger = logging.getLogger(__name__)

COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"
COUPANG_PLATFORMS = DELIVERY_PLATFORM_FAMILIES[COUPANG_SOURCE]
COUPANG_OPTIONS_DB = COUPANG_ORDERS_DETAIL_DB / "options"
POSFEED_ORDERS_DB = COUPANG_ORDERS_DETAIL_DB.parent / "posfeed_sales"
POSFEED_DETAIL_DB = COUPANG_ORDERS_DETAIL_DB.parent / "posfeed_sales_detail"
_RAW_DEDUP_NUMERIC_COLS = {"total_price", "menu_qty", "menu_price"}
_ITEM_NAME_OVERRIDE_COL = "_coupang_item_name_override"
_OBSERVED_PRICE_CATALOG: dict[tuple[str, str, str], int] | None = None
_SCOPED_PRICE_CATALOG: dict[tuple[str, str], dict[str, int]] = {}


def reconcile_coupang_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
    include_reingest_markers: bool = False,
) -> str:
    """TEST_STORES의 쿠팡이츠 행을 coupang_macro 직수집 기준으로 교정."""
    base_dates = _resolve_target_dates(stores, sale_date, lookback_days)

    total_removed = 0
    total_added = 0
    processed_dates = 0
    fallback_events: list[dict] = []
    partial_events: list[dict] = []
    missing_events: list[dict] = []
    store_map = _load_store_map()

    for store in stores:
        dates = _target_dates_for_store(
            store,
            base_dates,
            sale_date,
            lookback_days,
            include_reingest_markers=include_reingest_markers,
        )
        if not dates:
            continue
        processed_dates += len(dates)
        for ym in sorted({d[:7] for d in dates}):
            frames = [
                frame
                for frame in (_read_coupang_month(store, ym), _read_coupang_month(store, _next_ym(ym)))
                if not frame.empty
            ]
            raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if raw.empty:
                for date in dates:
                    if date[:7] != ym:
                        continue
                    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
                    clear_manual_reingest_marker(COUPANG_SOURCE, store, date)
                    _record_coupang_fallback_event(date, store, fallback_events, missing_events)
                    total_removed += removed
                    total_added += added
                continue

            raw = raw.copy()
            raw["sale_date"] = raw["order_date"].map(_parse_order_date)
            raw["order_time"] = raw["order_date"].map(_parse_order_time)
            raw = _deduplicate_raw(raw, store, ym)

            for date in dates:
                if date[:7] != ym:
                    continue
                day = raw[raw["sale_date"] == date].copy()
                if day.empty:
                    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
                    clear_manual_reingest_marker(COUPANG_SOURCE, store, date)
                    _record_coupang_fallback_event(date, store, fallback_events, missing_events)
                    total_removed += removed
                    total_added += added
                    continue

                frames = []
                for brand, brand_df in day.groupby("_source_brand", sort=False):
                    frames.append(_transform_to_unified(brand_df, store, str(brand).strip(), store_map))
                new_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=UNIFIED_COLUMNS)

                if not new_df.empty:
                    manual_total = int(
                        pd.to_numeric(new_df["total_price"], errors="coerce")
                        .fillna(0)
                        .sum()
                    )
                    partial = detect_manual_partial_collection(
                        date,
                        store,
                        COUPANG_PLATFORMS,
                        COUPANG_SOURCE,
                        manual_total,
                    )
                    if partial:
                        partial["platform"] = COUPANG_PLATFORM
                        if record_manual_partial_marker(
                            COUPANG_SOURCE,
                            store,
                            date,
                            partial,
                        ):
                            partial_events.append(partial)
                        logger.warning(
                            "쿠팡수동 부분수집 의심: store=%s date=%s 수동=%d %s=%d 부족=%d",
                            store,
                            date,
                            partial["manual_total"],
                            partial.get("baseline_label") or "POS",
                            partial.get("baseline_total") or partial["pos_total"],
                            partial["gap"],
                        )
                    else:
                        clear_manual_partial_marker(COUPANG_SOURCE, store, date)

                removed, added = _upsert_daily(new_df, date, store)
                clear_manual_reingest_marker(COUPANG_SOURCE, store, date)
                if new_df.empty:
                    _record_coupang_fallback_event(
                        date,
                        store,
                        fallback_events,
                        missing_events,
                    )
                else:
                    clear_manual_fallback_marker(COUPANG_SOURCE, store, date)
                total_removed += removed
                total_added += added

    if processed_dates == 0:
        return "쿠팡수동 교정 스킵 | 대상 날짜 없음"
    notify_manual_fallback("쿠팡수동", fallback_events)
    notify_manual_partial("쿠팡수동", partial_events)
    notify_manual_missing_all("쿠팡수동", missing_events)
    return (
        f"쿠팡수동 교정 완료 | 제거={total_removed}행 추가={total_added}행 "
        f"폴백={len(fallback_events)}건 부분수집={len(partial_events)}건 "
        f"무데이터={len(missing_events)}건"
    )


def _record_coupang_fallback_event(
    date: str,
    store: str,
    events: list[dict],
    missing: list[dict] | None = None,
) -> None:
    clear_manual_partial_marker(COUPANG_SOURCE, store, date)
    baseline = delivery_baseline_summary(date, store, COUPANG_PLATFORMS, COUPANG_SOURCE)
    amount = baseline["baseline_total"]
    order_cnt = baseline["baseline_order_cnt"]
    rows = baseline["baseline_rows"]
    if rows <= 0:
        event = {
            "date": date,
            "store": store,
            "platform": COUPANG_PLATFORM,
            "rows": 0,
            "alert_suppressed": True,
            "reason": "no_delivery_baseline",
            **baseline,
        }
        record_manual_fallback_marker(COUPANG_SOURCE, store, date, event)
        return
    event = {
        "date": date,
        "store": store,
        "platform": COUPANG_PLATFORM,
        "total_price": amount,
        "order_cnt": order_cnt,
        "rows": rows,
        **baseline,
    }
    if record_manual_fallback_marker(COUPANG_SOURCE, store, date, event):
        events.append(event)


def enforce_coupang_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠는 쿠팡수동만 남긴다."""
    dates = _resolve_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 최종 정리 스킵 | 대상 날짜 없음"

    store_set = {str(store).strip() for store in stores if str(store).strip()}
    total_removed = 0
    changed = 0
    for date in dates:
        path = _unified_daily_path(date)
        if not path.exists():
            continue
        df = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        store_s = df["store"].fillna("").astype(str).str.strip()
        platform_s = df["platform"].fillna("").astype(str).str.strip()
        source_s = df["source"].fillna("").astype(str).str.strip()
        date_s = df["sale_date"].fillna("").astype(str).str.strip()
        in_family = store_s.isin(store_set) & platform_s.isin(COUPANG_PLATFORMS)
        is_manual = source_s.eq(COUPANG_SOURCE)
        manual_keys = set(zip(store_s[in_family & is_manual], date_s[in_family & is_manual]))
        row_keys = pd.Series(list(zip(store_s, date_s)), index=df.index)
        has_manual = row_keys.isin(manual_keys)
        remove_mask = in_family & ~is_manual & has_manual
        removed = int(remove_mask.sum())
        if not removed:
            for store in store_set:
                clear_manual_reingest_marker(COUPANG_SOURCE, store, date)
            continue
        out = df[~remove_mask].reset_index(drop=True)
        _write_daily(path, out)
        total_removed += removed
        changed += 1
        for store in store_set:
            clear_manual_reingest_marker(COUPANG_SOURCE, store, date)

    return f"쿠팡수동 최종 정리 완료 | 파일={changed} 제거={total_removed}행"


def _resolve_target_dates(
    stores: list[str],
    sale_date: str | None,
    lookback_days: int | None,
) -> list[str]:
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")

    if sale_date:
        return [] if str(sale_date) == today else [str(sale_date)]
    if lookback_days is not None:
        now = pendulum.now("Asia/Seoul")
        dates = {
            (now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, lookback_days + 1)
        }
        return sorted(
            d for d in dates
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and d != today
        )

    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_source_dates(store))
    dates.update(_collect_existing_dates(stores))
    return sorted(
        d for d in dates
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and d != today
    )


def _target_dates_for_store(
    store: str,
    base_dates: list[str],
    sale_date: str | None,
    lookback_days: int | None,
    *,
    include_reingest_markers: bool = False,
) -> list[str]:
    """기본 날짜에 해당 매장의 재수집 마커만 더한다."""
    if lookback_days is None and not sale_date:
        return _resolve_target_dates([store], sale_date, lookback_days)
    dates = set(base_dates)
    if include_reingest_markers and not sale_date:
        dates.update(list_manual_reingest_dates(COUPANG_SOURCE, [store]))
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")
    return sorted(
        d for d in dates
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and d != today
    )


def _collect_source_dates(store: str) -> set[str]:
    dates: set[str] = set()
    for path in COUPANG_ORDERS_DB.glob(f"brand=*/store={store}/ym=*/orders_*.parquet"):
        try:
            df = pd.read_parquet(path, columns=["order_date"])
        except Exception as exc:
            logger.warning("쿠팡 날짜 수집 실패: %s | %s", path, exc)
            continue
        dates.update(d for d in df["order_date"].map(_parse_order_date) if d)
    return dates


def _collect_existing_dates(stores: list[str]) -> set[str]:
    store_set = {str(store).strip() for store in stores if str(store).strip()}
    dates: set[str] = set()
    for path in iter_unified_sales_files():
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "source"])
        except Exception as exc:
            logger.warning("unified 쿠팡수동 날짜 수집 실패: %s | %s", path, exc)
            continue
        mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().isin(COUPANG_PLATFORMS)
        )
        if mask.any():
            dates.update(df.loc[mask, "sale_date"].fillna("").astype(str).str.strip().tolist())
    return dates


def _next_ym(ym: str) -> str:
    y, m = map(int, ym.split("-"))
    return f"{y + 1}-01" if m == 12 else f"{y}-{m + 1:02d}"


def _read_coupang_month(store: str, ym: str) -> pd.DataFrame:
    frames = []
    for path in sorted(COUPANG_ORDERS_DB.glob(f"brand=*/store={store}/ym={ym}/orders_{ym}.parquet")):
        try:
            df = pd.read_parquet(path).fillna("")
        except Exception as exc:
            logger.warning("쿠팡 원천 로드 실패: %s | %s", path, exc)
            continue
        df["_source_brand"] = _parse_brand_from_path(path)
        df["_src_path"] = str(path)
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_brand_from_path(path) -> str:
    match = re.search(r"brand=([^/\\]+)", str(path))
    return match.group(1) if match else ""


def _parse_order_date(value: str) -> str:
    text = str(value).strip()
    match = re.match(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", text)
    if not match:
        return ""
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"


def _parse_order_time(value: str) -> str:
    text = str(value).strip()
    match = re.match(r"\d{4}\.\d{1,2}\.\d{1,2}\s+(\d{1,2}):(\d{2})", text)
    if not match:
        return ""
    return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}:00"


def _deduplicate_raw(df: pd.DataFrame, store: str, ym: str) -> pd.DataFrame:
    if {"order_id", "collected_at"}.issubset(df.columns):
        order_key = _clean_text_series(df["order_id"], df.index)
        collected_at = pd.to_datetime(df["collected_at"], errors="coerce", utc=True)
        valid = order_key.ne("") & collected_at.notna()
        if valid.any():
            latest_by_order = collected_at[valid].groupby(order_key[valid]).transform("max")
            keep_latest = ~valid
            keep_latest.loc[valid] = collected_at.loc[valid].eq(latest_by_order)
            latest_dropped = int((~keep_latest).sum())
            if latest_dropped:
                logger.warning(
                    "쿠팡 원천 최신 주문 스냅샷 적용: store=%s ym=%s 제거=%d행",
                    store,
                    ym,
                    latest_dropped,
                )
            df = df.loc[keep_latest].copy()

    cols = [
        "_src_path",
        "order_date",
        "order_id",
        "delivery_type",
        "order_status",
        "order_summary",
        "total_price",
        "is_cancelled",
        "item_menu",
        "menu_name",
        "menu_qty",
        "menu_price",
        "menu_options",
    ]
    cols = [col for col in cols if col in df.columns]
    before = len(df)
    key = pd.DataFrame(index=df.index)
    for col in cols:
        values = df[col]
        if col in _RAW_DEDUP_NUMERIC_COLS:
            key[col] = pd.to_numeric(
                values.astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            ).astype("Float64").astype(str)
        else:
            key[col] = values.fillna("").astype(str).str.strip()
            key[col] = key[col].replace({"nan": "", "None": "", "<NA>": ""})
    out = df[~key.duplicated(keep="last")].copy()
    dropped = before - len(out)
    if dropped:
        logger.warning("쿠팡 원천 중복 제거: store=%s ym=%s 제거=%d행", store, ym, dropped)
    return out


def _clean_text_series(value, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        series = value.reindex(index)
    else:
        series = pd.Series(value, index=index)
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "<NA>": ""})
    )


def _numeric_series(value, index: pd.Index, default: int = 0) -> pd.Series:
    if isinstance(value, pd.Series):
        series = value.reindex(index)
    else:
        series = pd.Series(value, index=index)
    return pd.to_numeric(
        series.fillna("").astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(default)


def _strip_options_apply_menu(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.search(r"\b외\s*\d+\s*개\b", text):
        return ""
    if "/" in text:
        text = text.rsplit("/", 1)[-1].strip()
    return text


def _load_option_parent_map(brand: str, store: str, ym: str) -> dict[str, str]:
    path = COUPANG_OPTIONS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "options.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("쿠팡 options 로드 실패, 부모 메뉴 fallback 스킵: %s | %s", path, exc)
        return {}

    required = {"옵션명", "적용메뉴"}
    if not required.issubset(df.columns):
        return {}

    option_to_menus: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        option = str(row.get("옵션명", "")).strip()
        menu = _strip_options_apply_menu(str(row.get("적용메뉴", "")).strip())
        if not option or not menu:
            continue
        option_to_menus.setdefault(option, set()).add(menu)

    return {
        option: next(iter(menus))
        for option, menus in option_to_menus.items()
        if len(menus) == 1
    }


def _read_csv_utf8_sig(path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="cp949").fillna("")


def _strip_posfeed_item_name(value: str) -> str:
    return re.sub(r"^'\+\s*", "", str(value or "").strip())


def _posfeed_order_paths(brand: str, store: str, ym: str) -> list:
    candidates = [
        POSFEED_ORDERS_DB / f"brand={brand}" / f"store={brand} {store}".strip() / f"ym={ym}" / "posfeed_orders.csv",
        POSFEED_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv",
    ]
    seen = set()
    paths = []
    for path in candidates:
        if path not in seen and path.exists():
            paths.append(path)
            seen.add(path)
    if paths:
        return paths
    return sorted(POSFEED_ORDERS_DB.glob(f"brand={brand}/store=*/ym={ym}/posfeed_orders.csv"))


def _load_posfeed_order_price_lines(
    brand: str,
    store: str,
    ym: str,
    order_ids: set[str],
) -> dict[str, list[dict]]:
    if not order_ids:
        return {}

    order_frames = []
    for path in _posfeed_order_paths(brand, store, ym):
        try:
            orders = _read_csv_utf8_sig(path)
        except Exception as exc:
            logger.warning("쿠팡수동 posfeed 주문 로드 실패: %s | %s", path, exc)
            continue
        required = {"외부 주문 번호", "주문 코드"}
        if not required.issubset(orders.columns):
            continue
        mask = orders["외부 주문 번호"].fillna("").astype(str).str.strip().isin(order_ids)
        if not mask.any():
            continue
        part = orders.loc[mask, ["외부 주문 번호", "주문 코드"]].copy()
        detail_path = POSFEED_DETAIL_DB / path.relative_to(POSFEED_ORDERS_DB)
        part["_detail_path"] = str(detail_path.parent / "posfeed_order_item.csv")
        order_frames.append(part)

    if not order_frames:
        return {}

    order_map = pd.concat(order_frames, ignore_index=True)
    order_map["외부 주문 번호"] = order_map["외부 주문 번호"].astype(str).str.strip()
    order_map["주문 코드"] = order_map["주문 코드"].astype(str).str.strip()
    result: dict[str, list[dict]] = {}
    for detail_path, group in order_map.groupby("_detail_path", sort=False):
        path = pd.io.common.stringify_path(detail_path)
        try:
            items = _read_csv_utf8_sig(path)
        except Exception as exc:
            logger.warning("쿠팡수동 posfeed 상세 로드 실패: %s | %s", path, exc)
            continue
        required = {"주문코드", "상품명", "수량", "단품가격", "합계"}
        if not required.issubset(items.columns):
            continue
        code_to_order = group.drop_duplicates("주문 코드").set_index("주문 코드")["외부 주문 번호"].to_dict()
        items = items[items["주문코드"].astype(str).str.strip().isin(code_to_order)].copy()
        if items.empty:
            continue
        items["_order_id"] = items["주문코드"].astype(str).str.strip().map(code_to_order)
        items["_item_name"] = items["상품명"].map(_strip_posfeed_item_name)
        items = items[
            items["_item_name"].ne("")
            & ~items["_item_name"].eq("배달비")
        ].copy()
        if items.empty:
            continue
        items["_qty"] = _numeric_series(items["수량"], items.index, default=1).astype(int).clip(lower=1)
        items["_unit_price"] = _numeric_series(items["단품가격"], items.index).astype(int)
        items["_line_sum"] = _numeric_series(items["합계"], items.index).astype(int)
        for order_id, order_items in items.groupby("_order_id", sort=False):
            paid_count = int(order_items["_line_sum"].ne(0).sum())
            if paid_count < 2:
                continue
            result[str(order_id).strip()] = [
                {
                    "item_name": row["_item_name"],
                    "qty": int(row["_qty"]),
                    "unit_price": int(row["_unit_price"]),
                }
                for _, row in order_items.iterrows()
            ]
    return result


def _stable_observed_price(values: list[int]) -> int:
    prices = [int(v) for v in values if int(v) > 0]
    if not prices:
        return 0
    counts = Counter(prices)
    price, count = counts.most_common(1)[0]
    if len(counts) == 1 or (count >= 3 and count / len(prices) >= 0.9):
        return int(price)
    return 0


def _build_observed_price_catalog() -> dict[tuple[str, str, str], int]:
    observations: dict[tuple[str, str, str], list[int]] = {}

    def add(brand: str, store: str, name: str, price: int) -> None:
        name = str(name or "").strip()
        price = int(price or 0)
        if not name or name == "배달비" or price <= 0:
            return
        for key in ((brand, store, name), (brand, "", name), ("", "", name)):
            observations.setdefault(key, []).append(price)

    for path in sorted(POSFEED_DETAIL_DB.glob("brand=*/store=*/ym=*/posfeed_order_item.csv")):
        try:
            items = _read_csv_utf8_sig(path)
        except Exception:
            continue
        if not {"상품명", "단품가격"}.issubset(items.columns):
            continue
        brand = next((part.split("=", 1)[1] for part in path.parts if part.startswith("brand=")), "")
        store = next((part.split("=", 1)[1] for part in path.parts if part.startswith("store=")), "")
        prices = _numeric_series(items["단품가격"], items.index).astype(int)
        for name, price in zip(items["상품명"].map(_strip_posfeed_item_name), prices):
            add(brand, store, name, int(price))

    for path in sorted(COUPANG_OPTIONS_DB.glob("brand=*/store=*/ym=*/options.csv")):
        try:
            options = _read_csv_utf8_sig(path)
        except Exception:
            continue
        if not {"옵션명", "옵션가격"}.issubset(options.columns):
            continue
        brand = next((part.split("=", 1)[1] for part in path.parts if part.startswith("brand=")), "")
        store = next((part.split("=", 1)[1] for part in path.parts if part.startswith("store=")), "")
        prices = _numeric_series(options["옵션가격"], options.index).astype(int)
        for name, price in zip(options["옵션명"].astype(str).str.strip(), prices):
            add(brand, store, name, int(price))

    return {
        key: price
        for key, values in observations.items()
        if (price := _stable_observed_price(values)) > 0
    }


def _observed_price_catalog() -> dict[tuple[str, str, str], int]:
    global _OBSERVED_PRICE_CATALOG
    if _OBSERVED_PRICE_CATALOG is None:
        _OBSERVED_PRICE_CATALOG = _build_observed_price_catalog()
    return _OBSERVED_PRICE_CATALOG


def _lookup_observed_price(brand: str, store: str, item_name: str) -> int:
    item_name = str(item_name or "").strip()
    if not item_name:
        return 0
    catalog = _scoped_observed_price_catalog(brand, store)
    price = catalog.get(item_name, 0)
    if price:
        return int(price)
    return 0


def _scoped_observed_price_catalog(brand: str, store: str) -> dict[str, int]:
    key = (str(brand or "").strip(), str(store or "").strip())
    if key in _SCOPED_PRICE_CATALOG:
        return _SCOPED_PRICE_CATALOG[key]

    observations: dict[str, list[int]] = {}

    def add(name: str, price: int) -> None:
        name = str(name or "").strip()
        price = int(price or 0)
        if not name or name == "배달비" or price <= 0:
            return
        observations.setdefault(name, []).append(price)

    store_candidates = [store, f"{brand} {store}".strip()]
    for store_name in dict.fromkeys(s for s in store_candidates if s):
        for path in sorted(POSFEED_DETAIL_DB.glob(f"brand={brand}/store={store_name}/ym=*/posfeed_order_item.csv")):
            try:
                items = _read_csv_utf8_sig(path)
            except Exception:
                continue
            if not {"상품명", "단품가격"}.issubset(items.columns):
                continue
            prices = _numeric_series(items["단품가격"], items.index).astype(int)
            for name, price in zip(items["상품명"].map(_strip_posfeed_item_name), prices):
                add(name, int(price))

        for path in sorted(COUPANG_OPTIONS_DB.glob(f"brand={brand}/store={store_name}/ym=*/options.csv")):
            try:
                options = _read_csv_utf8_sig(path)
            except Exception:
                continue
            if not {"옵션명", "옵션가격"}.issubset(options.columns):
                continue
            prices = _numeric_series(options["옵션가격"], options.index).astype(int)
            for name, price in zip(options["옵션명"].astype(str).str.strip(), prices):
                add(name, int(price))

    result = {
        name: price
        for name, values in observations.items()
        if (price := _stable_observed_price(values)) > 0
    }
    _SCOPED_PRICE_CATALOG[key] = result
    return result


def _resolve_parent_menu(df: pd.DataFrame, brand: str, store: str) -> pd.Series:
    index = df.index
    parent = _clean_text_series(df.get("item_menu", ""), index)
    raw_menu = _clean_text_series(df.get("menu_name", ""), index)
    parent = parent.mask(parent.eq(""), raw_menu)

    missing = parent.eq("")
    if missing.any():
        option = _clean_text_series(df.get("menu_options", ""), index)
        sale_date = _clean_text_series(df.get("sale_date", ""), index)
        for ym in sorted({date[:7] for date in sale_date[missing].tolist() if len(date) >= 7}):
            option_map = _load_option_parent_map(brand, store, ym)
            if not option_map:
                continue
            ym_mask = missing & sale_date.str.startswith(ym)
            mapped = option[ym_mask].map(option_map).fillna("")
            update_mask = pd.Series(False, index=index)
            update_mask.loc[mapped.index] = mapped.ne("")
            if update_mask.any():
                update_index = update_mask[update_mask].index
                parent.loc[update_index] = mapped.loc[update_index]
                missing = parent.eq("")

    summary = _clean_text_series(df.get("order_summary", ""), index).str.replace(
        r"\s*외\s*\d+건$",
        "",
        regex=True,
    ).str.strip()
    parent = parent.mask(parent.eq(""), summary)
    return parent


def _copy_order_base_row(group: pd.DataFrame) -> pd.Series:
    price = _numeric_series(group.get("menu_price", 0), group.index).astype(int)
    if price.gt(0).any():
        return group.loc[price[price.gt(0)].index[0]].copy()
    return group.iloc[0].copy()


def _rows_from_posfeed_lines(group: pd.DataFrame, lines: list[dict], parent_menu: str) -> list[pd.Series]:
    base = _copy_order_base_row(group)
    rows = []
    for seq, line in enumerate(lines):
        row = base.copy()
        item_name = str(line.get("item_name") or "").strip()
        row["menu_name"] = parent_menu or item_name
        row["item_menu"] = parent_menu or item_name
        row["menu_options"] = item_name
        row["menu_qty"] = int(line.get("qty") or 1)
        row["menu_price"] = int(line.get("unit_price") or 0)
        row[_ITEM_NAME_OVERRIDE_COL] = item_name
        if seq > 0:
            row["매출액"] = None
            row["total_price"] = None
        rows.append(row)
    return rows


def _apply_catalog_prices_to_order(group: pd.DataFrame, brand: str, store: str) -> list[pd.Series]:
    rows = [row.copy() for _, row in group.iterrows()]
    if not rows:
        return rows

    index = group.index
    parent_menu = _resolve_parent_menu(group, brand, store)
    menu_price = _numeric_series(group.get("menu_price", 0), index).astype(int)
    option_name = _clean_text_series(group.get("menu_options", ""), index)
    catalog_price = option_name.map(lambda value: _lookup_observed_price(brand, store, value)).astype(int)

    original_to_pos = {idx: pos for pos, idx in enumerate(index)}
    added_option_by_parent: dict[str, int] = {}
    extra_rows: list[pd.Series] = []

    for idx in index:
        pos = original_to_pos[idx]
        price = int(menu_price.loc[idx])
        option = str(option_name.loc[idx]).strip()
        observed = int(catalog_price.loc[idx])
        parent = str(parent_menu.loc[idx]).strip()

        rows[pos][_ITEM_NAME_OVERRIDE_COL] = ""
        if price <= 0 and observed > 0:
            rows[pos]["menu_price"] = observed
            rows[pos][_ITEM_NAME_OVERRIDE_COL] = option
            added_option_by_parent[parent] = added_option_by_parent.get(parent, 0) + observed
            continue

        if price > 0 and observed > 0 and option:
            rows[pos]["menu_price"] = max(price - observed, 0)
            option_row = rows[pos].copy()
            option_row["menu_price"] = observed
            option_row["menu_options"] = option
            option_row["item_menu"] = parent
            option_row["menu_name"] = parent
            option_row[_ITEM_NAME_OVERRIDE_COL] = option
            option_row["매출액"] = None
            option_row["total_price"] = None
            extra_rows.append(option_row)

    if added_option_by_parent:
        for parent, added_sum in added_option_by_parent.items():
            candidate_indices = [
                idx
                for idx in index
                if str(parent_menu.loc[idx]).strip() == parent and int(menu_price.loc[idx]) > 0
            ]
            if not candidate_indices:
                continue
            target_pos = original_to_pos[candidate_indices[0]]
            current = int(_numeric_series(pd.Series([rows[target_pos].get("menu_price", 0)]), pd.Index([0])).iloc[0])
            rows[target_pos]["menu_price"] = max(current - int(added_sum), 0)

    rows.extend(extra_rows)
    return rows


def _prepare_coupang_price_lines(df: pd.DataFrame, brand: str, store: str) -> pd.DataFrame:
    if df.empty:
        return df
    out_rows = []
    df = df.copy()
    if _ITEM_NAME_OVERRIDE_COL not in df.columns:
        df[_ITEM_NAME_OVERRIDE_COL] = ""
    df["_order_id_key"] = df["order_id"].fillna("").astype(str).str.strip()
    df["_ym_key"] = df["sale_date"].fillna("").astype(str).str[:7]

    posfeed_by_order: dict[str, list[dict]] = {}
    for ym, ym_df in df.groupby("_ym_key", sort=False):
        if not ym:
            continue
        order_ids = set(ym_df["_order_id_key"].dropna().astype(str).str.strip())
        posfeed_by_order.update(_load_posfeed_order_price_lines(brand, store, ym, order_ids))

    for order_id, group in df.groupby("_order_id_key", sort=False):
        parent = _resolve_parent_menu(group, brand, store).replace("", pd.NA).dropna()
        parent_name = str(parent.iloc[0]).strip() if not parent.empty else ""
        posfeed_lines = posfeed_by_order.get(order_id)
        if posfeed_lines:
            out_rows.extend(_rows_from_posfeed_lines(group, posfeed_lines, parent_name))
        else:
            out_rows.extend(_apply_catalog_prices_to_order(group, brand, store))

    if not out_rows:
        return df.drop(columns=["_order_id_key", "_ym_key"], errors="ignore")
    out = pd.DataFrame(out_rows).reset_index(drop=True)
    return out.drop(columns=["_order_id_key", "_ym_key"], errors="ignore")


def _allocate_order_amount(
    out: pd.DataFrame,
    amount: int,
    indices: pd.Index,
) -> None:
    if len(indices) == 0:
        return

    unit_price = pd.to_numeric(out.loc[indices, "unit_price"], errors="coerce").fillna(0).astype(int)
    qty = pd.to_numeric(out.loc[indices, "qty"], errors="coerce").fillna(1).astype(int)
    weights = (unit_price * qty).astype(int)
    target_indices = weights[weights.gt(0)].index
    if len(target_indices) == 0:
        target_indices = pd.Index([indices[0]])
        weights = pd.Series([1], index=target_indices)
    else:
        weights = weights.loc[target_indices]

    if amount == 0:
        out.loc[target_indices, "total_price"] = 0
        return

    weight_sum = int(weights.sum())
    if weight_sum == amount:
        allocations = weights.astype(int)
    elif weight_sum > 0:
        raw = weights.astype(float) * (amount / weight_sum)
        allocations = raw.round().astype(int)
        diff = int(amount - allocations.sum())
        if diff:
            adjust_idx = weights.abs().sort_values(ascending=False).index[0]
            allocations.loc[adjust_idx] = int(allocations.loc[adjust_idx] + diff)
    else:
        allocations = pd.Series([amount], index=target_indices, dtype=int)

    out.loc[target_indices, "total_price"] = allocations.astype(int)


def _transform_to_unified(
    df: pd.DataFrame,
    store: str,
    brand: str,
    store_map: dict,
) -> pd.DataFrame:
    df = _prepare_coupang_price_lines(df, brand, store)
    out = pd.DataFrame(index=df.index)
    out["sale_date"] = df["sale_date"]
    out["ym"] = df["sale_date"].str[:7]
    out["source"] = COUPANG_SOURCE
    out["brand"] = brand
    out["store"] = store
    out["platform"] = COUPANG_PLATFORM
    delivery_type = df.get("delivery_type", "").fillna("").astype(str).str.strip()
    out["order_type"] = delivery_type.map(lambda v: "배달_포장" if "포장" in v else "배달")
    out["order_id"] = df["order_id"].fillna("").astype(str).str.strip()
    out["order_time"] = df["order_time"]
    parent_menu = _resolve_parent_menu(df, brand, store)
    out["menu_name"] = parent_menu
    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    option_name = _clean_text_series(df.get("menu_options", ""), df.index)
    raw_menu_price = _clean_text_series(df.get("menu_price", ""), df.index)
    priced_menu = _numeric_series(df.get("menu_price", ""), df.index).gt(0) | (
        raw_menu_price.ne("") & raw_menu_price.str.lower().ne("nan")
    )
    item_name = option_name.mask(priced_menu | option_name.eq(""), parent_menu)
    if _ITEM_NAME_OVERRIDE_COL in df.columns:
        override = _clean_text_series(df[_ITEM_NAME_OVERRIDE_COL], df.index)
        item_name = item_name.mask(override.ne(""), override)
    out["item_name"] = fill_missing_manual_item_name(
        item_name,
        source=COUPANG_SOURCE,
        label="쿠팡",
        store=store,
        sale_date=out["sale_date"],
        order_id=out["order_id"],
    )
    out["qty"] = _numeric_series(df.get("menu_qty", 1), df.index, default=1).astype(int)
    out["unit_price"] = _numeric_series(df.get("menu_price", 0), df.index).astype(int)
    out["total_price"] = 0
    out["discount_amount"] = 0
    out["sale_type"] = df.get("is_cancelled", "").fillna("").astype(str).str.strip().str.upper().map(
        lambda value: "취소" if value == "Y" else "정상"
    )
    out["item_id"] = allocate_manual_item_ids(
        out[["source", "brand", "store", "item_name", "unit_price"]]
    )
    out["담당자"] = _lookup_store_meta(store_map, store, "담당자")
    out["region"] = _lookup_store_meta(store_map, store, "region")
    out["실오픈일"] = _lookup_store_meta(store_map, store, "실오픈일")
    out["collected_at"] = pendulum.now("Asia/Seoul").isoformat()

    maechul = (
        pd.to_numeric(
            df["매출액"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )
        if "매출액" in df.columns
        else pd.Series(float("nan"), index=df.index)
    )
    fallback_amount = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    out["order_cnt"] = 0
    for order_id, group in out.groupby("order_id", sort=False):
        if group.empty:
            continue
        indices = group.index
        is_cancel = bool(out.loc[indices, "sale_type"].eq("취소").any())
        order_sales = maechul.loc[indices]
        # 쿠팡 정산 매출액은 부분취소가 이미 차감된 순액이고 전체취소는 0/결측이다.
        if order_sales.notna().any():
            amount = order_sales.fillna(0).sum()
        elif is_cancel:
            amount = 0
        else:
            amount = fallback_amount.loc[indices].iloc[0]
        _allocate_order_amount(out, int(amount), indices)
        if not is_cancel:
            amount_rows = out.loc[indices, "total_price"].astype(int)
            order_cnt_idx = amount_rows.abs().sort_values(ascending=False).index[0]
            out.at[order_cnt_idx, "order_cnt"] = 1

    out["_pk"] = _make_unified_pk(out)
    return out.reindex(columns=UNIFIED_COLUMNS, fill_value="")


@unified_writer
def _upsert_daily(df_new: pd.DataFrame, date: str, store: str) -> tuple[int, int]:
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    path = _unified_daily_path(date)
    if path.exists():
        try:
            existing = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        except Exception as exc:
            logger.warning("쿠팡수동 교정 스킵, 기존 parquet 로드 실패: %s | %s", path, exc)
            return 0, 0
        store_s = existing["store"].fillna("").astype(str).str.strip()
        platform_s = existing["platform"].fillna("").astype(str).str.strip()
        source_s = existing["source"].fillna("").astype(str).str.strip()
        remove_mask = store_s.eq(store) & platform_s.isin(COUPANG_PLATFORMS)
        if df_new is None or df_new.empty:
            remove_mask = remove_mask & source_s.eq(COUPANG_SOURCE)
        removed = int(remove_mask.sum())
        existing = existing[~remove_mask]
    else:
        existing = pd.DataFrame(columns=UNIFIED_COLUMNS)
        removed = 0

    if df_new is None or df_new.empty:
        _write_daily(path, existing.reset_index(drop=True))
        return removed, 0

    out = pd.concat([existing, df_new], ignore_index=True)
    if "_pk" in out.columns:
        out = out.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
    _write_daily(path, out)
    return removed, len(df_new)


def _write_daily(path, df: pd.DataFrame) -> None:
    for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    out = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    save_unified_parquet(out, path)
