"""쿠팡 macro orders -> unified_sales 교정 파이프라인."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from modules.transform.utility.paths import COUPANG_ORDERS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES,
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    clear_manual_fallback_marker,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _unified_daily_path,
    iter_unified_sales_files,
    notify_manual_fallback,
    pos_delivery_summary,
    record_manual_fallback_marker,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import allocate_manual_item_ids

logger = logging.getLogger(__name__)

COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"
COUPANG_PLATFORMS = DELIVERY_PLATFORM_FAMILIES[COUPANG_SOURCE]


def reconcile_coupang_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 행을 coupang_macro 직수집 기준으로 교정."""
    dates = _resolve_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 교정 스킵 | 대상 날짜 없음"

    total_removed = 0
    total_added = 0
    fallback_events: list[dict] = []
    store_map = _load_store_map()

    for store in stores:
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
                    _record_coupang_fallback_event(date, store, fallback_events)
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
                    _record_coupang_fallback_event(date, store, fallback_events)
                    total_removed += removed
                    total_added += added
                    continue

                frames = []
                for brand, brand_df in day.groupby("_source_brand", sort=False):
                    frames.append(_transform_to_unified(brand_df, store, str(brand).strip(), store_map))
                new_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=UNIFIED_COLUMNS)
                removed, added = _upsert_daily(new_df, date, store)
                if new_df.empty:
                    _record_coupang_fallback_event(date, store, fallback_events)
                else:
                    clear_manual_fallback_marker(COUPANG_SOURCE, store, date)
                total_removed += removed
                total_added += added

    notify_manual_fallback("쿠팡수동", fallback_events)
    return f"쿠팡수동 교정 완료 | 제거={total_removed}행 추가={total_added}행 폴백={len(fallback_events)}건"


def _record_coupang_fallback_event(date: str, store: str, events: list[dict]) -> None:
    amount, order_cnt, rows = pos_delivery_summary(date, store, COUPANG_PLATFORMS, COUPANG_SOURCE)
    if rows <= 0:
        return
    event = {
        "date": date,
        "store": store,
        "platform": COUPANG_PLATFORM,
        "total_price": amount,
        "order_cnt": order_cnt,
        "rows": rows,
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
            continue
        out = df[~remove_mask].reset_index(drop=True)
        _write_daily(path, out)
        total_removed += removed
        changed += 1

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
        return [
            (now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, lookback_days + 1)
        ]

    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_source_dates(store))
    dates.update(_collect_existing_dates(stores))
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
    cols = [
        "_src_path",
        "order_date",
        "order_id",
        "delivery_type",
        "order_status",
        "order_summary",
        "total_price",
        "is_cancelled",
        "menu_name",
        "menu_qty",
        "menu_price",
        "menu_options",
    ]
    cols = [col for col in cols if col in df.columns]
    before = len(df)
    out = df.drop_duplicates(subset=cols, keep="last").copy()
    dropped = before - len(out)
    if dropped:
        logger.warning("쿠팡 원천 중복 제거: store=%s ym=%s 제거=%d행", store, ym, dropped)
    return out


def _transform_to_unified(
    df: pd.DataFrame,
    store: str,
    brand: str,
    store_map: dict,
) -> pd.DataFrame:
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
    out["menu_name"] = df["order_summary"].fillna("").astype(str).str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    item_name = df["menu_options"].fillna("").astype(str).str.strip()
    item_name = item_name.mask(item_name.eq("") | item_name.eq("nan"), df["menu_name"].fillna("").astype(str).str.strip())
    out["item_name"] = item_name
    out["qty"] = pd.to_numeric(df.get("menu_qty", 1), errors="coerce").fillna(1).astype(int)
    out["unit_price"] = pd.to_numeric(df.get("menu_price", 0), errors="coerce").fillna(0).astype(int)
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

    if "매출액" in df.columns:
        maechul = pd.to_numeric(
            df["매출액"].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )
        amount_by_order = maechul.groupby(out["order_id"]).max().to_dict()
    else:
        amount_by_order = {}
    fallback_amount_by_order = (
        pd.to_numeric(df["total_price"], errors="coerce")
        .fillna(0)
        .groupby(out["order_id"])
        .first()
        .astype(int)
        .to_dict()
    )
    out["order_cnt"] = 0
    for order_id, group in out.groupby("order_id", sort=False):
        if group.empty:
            continue
        idx = group.index[0]
        is_cancel = out.at[idx, "sale_type"] == "취소"
        amount = amount_by_order.get(order_id)
        if amount is None or pd.isna(amount):
            amount = 0 if is_cancel else fallback_amount_by_order.get(order_id, 0)
        out.at[idx, "total_price"] = int(amount)
        out.at[idx, "order_cnt"] = 0 if is_cancel else 1

    out["_pk"] = _make_unified_pk(out)
    return out.reindex(columns=UNIFIED_COLUMNS, fill_value="")


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
    df.reindex(columns=UNIFIED_COLUMNS, fill_value="").to_parquet(path, index=False, engine="pyarrow")
