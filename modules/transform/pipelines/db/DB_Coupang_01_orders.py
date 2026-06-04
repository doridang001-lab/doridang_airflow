"""Coupang orders collection pipeline."""

import logging
from pathlib import Path

import pandas as pd

from modules.extract.croling_coupang import (
    _ORDER_COLUMNS,
    collect_orders_all_pages,
    navigate_to_orders_with_date,
)
from modules.transform.utility.paths import COUPANG_ORDERS_DB

logger = logging.getLogger(__name__)


def collect_orders_for_driver(driver, store_info: dict, target_date: str) -> tuple[list[dict], dict]:
    store_id = store_info["store_id"]
    store_name = store_info["store_name"]
    account_id = store_info.get("account_id", "?")

    date_filter = navigate_to_orders_with_date(driver, store_id, target_date, account_id)
    collection = collect_orders_all_pages(driver, store_id, store_name, account_id)
    rows = collection["rows"]
    expected = int(collection.get("expected") or 0)
    unique_ids = len({row["order_id"] for row in rows if row.get("order_id")})
    incomplete_reason = collection.get("incomplete_reason")
    matched = unique_ids == expected if expected > 0 else None

    validation = {
        "expected": expected,
        "collected": unique_ids,
        "matched": matched,
        "date_filter_ok": bool(date_filter.get("applied")),
        "date_filter_failure_reason": date_filter.get("failure_reason"),
        "observed_date_label": date_filter.get("observed_date_label") or "",
        "observed_url": date_filter.get("observed_url") or "",
        "session_lost": bool(collection.get("session_lost")),
        "page_count": int(collection.get("page_count") or 0),
        "incomplete_reason": incomplete_reason,
        "api_filtered": False,
    }
    logger.info(
        "[%s] %s collection summary: expected=%d collected=%d page_count=%d date_filter_ok=%s session_lost=%s incomplete_reason=%s",
        account_id,
        store_name,
        expected,
        unique_ids,
        validation["page_count"],
        validation["date_filter_ok"],
        validation["session_lost"],
        incomplete_reason,
    )

    return rows, validation


def save_orders_csv(rows: list[dict], brand: str, store: str, target_date: str) -> Path:
    ym = target_date[:7]
    out_dir = COUPANG_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"orders_{ym}.csv"

    new_df = pd.DataFrame(rows, columns=_ORDER_COLUMNS)
    new_order_ids = set(new_df["order_id"].dropna().unique())

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        existing = existing[~existing["order_id"].isin(new_order_ids)]
        combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
    else:
        combined = new_df.astype(str)

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("saved orders csv: %s (%d rows)", out_path, len(combined))
    return out_path
