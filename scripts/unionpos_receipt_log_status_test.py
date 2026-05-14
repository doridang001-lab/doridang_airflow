import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# Ensure local imports work when run from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.playwright_launcher import launch_chromium  # noqa: E402
from modules.transform.pipelines.db.DB_UnionPOS_Receipt import (  # noqa: E402
    UNIONPOS_RECEIPT_URL,
    _login,
    _parse_page_receipts,
    _parse_receipt_items_popup,
    _set_date_and_search,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "sale_date",
        nargs="?",
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("sale_date=%s limit=%s", args.sale_date, args.limit)

    with sync_playwright() as p:
        browser = launch_chromium(
            p,
            headless=not args.headed,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--window-size=1920,1080",
            ],
        )
        try:
            bctx = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = bctx.new_page()

            _login(page)
            page.goto(UNIONPOS_RECEIPT_URL, wait_until="domcontentloaded", timeout=30_000)
            _set_date_and_search(page, args.sale_date)

            receipts = _parse_page_receipts(page)
            logging.info("receipts_on_page=%s", len(receipts))
            if not receipts:
                return 0

            for r in receipts[: max(args.limit, 0)]:
                rec = r["data"]
                onclick = r["onclick"]
                receipt_no = rec.get("영수증번호")
                sale_dt = rec.get("판매일시")
                if not onclick:
                    logging.warning("skip receipt_no=%r (no onclick)", receipt_no)
                    continue

                items, log_status = _parse_receipt_items_popup(
                    page,
                    bctx,
                    onclick,
                    receipt_no,
                    sale_dt,
                )
                print(f"receipt_no={receipt_no} items={len(items)} log_status={log_status!r}")

            return 0
        finally:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
