import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# Ensure local imports work when run from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.playwright_launcher import launch_chromium  # noqa: E402
from modules.transform.pipelines.db.DB_UnionPOS_Receipt import _collect_session  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect UnionPOS receipts for a single sale_date.")
    parser.add_argument(
        "sale_date",
        nargs="?",
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("sale_date=%s", args.sale_date)

    saved_files: list[str] = []

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
            _collect_session(page, bctx, [args.sale_date], saved_files, context={})
        finally:
            browser.close()

    print("saved_files:")
    for p in saved_files:
        print(" -", p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

