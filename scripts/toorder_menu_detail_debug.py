import logging
import os
import sys
from pathlib import Path
import argparse

import pandas as pd
import pendulum

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dags.db.DB_ToOrderMenu_Dags import _load_store_detail_excel
from modules.extract.crawling_toorder_menu import (
    OPTION_ANALYSIS_URL,
    download_first_menu_detail_for_debug,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


TOORDER_ID = "doridang15"
TOORDER_PW = os.getenv("TOORDER_PW", "ehfl5233!")
DEBUG_DOWNLOAD_DIR = ROOT_DIR / "tmp" / "toorder_menu_debug"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--page", choices=["menu", "option"], default="menu")
    args = parser.parse_args()

    kst = pendulum.timezone("Asia/Seoul")
    target_date = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")
    page_url = OPTION_ANALYSIS_URL if args.page == "option" else None

    result = download_first_menu_detail_for_debug(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        target_date=target_date,
        download_dir=DEBUG_DOWNLOAD_DIR,
        page_url=page_url or "https://ceo.toorder.co.kr/dashboard/product-analysis/product-sales-date",
    )
    if result.get("error"):
        raise RuntimeError(result["error"])

    menu_name = result["menu_name"]
    file_path = Path(result["file_path"])
    logger.info("검증 날짜: %s", target_date)
    logger.info("검증 메뉴: %s", menu_name)
    logger.info("원본 파일: %s", file_path)

    raw_preview = pd.read_excel(file_path, header=None, dtype=str).iloc[:12, :10]
    print("\n=== Raw Preview (top 12 x 10) ===")
    print(raw_preview.fillna("").to_string(index=False, header=False))

    parsed = _load_store_detail_excel(file_path, menu_name, target_date)
    print("\n=== Parsed Preview ===")
    print(parsed.head(20).to_string(index=False))

    print("\n=== Parsed Store Names ===")
    stores = sorted(parsed["매장명"].dropna().astype(str).str.strip().unique().tolist())
    for store in stores[:100]:
        print(store)
    print(f"\nrows={len(parsed)}, unique_stores={len(stores)}")


if __name__ == "__main__":
    main()
