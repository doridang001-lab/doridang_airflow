"""나홀로 역삼점 orders 단독 재시도 스크립트.

WSL에서 실행:
  cd /mnt/c/airflow && source .venv_wsl/bin/activate
  python tmp/retry_naholo_yeoksam_orders.py
"""
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

# WSL / Windows 양쪽 경로 호환
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import pendulum
from modules.extract.croling_beamin import (
    get_store_options,
    launch_browser,
    login_baemin,
    wait_for_page,
)
from modules.transform.pipelines.db.DB_Beamin_04_orders import collect_orders_for_account
from modules.transform.utility.paths import ONEDRIVE_DB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# doriys 계정 = 역삼 담당 (도리당+나홀로 공동 계정)
ACCOUNT_ID = "doriys"
PASSWORD   = "ever123@"

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]


def _parse_store(opt: dict) -> dict | None:
    text = opt.get("text", "")
    brand = next(
        (b for b in KNOWN_BRANDS if re.search(rf"(?<![가-힣]){re.escape(b)}", text)),
        None,
    )
    if brand != "나홀로":
        return None
    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]
    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def main():
    target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")
    logger.info("재시도 대상 날짜: %s", target_date)

    # 1. 로그인 → 드롭다운으로 나홀로 역삼점 store_id 탐색
    driver = None
    naholo_stores = []
    try:
        driver = launch_browser(ACCOUNT_ID)
        if not login_baemin(driver, ACCOUNT_ID, PASSWORD):
            logger.error("로그인 실패")
            return

        if urlparse(driver.current_url).hostname != "self.baemin.com":
            driver.set_page_load_timeout(45)
            driver.get("https://self.baemin.com/")

        if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
            logger.error("대시보드 로드 실패")
            return

        raw_options = get_store_options(driver)
        logger.info("드롭다운 옵션: %s", raw_options)

        seen: set[str] = set()
        for o in raw_options:
            parsed = _parse_store(o)
            if parsed and parsed["store_id"] not in seen:
                seen.add(parsed["store_id"])
                naholo_stores.append(parsed)

        logger.info("나홀로 매장 목록: %s", naholo_stores)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    if not naholo_stores:
        logger.error("나홀로 역삼점을 드롭다운에서 찾지 못했습니다")
        return

    # 2. orders 수집 (정상 + 주문취소)
    failed = collect_orders_for_account(
        ACCOUNT_ID, PASSWORD, naholo_stores, target_date=target_date
    )
    if failed:
        logger.error("수집 실패 매장: %s", [s["store"] for s in failed])
    else:
        logger.info("완료: 나홀로 역삼점 orders 재수집 성공")


if __name__ == "__main__":
    main()
