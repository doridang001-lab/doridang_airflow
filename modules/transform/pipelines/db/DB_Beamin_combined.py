"""배민 계정별 통합 수집 파이프라인.

=== 계정 단위 흐름 ===
  로그인
    → now 지표 수집 (우리가게NOW)
    → 우리가게 클릭 현황 수집 (이번달 + 저번달)
    → 로그아웃

한 번 로그인한 브라우저 세션으로 두 수집을 모두 처리한다.
"""

import logging
import random
import re
import time
from urllib.parse import urlparse as _urlparse

import pendulum

from modules.extract.croling_beamin import (
    TIMING,
    get_store_options,
    launch_browser,
    login_baemin,
    logout_baemin,
    wait_for_page,
)
from modules.transform.pipelines.db.DB_Beamin_01_now import collect_now_for_driver
from modules.transform.pipelines.db.DB_Beamin_02_woori_shop_click import (
    collect_woori_for_driver,
)
from modules.transform.pipelines.db.DB_Beamin_03_shop_change import (
    collect_shop_change_for_driver,
)

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]


def collect_now_and_woori(account_list: list[dict]) -> str:
    """계정별로 로그인 → now → 우리가게 클릭 → 로그아웃 순서로 수집."""
    success, fail = 0, 0

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        try:
            driver = launch_browser(account_id)

            if not login_baemin(driver, account_id, account["password"]):
                logger.warning("로그인 실패: %s", account_id)
                fail += 1
                continue

            logger.info("로그인 성공: %s", account_id)

            # 메인 대시보드로 이동
            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception as e:
                    logger.warning("대시보드 이동 타임아웃 (계속 진행): %s", e)
            else:
                time.sleep(2)

            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                logger.warning("메인 대시보드 로드 실패: %s", account_id)
                fail += 1
                continue

            # 매장 목록 1회 조회 (두 수집 공유) - store_id 기준 중복 제거
            raw_options = get_store_options(driver)
            seen_ids: set[str] = set()
            store_list = []
            for o in raw_options:
                parsed = _parse_store_option(o)
                if parsed and parsed["store_id"] not in seen_ids:
                    seen_ids.add(parsed["store_id"])
                    store_list.append(parsed)

            if not store_list:
                logger.warning(
                    "수집 대상 브랜드 없음: %s (옵션=%s)", account_id, raw_options
                )
                fail += 1
                continue

            logger.info("수집 대상 매장: %s", store_list)

            # 1단계: now 수집
            logger.info("=== now 수집 시작 [%s] ===", account_id)
            collect_now_for_driver(driver, account_id, store_list)

            # 2단계: 우리가게 클릭 수집
            logger.info("=== 우리가게 클릭 수집 시작 [%s] ===", account_id)
            collect_woori_for_driver(driver, store_list)

            # 3단계: 매장 변경이력 수집
            logger.info("=== 매장 변경이력 수집 시작 [%s] ===", account_id)
            collect_shop_change_for_driver(driver, store_list)

            logout_baemin(driver, account_id)
            logger.info("로그아웃 완료: %s", account_id)

            wait_sec = random.uniform(*TIMING["logout_wait"])
            logger.info("다음 계정까지 %.0f초 대기", wait_sec)
            time.sleep(wait_sec)

            success += 1

        except Exception as e:
            logger.error("처리 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return summary


def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None."""
    text = opt.get("text", "")
    brand = next((b for b in KNOWN_BRANDS if b in text), None)
    if not brand:
        return None

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}
