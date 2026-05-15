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
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    collect_orders_for_account,
)

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]


def collect_now_and_woori(account_list: list[dict]) -> str:
    """계정별로 로그인 → now → 우리가게 클릭 → 매장변경이력 순서로 수집.

    Chrome OOM 방지를 위해 2~3단계를 매장별 독립 Chrome 세션으로 실행한다.
      1단계(now):       공유 Chrome — 같은 페이지에서 JS 매장 전환, 메모리 부담 낮음
      2~3단계(woori+변경이력): 매장별 신규 Chrome — 통계 페이지 반복 로드로 OOM 발생 방지
      4단계(orders):    기존과 동일, 매장별 독립 Chrome
    """
    success, fail = 0, 0

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        store_list: list[dict] = []

        # ── 매장 목록 조회 (Chrome 최소 사용: 로그인 + 드롭다운 읽기만) ─────────
        try:
            logged_in = False
            for attempt in range(2):
                driver = launch_browser(account_id)
                if login_baemin(driver, account_id, account["password"]):
                    logged_in = True
                    break
                logger.warning("로그인 실패 (시도 %d/2): %s", attempt + 1, account_id)
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            if not logged_in:
                fail += 1
                continue

            logger.info("로그인 성공: %s", account_id)

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

            raw_options = get_store_options(driver)
            seen_ids: set[str] = set()
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

        except Exception as e:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if not store_list:
            continue

        # ── 매장별 독립 Chrome: 1단계(now) + 2단계(우리가게) + 3단계(변경이력) ──
        # Chrome 1개당 단일 매장만 처리 → React SPA DOM 축적 없음 → OOM 방지
        for store_info in store_list:
            driver = None
            try:
                driver = launch_browser(account_id)
                if not login_baemin(driver, account_id, account["password"]):
                    logger.warning(
                        "per-store 로그인 실패: %s / %s",
                        account_id, store_info["store"],
                    )
                    continue

                # now 수집: 대시보드에서 이 매장만 선택
                if _urlparse(driver.current_url).hostname != "self.baemin.com":
                    try:
                        driver.set_page_load_timeout(45)
                        driver.get("https://self.baemin.com/")
                    except Exception as e:
                        logger.warning("대시보드 이동 타임아웃: %s", e)
                else:
                    time.sleep(2)

                if wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                    logger.info(
                        "=== now 수집 [%s / %s] ===",
                        account_id, store_info["store"],
                    )
                    collect_now_for_driver(driver, account_id, [store_info])
                else:
                    logger.warning(
                        "대시보드 로드 실패, now 스킵: %s / %s",
                        account_id, store_info["store"],
                    )

                # 우리가게 + 변경이력 수집
                logger.info(
                    "=== 우리가게+변경이력 수집 [%s / %s] ===",
                    account_id, store_info["store"],
                )
                collect_woori_for_driver(driver, [store_info])
                collect_shop_change_for_driver(driver, [store_info])

                try:
                    logout_baemin(driver, account_id)
                except Exception:
                    pass

            except Exception as e:
                logger.error(
                    "매장 수집 실패 [%s / %s]: %s",
                    account_id, store_info["store"], e, exc_info=True,
                )
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            time.sleep(random.uniform(3.0, 6.0))

        wait_sec = random.uniform(*TIMING["logout_wait"])
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)

        success += 1

        # ── 4단계: 주문내역 수집 (독립 Chrome — 기존 유지) ──────────────────────
        logger.info("=== 주문내역 수집 시작 [%s] ===", account_id)
        try:
            collect_orders_for_account(account_id, account["password"], store_list)
        except Exception as e:
            logger.error("주문내역 수집 실패 [%s]: %s", account_id, e, exc_info=True)

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return summary


def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None."""
    text = opt.get("text", "")
    # 한국어 문자가 앞에 붙으면 다른 브랜드명이므로 제외 (예: "곱도리당" → "도리당" 오매칭 방지)
    brand = next(
        (b for b in KNOWN_BRANDS if re.search(rf"(?<![가-힣]){re.escape(b)}", text)),
        None,
    )
    if not brand:
        return None

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}
