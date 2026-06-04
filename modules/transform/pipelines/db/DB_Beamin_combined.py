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
    is_driver_crash_error,
    launch_browser,
    login_baemin,
    logout_baemin,
    recover_driver_for_stage,
    restart_driver_if_dead,
    wait_for_page,
)
from modules.transform.pipelines.db.DB_Beamin_01_now import collect_now_for_driver
from modules.transform.pipelines.db.DB_Beamin_02_woori_shop_click import (
    collect_woori_for_driver,
)
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    collect_orders_for_account,
    collect_orders_for_driver,
)
from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import (
    collect_ad_funnel_for_account,
    collect_ad_funnel_for_driver,
)

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]
BRAND_COLLECTION_ORDER = {"나홀로": 0, "도리당": 1}


def _store_collection_sort_key(store_info: dict) -> tuple[int, str]:
    return (
        BRAND_COLLECTION_ORDER.get(store_info.get("brand", ""), 99),
        store_info.get("store", ""),
    )


# 드라이버 크래시 감지/복구는 croling_beamin로 공통화됨 (shop_change와 공유).
# 기존 호출부 호환을 위해 모듈 레벨 별칭 유지.
_is_driver_crash_error = is_driver_crash_error
_restart_driver_if_dead = restart_driver_if_dead
_recover_driver_for_stage = recover_driver_for_stage


def collect_now_and_woori(account_list: list[dict], target_date: str | None = None) -> dict:
    """계정별로 로그인 → now → 우리가게 클릭 → 매장변경이력 순서로 수집.

    Chrome OOM 방지를 위해 2~3단계를 매장별 독립 Chrome 세션으로 실행한다.
      1단계(now):       공유 Chrome — 같은 페이지에서 JS 매장 전환, 메모리 부담 낮음
      2~3단계(woori+변경이력): 매장별 신규 Chrome — 통계 페이지 반복 로드로 OOM 발생 방지
      4단계(orders):    기존과 동일, 매장별 독립 Chrome

    target_date: orders CSV 저장 시 날짜 라벨 override (None이면 어제). 브라우저는 항상 어제 날짜 조회.

    Returns:
        {"summary": str, "failed": {"accounts": [...], "stores": [...], "orders": [...]}}
    """
    success, fail = 0, 0
    failed_accounts: list[dict] = []
    failed_stores: list[dict] = []
    failed_orders: list[dict] = []
    failed_ads: list[dict] = []
    validation_results: list[dict] = []
    ad_store_infos: list[dict] = []
    store_info_per_account_list: list[dict] = []

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        store_list: list[dict] = []

        # ── 매장 목록 조회 (Chrome 최소 사용: 로그인 + 드롭다운 읽기만) ─────────
        try:
            logged_in = False
            for attempt in range(2):
                try:
                    driver = launch_browser(account_id)
                except Exception as e:
                    logger.warning("브라우저 실행 실패 (시도 %d/2): %s | %s", attempt + 1, account_id, e)
                    time.sleep(3.0)
                    continue
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
                failed_accounts.append(account)
                continue

            logger.info("로그인 성공: %s", account_id)

            driver = _recover_driver_for_stage(driver, account, "bootstrap")
            if driver is None:
                fail += 1
                failed_accounts.append(account)
                continue

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
                failed_accounts.append(account)
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
                failed_accounts.append(account)
                continue

            logger.info("수집 대상 매장: %s", store_list)

        except Exception as e:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
            failed_accounts.append(account)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if not store_list:
            continue

        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

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
                    failed_stores.append({"account": account, "store": store_info})
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

                # 우리가게 수집 (실패해도 주문내역/광고는 계속 진행)
                try:
                    collect_woori_for_driver(driver, [store_info])
                except Exception as e:
                    logger.warning(
                        "우리가게 실패 (orders 계속): %s / %s: %s",
                        account_id, store_info["store"], e,
                    )

                # Chrome이 죽었으면 재시작
                driver = _restart_driver_if_dead(
                    driver, account_id, account["password"]
                )
                if driver is None:
                    logger.error(
                        "Chrome 재시작 실패, 매장 건너뜀: %s / %s",
                        account_id, store_info["store"],
                    )
                    failed_stores.append({"account": account, "store": store_info})
                    continue

                # 주문내역 수집 (같은 Chrome 세션 재사용 — 로그인 생략)
                logger.info(
                    "=== 주문내역 수집 [%s / %s] ===",
                    account_id, store_info["store"],
                )
                orders_result = collect_orders_for_driver(driver, store_info, target_date=target_date)
                ok = orders_result.get("ok") if isinstance(orders_result, dict) else bool(orders_result)
                if not ok:
                    logger.warning(
                        "주문내역 수집 실패, retry 대상 등록: %s / %s",
                        account_id, store_info["store"],
                    )
                    failed_orders.append({"account": account, "stores": [store_info]})
                validation_results.extend(
                    orders_result.get("validation", []) if isinstance(orders_result, dict) else []
                )

                # 광고 funnel 수집 (같은 Chrome 세션 재사용 — 로그인 생략)
                logger.info(
                    "=== 광고 funnel 수집 [%s / %s] ===",
                    account_id, store_info["store"],
                )
                ad_store_infos.append({
                    **store_info,
                    "account_id": account["account_id"],
                    "password": account["password"],
                })
                if not collect_ad_funnel_for_driver(driver, store_info, target_date=target_date):
                    logger.warning(
                        "광고 funnel 수집 실패, retry 대상 등록: %s / %s",
                        account_id, store_info["store"],
                    )
                    failed_ads.append({"account": account, "stores": [store_info]})

                try:
                    logout_baemin(driver, account_id)
                except Exception:
                    pass

            except Exception as e:
                logger.error(
                    "매장 수집 실패 [%s / %s]: %s",
                    account_id, store_info["store"], e, exc_info=True,
                )
                failed_stores.append({"account": account, "store": store_info})
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            time.sleep(random.uniform(1.5, 3.0))

        wait_sec = random.uniform(*TIMING["logout_wait"])
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)

        success += 1

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return {
        "summary": summary,
        "failed": {
            "accounts": failed_accounts,
            "stores": failed_stores,
            "orders": failed_orders,
            "ads": failed_ads,
        },
        "validation": validation_results,
        "ad_stores": ad_store_infos,
        "store_info_per_account": store_info_per_account_list,
    }


def collect_now_and_woori(account_list: list[dict], target_date: str | None = None) -> dict:
    """Override earlier implementation to recover dead Chrome sessions between stages."""
    success, fail = 0, 0
    failed_accounts: list[dict] = []
    failed_stores: list[dict] = []
    failed_orders: list[dict] = []
    failed_ads: list[dict] = []
    validation_results: list[dict] = []
    ad_store_infos: list[dict] = []
    store_info_per_account_list: list[dict] = []

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        store_list: list[dict] = []

        try:
            logged_in = False
            for attempt in range(2):
                try:
                    driver = launch_browser(account_id)
                except Exception as e:
                    logger.warning("브라우저 실행 실패 (%d/2) %s | %s", attempt + 1, account_id, e)
                    time.sleep(3.0)
                    continue
                if login_baemin(driver, account_id, account["password"]):
                    logged_in = True
                    break
                logger.warning("로그인 실패 (%d/2): %s", attempt + 1, account_id)
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            if not logged_in:
                fail += 1
                failed_accounts.append(account)
                continue

            driver = _recover_driver_for_stage(driver, account, "bootstrap")
            if driver is None:
                fail += 1
                failed_accounts.append(account)
                continue

            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception as e:
                    logger.warning("대시보드 이동 실패(계속 진행): %s", e)
            else:
                time.sleep(2)

            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                logger.warning("메인 대시보드 로드 실패: %s", account_id)
                fail += 1
                failed_accounts.append(account)
                continue

            raw_options = get_store_options(driver)
            seen_ids: set[str] = set()
            for option in raw_options:
                parsed = _parse_store_option(option)
                if parsed and parsed["store_id"] not in seen_ids:
                    seen_ids.add(parsed["store_id"])
                    store_list.append(parsed)
            store_list.sort(key=_store_collection_sort_key)

            if not store_list:
                logger.warning("수집 대상 브랜드 없음: %s (%s)", account_id, raw_options)
                fail += 1
                failed_accounts.append(account)
                continue
            logger.info(
                "수집 매장 순서: %s",
                [f"{s['brand']} {s['store']}" for s in store_list],
            )
        except Exception as e:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
            failed_accounts.append(account)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

        if not store_list:
            continue

        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

        for store_info in store_list:
            driver = None
            try:
                driver = launch_browser(account_id)
                if not login_baemin(driver, account_id, account["password"]):
                    logger.warning("per-store 로그인 실패: %s / %s", account_id, store_info["store"])
                    failed_stores.append({"account": account, "store": store_info})
                    continue

                driver = _recover_driver_for_stage(driver, account, "post_login")
                if driver is None:
                    failed_stores.append({"account": account, "store": store_info})
                    continue

                if _urlparse(driver.current_url).hostname != "self.baemin.com":
                    try:
                        driver.set_page_load_timeout(45)
                        driver.get("https://self.baemin.com/")
                    except Exception as e:
                        logger.warning("대시보드 이동 실패: %s", e)
                else:
                    time.sleep(2)

                if wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                    logger.info("=== now 수집 [%s / %s] ===", account_id, store_info["store"])
                    collect_now_for_driver(driver, account_id, [store_info])
                else:
                    logger.warning("대시보드 로드 실패, now 스킵: %s / %s", account_id, store_info["store"])

                driver = _recover_driver_for_stage(driver, account, "after_now")
                if driver is None:
                    failed_stores.append({"account": account, "store": store_info})
                    continue

                try:
                    collect_woori_for_driver(driver, [store_info])
                except Exception as e:
                    logger.warning("우리가게 실패 (orders 계속): %s / %s: %s", account_id, store_info["store"], e)
                    failed_stores.append({"account": account, "store": store_info})

                logger.info("=== 주문내역 수집 [%s / %s] ===", account_id, store_info["store"])
                try:
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                    orders_result = collect_orders_for_account(
                        account_id,
                        account["password"],
                        [store_info],
                        target_date=target_date,
                    )
                except Exception as e:
                    if _is_driver_crash_error(e):
                        failed_orders.append({"account": account, "stores": [store_info]})
                        orders_result = {"ok": False, "validation": []}
                    else:
                        raise

                order_failed = (
                    orders_result.get("failed", [])
                    if isinstance(orders_result, dict)
                    else ([] if orders_result else [store_info])
                )
                if order_failed:
                    logger.warning("주문내역 수집 실패, retry 대상 등록: %s / %s", account_id, store_info["store"])
                    failed_orders.append({"account": account, "stores": [store_info]})
                validation_results.extend(
                    orders_result.get("validation", []) if isinstance(orders_result, dict) else []
                )

                logger.info("=== 광고 funnel 수집 [%s / %s] ===", account_id, store_info["store"])
                ad_store_infos.append({
                    **store_info,
                    "account_id": account["account_id"],
                    "password": account["password"],
                })
                try:
                    ad_failed = collect_ad_funnel_for_account(
                        account_id,
                        account["password"],
                        [store_info],
                        target_date=target_date,
                    )
                except Exception as e:
                    if _is_driver_crash_error(e):
                        failed_ads.append({"account": account, "stores": [store_info]})
                        logger.warning("광고 funnel dead session: %s / %s: %s", account_id, store_info["store"], e)
                        ad_failed = [store_info]
                    else:
                        raise
                if ad_failed:
                    logger.warning("광고 funnel 수집 실패, retry 대상 등록: %s / %s", account_id, store_info["store"])
                    failed_ads.append({"account": account, "stores": [store_info]})

                try:
                    logout_baemin(driver, account_id)
                except Exception:
                    pass

            except Exception as e:
                logger.error("매장 수집 실패 [%s / %s]: %s", account_id, store_info["store"], e, exc_info=True)
                failed_stores.append({"account": account, "store": store_info})
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            time.sleep(random.uniform(1.5, 3.0))

        wait_sec = random.uniform(*TIMING["logout_wait"])
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)
        success += 1

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return {
        "summary": summary,
        "failed": {
            "accounts": failed_accounts,
            "stores": failed_stores,
            "orders": failed_orders,
            "ads": failed_ads,
        },
        "validation": validation_results,
        "ad_stores": ad_store_infos,
        "store_info_per_account": store_info_per_account_list,
    }


def retry_once_failed(failed: dict, target_date: str | None = None) -> str:
    """실패한 계정/매장만 1회 재시도."""
    n_accounts = len(failed.get("accounts", []))
    n_stores = len(failed.get("stores", []))
    n_orders = len(failed.get("orders", []))
    logger.info("재시도 시작: accounts=%d stores=%d orders=%d", n_accounts, n_stores, n_orders)

    # 1. 계정 레벨 실패 → 해당 계정 전체 재수집
    if failed.get("accounts"):
        collect_now_and_woori(failed["accounts"], target_date=target_date)

    # 2. per-store 레벨 실패 → 해당 계정 로그인 후 해당 매장만
    for item in failed.get("stores", []):
        account = item["account"]
        store_info = item["store"]
        account_id = account["account_id"]
        driver = None
        try:
            driver = launch_browser(account_id)
            if not login_baemin(driver, account_id, account["password"]):
                logger.warning("재시도 로그인 실패: %s / %s", account_id, store_info["store"])
                continue

            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception:
                    pass
            else:
                time.sleep(2)

            if wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                collect_now_for_driver(driver, account_id, [store_info])
            collect_woori_for_driver(driver, [store_info])

            try:
                logout_baemin(driver, account_id)
            except Exception:
                pass

            logger.info("재시도 성공: %s / %s", account_id, store_info["store"])
        except Exception as e:
            logger.error("재시도 실패 [%s / %s]: %s", account_id, store_info["store"], e)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
        time.sleep(random.uniform(3.0, 6.0))

    # 3. orders 레벨 실패 → 해당 계정+매장 orders만
    for item in failed.get("orders", []):
        account = item["account"]
        stores = item["stores"]
        try:
            result = collect_orders_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            still_failed = result.get("failed", []) if isinstance(result, dict) else result
            logger.info(
                "orders 재시도 완료: %s (실패 %d건)", account["account_id"], len(still_failed)
            )
        except Exception as e:
            logger.error("orders 재시도 실패 [%s]: %s", account["account_id"], e)

    # 4. ads 레벨 실패 → 해당 계정+매장 광고 funnel만
    n_ads = len(failed.get("ads", []))
    for item in failed.get("ads", []):
        account = item["account"]
        stores = item["stores"]
        try:
            collect_ad_funnel_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            logger.info("ads 재시도 성공: %s", account["account_id"])
        except Exception as e:
            logger.error("ads 재시도 실패 [%s]: %s", account["account_id"], e)

    return f"재시도 완료: accounts={n_accounts} stores={n_stores} orders={n_orders} ads={n_ads}"


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
