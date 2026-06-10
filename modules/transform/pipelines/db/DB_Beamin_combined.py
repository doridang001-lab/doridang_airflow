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
from datetime import UTC, datetime
from typing import Any
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
from modules.transform.pipelines.db.DB_Beamin_03_shop_change import (
    collect_shop_change_for_driver,
)
from modules.transform.pipelines.db.beamin_stability import resolve_stability_profile

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


def _new_runtime_metrics(profile_name: str, account_list: list[dict]) -> dict:
    return {
        "profile": profile_name,
        "started_at": datetime.now(UTC).isoformat(),
        "total_accounts": len(account_list),
        "requested_store_count": 0,
        "browser_launch_count": 0,
        "login_attempt_count": 0,
        "login_failure_count": 0,
        "session_recovery_count": 0,
        "page_timeout_count": 0,
        "account_wait_sec_total": 0.0,
        "failed_accounts": [],
        "failed_stores": [],
    }


def _record_failed_store(metrics: dict, account_id: str, store_name: str, reason: str) -> None:
    metrics["failed_stores"].append(
        {"account_id": account_id, "store_name": store_name, "reason": reason}
    )


def _build_account_session(account: dict, metrics: dict) -> Any | None:
    account_id = account["account_id"]
    for attempt in range(2):
        metrics["browser_launch_count"] += 1
        try:
            driver = launch_browser(account_id)
        except Exception as exc:
            logger.warning("브라우저 실행 실패 (%d/2): %s | %s", attempt + 1, account_id, exc)
            time.sleep(3.0)
            continue
        metrics["login_attempt_count"] += 1
        try:
            if login_baemin(driver, account_id, account["password"]):
                return driver
        except Exception as exc:
            logger.warning("로그인 중 드라이버 충돌 (%d/2): %s | %s", attempt + 1, account_id, exc)
        metrics["login_failure_count"] += 1
        logger.warning("로그인 실패 (%d/2): %s", attempt + 1, account_id)
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(1.0)
    return None


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
        requested_store_name = str(account.get("store_name") or "").strip()

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

    total = success + fail
    summary = f"성공 {success}/{total} 계정"
    logger.info(summary)

    # 전 계정 실패(0/N)는 보통 환경적 장애(예: DNS 블립, 네트워크 단절)다.
    # SUCCESS로 조용히 넘어가면 당일 수집이 통째로 비는 '조용한 데이터 손실'이 된다.
    # → 예외를 던져 태스크를 FAILED로 만들어 Airflow 재시도(retry_delay 후 환경 회복 기대)로 잇는다.
    if success == 0 and total > 0:
        raise RuntimeError(f"배민 collect_all 전 계정 수집 실패({summary}) — 환경/네트워크 장애 의심, 재시도 필요")

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
            if requested_store_name:
                filtered_store_list = [
                    store_info
                    for store_info in store_list
                    if requested_store_name
                    in {
                        str(store_info.get("store") or "").strip(),
                        f"{str(store_info.get('brand') or '').strip()} {str(store_info.get('store') or '').strip()}".strip(),
                    }
                ]
                if filtered_store_list:
                    store_list = filtered_store_list
            store_list.sort(key=_store_collection_sort_key)
            logger.info(
                "store option filter detail account=%s requested=%s raw_options=%d selected=%s",
                account_id,
                requested_store_name or "<all>",
                len(raw_options),
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )
            logger.info(
                "selected store list [%s]: %s",
                requested_store_name or "<all>",
                [f"{s['brand']} {s['store']}" for s in store_list],
            )

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

    total = success + fail
    summary = f"성공 {success}/{total} 계정"
    logger.info(summary)

    # 전 계정 실패(0/N)는 보통 환경적 장애(예: DNS 블립, 네트워크 단절)다.
    # SUCCESS로 조용히 넘어가면 당일 수집이 통째로 비는 '조용한 데이터 손실'이 된다.
    # → 예외를 던져 태스크를 FAILED로 만들어 Airflow 재시도(retry_delay 후 환경 회복 기대)로 잇는다.
    if success == 0 and total > 0:
        raise RuntimeError(f"배민 collect_all 전 계정 수집 실패({summary}) — 환경/네트워크 장애 의심, 재시도 필요")

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


def _split_requested_store_name(requested_store_name: str) -> tuple[str | None, str]:
    """계정/override 매장명을 (brand, store) 형태로 분해한다."""
    value = str(requested_store_name or "").strip()
    for brand in KNOWN_BRANDS:
        prefix = f"{brand} "
        if value.startswith(prefix):
            return brand, value[len(prefix):].strip()
    return None, value


def _filter_store_list_for_request(store_list: list[dict], requested_store_name: str) -> list[dict]:
    """대표 매장명이 주어지면 같은 지점의 sister brand 옵션까지 함께 남긴다."""
    _, requested_store = _split_requested_store_name(requested_store_name)
    if not requested_store:
        return list(store_list)

    return [
        store_info
        for store_info in store_list
        if requested_store == str(store_info.get("store") or "").strip()
    ]


def collect_now_and_woori(
    account_list: list[dict],
    target_date: str | None = None,
    stability_profile: str | None = None,
) -> dict:
    profile = resolve_stability_profile(stability_profile)
    metrics = _new_runtime_metrics(profile["name"], account_list)
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
        requested_store_name = str(account.get("store_name") or "").strip()
        bootstrap_driver = _build_account_session(account, metrics)
        if bootstrap_driver is None:
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            continue

        store_list: list[dict] = []
        try:
            try:
                bootstrap_driver.set_page_load_timeout(45)
                bootstrap_driver.get("https://self.baemin.com/")
            except Exception as _nav_exc:
                logger.warning("대시보드 이동 타임아웃 (계속 진행): %s", _nav_exc)

            if not wait_for_page(bootstrap_driver, "select[class*='ShopSelect']", timeout=60):
                metrics["page_timeout_count"] += 1
                raise RuntimeError(f"main dashboard load failed: {account_id}")

            raw_options = get_store_options(bootstrap_driver)
            seen_ids: set[str] = set()
            for option in raw_options:
                parsed = _parse_store_option(option)
                if parsed and parsed["store_id"] not in seen_ids:
                    seen_ids.add(parsed["store_id"])
                    store_list.append(parsed)
            if requested_store_name:
                store_list = _filter_store_list_for_request(store_list, requested_store_name)
            store_list.sort(key=_store_collection_sort_key)
            logger.info("수집 매장 순서: %s", [f"{s['brand']} {s['store']}" for s in store_list])
        except Exception as exc:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, exc, exc_info=True)
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            store_list = []
        finally:
            try:
                bootstrap_driver.quit()
            except Exception:
                pass

        if not store_list:
            continue

        metrics["requested_store_count"] += len(store_list)
        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

        driver = None
        recovery_count = 0
        for index, store_info in enumerate(store_list, start=1):
            store_name = f"{store_info['brand']} {store_info['store']}"
            for store_attempt in range(2):
                try:
                    if driver is None:
                        driver = _build_account_session(account, metrics)
                        if driver is None:
                            raise RuntimeError(f"session bootstrap failed: {account_id}")

                    if _urlparse(driver.current_url).hostname != "self.baemin.com":
                        driver.set_page_load_timeout(45)
                        driver.get("https://self.baemin.com/")
                    else:
                        time.sleep(1.0)

                    if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                        # SPA 상태 꼬임 가능성 → 강제 reload 후 재대기
                        try:
                            driver.set_page_load_timeout(45)
                            driver.get("https://self.baemin.com/")
                        except Exception:
                            pass
                        if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                            metrics["page_timeout_count"] += 1
                            raise RuntimeError(f"dashboard not ready: {store_name}")

                    _brand = store_info.get("brand", "")
                    logger.info("=== now 수집 brand=%s [%s / %s] ===", _brand, account_id, store_name)
                    collect_now_for_driver(driver, account_id, [store_info])

                    logger.info("=== 우리가게 수집 brand=%s [%s / %s] ===", _brand, account_id, store_name)
                    collect_woori_for_driver(driver, [store_info])

                    logger.info("=== 변경이력 수집 brand=%s [%s / %s] ===", _brand, account_id, store_name)
                    try:
                        collect_shop_change_for_driver(driver, [store_info])
                    except Exception as exc:
                        logger.warning("변경이력 수집 실패(무시): %s / %s / %s", account_id, store_name, exc)
                    finally:
                        try:
                            driver.set_page_load_timeout(45)
                            driver.get("about:blank")
                        except Exception:
                            pass

                    logger.info("=== 주문내역 수집 brand=%s [%s / %s] ===", _brand, account_id, store_name)
                    orders_result = collect_orders_for_driver(driver, store_info, target_date=target_date)
                    if not orders_result.get("ok"):
                        failed_orders.append({"account": account, "stores": [store_info]})
                        _record_failed_store(metrics, account_id, store_name, "orders failed")
                    validation_results.extend(orders_result.get("validation", []))

                    logger.info("=== 광고 funnel 수집 brand=%s [%s / %s] ===", _brand, account_id, store_name)
                    ad_store_infos.append(
                        {
                            **store_info,
                            "account_id": account["account_id"],
                            "password": account["password"],
                        }
                    )
                    if not collect_ad_funnel_for_driver(driver, store_info, target_date=target_date):
                        failed_ads.append({"account": account, "stores": [store_info]})
                        _record_failed_store(metrics, account_id, store_name, "ad_funnel failed")
                    break
                except Exception as exc:
                    is_session_issue = _is_driver_crash_error(exc) or "session" in str(exc).lower()
                    if is_session_issue:
                        recovery_count += 1
                        metrics["session_recovery_count"] += 1
                        logger.warning("세션 문제 감지, 재생성 시도: %s / %s / %s", account_id, store_name, exc)
                        try:
                            if driver:
                                driver.quit()
                        except Exception:
                            pass
                        driver = None
                        if recovery_count > profile["max_session_recovery_per_account"] or store_attempt >= 1:
                            failed_stores.append({"account": account, "store": store_info})
                            _record_failed_store(metrics, account_id, store_name, f"session issue: {exc}")
                            break
                        continue

                    logger.warning("매장 수집 실패: %s / %s / %s", account_id, store_name, exc)
                    failed_stores.append({"account": account, "store": store_info})
                    _record_failed_store(metrics, account_id, store_name, str(exc))
                    break

            if driver is not None and index % int(profile["driver_restart_every_stores"]) == 0:
                try:
                    logout_baemin(driver, account_id)
                except Exception:
                    pass
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
            time.sleep(random.uniform(1.0, 2.0))

        if driver is not None:
            try:
                logout_baemin(driver, account_id)
            except Exception:
                pass
            try:
                driver.quit()
            except Exception:
                pass

        wait_sec = random.uniform(*profile["account_wait_range"])
        metrics["account_wait_sec_total"] += round(wait_sec, 1)
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)
        success += 1

    total = success + fail
    summary = f"성공 {success}/{total} 계정"
    metrics["ended_at"] = datetime.now(UTC).isoformat()
    logger.info(summary)

    if success == 0 and total > 0:
        raise RuntimeError(f"배민 collect_all 전체 계정 수집 실패({summary})")

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
        "metrics": metrics,
    }
