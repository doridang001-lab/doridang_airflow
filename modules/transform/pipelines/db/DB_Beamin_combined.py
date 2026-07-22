"""배민 계정별 통합 수집 파이프라인.

=== 계정 단위 흐름 ===
  로그인
    → 매장 목록/store_id 확인
    → 우리가게 클릭 현황 수집 (이번달 + 저번달)
    → 로그아웃

한 번 로그인한 브라우저 세션으로 수집 단계를 순차 처리한다.
"""

import logging
import random
import re
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse as _urlparse

import pendulum
import pandas as pd

from modules.extract.croling_beamin import (
    TIMING,
    get_store_options,
    is_on_main_dashboard,
    is_driver_crash_error,
    launch_browser,
    login_baemin,
    logout_baemin,
    quit_driver_safely,
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
from modules.transform.utility.store_normalize import lookup_store_key, strip_brand

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["도리당", "나홀로"]
BRAND_COLLECTION_ORDER = {"도리당": 0, "나홀로": 1}
_BRANCH_SUFFIX_RE = re.compile(r"(?:점|지점|분점|직영점)$")
SHOP_CHANGE_COLLECTION_WEEKDAY = pendulum.SATURDAY


def _should_collect_shop_change(now: pendulum.DateTime | None = None) -> bool:
    current = now or pendulum.now(KST)
    return current.in_timezone(KST).day_of_week == SHOP_CHANGE_COLLECTION_WEEKDAY


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
        "login_second_pass_accounts": 0,
        "login_second_pass_success": 0,
        "login_second_pass_failed": 0,
        "completed_accounts": 0,
        "partial_accounts": 0,
        "failed_orders_count": 0,
        "failed_ads_count": 0,
    }


def _record_failed_store(metrics: dict, account_id: str, store_name: str, reason: str) -> None:
    metrics["failed_stores"].append(
        {"account_id": account_id, "store_name": store_name, "reason": reason}
    )


def _build_account_session(account: dict, metrics: dict, profile: dict | None = None) -> Any | None:
    account_id = account["account_id"]
    profile = profile or {}
    max_attempts = int(profile.get("login_attempts") or 2)
    wait_range = profile.get("login_retry_wait_range") or (1.0, 3.0)
    for attempt in range(max_attempts):
        metrics["browser_launch_count"] += 1
        driver = None
        try:
            driver = launch_browser(account_id)
        except Exception as exc:
            logger.warning(
                "브라우저 실행 실패 (%d/%d): %s | %s",
                attempt + 1,
                max_attempts,
                account_id,
                exc,
            )
            time.sleep(random.uniform(*wait_range))
            continue
        metrics["login_attempt_count"] += 1
        try:
            if login_baemin(driver, account_id, account["password"]):
                return driver
        except Exception as exc:
            logger.warning(
                "로그인 중 드라이버 충돌 (%d/%d): %s | %s",
                attempt + 1,
                max_attempts,
                account_id,
                exc,
            )
        metrics["login_failure_count"] += 1
        logger.warning("로그인 실패 (%d/%d): %s", attempt + 1, max_attempts, account_id)
        quit_driver_safely(driver, account_id)
        time.sleep(random.uniform(*wait_range))
    return None


_SESSION_ISSUE_MARKERS = (
    "remote end closed",
    "connection aborted",
    "remotedisconnected",
    "connection refused",
    "max retries exceeded",
    "newconnectionerror",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "cannot connect to chrome",
    "session not created",
    "timed out receiving message from renderer",
)


def _is_recoverable_session_issue(exc: Exception) -> bool:
    error_text = str(exc).lower()
    return is_driver_crash_error(exc) or any(
        marker in error_text for marker in _SESSION_ISSUE_MARKERS
    )


def _close_driver_safely(driver: Any, account_id: str) -> None:
    try:
        if driver:
            logout_baemin(driver, account_id)
    except Exception:
        pass
    quit_driver_safely(driver, account_id)


def _quit_driver_safely(driver: Any) -> None:
    quit_driver_safely(driver)


def _build_dashboard_session(account: dict, metrics: dict, profile: dict) -> Any | None:
    account_id = account["account_id"]
    max_recovery = int(profile["max_session_recovery_per_account"])
    for attempt in range(max_recovery + 1):
        driver = _build_account_session(account, metrics, profile)
        if driver is None:
            return None
        try:
            if not is_on_main_dashboard(driver.current_url):
                driver.set_page_load_timeout(45)
                driver.get("https://self.baemin.com/")
            return driver
        except Exception as exc:
            if _is_recoverable_session_issue(exc):
                _close_driver_safely(driver, account_id)
                if attempt < max_recovery:
                    metrics["session_recovery_count"] += 1
                    logger.info(
                        "초기 대시보드 세션 문제 감지, 재생성 시도: %s / %s",
                        account_id,
                        exc,
                    )
                    continue
                logger.warning("초기 대시보드 세션 문제 재생성 한도 초과: %s / %s", account_id, exc)
                return None
            logger.info("대시보드 이동 타임아웃 (계속 진행): %s", exc)
            return driver
    return None


def _ensure_dashboard_store_select(
    account: dict,
    driver: Any,
    metrics: dict,
    profile: dict,
) -> Any | None:
    account_id = account["account_id"]
    max_recovery = int(profile["max_session_recovery_per_account"])
    current_driver = driver
    for attempt in range(max_recovery + 1):
        if wait_for_page(current_driver, "select[class*='ShopSelect']", timeout=60):
            return current_driver
        metrics["page_timeout_count"] += 1
        logger.warning(
            "대시보드 매장목록 로드 실패, 세션 재생성 시도: %s (%d/%d)",
            account_id,
            attempt + 1,
            max_recovery + 1,
        )
        _close_driver_safely(current_driver, account_id)
        if attempt >= max_recovery:
            return None
        metrics["session_recovery_count"] += 1
        current_driver = _build_dashboard_session(account, metrics, profile)
        if current_driver is None:
            return None
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
                quit_driver_safely(driver, account_id)
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

            logger.info(
                "수집 대상 매장/store_id: %s",
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )

        except Exception as e:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
            failed_accounts.append(account)
        finally:
            if driver:
                quit_driver_safely(driver, account_id)

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

                # store_id 확인: 대시보드 드롭다운이 정상 로드되는지만 확인
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
                        "store_id 확인 완료 [%s / %s]: %s",
                        account_id, store_info["store"],
                        store_info["store_id"],
                    )
                    collect_now_for_driver(driver, account_id, [store_info])
                else:
                    logger.warning(
                        "대시보드 로드 실패, store_id 확인 스킵: %s / %s",
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
                    quit_driver_safely(driver, account_id)

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
                quit_driver_safely(driver, account_id)
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
                "수집 매장 순서/store_id: %s",
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )
        except Exception as e:
            logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
            failed_accounts.append(account)
        finally:
            if driver:
                quit_driver_safely(driver, account_id)

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
                    logger.info(
                        "store_id 확인 완료 [%s / %s]: %s",
                        account_id,
                        store_info["store"],
                        store_info["store_id"],
                    )
                    collect_now_for_driver(driver, account_id, [store_info])
                else:
                    logger.warning("대시보드 로드 실패, store_id 확인 스킵: %s / %s", account_id, store_info["store"])

                driver = _recover_driver_for_stage(driver, account, "after_store_id_check")
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
                        quit_driver_safely(driver, account_id)
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
                    quit_driver_safely(driver, account_id)

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


def retry_once_failed(
    failed: dict,
    target_date: str | None = None,
    stability_profile: str | None = None,
) -> dict:
    """실패한 계정/매장만 1회 재시도."""
    n_accounts = len(failed.get("accounts", []))
    n_stores = len(failed.get("stores", []))
    n_orders = len(failed.get("orders", []))
    n_ads = len(failed.get("ads", []))
    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": []}

    def extend_residual(result_failed: dict | None) -> None:
        data = result_failed or {}
        for key in ("accounts", "stores", "orders", "ads"):
            residual_failed[key].extend(data.get(key) or [])

    logger.info(
        "재시도 시작: accounts=%d stores=%d orders=%d ads=%d profile=%s",
        n_accounts,
        n_stores,
        n_orders,
        n_ads,
        stability_profile,
    )

    # 1. 계정 레벨 실패 → 해당 계정 전체 재수집
    if failed.get("accounts"):
        kwargs = {"target_date": target_date}
        if stability_profile is not None:
            kwargs["stability_profile"] = stability_profile
        result = collect_now_and_woori(failed["accounts"], **kwargs)
        if isinstance(result, dict):
            extend_residual(result.get("failed"))

    # 2. per-store 레벨 실패 → 해당 계정 로그인 후 해당 매장만
    for item in failed.get("stores", []):
        account = item["account"]
        store_info = item["store"]
        account_id = account["account_id"]
        try:
            logger.info(
                "store_id 힌트 주문 재시도: %s / %s#%s",
                account_id,
                store_info.get("store"),
                store_info.get("store_id"),
            )
            result = collect_orders_for_account(
                account_id,
                account["password"],
                [store_info],
                target_date=target_date,
            )
            still_failed = result.get("failed", []) if isinstance(result, dict) else result
            if still_failed:
                logger.warning(
                    "store 주문 재시도 잔여 실패: %s / %s (%d건)",
                    account_id,
                    store_info.get("store"),
                    len(still_failed),
                )
                residual_failed["stores"].append(item)
            else:
                logger.info("재시도 성공: %s / %s", account_id, store_info["store"])
        except Exception as e:
            logger.error("재시도 실패 [%s / %s]: %s", account_id, store_info["store"], e)
            residual_failed["stores"].append(item)
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
            if still_failed:
                logger.info("orders 2차 재시도 건수 %d건 (30초 대기)", len(still_failed))
                time.sleep(30)
                result2 = collect_orders_for_account(
                    account["account_id"], account["password"], still_failed, target_date=target_date
                )
                still2 = result2.get("failed", []) if isinstance(result2, dict) else result2
                logger.info(
                    "orders 2차 재시도 완료 %s: 잔여 실패 %d건", account["account_id"], len(still2)
                )
                if still2:
                    residual_failed["orders"].append({"account": account, "stores": still2})
            elif still_failed:
                residual_failed["orders"].append({"account": account, "stores": still_failed})
        except Exception as e:
            logger.error("orders 재시도 실패 [%s]: %s", account["account_id"], e)
            residual_failed["orders"].append(item)

    # 4. ads 레벨 실패 → 해당 계정+매장 광고 funnel만
    for item in failed.get("ads", []):
        account = item["account"]
        stores = item["stores"]
        try:
            ad_failed = collect_ad_funnel_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            if ad_failed:
                logger.warning("ads 재시도 후 잔여 실패 %d건: %s", len(ad_failed), account["account_id"])
                residual_failed["ads"].append({"account": account, "stores": ad_failed})
            else:
                logger.info("ads 재시도 성공: %s", account["account_id"])
        except Exception as e:
            logger.warning("ads 1차 실패, 30초 대기 후 2차 재시도 [%s]: %s", account["account_id"], e)
            time.sleep(30)
            try:
                ad_failed = collect_ad_funnel_for_account(
                    account["account_id"], account["password"], stores, target_date=target_date
                )
                if ad_failed:
                    logger.error("ads 2차 재시도 후 잔여 실패 %d건: %s", len(ad_failed), account["account_id"])
                    residual_failed["ads"].append({"account": account, "stores": ad_failed})
                else:
                    logger.info("ads 2차 재시도 성공: %s", account["account_id"])
            except Exception as e2:
                logger.error("ads 2차 재시도 실패 [%s]: %s", account["account_id"], e2)
                residual_failed["ads"].append(item)

    residual_counts = {key: len(value) for key, value in residual_failed.items()}
    summary = (
        f"재시도 완료: 대상 accounts={n_accounts} stores={n_stores} orders={n_orders} ads={n_ads} "
        f"→ 잔여 accounts={residual_counts['accounts']} stores={residual_counts['stores']} "
        f"orders={residual_counts['orders']} ads={residual_counts['ads']}"
    )
    return {"summary": summary, "residual_failed": residual_failed}


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
    if matches:
        store = matches[-1]
    else:
        # 접미사 없는 테스트 매장은 브랜드 접두어를 제거해야 요청 매장명과 비교 가능하다.
        store = re.sub(rf"^{re.escape(brand)}\s*", "", text).strip() or text[:20]

    store = lookup_store_key(brand, store)
    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _build_store_list_from_options(raw_options: list[dict]) -> list[dict]:
    """드롭다운 옵션을 중복 제거된 매장 목록으로 변환한다."""
    store_list: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for option in raw_options:
        parsed = _parse_store_option(option)
        if not parsed:
            continue
        key = (
            str(parsed.get("store_id") or ""),
            str(parsed.get("brand") or ""),
            str(parsed.get("store") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        store_list.append(parsed)
    return store_list


KNOWN_STORE_ID_BY_REQUEST = {
    ("도리당", "해운대중동점"): "14902498",
    ("도리당", "법흥리점"): "14352139",
    ("도리당", "송파삼전점"): "14535911",
    ("나홀로", "송파삼전점"): "14778331",
    ("도리당", "동탄영천점"): "14705387",
    ("도리당", "중랑면목점"): "14928824",
    ("도리당", "시흥배곧점"): "14855975",
    ("도리당", "창원내서점"): "14818564",
    ("나홀로", "창원내서점"): "14875710",
    ("도리당", "행신점"): "14658916",
    ("도리당", "강원영월점"): "14853280",
    ("나홀로", "강원영월점"): "14853281",
    ("도리당", "평택비전점"): "14681150",
    ("도리당", "부산장림점"): "14689717",
    ("도리당", "경북상주점"): "14818072",
    ("나홀로", "경북상주점"): "14850832",
}


def _split_requested_store_name(requested_store_name: str) -> tuple[str | None, str]:
    """계정/override 매장명을 (brand, store) 형태로 분해한다."""
    value = str(requested_store_name or "").strip()
    for brand in KNOWN_BRANDS:
        prefix = f"{brand} "
        if value.startswith(prefix):
            return brand, value[len(prefix):].strip()
    return None, value


def _store_list_from_account_hint(account: dict) -> list[dict]:
    requested_store_name = str(account.get("store_name") or "").strip()
    brand, store = _split_requested_store_name(requested_store_name)
    if not store:
        return []

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", store)
    if matches:
        store = matches[-1]
    if brand:
        store = lookup_store_key(brand, store)

    store_id = str(account.get("store_id") or "").strip()
    if not store_id and brand:
        store_id = KNOWN_STORE_ID_BY_REQUEST.get((brand, store), "")

    if not (brand and store and store_id):
        return []
    return [{"store_id": store_id, "brand": brand, "store": store}]


def _normalize_store_token(value: str) -> str:
    """비교용 정규화: 브랜드 접두어 제거 → 공백 제거 → 말단 지점 접미사 제거."""
    text = strip_brand(pd.Series([str(value or "")])).iloc[0]
    text = re.sub(r"\s+", "", str(text)).strip()
    return _BRANCH_SUFFIX_RE.sub("", text)


def _filter_store_list_for_request(store_list: list[dict], requested_store_name: str) -> list[dict]:
    """대표 매장명이 주어지면 같은 지점의 sister brand 옵션까지 함께 남긴다."""
    requested_brand, requested_store = _split_requested_store_name(requested_store_name)
    if not requested_store:
        return list(store_list)

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", requested_store)
    if matches:
        requested_store = matches[-1]
    if requested_brand:
        requested_store = lookup_store_key(requested_brand, requested_store)

    requested_norm = _normalize_store_token(requested_store)

    exact = [
        store_info
        for store_info in store_list
        if _normalize_store_token(store_info.get("store")) == requested_norm
    ]
    if exact:
        return exact

    return [
        store_info
        for store_info in store_list
        if requested_norm
        and (
            requested_norm in _normalize_store_token(store_info.get("store"))
            or _normalize_store_token(store_info.get("store")) in requested_norm
        )
    ]


def collect_now_and_woori(
    account_list: list[dict],
    target_date: str | None = None,
    stability_profile: str | None = None,
    woori_only: bool = False,
    *,
    _allow_login_second_pass: bool = True,
    _raise_on_total_failure: bool = True,
) -> dict:
    profile = resolve_stability_profile(stability_profile)
    metrics = _new_runtime_metrics(profile["name"], account_list)
    success, fail = 0, 0
    failed_accounts: list[dict] = []
    login_lost_accounts: list[dict] = []
    failed_stores: list[dict] = []
    failed_orders: list[dict] = []
    failed_ads: list[dict] = []
    validation_results: list[dict] = []
    ad_store_infos: list[dict] = []
    store_info_per_account_list: list[dict] = []
    collect_shop_change_today = _should_collect_shop_change()

    def _mark_failed_account(account: dict, account_id: str, *, login_stage: bool) -> None:
        failed_accounts.append(account)
        metrics["failed_accounts"].append(account_id)
        if login_stage:
            login_lost_accounts.append(account)

    def _merge_numeric_metrics(source: dict | None) -> None:
        for key in (
            "requested_store_count",
            "browser_launch_count",
            "login_attempt_count",
            "login_failure_count",
            "session_recovery_count",
            "page_timeout_count",
            "account_wait_sec_total",
        ):
            value = (source or {}).get(key)
            if isinstance(value, (int, float)):
                metrics[key] += value

    for account in account_list:
        account_id = account["account_id"]
        requested_store_name = str(account.get("store_name") or "").strip()
        store_list: list[dict] = _store_list_from_account_hint(account)
        used_store_hint = bool(store_list)
        bootstrap_driver = None
        if store_list:
            logger.info(
                "known store_id hint 사용 [%s]: %s",
                account_id,
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )
        else:
            bootstrap_driver = _build_dashboard_session(account, metrics, profile)
            if bootstrap_driver is None:
                fail += 1
                _mark_failed_account(account, account_id, login_stage=True)
                continue

            try:
                bootstrap_driver = _ensure_dashboard_store_select(account, bootstrap_driver, metrics, profile)
                if bootstrap_driver is None:
                    raise RuntimeError(f"main dashboard load failed: {account_id}")

                raw_options = get_store_options(bootstrap_driver)
                store_list = _build_store_list_from_options(raw_options)
                if requested_store_name:
                    store_list = _filter_store_list_for_request(store_list, requested_store_name)
                    if not store_list:
                        available = [
                            f"{parsed['brand']} {parsed['store']}"
                            for parsed in (_parse_store_option(option) for option in raw_options)
                            if parsed
                        ]
                        logger.error(
                            "요청 매장 매칭 실패 [%s]: 요청=%r / 가용 옵션=%s",
                            account_id,
                            requested_store_name,
                            available,
                        )
                        fail += 1
                        _mark_failed_account(account, account_id, login_stage=False)
                        continue
                store_list.sort(key=_store_collection_sort_key)
                logger.info(
                    "수집 매장 순서/store_id: %s",
                    [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
                )
            except Exception as exc:
                logger.error("매장 목록 조회 실패 [%s]: %s", account_id, exc, exc_info=True)
                fail += 1
                _mark_failed_account(account, account_id, login_stage=True)
                store_list = []
            finally:
                if not store_list:
                    quit_driver_safely(bootstrap_driver, account_id)

        if not store_list:
            continue

        metrics["requested_store_count"] += len(store_list)
        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

        driver = bootstrap_driver
        bootstrap_driver = None
        recovery_count = 0
        active_store_ids = {store_info["store_id"] for store_info in store_list}

        def _store_name(store_info: dict) -> str:
            return f"{store_info['brand']} {store_info['store']}"

        def _is_session_issue(exc: Exception) -> bool:
            return _is_recoverable_session_issue(exc)

        def _quit_current_driver() -> None:
            nonlocal driver
            try:
                if driver:
                    logout_baemin(driver, account_id)
            except Exception:
                pass
            quit_driver_safely(driver, account_id)
            driver = None

        def _ensure_driver() -> None:
            nonlocal driver
            if driver is None:
                driver = _build_account_session(account, metrics, profile)
                if driver is None:
                    raise RuntimeError(f"session bootstrap failed: {account_id}")

        def _ensure_dashboard(store_name: str) -> None:
            _ensure_driver()
            if not is_on_main_dashboard(driver.current_url):
                driver.set_page_load_timeout(45)
                driver.get("https://self.baemin.com/")
            else:
                time.sleep(1.0)

            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception:
                    pass
                if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                    metrics["page_timeout_count"] += 1
                    raise RuntimeError(f"dashboard not ready: {store_name}")

        def _run_stage(
            stage_label: str,
            collector,
            *,
            require_dashboard: bool = False,
            fatal: bool = True,
        ) -> None:
            nonlocal recovery_count
            logger.info(
                "=== %s 단계 시작 [%s]: %s ===",
                stage_label,
                account_id,
                [_store_name(s) for s in store_list if s["store_id"] in active_store_ids],
            )
            for store_info in store_list:
                if store_info["store_id"] not in active_store_ids:
                    continue
                store_name = _store_name(store_info)
                for store_attempt in range(2):
                    try:
                        if require_dashboard:
                            _ensure_dashboard(store_name)
                        else:
                            _ensure_driver()
                        logger.info(
                            "=== %s brand=%s [%s / %s] ===",
                            stage_label,
                            store_info.get("brand", ""),
                            account_id,
                            store_name,
                        )
                        collector(store_info)
                        break
                    except Exception as exc:
                        if _is_session_issue(exc):
                            recovery_count += 1
                            metrics["session_recovery_count"] += 1
                            logger.info(
                                "세션 문제 감지, 재생성 시도: %s / %s / %s",
                                account_id,
                                store_name,
                                exc,
                            )
                            _quit_current_driver()
                            if (
                                recovery_count > profile["max_session_recovery_per_account"]
                                or store_attempt >= 1
                            ):
                                if fatal:
                                    active_store_ids.discard(store_info["store_id"])
                                    failed_stores.append({"account": account, "store": store_info})
                                    _record_failed_store(
                                        metrics,
                                        account_id,
                                        store_name,
                                        f"{stage_label} session issue: {exc}",
                                    )
                                else:
                                    logger.info(
                                        "%s 실패(무시): %s / %s / %s",
                                        stage_label,
                                        account_id,
                                        store_name,
                                        exc,
                                    )
                                break
                            continue

                        if fatal:
                            logger.warning("%s 실패: %s / %s / %s", stage_label, account_id, store_name, exc)
                            active_store_ids.discard(store_info["store_id"])
                            failed_stores.append({"account": account, "store": store_info})
                            _record_failed_store(metrics, account_id, store_name, f"{stage_label}: {exc}")
                        else:
                            logger.info("%s 실패(무시): %s / %s / %s", stage_label, account_id, store_name, exc)
                        break

                time.sleep(random.uniform(1.0, 2.0))
            logger.info("=== %s 단계 완료 [%s] ===", stage_label, account_id)

        if used_store_hint:
            logger.info("known store_id hint 경로: NOW 대시보드 수집 스킵 [%s]", account_id)
        else:
            _run_stage(
                "NOW 수집",
                lambda store_info: collect_now_for_driver(driver, account_id, [store_info]),
                require_dashboard=True,
                fatal=False,
            )
        _run_stage(
            "우리가게 수집",
            lambda store_info: collect_woori_for_driver(driver, [store_info]),
            require_dashboard=used_store_hint,
            fatal=False,
        )
        if used_store_hint:
            logger.info("known store_id hint 경로: 변경이력 수집 스킵 [%s]", account_id)
        elif not collect_shop_change_today:
            logger.info("변경이력 수집 스킵: 매주 토요일 전용 [%s]", account_id)
        else:
            _run_stage(
                "변경이력 수집",
                lambda store_info: collect_shop_change_for_driver(driver, [store_info]),
                fatal=False,
            )

        if woori_only:
            logger.info("woori_only=true: 주문내역/광고 funnel 수집 스킵 [%s]", account_id)
        else:
            logger.info(
                "=== 주문내역 수집 단계 시작 [%s]: %s ===",
                account_id,
                [_store_name(s) for s in store_list if s["store_id"] in active_store_ids],
            )
            for store_info in store_list:
                if store_info["store_id"] not in active_store_ids:
                    continue
                store_name = _store_name(store_info)
                for store_attempt in range(2):
                    try:
                        _ensure_driver()
                        logger.info(
                            "=== 주문내역 수집 brand=%s [%s / %s] ===",
                            store_info.get("brand", ""),
                            account_id,
                            store_name,
                        )
                        orders_result = collect_orders_for_driver(driver, store_info, target_date=target_date)
                        if not orders_result.get("ok"):
                            failed_orders.append({"account": account, "stores": [store_info]})
                            _record_failed_store(metrics, account_id, store_name, "orders failed")
                        validation_results.extend(orders_result.get("validation", []))
                        break
                    except Exception as exc:
                        if _is_session_issue(exc):
                            recovery_count += 1
                            metrics["session_recovery_count"] += 1
                            logger.info("세션 문제 감지, 재생성 시도: %s / %s / %s", account_id, store_name, exc)
                            _quit_current_driver()
                            if (
                                recovery_count > profile["max_session_recovery_per_account"]
                                or store_attempt >= 1
                            ):
                                active_store_ids.discard(store_info["store_id"])
                                failed_orders.append({"account": account, "stores": [store_info]})
                                _record_failed_store(metrics, account_id, store_name, f"orders session issue: {exc}")
                                break
                            continue
                        logger.warning("주문내역 수집 실패: %s / %s / %s", account_id, store_name, exc)
                        active_store_ids.discard(store_info["store_id"])
                        failed_orders.append({"account": account, "stores": [store_info]})
                        _record_failed_store(metrics, account_id, store_name, f"orders: {exc}")
                        break
                time.sleep(random.uniform(1.0, 2.0))
            logger.info("=== 주문내역 수집 단계 완료 [%s] ===", account_id)

            if used_store_hint:
                logger.info("known store_id hint 경로: 광고 funnel 수집 스킵 [%s]", account_id)
            else:
                logger.info(
                    "=== 광고 funnel 수집 단계 시작 [%s]: %s ===",
                    account_id,
                    [_store_name(s) for s in store_list if s["store_id"] in active_store_ids],
                )
                for store_info in store_list:
                    if store_info["store_id"] not in active_store_ids:
                        continue
                    store_name = _store_name(store_info)
                    for store_attempt in range(2):
                        try:
                            _ensure_driver()
                            logger.info(
                                "=== 광고 funnel 수집 brand=%s [%s / %s] ===",
                                store_info.get("brand", ""),
                                account_id,
                                store_name,
                            )
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
                            if _is_session_issue(exc):
                                recovery_count += 1
                                metrics["session_recovery_count"] += 1
                                logger.info("세션 문제 감지, 재생성 시도: %s / %s / %s", account_id, store_name, exc)
                                _quit_current_driver()
                                if (
                                    recovery_count > profile["max_session_recovery_per_account"]
                                    or store_attempt >= 1
                                ):
                                    failed_ads.append({"account": account, "stores": [store_info]})
                                    _record_failed_store(
                                        metrics, account_id, store_name, f"ad_funnel session issue: {exc}"
                                    )
                                    break
                                continue
                            logger.warning("광고 funnel 수집 실패: %s / %s / %s", account_id, store_name, exc)
                            failed_ads.append({"account": account, "stores": [store_info]})
                            _record_failed_store(metrics, account_id, store_name, f"ad_funnel: {exc}")
                            break
                    time.sleep(random.uniform(1.0, 2.0))

        if driver is not None:
            try:
                logout_baemin(driver, account_id)
            except Exception:
                pass
            quit_driver_safely(driver, account_id)

        wait_sec = random.uniform(*profile["account_wait_range"])
        metrics["account_wait_sec_total"] += round(wait_sec, 1)
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)
        success += 1

    if _allow_login_second_pass and login_lost_accounts:
        logger.info(
            "로그인/대시보드 단계 유실 계정 second pass 시작: %d개",
            len(login_lost_accounts),
        )
        metrics["login_second_pass_accounts"] += len(login_lost_accounts)
        for account in login_lost_accounts:
            account_id = account["account_id"]
            wait_sec = random.uniform(60.0, 120.0)
            logger.info("second pass 전 대기: %s %.0f초", account_id, wait_sec)
            time.sleep(wait_sec)
            try:
                second_result = collect_now_and_woori(
                    [account],
                    target_date=target_date,
                    stability_profile=profile["name"],
                    woori_only=woori_only,
                    _allow_login_second_pass=False,
                    _raise_on_total_failure=False,
                )
            except Exception as exc:
                logger.warning("second pass 실패 유지: %s / %s", account_id, exc, exc_info=True)
                continue

            second_failed_accounts = second_result.get("failed", {}).get("accounts") or []
            second_failed_ids = {str(item.get("account_id") or "") for item in second_failed_accounts}
            _merge_numeric_metrics(second_result.get("metrics"))
            if account_id not in second_failed_ids:
                failed_accounts[:] = [
                    item for item in failed_accounts if str(item.get("account_id") or "") != account_id
                ]
                metrics["failed_accounts"] = [
                    item for item in metrics["failed_accounts"] if str(item) != account_id
                ]
                fail = max(0, fail - 1)
                success += 1
                metrics["login_second_pass_success"] += 1
                logger.info("second pass 성공 반영: %s", account_id)
            else:
                metrics["login_second_pass_failed"] += 1
                logger.warning("second pass 후에도 계정 유실 유지: %s", account_id)

            second_failed = second_result.get("failed", {})
            failed_stores.extend(second_failed.get("stores") or [])
            failed_orders.extend(second_failed.get("orders") or [])
            failed_ads.extend(second_failed.get("ads") or [])
            validation_results.extend(second_result.get("validation") or [])
            ad_store_infos.extend(second_result.get("ad_stores") or [])
            store_info_per_account_list.extend(second_result.get("store_info_per_account") or [])

    failed_account_ids = {str(account.get("account_id") or "") for account in failed_accounts}
    product_failed_account_ids: set[str] = set()
    for item in failed_stores:
        product_failed_account_ids.add(str((item.get("account") or {}).get("account_id") or ""))
    for item in [*failed_orders, *failed_ads]:
        product_failed_account_ids.add(str((item.get("account") or {}).get("account_id") or ""))
    product_failed_account_ids.discard("")
    partial_account_ids = product_failed_account_ids - failed_account_ids
    metrics["partial_accounts"] = len(partial_account_ids)
    metrics["completed_accounts"] = max(0, success - metrics["partial_accounts"])
    metrics["failed_orders_count"] = sum(len(item.get("stores") or []) for item in failed_orders)
    metrics["failed_ads_count"] = sum(len(item.get("stores") or []) for item in failed_ads)

    total = success + fail
    summary = f"성공 {success}/{total} 계정"
    metrics["ended_at"] = datetime.now(UTC).isoformat()
    logger.info(summary)

    if _raise_on_total_failure and success == 0 and total > 0:
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


def collect_orders_only(
    account_list: list[dict],
    target_date: str | None = None,
    stability_profile: str | None = None,
) -> dict:
    """로그인 후 매장 목록을 확인하고 주문내역(orders)만 수집한다."""
    profile = resolve_stability_profile(stability_profile)
    if target_date is None:
        target_date = pendulum.now(KST).format("YYYY-MM-DD")

    metrics = _new_runtime_metrics(profile["name"], account_list)
    success, fail = 0, 0
    failed_accounts: list[dict] = []
    failed_stores: list[dict] = []
    failed_orders: list[dict] = []
    validation_results: list[dict] = []
    store_info_per_account_list: list[dict] = []

    for account in account_list:
        account_id = account["account_id"]
        requested_store_name = str(account.get("store_name") or "").strip()
        bootstrap_driver = _build_dashboard_session(account, metrics, profile)
        if bootstrap_driver is None:
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            continue

        store_list: list[dict] = []
        try:
            bootstrap_driver = _ensure_dashboard_store_select(account, bootstrap_driver, metrics, profile)
            if bootstrap_driver is None:
                raise RuntimeError(f"main dashboard load failed: {account_id}")

            raw_options = get_store_options(bootstrap_driver)
            store_list = _build_store_list_from_options(raw_options)
            if requested_store_name:
                store_list = _filter_store_list_for_request(store_list, requested_store_name)
                if not store_list:
                    available = [
                        f"{parsed['brand']} {parsed['store']}"
                        for parsed in (_parse_store_option(option) for option in raw_options)
                        if parsed
                    ]
                    logger.error(
                        "orders-only 요청 매장 매칭 실패 [%s]: 요청=%r / 가용 옵션=%s",
                        account_id,
                        requested_store_name,
                        available,
                    )
            store_list.sort(key=_store_collection_sort_key)
            logger.info(
                "orders-only 수집 매장/store_id: %s",
                [f"{s['brand']} {s['store']}#{s['store_id']}" for s in store_list],
            )
        except Exception as exc:
            logger.error("orders-only 매장 목록 조회 실패 [%s]: %s", account_id, exc, exc_info=True)
            store_list = []

        if not store_list:
            _quit_driver_safely(bootstrap_driver)
            fail += 1
            failed_accounts.append(account)
            metrics["failed_accounts"].append(account_id)
            continue

        metrics["requested_store_count"] += len(store_list)
        store_info_per_account_list.append({"account_id": account_id, "stores": list(store_list)})

        driver = bootstrap_driver
        recovery_count = 0
        active_store_ids = {store_info["store_id"] for store_info in store_list}
        for store_info in store_list:
            if store_info["store_id"] not in active_store_ids:
                continue
            store_name = f"{store_info.get('brand', '')} {store_info.get('store', '')}".strip()
            try:
                logger.info(
                    "=== Today 주문내역 수집 brand=%s [%s / %s] ===",
                    store_info.get("brand", ""),
                    account_id,
                    store_name,
                )
                result = collect_orders_for_driver(driver, store_info, target_date=target_date)
            except Exception as exc:
                if _is_recoverable_session_issue(exc):
                    recovery_count += 1
                    metrics["session_recovery_count"] += 1
                    logger.info(
                        "orders-only 세션 문제 감지, 계정 세션 재생성 시도: %s / %s / %s",
                        account_id,
                        store_name,
                        exc,
                    )
                    _quit_driver_safely(driver)
                    if recovery_count > profile["max_session_recovery_per_account"]:
                        active_store_ids.discard(store_info["store_id"])
                        failed_stores.append({"account": account, "store": store_info})
                        _record_failed_store(metrics, account_id, store_name, f"orders session issue: {exc}")
                        failed_orders.append({"account": account, "stores": [store_info]})
                        continue
                    driver = _build_dashboard_session(account, metrics, profile)
                    if driver is not None:
                        try:
                            result = collect_orders_for_driver(
                                driver,
                                store_info,
                                target_date=target_date,
                            )
                        except Exception as retry_exc:
                            logger.warning(
                                "orders-only 재생성 후 수집 실패: %s / %s / %s",
                                account_id,
                                store_name,
                                retry_exc,
                            )
                            active_store_ids.discard(store_info["store_id"])
                            failed_stores.append({"account": account, "store": store_info})
                            _record_failed_store(
                                metrics, account_id, store_name, f"orders retry failed: {retry_exc}"
                            )
                            failed_orders.append({"account": account, "stores": [store_info]})
                            continue
                    else:
                        active_store_ids.discard(store_info["store_id"])
                        failed_stores.append({"account": account, "store": store_info})
                        _record_failed_store(metrics, account_id, store_name, "orders session rebuild failed")
                        failed_orders.append({"account": account, "stores": [store_info]})
                        continue
                else:
                    logger.warning("orders-only 수집 실패: %s / %s / %s", account_id, store_name, exc)
                    active_store_ids.discard(store_info["store_id"])
                    failed_stores.append({"account": account, "store": store_info})
                    _record_failed_store(metrics, account_id, store_name, f"orders: {exc}")
                    failed_orders.append({"account": account, "stores": [store_info]})
                    continue

            if not isinstance(result, dict) or not result.get("ok"):
                failed_orders.append({"account": account, "stores": [store_info]})
            validation_results.extend(result.get("validation", []) if isinstance(result, dict) else [])
            time.sleep(random.uniform(1.0, 2.0))

        _quit_driver_safely(driver)
        success += 1

        wait_sec = random.uniform(*profile["account_wait_range"])
        metrics["account_wait_sec_total"] += round(wait_sec, 1)
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)

    total = success + fail
    summary = f"orders-only 성공 {success}/{total} 계정 (target={target_date})"
    metrics["ended_at"] = datetime.now(UTC).isoformat()
    logger.info(summary)

    if success == 0 and total > 0:
        raise RuntimeError(f"배민 orders-only 전 계정 수집 실패({summary})")

    return {
        "summary": summary,
        "failed": {"orders": failed_orders, "accounts": failed_accounts, "stores": failed_stores},
        "validation": validation_results,
        "store_info_per_account": store_info_per_account_list,
        "metrics": metrics,
    }
