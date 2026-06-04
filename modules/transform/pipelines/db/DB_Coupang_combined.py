"""쿠팡이츠 수집 오케스트레이션.

매장별 독립 Chrome 세션으로 주문내역 수집.
배민 DB_Beamin_combined.py 패턴을 따른다.
"""

import logging
import random
import re
import time

import pandas as pd
import pendulum

from modules.extract.croling_coupang import (
    _clean_cache_only,
    get_store_id_from_url,
    is_driver_crash_error,
    is_login_blocked,
    launch_browser,
    login_coupang,
    release_driver,
)
from modules.transform.pipelines.db.DB_Coupang_01_orders import (
    collect_orders_for_driver,
    save_orders_csv,
)
from modules.transform.utility.paths import ONEDRIVE_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
_CSV_PATH = ONEDRIVE_DB / "sales_employee.csv"

_KNOWN_BRANDS = ["도리당", "나홀로"]
_MAX_COLLECT_ATTEMPTS = 3


def _parse_brand_store(store_name: str) -> tuple[str, str]:
    """'도리당 송파삼전점' → ('도리당', '송파삼전점')."""
    for brand in _KNOWN_BRANDS:
        if store_name.startswith(brand + " "):
            store = store_name[len(brand) + 1:].strip()
            return brand, store
    parts = store_name.split(" ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return store_name, store_name


def load_coupang_accounts(target_stores: list[str], exact: bool = True) -> list[dict]:
    """sales_employee.csv에서 쿠팡이츠 계정 로드.

    반환: [{"account_id", "password", "store_name", "brand", "store"}]
    store_id는 여기서 알 수 없음 → collect 단계에서 URL 추출.
    """
    df = pd.read_csv(_CSV_PATH, dtype=str)
    df = df[df["플랫폼"] == "쿠팡이츠"].copy()

    if target_stores:
        if exact:
            df = df[df["매장명"].isin(target_stores)]
        else:
            pattern = "|".join(target_stores)
            df = df[df["매장명"].str.contains(pattern, na=False)]

    accounts = []
    for _, row in df.iterrows():
        store_name = str(row["매장명"]).strip()
        brand, store = _parse_brand_store(store_name)
        accounts.append({
            "account_id": str(row["계정ID"]).strip(),
            "password":   str(row["계정PW"]).strip(),
            "store_name": store_name,
            "brand": brand,
            "store": store,
        })

    logger.info("쿠팡 계정 로드: %d개 %s", len(accounts), [a["store_name"] for a in accounts])
    return accounts


def collect_orders_for_account(account: dict, target_date: str) -> dict:
    """계정 단위 주문 수집 (독립 Chrome).

    Args:
        account: load_coupang_accounts() 반환값 원소
        target_date: "YYYY-MM-DD"

    Returns:
        {"success", "store_name", "store_id", "validation", "error"}
    """
    account_id = account["account_id"]
    password = account["password"]
    store_name = account["store_name"]
    brand = account["brand"]
    store = account["store"]

    result = {
        "success": False,
        "store_name": store_name,
        "store_id": None,
        "validation": None,
        "error": None,
    }

    driver = None
    try:
        # 배민과 동일: 전체 프로파일 삭제 금지, 캐시만 정리하여 세션 유지
        _clean_cache_only(account_id)
        driver = launch_browser(account_id)

        if not login_coupang(driver, account_id, password):
            logger.warning("[%s] 로그인 실패", account_id)
            result["error"] = "로그인 실패"
            return result

        store_id = get_store_id_from_url(driver, account_id)
        if not store_id:
            result["error"] = "store_id URL 추출 실패"
            return result
        result["store_id"] = store_id
        logger.info(
            "[%s] orders base route resolved store_id=%s for store_name=%s; store_id will be used as collection metadata only",
            account_id,
            store_id,
            store_name,
        )

        store_info = {
            "store_id": store_id,
            "store_name": store_name,
            "brand": brand,
            "account_id": account_id,
        }
        rows, validation = collect_orders_for_driver(driver, store_info, target_date)
        result["validation"] = validation

        if rows:
            save_orders_csv(rows, brand, store, target_date)

        result["success"] = True

    except Exception as e:
        logger.error("[%s] 수집 중 오류: %s", account_id, e, exc_info=True)
        result["error"] = str(e)
    finally:
        if driver:
            release_driver(driver)

    return result


def _build_validation_error(account_id: str, store_name: str, target_date: str, validation: dict | None) -> str | None:
    if not validation:
        return None

    if not validation.get("date_filter_ok"):
        reason = validation.get("date_filter_failure_reason") or "ui date filter failed"
        observed_label = validation.get("observed_date_label") or "<empty>"
        observed_url = validation.get("observed_url") or "<empty>"
        return (
            f"[{account_id}] ui date filter failed for {store_name} target_date={target_date} "
            f"failure_reason={reason} observed_date_label={observed_label} observed_url={observed_url}"
        )

    if validation.get("session_lost"):
        return (
            f"[{account_id}] browser session lost for {store_name} "
            f"page_count={validation.get('page_count', 0)}"
        )

    if validation.get("incomplete_reason"):
        return f"[{account_id}] incomplete collection for {store_name}: {validation['incomplete_reason']}"

    expected = int(validation.get("expected") or 0)
    collected = int(validation.get("collected") or 0)
    if expected > 0 and collected == 0:
        return f"[{account_id}] zero orders collected for {store_name}"

    return None


def collect_orders_for_account(account: dict, target_date: str) -> dict:
    """Collect orders with strict validation; save only verified rows."""
    account_id = account["account_id"]
    password = account["password"]
    store_name = account["store_name"]
    brand = account["brand"]
    store = account["store"]

    result = {
        "success": False,
        "store_name": store_name,
        "store_id": None,
        "validation": None,
        "blocked": False,
        "error": None,
    }

    for attempt in range(1, _MAX_COLLECT_ATTEMPTS + 1):
        driver = None
        try:
            _clean_cache_only(account_id)
            driver = launch_browser(account_id)

            if not login_coupang(driver, account_id, password):
                # 서버측 차단이면 재시도가 차단을 악화시키므로 즉시 중단
                if is_login_blocked(driver):
                    result["blocked"] = True
                    result["error"] = "login blocked (쿠팡 봇탐지/레이트리밋 — 재시도 중단)"
                    logger.warning(
                        "[%s] 로그인 차단 감지 → 재시도 중단 (attempt=%d). 수 시간 쿨다운 필요",
                        account_id,
                        attempt,
                    )
                    return result
                result["error"] = "login failed"
                logger.warning(
                    "[%s] login failed attempt=%d/%d",
                    account_id,
                    attempt,
                    _MAX_COLLECT_ATTEMPTS,
                )
                if attempt < _MAX_COLLECT_ATTEMPTS:
                    time.sleep(8.0)
                    continue
                return result

            store_id = get_store_id_from_url(driver, account_id)
            if not store_id:
                result["error"] = "store_id extraction failed"
                logger.warning(
                    "[%s] store_id extraction failed attempt=%d/%d",
                    account_id,
                    attempt,
                    _MAX_COLLECT_ATTEMPTS,
                )
                if attempt < _MAX_COLLECT_ATTEMPTS:
                    time.sleep(5.0)
                    continue
                return result
            result["store_id"] = store_id
            logger.info(
                "[%s] orders base route resolved store_id=%s for store_name=%s; store_id will be used as collection metadata only",
                account_id,
                store_id,
                store_name,
            )

            store_info = {
                "store_id": store_id,
                "store_name": store_name,
                "brand": brand,
                "account_id": account_id,
            }
            rows, validation = collect_orders_for_driver(driver, store_info, target_date)
            result["validation"] = validation

            validation_error = _build_validation_error(account_id, store_name, target_date, validation)
            if validation_error:
                logger.warning(
                    "[%s] validation failed for %s attempt=%d/%d: %s",
                    account_id,
                    store_name,
                    attempt,
                    _MAX_COLLECT_ATTEMPTS,
                    validation_error,
                )
                result["error"] = validation_error
                if attempt < _MAX_COLLECT_ATTEMPTS:
                    time.sleep(5.0)
                    continue
                return result

            if rows:
                save_orders_csv(rows, brand, store, target_date)

            result["success"] = True
            result["error"] = None
            return result
        except Exception as exc:
            result["error"] = str(exc)
            if is_driver_crash_error(exc) and attempt < _MAX_COLLECT_ATTEMPTS:
                logger.warning(
                    "[%s] Chrome crash attempt=%d/%d, restart full collection: %s",
                    account_id,
                    attempt,
                    _MAX_COLLECT_ATTEMPTS,
                    exc,
                )
                time.sleep(5.0)
                continue
            logger.error(
                "[%s] collection error attempt=%d/%d: %s",
                account_id,
                attempt,
                _MAX_COLLECT_ATTEMPTS,
                exc,
                exc_info=True,
            )
            return result
        finally:
            if driver:
                release_driver(driver)

    return result


def load_coupang_accounts(target_stores: list[str], exact: bool = True) -> list[dict]:
    """Override loader using the current CSV schema."""
    df = pd.read_csv(_CSV_PATH, dtype=str)
    df = df[df["플랫폼"].astype(str).str.strip() == "쿠팡이츠"].copy()

    if target_stores:
        normalized_targets = [str(s).strip() for s in target_stores if str(s).strip()]
        if exact:
            df = df[df["매장명"].astype(str).str.strip().isin(normalized_targets)]
        else:
            pattern = "|".join(re.escape(s) for s in normalized_targets)
            df = df[df["매장명"].astype(str).str.contains(pattern, na=False, regex=True)]

    accounts = []
    for _, row in df.iterrows():
        store_name = str(row["매장명"]).strip()
        brand, store = _parse_brand_store(store_name)
        accounts.append(
            {
                "account_id": str(row["계정ID"]).strip(),
                "password": str(row["계정PW"]).strip(),
                "store_name": store_name,
                "brand": brand,
                "store": store,
            }
        )

    logger.info("쿠팡 계정 로드: %d개 %s", len(accounts), [a["store_name"] for a in accounts])
    return accounts


def collect_all(account_list: list[dict], target_date: str | None = None, force: bool = False) -> dict:
    """전체 계정 순회 수집.

    Returns:
        {"summary": str, "failed": list, "validation": list, "blocked": bool,
         "success_count": int, "total_count": int}
    """
    if not target_date:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    logger.info("쿠팡 수집 시작: %d개 계정, 대상일: %s", len(account_list), target_date)
    if force:
        logger.info("force=True 지정됨: 수동 재실행 요청으로 기록")

    success_list = []
    failed_list = []
    blocked_list = []
    validation_list = []

    for idx, account in enumerate(account_list):
        store_name = account["store_name"]
        logger.info("─── [%d/%d] %s ───", idx + 1, len(account_list), store_name)

        result = collect_orders_for_account(account, target_date)

        if result["success"]:
            success_list.append(store_name)
            if result["validation"]:
                validation_list.append({
                    "store": store_name,
                    **result["validation"],
                })
        elif result.get("blocked"):
            # 차단 계정은 retry 대상에서 제외 (재시도가 차단을 악화시킴)
            blocked_list.append(store_name)
            logger.warning("수집 차단 [%s]: %s", store_name, result.get("error"))
        else:
            failed_list.append(account)
            logger.warning("수집 실패 [%s]: %s", store_name, result.get("error"))

        if idx < len(account_list) - 1:
            time.sleep(random.uniform(2.0, 4.0))

    summary = (
        f"쿠팡 수집 완료: 성공 {len(success_list)}/{len(account_list)} 매장 "
        f"(실패 {len(failed_list)}, 차단 {len(blocked_list)})"
    )
    if blocked_list:
        summary += f" — ⚠️ 차단됨: {blocked_list} (수 시간 후 재실행 권장)"
    logger.info(summary)

    return {
        "summary": summary,
        "failed": failed_list,
        "validation": validation_list,
        "blocked": bool(blocked_list),
        "blocked_stores": blocked_list,
        "success_count": len(success_list),
        "total_count": len(account_list),
    }


def retry_once_failed(
    failed: list[dict],
    target_date: str | None = None,
    force: bool = False,
    return_result: bool = False,
) -> str | dict:
    """실패 계정 1회 재시도.

    Args:
        failed: collect_all() 반환의 failed 리스트
        target_date: "YYYY-MM-DD"
        force: DAG conf의 force 값을 전달받아 수동 재실행 의도를 로그에 남김
        return_result: True이면 Airflow 판정용 구조화 결과를 반환
    """
    if not failed:
        result = {
            "summary": "재시도 대상 없음",
            "success_count": 0,
            "total_count": 0,
            "blocked": False,
        }
        return result if return_result else result["summary"]
    if not target_date:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    logger.info("재시도 시작 전 대기: 15초 (이전 Chrome 종료 대기)")
    if force:
        logger.info("force=True 지정됨: 수동 재시도 요청으로 기록")
    time.sleep(15)
    logger.info("재시도 시작: %d개 계정", len(failed))
    success = 0
    blocked = False
    for account in failed:
        result = collect_orders_for_account(account, target_date)
        if result["success"]:
            success += 1
        elif result.get("blocked"):
            # 재시도 중 차단 감지 → 나머지 중단 (더 때리면 차단 악화)
            logger.warning("재시도 중 차단 감지 [%s] → 재시도 전체 중단", account.get("store_name"))
            blocked = True
            summary = f"재시도 중단: 차단 감지 ({success}/{len(failed)} 성공까지)"
            structured = {
                "summary": summary,
                "success_count": success,
                "total_count": len(failed),
                "blocked": blocked,
            }
            return structured if return_result else summary
        time.sleep(random.uniform(2.0, 4.0))

    summary = f"재시도 완료: {success}/{len(failed)} 성공"
    structured = {
        "summary": summary,
        "success_count": success,
        "total_count": len(failed),
        "blocked": blocked,
    }
    return structured if return_result else summary
