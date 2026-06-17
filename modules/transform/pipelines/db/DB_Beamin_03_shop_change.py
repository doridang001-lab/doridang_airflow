"""배민 매장 변경이력 수집 파이프라인.

수집 흐름:
  로그인 → 매장별 변경이력 페이지 조회 → 테이블 추출 → 로그아웃

수집 URL:
  https://self.baemin.com/history/change/shop

저장 경로 (덮어쓰기):
  analytics/baemin_macro/shop_change/
    brand={brand}/store={store}/ym={YYYY-MM}/shop_change.csv
"""

import logging
import random
import re
import time
from datetime import UTC, datetime
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

from selenium.common.exceptions import WebDriverException
from modules.extract.croling_beamin import (
    TIMING,
    clean_chrome_profile,
    human_click,
    is_driver_crash_error,
    launch_browser,
    login_baemin,
    logout_baemin,
    quit_driver_safely,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_SHOP_CHANGE_DB, BAEMIN_SHOP_OPERATION_DB
from modules.transform.pipelines.db.beamin_store_io import find_tables, read_file, read_table, write_table
from modules.transform.utility.notifier import send_telegram, _send_email_alert
from modules.transform.pipelines.db.beamin_stability import resolve_stability_profile

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]
BRAND_COLLECTION_ORDER = {"나홀로": 0, "도리당": 1}

SHOP_CHANGE_URL = "https://self.baemin.com/history/change/shop"
SHOP_OPERATION_URL = "https://self.baemin.com/shops/{store_id}/manage/operation"
OP_HOUR_P_CSS = "p[class*='ShopOperationHourBody-module']"

OP_CSV_COLUMNS = ['수집일시', '매장명', 'store_id', 'brand', '요일', '운영시간']
SELECT_CSS = "select[class*='Select-module']"
QUERY_BTN_CSS = "button.button.medium.primary.self-ds"
ITEM_CSS = "li.ListItem.self-ds"
_PAGE_SETTLE = 4.0
_MAX_NO_NEW = 6      # 연속 새 항목 없으면 종료
_MAX_ITER = 300      # 최대 스크롤 반복
_COLLECT_DAYS = 60   # 60일 이전 항목은 수집 대상 제외

CSV_COLUMNS = [
    '수집일시', '매장명', 'store_id', '지역명',
    '대분류', '분류', '변경시간', '작업자',
    '변경후_정기휴무일', '변경후_임시휴무일',
    '변경후_사유', '변경후_가게중지', '변경후_배민배달',
    '변경후_운영시간',
    '변경전_정기휴무일', '변경전_임시휴무일',
    '변경전_사유', '변경전_가게중지', '변경전_배민배달',
    '변경전_운영시간',
]


_MAX_BROWSER_RETRY = 3  # Chrome 크래시 시 재시도 횟수


# ⚠️ DEAD CODE 경고 (과거 리팩터 잔재): 아래 함수들은 동일 이름이 파일 뒤쪽에서
#    재정의되어 Python이 항상 "뒤쪽(live) 정의"를 사용한다. 앞쪽 정의는 호출되지 않는다.
#    수정 시 반드시 live 정의를 편집할 것:
#       collect_shop_change       : dead=86  / live=210
#       _wait_for_item_expanded   : dead=662 / live=1257
#       _extract_change_rows      : dead=762 / live=1367   (증분수집 R1 적용 위치)
#       _parse_change_history_item: dead=866 / live=1268
#       _save_csv                 : dead=1172 / live=1496
#    (물리 삭제는 작동 코드 손상 위험이 있어 보류 — 마커로 혼동만 차단)
def _store_collection_sort_key(store_info: dict) -> tuple[int, str, str]:
    return (
        BRAND_COLLECTION_ORDER.get(store_info.get("brand", ""), 99),
        store_info.get("store", ""),
        str(store_info.get("store_id", "")),
    )


def collect_shop_change(account_list: list[dict]) -> str:
    """각 배민 계정에 대해 매장 변경이력을 수집한다 (단독 실행)."""
    success, fail = 0, 0

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        for attempt in range(_MAX_BROWSER_RETRY + 1):
            try:
                clean_chrome_profile(account_id)
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, account["password"]):
                    logger.warning("로그인 실패: %s", account_id)
                    fail += 1
                    break  # 로그인 실패는 재시도 의미 없음

                logger.info("로그인 성공: %s", account_id)

                store_list = _load_shop_change_store_list(
                    driver,
                    account_id,
                    requested_store_names=None,
                )

                if not store_list:
                    logger.warning("수집 대상 브랜드 없음: %s", account_id)
                    fail += 1
                    break

                collect_shop_change_for_driver(driver, store_list)

                logout_baemin(driver, account_id)
                logger.info("로그아웃 완료: %s", account_id)

                wait_sec = random.uniform(*TIMING["logout_wait"])
                logger.info("다음 계정까지 %.0f초 대기", wait_sec)
                time.sleep(wait_sec)

                success += 1
                break  # 성공

            except Exception as e:
                # WebDriverException 또는 urllib3 연결 끊김 → Chrome 크래시로 판정
                is_crash = isinstance(e, WebDriverException) or any(
                    kw in str(e) for kw in (
                        "Remote end closed", "Connection aborted", "RemoteDisconnected",
                        "Connection refused", "Max retries exceeded", "NewConnectionError",
                    )
                )
                if is_crash and attempt < _MAX_BROWSER_RETRY:
                    logger.warning(
                        "Chrome 크래시, %d/%d 재시도: %s | %s",
                        attempt + 1, _MAX_BROWSER_RETRY, account_id, e,
                    )
                    time.sleep(5.0)
                    # 다음 attempt에서 브라우저 새로 실행 (_force_kill_chrome은 finally에서 처리)
                else:
                    level = "처리 실패 (재시도 소진)" if is_crash else "처리 실패"
                    logger.error("%s [%s]: %s", level, account_id, e, exc_info=True)
                    fail += 1
                    break

            finally:
                # 항상 정리 — 재시도 시 다음 iteration 시작에서 새 브라우저 생성
                try:
                    if driver:
                        driver.quit()
                except Exception:
                    pass
                _force_kill_chrome(account_id)  # 크래시 여부 무관하게 항상 정리
                driver = None

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return summary


def _is_driver_crash_error(exc: Exception) -> bool:
    # 공통 키워드 판정 + shop_change는 모든 WebDriverException을 크래시로 간주(보수적).
    return isinstance(exc, WebDriverException) or is_driver_crash_error(exc)
_SHOP_CHANGE_ALERT_LIMIT = 20
_SHOP_CHANGE_ALERT_STATE = {
    "collection_failures": [],
    "parse_failures": [],
}


def _reset_shop_change_alerts() -> None:
    _SHOP_CHANGE_ALERT_STATE["collection_failures"] = []
    _SHOP_CHANGE_ALERT_STATE["parse_failures"] = []


def _push_shop_change_alert(kind: str, target: str, detail: str) -> None:
    bucket = _SHOP_CHANGE_ALERT_STATE.setdefault(kind, [])
    bucket.append({"target": str(target), "detail": str(detail)})


def _flush_shop_change_alerts() -> None:
    collection_failures = _SHOP_CHANGE_ALERT_STATE.get("collection_failures", [])
    parse_failures = _SHOP_CHANGE_ALERT_STATE.get("parse_failures", [])
    if not collection_failures and not parse_failures:
        return

    lines = [
        "DAG: DB_Beamin_Macro_Dags",
        "Task: collect_shop_change",
        f"collection_failures={len(collection_failures)}",
        f"parse_failures={len(parse_failures)}",
    ]
    for entry in collection_failures[:_SHOP_CHANGE_ALERT_LIMIT]:
        lines.append(f"[collect] {entry['target']} | {entry['detail']}")
    if len(collection_failures) > _SHOP_CHANGE_ALERT_LIMIT:
        lines.append(f"[collect] ... {len(collection_failures) - _SHOP_CHANGE_ALERT_LIMIT} more")
    for entry in parse_failures[:_SHOP_CHANGE_ALERT_LIMIT]:
        lines.append(f"[parse] {entry['target']} | {entry['detail']}")
    if len(parse_failures) > _SHOP_CHANGE_ALERT_LIMIT:
        lines.append(f"[parse] ... {len(parse_failures) - _SHOP_CHANGE_ALERT_LIMIT} more")

    body = "\n".join(lines)
    send_telegram(body)


def collect_shop_change(account_list: list[dict]) -> str:
    """Override earlier implementation to isolate dead Chrome sessions per store."""
    success, fail = 0, 0
    store_fail = 0
    _reset_shop_change_alerts()

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        store_list: list[dict] = []
        bootstrap_failed = False
        for attempt in range(_MAX_BROWSER_RETRY + 1):
            try:
                clean_chrome_profile(account_id)
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, account["password"]):
                    logger.warning("로그인 실패: %s", account_id)
                    _push_shop_change_alert("collection_failures", account_id, "login failed")
                    fail += 1
                    bootstrap_failed = True
                    break

                logger.info("로그인 성공: %s", account_id)
                store_list = _load_shop_change_store_list(
                    driver,
                    account_id,
                    requested_store_names=None,
                )
                break
            except Exception as e:
                if _is_driver_crash_error(e) and attempt < _MAX_BROWSER_RETRY:
                    logger.warning(
                        "매장 목록 조회 중 Chrome 크래시, 재시도 %d/%d [%s]: %s",
                        attempt + 1,
                        _MAX_BROWSER_RETRY,
                        account_id,
                        e,
                    )
                    time.sleep(5.0)
                else:
                    logger.error("매장 목록 조회 실패 [%s]: %s", account_id, e, exc_info=True)
                    _push_shop_change_alert("collection_failures", account_id, f"load store list failed: {e}")
                    fail += 1
                    bootstrap_failed = True
                    break
            finally:
                quit_driver_safely(driver, account_id)
                _force_kill_chrome(account_id)  # 프로파일 점유 잔여 프로세스 보강 정리
                driver = None

        if bootstrap_failed:
            continue

        if not store_list:
            logger.warning("수집 대상 브랜드 없음: %s", account_id)
            _push_shop_change_alert("collection_failures", account_id, "no stores to collect")
            fail += 1
            continue

        for store_info in store_list:
            target_label = _format_store_target(store_info)
            store_done = False
            for attempt in range(_MAX_BROWSER_RETRY + 1):
                try:
                    clean_chrome_profile(account_id)
                    driver = launch_browser(account_id)
                    if not login_baemin(driver, account_id, account["password"]):
                        raise RuntimeError(f"login failed for {target_label}")

                    collect_shop_change_for_driver(driver, [store_info])

                    try:
                        logout_baemin(driver, account_id)
                    except Exception:
                        pass
                    logger.info("store 변경이력 수집 완료: %s", target_label)
                    store_done = True
                    break
                except Exception as e:
                    if attempt < _MAX_BROWSER_RETRY:
                        logger.warning(
                            "store 변경이력 수집 중 오류 발생, 재시도 %d/%d %s | %s",
                            attempt + 1,
                            _MAX_BROWSER_RETRY,
                            target_label,
                            e,
                        )
                        time.sleep(5.0)
                        continue

                    logger.error(
                        "store 변경이력 수집 최종 실패 [%s]: %s",
                        target_label,
                        e,
                        exc_info=True,
                    )
                    _push_shop_change_alert("collection_failures", target_label, str(e))
                    store_fail += 1
                    break
                finally:
                    try:
                        if driver:
                            driver.quit()
                    except Exception:
                        pass
                    _force_kill_chrome(account_id)
                    driver = None

            if not store_done:
                logger.warning("store 건너뜀: %s", target_label)

        wait_sec = random.uniform(*TIMING["logout_wait"])
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)
        success += 1

    summary = f"성공 {success}/{success + fail} 계정 / store_fail={store_fail}"
    _flush_shop_change_alerts()
    logger.info(summary)
    if fail > 0 or store_fail > 0:
        raise RuntimeError(f"collect_shop_change partial failure: {summary}")
    return summary


def collect_shop_change_for_driver(driver, store_list: list[dict]) -> None:
    """이미 로그인된 driver로 매장 변경이력을 수집한다 (login/logout 없음).

    combined 파이프라인에서 같은 브라우저 세션을 공유할 때 사용.
    매장 루프마다 페이지를 재방문해 StaleElementReferenceException을 방지한다.
    """
    # 변경이력 페이지 최초 진입해 select 옵션 교차 검증
    try:
        driver.set_page_load_timeout(5)
        driver.get("about:blank")  # 이전 DOM 해제
    except Exception:
        pass
    try:
        driver.set_page_load_timeout(25)
        driver.get(SHOP_CHANGE_URL)
    except Exception as e:
        logger.warning("변경이력 페이지 첫 진입 타임아웃 (계속): %s", e)

    if not wait_for_page(driver, SELECT_CSS, timeout=30):
        logger.warning("변경이력 페이지 select 로드 실패 → 수집 건너뜀")
        return

    page_options = _get_page_options(driver)  # {store_id: text}
    matched_stores = sorted(
        _dedupe_store_targets([s for s in store_list if s["store_id"] in page_options]),
        key=_store_collection_sort_key,
    )

    if not matched_stores:
        logger.warning(
            "변경이력 페이지에 수집 대상 매장 없음 (page=%s)", list(page_options.keys())
        )
        return

    logger.info(
        "변경이력 수집 대상 매장: %s",
        [_format_store_target(s) for s in matched_stores],
    )
    logger.info(
        "변경이력 수집 매장 순서: %s",
        [f"{s['brand']} {s['store']}" for s in matched_stores],
    )

    for index, store_info in enumerate(matched_stores):
        store_id = store_info["store_id"]
        target_label = _format_store_target(store_info)
        logger.info("변경이력 수집 시작: %s", target_label)

        if index > 0 and not _ensure_shop_change_page(driver, target_label):
            logger.warning("select 로드 실패, 건너뜀: %s", target_label)
            continue

        try:
            sel_elem = driver.find_element(By.CSS_SELECTOR, SELECT_CSS)
            Select(sel_elem).select_by_value(store_id)
            time.sleep(1.0)

            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, QUERY_BTN_CSS))
            )
            human_click(driver, btn)
            logger.info("조회 클릭 완료: %s", target_label)

        except Exception as e:
            if _is_driver_crash_error(e):
                raise
            logger.warning("매장 선택/조회 실패 (%s): %s", target_label, e)
            continue

        result_state = _wait_for_change_results(driver, target_label)
        if result_state != "ready":
            logger.warning("조회 결과 없음 또는 로드 실패: %s (%s)", target_label, result_state)
            continue
        time.sleep(0.5)  # React 초기 렌더링 안정화 대기
        full_name = _get_full_name(page_options.get(store_id, store_info["store"]))
        rows = _extract_change_rows(driver, store_info, full_name)

        if rows:
            saved = _save_csv(rows, store_info["brand"], store_info["store"])
            logger.info(
                "저장 완료: %s -> %s (%d건)",
                target_label,
                saved,
                len(rows),
            )
        else:
            logger.warning("데이터 없음: %s", target_label)

        time.sleep(random.uniform(1.5, 3.0))

    # 변경이력 수집 완료 후 운영시간 수집
    logger.info("운영시간 수집 시작: %d개 매장", len(matched_stores))
    for store_info in matched_stores:
        full_name = _get_full_name(page_options.get(store_info["store_id"], store_info["store"]))
        try:
            op_rows = _collect_one_store_operation(driver, store_info, full_name)
        except Exception as e:
            if _is_driver_crash_error(e):
                raise
            logger.warning("운영시간 수집 실패: %s | %s", _format_store_target(store_info), e)
            op_rows = []
        if op_rows:
            _save_operation_csv(op_rows, store_info["brand"], store_info["store"])
        else:
            logger.warning("운영시간 데이터 없음: %s", _format_store_target(store_info))
        time.sleep(random.uniform(1.0, 2.0))

    # 월간 영업시간·영업일수 계산
    from modules.transform.pipelines.db.DB_Beamin_monthly_operation import compute_monthly_operation  # noqa: PLC0415
    ym = pendulum.now(KST).format("YYYY-MM")
    for store_info in matched_stores:
        try:
            compute_monthly_operation(
                store_info["brand"], store_info["store"], store_info["store_id"], ym
            )
        except Exception as e:
            logger.warning("월간 운영 요약 실패 (%s): %s", _format_store_target(store_info), e)


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------

def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None.

    한글 단어 경계 체크: '곱도리당'이 '도리당'으로 매칭되는 것을 방지.
    앞뒤에 한글이 없는 경우에만 브랜드로 인정.
    """
    text = opt.get("text", "")
    brand = next(
        (b for b in KNOWN_BRANDS
         if re.search(rf"(?<![가-힣]){re.escape(b)}(?![가-힣])", text)),
        None,
    )
    if not brand:
        return None

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _load_shop_change_store_list(
    driver,
    account_id: str,
    requested_store_names: list[str] | None = None,
) -> list[dict]:
    """변경이력 페이지 select 옵션에서 대상 매장 목록 구성."""
    try:
        driver.set_page_load_timeout(25)
        driver.get(SHOP_CHANGE_URL)
    except Exception as e:
        logger.warning("변경이력 페이지 진입 타임아웃 (계속 진행): %s", e)

    if not wait_for_page(driver, SELECT_CSS, timeout=30):
        logger.warning("변경이력 페이지 select 로드 실패: %s", account_id)
        return []

    raw_options = [
        {"store_id": store_id, "text": text}
        for store_id, text in _get_page_options(driver).items()
        if store_id
    ]
    store_list = []
    for option in raw_options:
        store_info = _parse_store_option(option)
        if not store_info:
            continue
        if requested_store_names and not _matches_requested_store(
            store_info, option["text"], requested_store_names
        ):
            continue
        store_list.append(store_info)

    return sorted(
        _dedupe_store_targets([s for s in store_list if s]),
        key=_store_collection_sort_key,
    )


def _normalize_store_match_text(value: str) -> str:
    """매장명 비교용: 공백/구분자/ID 차이를 제거한다."""
    value = re.sub(r"\d+", "", value or "")
    return re.sub(r"[^0-9A-Za-z가-힣]+", "", value)


def _matches_requested_store(
    store_info: dict,
    option_text: str,
    requested_store_names: list[str],
) -> bool:
    option_key = _normalize_store_match_text(
        f'{option_text} {store_info["brand"]} {store_info["store"]}'
    )
    brand = _normalize_store_match_text(store_info["brand"])
    store = _normalize_store_match_text(store_info["store"])

    for requested in requested_store_names:
        requested_key = _normalize_store_match_text(requested)
        if not requested_key:
            continue
        if requested_key in option_key or option_key in requested_key:
            return True
        requested_has_brand = any(
            _normalize_store_match_text(known) in requested_key
            for known in KNOWN_BRANDS
        )
        if store and store in requested_key:
            if not requested_has_brand or brand in requested_key:
                return True
    return False


def _ensure_shop_change_page(driver, target_label: str) -> bool:
    """현재 세션에서 변경이력 페이지 select 사용 가능 여부 확인, 필요 시 1회 재진입."""
    if wait_for_page(driver, SELECT_CSS, timeout=10):
        return True

    try:
        driver.set_page_load_timeout(25)
        driver.get(SHOP_CHANGE_URL)
    except Exception as e:
        logger.warning("변경이력 페이지 재진입 타임아웃 (%s): %s", target_label, e)

    return wait_for_page(driver, SELECT_CSS, timeout=30)


def _dedupe_store_targets(store_list: list[dict]) -> list[dict]:
    """같은 (brand, store_id) 중복 옵션만 제거하고 순서는 유지."""
    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for store_info in store_list:
        key = (store_info["brand"], store_info["store_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(store_info)
    return deduped


def _format_store_target(store_info: dict) -> str:
    """로그용 매장 식별자."""
    return f'{store_info["brand"]}/{store_info["store"]}/{store_info["store_id"]}'


def _get_page_options(driver) -> dict[str, str]:
    """변경이력 페이지 select에서 {store_id: text} 반환."""
    return driver.execute_script(r"""
        const sel = document.querySelector('select[class*="Select-module"]');
        if (!sel) return {};
        const res = {};
        for (const o of sel.options) res[o.value] = o.textContent.trim();
        return res;
    """) or {}


def _get_full_name(option_text: str) -> str:
    """드롭다운 옵션 텍스트에서 매장 상호명 추출.
    '[브랜드] 상호명 14364235' → '상호명'
    """
    # 끝의 숫자(ID) 제거
    name = re.sub(r'\s*\d+\s*$', '', option_text).strip()
    # 앞의 [브랜드] 제거
    name = re.sub(r'^\[.*?\]\s*', '', name).strip()
    return name or option_text


def _wait_for_change_results(driver, target_label: str, timeout: float = 20.0) -> str:
    """Wait until the first change-history item is actually rendered."""
    logger.info("조회 결과 대기 시작: %s", target_label)

    start = time.monotonic()
    saw_loading = False
    logged_virtual = False

    while time.monotonic() - start < timeout:
        try:
            item_count = len(driver.find_elements(By.CSS_SELECTOR, ITEM_CSS))
            virtual_count = len(driver.find_elements(By.CSS_SELECTOR, "div[data-index]"))
            loading = driver.execute_script(
                """
                const text = document.body ? document.body.innerText : "";
                return ['\uB85C\uB529', '\uBD88\uB7EC\uC624\uB294 \uC911', '\uC870\uD68C \uC911'].some((keyword) => text.includes(keyword));
                """
            )
        except StaleElementReferenceException:
            time.sleep(0.2)
            continue
        except Exception:
            item_count = 0
            virtual_count = 0
            loading = False

        if item_count > 0:
            logger.info("첫 항목 감지: %s", target_label)
            return "ready"

        if loading and not saw_loading:
            logger.info("조회 결과 로딩 시작: %s", target_label)
            saw_loading = True

        if virtual_count > 0 and not logged_virtual:
            logger.info("가상 리스트 컨테이너 감지: %s (%d개)", target_label, virtual_count)
            logged_virtual = True

        if not loading and saw_loading and virtual_count == 0:
            break

        time.sleep(0.5)

    if saw_loading:
        logger.info("조회 결과 없음: %s", target_label)
        return "empty"

    logger.warning("조회 결과 대기 시간 초과: %s", target_label)
    return "timeout"


def _get_item_summary(item) -> tuple[str, str, str]:
    """Extract title, date and item key from a visible list item."""
    title_els = item.find_elements(By.CSS_SELECTOR, "h5.flex-1")
    date_els = item.find_elements(By.CSS_SELECTOR, "time.ListItem-date")
    title = title_els[0].text.strip() if title_els else ""
    date = date_els[0].get_attribute("date") if date_els else ""
    return title, date, f"{title}|{date}"


def _wait_for_item_expanded(item, attempts: int = 10, delay: float = 0.2) -> bool:
    """Poll until the expanded detail panel is visible AND has rendered text."""
    for _ in range(attempts):
        try:
            contents = item.find_elements(By.CSS_SELECTOR, ".ListItem-content.on")
            if contents:
                content = contents[0]
                if (content.get_attribute("innerText") or "").strip():
                    return True
        except StaleElementReferenceException:
            return False
        except Exception as e:
            if _is_driver_crash_error(e):
                raise
            return False
        time.sleep(delay)
    return False


def _extract_section_texts(content) -> tuple[str, str]:
    """Read after/before text from ZwKd section.

    DOM structure (2026-05 확인):
      <div class="ZwKd">
        <div>
          <div>[변경 후]</div>   ← sub[0]: label
          <div>내용...</div>     ← sub[1]: value
        </div>
        <div class="fKIx">
          <div>[변경 전]</div>   ← sub[0]: label
          <div>내용...</div>     ← sub[1]: value
        </div>
      </div>
    """
    try:
        change_contents = content.find_element(
            By.CSS_SELECTOR, '[class*="HistoryItemContents-module__ZwKd"]'
        )
    except Exception:
        return "", ""

    after_text = ""
    before_text = ""
    for section in change_contents.find_elements(By.XPATH, "./*"):
        sub = section.find_elements(By.XPATH, "./*")
        if len(sub) < 2:
            continue
        lbl = (sub[0].get_attribute("innerText") or "").strip()
        val = (sub[1].get_attribute("innerText") or "").strip()
        if "[변경 후]" in lbl:
            after_text = val
        elif "[변경 전]" in lbl:
            before_text = val

    return after_text, before_text


def _force_kill_chrome(account_id: str) -> None:
    """크래시 후 프로파일 디렉토리를 점유한 Chrome 프로세스를 강제 종료한다."""
    import subprocess
    try:
        subprocess.run(
            ["pkill", "-f", f"chrome_profiles/{account_id}"],
            capture_output=True, timeout=5,
        )
        logger.info("Chrome 프로세스 강제 종료: %s", account_id)
    except Exception as e:
        logger.warning("Chrome 강제 종료 실패 (무시): %s | %s", account_id, e)


def _scroll_to_top(driver) -> None:
    """Scroll the virtualized list container to the top."""
    driver.execute_script(
        """
        const container = document.querySelector('div[style*="overflow"]');
        if (container) {
            container.scrollTop = 0;
            return;
        }
        window.scrollTo(0, 0);
        """
    )


def _scroll_by(driver, offset: int) -> None:
    """Scroll the virtualized list container if present, otherwise the window."""
    driver.execute_script(
        """
        const offset = arguments[0];
        const container = document.querySelector('div[style*="overflow"]');
        if (container) {
            container.scrollTop += offset;
            return;
        }
        window.scrollBy(0, offset);
        """,
        offset,
    )


def _extract_change_rows(driver, store_info: dict, full_name: str) -> list[dict]:
    """Collect virtualized change-history items one by one."""
    title_pause = "영업임시중지"
    title_holiday = "휴무일"
    title_op_time = "운영시간"

    iso = pendulum.now(KST).isoformat()
    area_name = store_info["store"]
    target_label = _format_store_target(store_info)
    processed: set[str] = set()
    failed: set[str] = set()
    all_rows: list[dict] = []
    no_new = 0

    _scroll_to_top(driver)
    time.sleep(0.3)

    for _ in range(_MAX_ITER):
        try:
            items = driver.find_elements(By.CSS_SELECTOR, ITEM_CSS)
        except Exception:
            items = []

        target_item = None
        target_key = ""
        target_title = ""

        for item in items:
            try:
                title, _, item_key = _get_item_summary(item)
                if item_key in processed or item_key in failed:
                    continue

                is_pause = title_pause in title
                is_holiday = title_holiday in title
                is_op_time = title_op_time in title
                if not is_pause and not is_holiday and not is_op_time:
                    processed.add(item_key)
                    continue

                target_item = item
                target_key = item_key
                target_title = title
                break
            except StaleElementReferenceException:
                target_item = None
                break

        if target_item is None:
            no_new += 1
            if no_new >= _MAX_NO_NEW:
                break
            logger.info("스크롤 후 다음 항목 대기: %s (%d/%d)", target_label, no_new, _MAX_NO_NEW)
            _scroll_by(driver, 400)
            time.sleep(random.uniform(0.3, 0.5))
            continue

        no_new = 0

        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                target_item,
            )
            time.sleep(random.uniform(0.15, 0.25))
            logger.info("항목 확장 시도: %s -> %s", target_label, target_title)

            # .ListItem-content.on: 두 클래스 모두 있는 요소만 (CSS 선택자로 정확히 체크)
            expanded_divs = target_item.find_elements(By.CSS_SELECTOR, ".ListItem-content.on")
            is_expanded = bool(expanded_divs) and any(
                (div.get_attribute("innerText") or "").strip() for div in expanded_divs
            )

            if not is_expanded:
                # 타이틀 클릭으로 상세 패널 펼치기
                title_els = target_item.find_elements(By.CSS_SELECTOR, "h5.flex-1")
                click_target = title_els[0] if title_els else target_item
                human_click(driver, click_target)
                time.sleep(0.8)  # React 렌더 시작 대기
                if not _wait_for_item_expanded(target_item):
                    # innerText 로드가 느린 경우 — 건너뛰지 않고 파싱 계속 시도
                    logger.warning("항목 확장 대기 초과, 파싱 시도: %s -> %s", target_label, target_key)

            logger.info("항목 확장 완료: %s -> %s", target_label, target_title)
            row = _parse_change_history_item(target_item, iso, store_info, full_name, area_name)
            if row:
                all_rows.append(row)
                processed.add(target_key)
                logger.info("항목 파싱 완료: %s -> %s", target_label, target_title)
            else:
                logger.warning("항목 파싱 실패, 계속 진행: %s -> %s", target_label, target_key)
                failed.add(target_key)

            logger.info("스크롤 후 다음 항목 대기: %s", target_label)
            _scroll_by(driver, 300)
            time.sleep(random.uniform(0.2, 0.4))
        except StaleElementReferenceException:
            time.sleep(0.2)
            continue

    logger.info("변경이력 추출 완료: %s -> %d행", target_label, len(all_rows))
    return all_rows


def _parse_change_history_item(
    item, iso: str, store_info: dict, full_name: str, area_name: str
) -> dict | None:
    """Parse one expanded change-history item."""
    label_category = "분류"
    label_changed_at = "변경시간"
    label_worker = "작업자"
    title_pause = "영업임시중지"
    title_holiday = "휴무일"
    title_op_time = "운영시간"
    title, date_attr, _ = _get_item_summary(item)
    change_datetime = date_attr

    try:
        content = item.find_element(By.CSS_SELECTOR, ".ListItem-content.on")
    except Exception:
        content = None

    category = ""
    worker = ""

    if content is not None:
        try:
            details_div = content.find_element(
                By.CSS_SELECTOR, '[class*="HistoryItemContents-module__rs7S"]'
            )
        except Exception:
            details_div = None

        if details_div is not None:
            detail_rows = details_div.find_elements(
                By.CSS_SELECTOR, '[class*="HistoryItemContents-module__Zcx3"]'
            ) or details_div.find_elements(By.XPATH, ".//*")

            for row in detail_rows:
                text = (row.get_attribute("innerText") or "").strip()
                if not text:
                    continue
                label_els = row.find_elements(
                    By.CSS_SELECTOR, '[class*="HistoryItemContents-module__sGh2"]'
                )
                value_els = row.find_elements(
                    By.CSS_SELECTOR, '[class*="HistoryItemContents-module__FXZ7"]'
                )
                if label_els and value_els:
                    label = (label_els[0].get_attribute("innerText") or "").strip()
                    value = " ".join(
                        (v.get_attribute("innerText") or "").strip()
                        for v in value_els
                        if (v.get_attribute("innerText") or "").strip()
                    )
                else:
                    parts = [part.strip() for part in text.splitlines() if part.strip()]
                    if len(parts) < 2:
                        continue
                    label = parts[0]
                    value = " ".join(parts[1:])
                if label == label_category:
                    category = value
                elif label == label_changed_at:
                    change_datetime = value
                elif label == label_worker:
                    worker = value
        else:
            logger.warning("상세 메타 영역 없음: %s -> %s", _format_store_target(store_info), title)

    after_text = ""
    before_text = ""
    if content is not None:
        after_text, before_text = _extract_section_texts(content)

    content_text = (content.get_attribute("innerText") or "").strip() if content is not None else ""
    if (not after_text and not before_text) and content_text:
        after_text, before_text = _split_after_before_text(content_text)

    if not category and content_text:
        category = _extract_labeled_value(content_text, label_category)
    if not worker and content_text:
        worker = _extract_labeled_value(content_text, label_worker)
    if not change_datetime and content_text:
        change_datetime = _extract_labeled_value(content_text, label_changed_at) or date_attr

    base = {column: "" for column in CSV_COLUMNS}
    base["수집일시"] = iso
    base["매장명"] = full_name
    base["store_id"] = store_info["store_id"]
    base["지역명"] = area_name
    base["대분류"] = title
    base["분류"] = category
    base["변경시간"] = change_datetime
    base["작업자"] = worker

    if not category and not worker and not after_text and not before_text and not content_text:
        logger.warning("확장된 상세 텍스트 없음: %s -> %s", _format_store_target(store_info), title)
        return base

    if not category and not worker and not after_text and not before_text:
        logger.warning(
            "상세 파싱 데이터 비어있음: %s -> %s",
            _format_store_target(store_info),
            title,
        )

    if title_pause in title:
        after_d = _parse_business_pause_data(after_text or content_text)
        before_d = _parse_business_pause_data(before_text)
        base["변경후_사유"] = after_d["reason"]
        base["변경후_가게중지"] = after_d["store_closure"]
        base["변경후_배민배달"] = after_d["baemin_delivery"]
        base["변경전_사유"] = before_d["reason"]
        base["변경전_가게중지"] = before_d["store_closure"]
        base["변경전_배민배달"] = before_d["baemin_delivery"]
    elif title_holiday in title:
        after_h = _parse_holiday_data(after_text or content_text)
        before_h = _parse_holiday_data(before_text)
        base["변경후_정기휴무일"] = after_h["regular"]
        base["변경후_임시휴무일"] = after_h["temp"]
        base["변경전_정기휴무일"] = before_h["regular"]
        base["변경전_임시휴무일"] = before_h["temp"]
    elif title_op_time in title:
        base["변경후_운영시간"] = after_text or content_text
        base["변경전_운영시간"] = before_text

    return base


def _split_after_before_text(content_text: str) -> tuple[str, str]:
    """Fallback split for expanded content text."""
    after_text = ""
    before_text = ""

    after_match = re.search(r"\[변경 후\]\s*(.*?)(?=\[변경 전\]|\Z)", content_text, re.DOTALL)
    before_match = re.search(r"\[변경 전\]\s*(.*)\Z", content_text, re.DOTALL)

    if after_match:
        after_text = after_match.group(1).strip()
    if before_match:
        before_text = before_match.group(1).strip()

    return after_text, before_text


def _extract_labeled_value(content_text: str, label: str) -> str:
    """Fallback parse for `분류`, `변경시간`, `작업자` labels from raw text."""
    lines = [line.strip() for line in content_text.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if line == label and index + 1 < len(lines):
            return lines[index + 1]
        if line.startswith(f"{label} :"):
            return line.split(":", 1)[1].strip()
        if line.startswith(f"{label}:"):
            return line.split(":", 1)[1].strip()
        if line.startswith(f"{label} ："):
            return line.split("：", 1)[1].strip()
    return ""


def _parse_business_pause_data(text: str) -> dict:
    """영업임시중지 변경 전/후 텍스트에서 사유 + 배민배달 시간 파싱.

    실제 DOM 구조 (2026-05 확인):
      영업 임시중지 사유 : 가게사정
      가게 영업 임시중지
      - 시간 : 26. 5. 27. 11:37 ~ 26. 5. 27. 12:37
      주문유형 : 배민배달
      - 시간 : 26. 5. 27. 11:37 ~ 26. 5. 27. 12:37
      주문유형 : 가게배달  ← 수집 안 함
      주문유형 : 픽업      ← 수집 안 함
    """
    result = {"reason": "없음", "store_closure": "없음", "baemin_delivery": "없음"}
    if not text:
        return result

    reason_match = re.search(r"영업 임시중지 사유\s*[:：]\s*(.+)", text)
    if reason_match:
        result["reason"] = reason_match.group(1).strip()

    time_pattern = re.compile(
        r"시간\s*[:：]\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2}):(\d{2})"
        r"\s*~\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2}):(\d{2})"
    )

    def _extract_time(section: str) -> str:
        if "없음" in section:
            return "없음"
        m = time_pattern.search(section)
        if not m:
            return "없음"
        y1, mo1, d1, h1, min1, y2, mo2, d2, h2, min2 = m.groups()
        return (
            f"{y1.zfill(2)}.{mo1.zfill(2)}.{d1.zfill(2)} {h1.zfill(2)}:{min1}"
            f" ~ {y2.zfill(2)}.{mo2.zfill(2)}.{d2.zfill(2)} {h2.zfill(2)}:{min2}"
        )

    # 가게 전체 임시중지 시간
    store_match = re.search(
        r"가게 영업 임시중지\s*\n(.*?)(?=\n주문유형|\Z)",
        text, re.DOTALL
    )
    if store_match:
        result["store_closure"] = _extract_time(store_match.group(1).strip())

    # 배민배달 / 과거 명칭 알뜰·한집배달
    baemin_match = re.search(
        r"주문유형\s*:\s*(?:배민배달|알뜰[··]한집배달)\s*\n(.*?)(?=\n주문유형|\Z)",
        text, re.DOTALL
    )
    if baemin_match:
        result["baemin_delivery"] = _extract_time(baemin_match.group(1).strip())

    return result


def _parse_holiday_data(text: str) -> dict:
    """휴무일 변경 전/후 텍스트에서 정기휴무일 + 임시휴무일 파싱.

    실제 DOM 구조 (2026-05 확인):
      정기휴무일
      - 매주 화요일
      임시휴무일
      - 2026-05-28 ~ 2026-05-28
    """
    result = {"regular": "", "temp": ""}
    if not text:
        return result

    regular_match = re.search(r"정기휴무일\s*\n(.*?)(?=\n임시휴무일|\Z)", text, re.DOTALL)
    if regular_match:
        lines = [l.strip().lstrip("- ") for l in regular_match.group(1).splitlines() if l.strip()]
        result["regular"] = ", ".join(lines)

    temp_match = re.search(r"임시휴무일\s*\n(.*?)\Z", text, re.DOTALL)
    if temp_match:
        lines = [l.strip().lstrip("- ") for l in temp_match.group(1).splitlines() if l.strip()]
        result["temp"] = ", ".join(lines)

    return result


def _collect_one_store_operation(driver, store_info: dict, full_name: str) -> list[dict]:
    """매장 운영시간 페이지에서 요일/시간 쌍을 수집한다."""
    store_id = store_info["store_id"]
    url = SHOP_OPERATION_URL.format(store_id=store_id)
    try:
        driver.set_page_load_timeout(25)
        driver.get(url)
    except Exception as e:
        logger.warning("운영시간 페이지 로드 타임아웃 (%s): %s", store_id, e)

    if not wait_for_page(driver, OP_HOUR_P_CSS, timeout=15):
        logger.warning("운영시간 요소 로드 실패: %s", store_id)
        return []

    iso = pendulum.now(KST).isoformat()
    rows = []
    try:
        p_els = driver.find_elements(By.CSS_SELECTOR, OP_HOUR_P_CSS)
        for p_el in p_els:
            day = (p_el.get_attribute("innerText") or "").strip()
            try:
                parent = p_el.find_element(By.XPATH, "..")
                time_els = parent.find_elements(By.CSS_SELECTOR, "time")
                hours = (time_els[0].get_attribute("innerText") or "").strip() if time_els else ""
            except Exception:
                hours = ""
            if day:
                rows.append({
                    "수집일시": iso,
                    "매장명": full_name,
                    "store_id": store_id,
                    "brand": store_info["brand"],
                    "요일": day,
                    "운영시간": hours,
                })
    except Exception as e:
        logger.warning("운영시간 파싱 오류 (%s): %s", store_id, e)

    logger.info("운영시간 수집: %s -> %d행", store_id, len(rows))
    return rows


def _save_operation_csv(rows: list[dict], brand: str, store: str) -> Path:
    """운영시간 CSV에 수집일시 기준 월 파티션으로 누적 저장."""
    ym = pendulum.now(KST).format("YYYY-MM")
    out_dir = BAEMIN_SHOP_OPERATION_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "shop_operation.csv"

    new_df = pd.DataFrame(rows, columns=OP_CSV_COLUMNS).astype(str)

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        for col in OP_CSV_COLUMNS:
            if col not in existing.columns:
                existing[col] = ""
        new_isos = set(new_df["수집일시"])
        mask = ~existing["수집일시"].isin(new_isos)
        combined = pd.concat([existing[mask][OP_CSV_COLUMNS], new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("운영시간 CSV 저장: %s -> %s (%d행)", store, out_path, len(combined))
    return out_path


def collect_shop_change(account_list: list[dict], stability_profile: str | None = None) -> dict:
    profile = resolve_stability_profile(stability_profile)
    success, fail = 0, 0
    store_fail = 0
    metrics = {
        "profile": profile["name"],
        "started_at": datetime.now(UTC).isoformat(),
        "total_accounts": len(account_list),
        "browser_launch_count": 0,
        "login_attempt_count": 0,
        "login_failure_count": 0,
        "session_recovery_count": 0,
        "account_wait_sec_total": 0.0,
        "failed_accounts": [],
        "failed_stores": [],
    }
    _reset_shop_change_alerts()

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        try:
            metrics["browser_launch_count"] += 1
            driver = launch_browser(account_id)
            metrics["login_attempt_count"] += 1
            if not login_baemin(driver, account_id, account["password"]):
                metrics["login_failure_count"] += 1
                raise RuntimeError(f"login failed: {account_id}")

            requested_store_names = [str(account.get("store_name") or "").strip()] if account.get("store_name") else None
            store_list = _load_shop_change_store_list(
                driver,
                account_id,
                requested_store_names=requested_store_names,
            )
            if not store_list:
                raise RuntimeError(f"no stores to collect: {account_id}")

            for index, store_info in enumerate(store_list, start=1):
                try:
                    collect_shop_change_for_driver(driver, [store_info])
                except Exception as exc:
                    if _is_driver_crash_error(exc):
                        metrics["session_recovery_count"] += 1
                        quit_driver_safely(driver, account_id)
                        _force_kill_chrome(account_id)
                        metrics["browser_launch_count"] += 1
                        driver = launch_browser(account_id)
                        metrics["login_attempt_count"] += 1
                        if not login_baemin(driver, account_id, account["password"]):
                            metrics["login_failure_count"] += 1
                            raise RuntimeError(f"session recovery login failed: {account_id}") from exc
                        collect_shop_change_for_driver(driver, [store_info])
                    else:
                        store_fail += 1
                        metrics["failed_stores"].append(
                            {"account_id": account_id, "store_name": f"{store_info['brand']} {store_info['store']}", "reason": str(exc)}
                        )
                        _push_shop_change_alert("collection_failures", _format_store_target(store_info), str(exc))

                if index % int(profile["driver_restart_every_stores"]) == 0:
                    try:
                        logout_baemin(driver, account_id)
                    except Exception:
                        pass
                    quit_driver_safely(driver, account_id)
                    _force_kill_chrome(account_id)
                    metrics["browser_launch_count"] += 1
                    driver = launch_browser(account_id)
                    metrics["login_attempt_count"] += 1
                    if not login_baemin(driver, account_id, account["password"]):
                        metrics["login_failure_count"] += 1
                        raise RuntimeError(f"periodic relogin failed: {account_id}")

            success += 1
        except Exception as exc:
            logger.error("collect_shop_change account failure [%s]: %s", account_id, exc, exc_info=True)
            fail += 1
            metrics["failed_accounts"].append(account_id)
            _push_shop_change_alert("collection_failures", account_id, str(exc))
        finally:
            if driver is not None:
                try:
                    logout_baemin(driver, account_id)
                except Exception:
                    pass
                quit_driver_safely(driver, account_id)
            _force_kill_chrome(account_id)

        wait_sec = random.uniform(*profile["account_wait_range"])
        metrics["account_wait_sec_total"] += round(wait_sec, 1)
        logger.info("다음 계정까지 %.0f초 대기", wait_sec)
        time.sleep(wait_sec)

    summary = f"성공 {success}/{success + fail} 계정 / store_fail={store_fail}"
    metrics["ended_at"] = datetime.now(UTC).isoformat()
    _flush_shop_change_alerts()
    logger.info(summary)
    return {"summary": summary, "metrics": metrics}


def collect_shop_change_for_driver(driver, store_list: list[dict]) -> None:
    try:
        driver.set_page_load_timeout(5)
        driver.get("about:blank")
    except Exception:
        pass
    try:
        driver.set_page_load_timeout(25)
        driver.get(SHOP_CHANGE_URL)
    except Exception as exc:
        logger.warning("변경이력 페이지 진입 지연: %s", exc)

    if not wait_for_page(driver, SELECT_CSS, timeout=30):
        raise RuntimeError("shop_change select load failed")

    page_options = _get_page_options(driver)
    matched_stores = sorted(
        _dedupe_store_targets([s for s in store_list if s["store_id"] in page_options]),
        key=_store_collection_sort_key,
    )
    if not matched_stores:
        logger.info("변경이력 대상 없음: %s", [s.get("store_id") for s in store_list])
        return

    for index, store_info in enumerate(matched_stores):
        store_id = store_info["store_id"]
        target_label = _format_store_target(store_info)
        logger.info("변경이력 수집 시작: %s", target_label)
        if index > 0 and not _ensure_shop_change_page(driver, target_label):
            raise RuntimeError(f"select reload failed: {target_label}")

        sel_elem = driver.find_element(By.CSS_SELECTOR, SELECT_CSS)
        Select(sel_elem).select_by_value(store_id)
        time.sleep(1.0)
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, QUERY_BTN_CSS))
        )
        human_click(driver, btn)
        result_state = _wait_for_change_results(driver, target_label)
        if result_state != "ready":
            raise RuntimeError(f"query state={result_state}: {target_label}")

        time.sleep(0.5)
        full_name = _get_full_name(page_options.get(store_id, store_info["store"]))
        rows = _extract_change_rows(driver, store_info, full_name)
        if rows:
            saved = _save_csv(rows, store_info["brand"], store_info["store"])
            logger.info("저장 완료: %s -> %s (%d건)", target_label, saved, len(rows))
        else:
            logger.info("정상 빈값 신호: %s", target_label)

    logger.info("운영시간 수집 시작: %d개 매장", len(matched_stores))
    for store_info in matched_stores:
        full_name = _get_full_name(page_options.get(store_info["store_id"], store_info["store"]))
        op_rows = _collect_one_store_operation(driver, store_info, full_name)
        if op_rows:
            _save_operation_csv(op_rows, store_info["brand"], store_info["store"])
        else:
            logger.info("운영시간 빈값: %s", _format_store_target(store_info))

    from modules.transform.pipelines.db.DB_Beamin_monthly_operation import compute_monthly_operation  # noqa: PLC0415

    ym = pendulum.now(KST).format("YYYY-MM")
    for store_info in matched_stores:
        try:
            compute_monthly_operation(
                store_info["brand"], store_info["store"], store_info["store_id"], ym
            )
        except Exception as exc:
            logger.warning("월간 운영 요약 실패 (%s): %s", _format_store_target(store_info), exc)


def _save_csv(rows: list[dict], brand: str, store: str) -> Path:
    """upsert 방식으로 CSV 저장 (ym 파티션).

    같은 (변경시간 + 대분류) 조합이면 덮어쓰고, 신규 항목은 추가.
    컬럼 순서는 CSV_COLUMNS 고정.
    """
    ym = pendulum.now(KST).format("YYYY-MM")
    out_dir = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "shop_change.csv"

    new_df = pd.DataFrame(rows, columns=CSV_COLUMNS).astype(str)

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # 기존 파일에 없는 컬럼 추가 (스키마 호환)
        for col in CSV_COLUMNS:
            if col not in existing.columns:
                existing[col] = ""
        new_keys = set(zip(new_df["변경시간"], new_df["대분류"]))
        mask = ~existing.apply(
            lambda r: (r.get("변경시간", ""), r.get("대분류", "")) in new_keys, axis=1
        )
        combined = pd.concat([existing[mask][CSV_COLUMNS], new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info(
        "CSV 병합 저장 완료: brand=%s store=%s → %s (%d행)",
        brand,
        store,
        out_path,
        len(combined),
    )
    return out_path


def _element_text(el) -> str:
    if el is None:
        return ""
    for attr in ("innerText", "textContent"):
        try:
            value = el.get_attribute(attr)
        except Exception:
            value = ""
        if value and str(value).strip():
            return str(value).strip()
    return ""


def _expanded_content_text(item) -> str:
    try:
        content = item.find_element(By.CSS_SELECTOR, ".ListItem-content.on")
    except Exception:
        content = None

    text = _element_text(content)
    if text:
        return text

    try:
        text = item.parent.execute_script(
            """
            const root = arguments[0];
            const selectors = [
              '.ListItem-content.on',
              '[class*="ListItem-content"][class*="on"]',
              '[class*="HistoryItemContents"]',
            ];
            for (const selector of selectors) {
              const node = root.querySelector(selector);
              if (!node) continue;
              const text = (node.innerText || node.textContent || '').trim();
              if (text) return text;
            }
            return (root.innerText || root.textContent || '').trim();
            """,
            item,
        )
        return (text or "").strip()
    except Exception:
        return ""


def _wait_for_item_expanded(item, attempts: int = 10, delay: float = 0.2) -> bool:
    for _ in range(attempts):
        try:
            if _expanded_content_text(item):
                return True
        except StaleElementReferenceException:
            return False
        time.sleep(delay)
    return False


def _parse_change_history_item(
    item, iso: str, store_info: dict, full_name: str, area_name: str
) -> dict | None:
    title, date_attr, _ = _get_item_summary(item)
    base = {column: "" for column in CSV_COLUMNS}
    base["수집일시"] = iso
    base["매장명"] = full_name
    base["store_id"] = store_info["store_id"]
    base["지역명"] = area_name
    base["대분류"] = title
    base["변경시간"] = date_attr

    content_text = _expanded_content_text(item)
    if not content_text:
        logger.warning("확장된 상세 텍스트 없음: %s -> %s", _format_store_target(store_info), title)
        _push_shop_change_alert("parse_failures", _format_store_target(store_info), f"missing detail text: {title}")
        return base

    after_text, before_text = _split_after_before_text(content_text)
    label_category = "분류"
    label_changed_at = "변경시간"
    label_worker = "작업자"
    base["분류"] = _extract_labeled_value(content_text, label_category)
    base["변경시간"] = _extract_labeled_value(content_text, label_changed_at) or date_attr
    base["작업자"] = _extract_labeled_value(content_text, label_worker)

    if "영업임시중지" in title:
        after_d = _parse_business_pause_data(after_text or content_text)
        before_d = _parse_business_pause_data(before_text)
        base["변경후_사유"] = after_d["reason"]
        base["변경후_가게중지"] = after_d["store_closure"]
        base["변경후_배민배달"] = after_d["baemin_delivery"]
        base["변경전_사유"] = before_d["reason"]
        base["변경전_가게중지"] = before_d["store_closure"]
        base["변경전_배민배달"] = before_d["baemin_delivery"]
    elif "휴무일" in title:
        after_h = _parse_holiday_data(after_text or content_text)
        before_h = _parse_holiday_data(before_text)
        base["변경후_정기휴무일"] = after_h["regular"]
        base["변경후_임시휴무일"] = after_h["temp"]
        base["변경전_정기휴무일"] = before_h["regular"]
        base["변경전_임시휴무일"] = before_h["temp"]
    elif "운영시간" in title:
        base["변경후_운영시간"] = after_text or content_text
        base["변경전_운영시간"] = before_text

    return base


def _row_signature(row: dict) -> tuple[str, ...]:
    return (
        str(row.get("대분류", "")),
        str(row.get("변경시간", "")),
        str(row.get("작업자", "")),
        str(row.get("변경후_운영시간", "")),
        str(row.get("변경전_운영시간", "")),
        str(row.get("변경후_사유", "")),
        str(row.get("변경전_사유", "")),
        str(row.get("변경후_정기휴무일", "")),
        str(row.get("변경전_정기휴무일", "")),
        str(row.get("변경후_임시휴무일", "")),
        str(row.get("변경전_임시휴무일", "")),
        str(row.get("변경후_가게중지", "")),
        str(row.get("변경전_가게중지", "")),
        str(row.get("변경후_배민배달", "")),
        str(row.get("변경전_배민배달", "")),
    )


def _last_collected_date(brand: str, store: str):
    """이미 저장된 변경이력 CSV(전 ym 파티션)에서 가장 최근 '변경시간' 날짜를 반환.

    증분 수집용 — 저장본이 없으면 None. 매 실행 60일 전체 재스크롤 대신
    마지막 수집 지점까지만 스크롤하기 위한 기준.
    """
    try:
        paths = find_tables(
            BAEMIN_SHOP_CHANGE_DB, f"brand={brand}/store={store}/ym=*/shop_change"
        )
    except Exception:
        return None

    max_date = None
    for p in paths:
        try:
            df = read_file(p, columns=["변경시간"])
        except Exception:
            continue
        dates = pd.to_datetime(df["변경시간"], errors="coerce").dropna()
        if dates.empty:
            continue
        d = dates.max().date()
        if max_date is None or d > max_date:
            max_date = d
    return max_date


def _existing_change_keys(brand: str, store: str) -> set[tuple[str, str]]:
    """Return previously saved (date, category) keys for a store.

    CSV의 '변경시간'은 'YYYY-MM-DD HH:MM:SS' 형식이지만, DOM의 date_attr은
    'YYYY-MM-DD' 날짜만 제공한다. 앞 10자리(날짜)만 추출해 비교하여
    이미 수집된 항목을 클릭 없이 즉시 스킵할 수 있게 한다.
    같은 날짜·같은 대분류의 중복 수집은 _row_signature dedup이 방어한다.
    """
    keys: set[tuple[str, str]] = set()
    try:
        paths = find_tables(
            BAEMIN_SHOP_CHANGE_DB, f"brand={brand}/store={store}/ym=*/shop_change"
        )
    except Exception:
        return keys

    for p in paths:
        try:
            df = read_file(p, columns=["변경시간", "대분류"]).fillna("")
        except Exception:
            continue
        for _, row in df.iterrows():
            changed_at = str(row.get("변경시간", ""))[:10].strip()
            category = str(row.get("대분류", "")).strip()
            if changed_at and category:
                keys.add((changed_at, category))
    return keys


def _extract_change_rows(driver, store_info: dict, full_name: str) -> list[dict]:
    """Override earlier implementation to avoid under-collecting virtualized rows."""
    iso = pendulum.now(KST).isoformat()
    area_name = store_info["store"]
    target_label = _format_store_target(store_info)
    processed_rows: set[tuple[str, ...]] = set()
    failed_items: set[str] = set()
    existing_keys = _existing_change_keys(
        store_info.get("brand", ""), store_info.get("store", "")
    )
    skipped_existing: set[tuple[str, str]] = set()
    all_rows: list[dict] = []
    no_new = 0
    # 증분 커트오프: 60일 floor와 (마지막 저장 변경일 - 1일 overlap) 중 더 최근.
    # 저장은 signature dedup이라 overlap 재수집은 무해. 첫 수집만 60일 전체.
    floor_date = pendulum.now(KST).subtract(days=_COLLECT_DAYS).date()
    last_saved = _last_collected_date(store_info.get("brand", ""), store_info.get("store", ""))
    if last_saved is not None:
        incr_date = last_saved - timedelta(days=1)
        cutoff_date = max(floor_date, incr_date)
        logger.info(
            "증분 커트오프: %s (마지막 저장=%s, 60일floor=%s)",
            cutoff_date, last_saved, floor_date,
        )
    else:
        cutoff_date = floor_date
        logger.info("최초 수집 커트오프(60일): %s", cutoff_date)

    _scroll_to_top(driver)
    time.sleep(0.3)

    hit_cutoff = False
    for _ in range(_MAX_ITER):
        if hit_cutoff:
            break
        try:
            items = driver.find_elements(By.CSS_SELECTOR, ITEM_CSS)
        except Exception as e:
            if _is_driver_crash_error(e):
                raise
            items = []

        visible_progress = 0
        for item in items:
            item_key = ""
            try:
                title, date_attr, item_key = _get_item_summary(item)
                if item_key in failed_items or not item_key:
                    continue
                if not title.strip():
                    continue
                summary_key = (str(date_attr or "")[:10].strip(), str(title or "").strip())
                if summary_key in existing_keys:
                    skipped_existing.add(summary_key)
                    continue

                # 커트오프 이전 항목 → 정렬 내림차순이므로 이후 항목도 모두 오래됨
                if date_attr:
                    try:
                        if pendulum.parse(date_attr[:10]).date() < cutoff_date:
                            logger.info(
                                "커트오프(%s) 이전 항목 도달, 수집 종료: %s -> %s",
                                cutoff_date, target_label, date_attr,
                            )
                            hit_cutoff = True
                            break
                    except Exception:
                        pass

                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                    item,
                )
                time.sleep(random.uniform(0.1, 0.2))

                expanded_divs = item.find_elements(By.CSS_SELECTOR, ".ListItem-content.on")
                is_expanded = bool(expanded_divs) and any(
                    (div.get_attribute("innerText") or "").strip() for div in expanded_divs
                )
                if not is_expanded:
                    title_els = item.find_elements(By.CSS_SELECTOR, "h5.flex-1")
                    click_target = title_els[0] if title_els else item
                    human_click(driver, click_target)
                    time.sleep(0.8)
                    if not _wait_for_item_expanded(item):
                        # 1회 재클릭 후 더 길게 재대기 (확장 전 파싱으로 인한 누락 방지)
                        try:
                            human_click(driver, click_target)
                        except StaleElementReferenceException:
                            pass
                        time.sleep(1.0)
                        if not _wait_for_item_expanded(item, attempts=15, delay=0.3):
                            logger.warning("항목 확장 대기 실패(재시도 후), 파싱 시도: %s -> %s", target_label, item_key)

                row = _parse_change_history_item(item, iso, store_info, full_name, area_name)
                if row is None:
                    failed_items.add(item_key)
                    continue

                signature = _row_signature(row)
                if signature in processed_rows:
                    continue

                processed_rows.add(signature)
                all_rows.append(row)
                visible_progress += 1
                logger.info("항목 파싱 완료: %s -> %s", target_label, title)
            except StaleElementReferenceException:
                time.sleep(0.2)
                continue
            except Exception as e:
                if _is_driver_crash_error(e):
                    raise
                logger.warning("항목 처리 실패, 계속 진행: %s -> %s", target_label, e)
                if item_key:
                    failed_items.add(item_key)
                continue

        if visible_progress == 0:
            no_new += 1
            if no_new >= _MAX_NO_NEW:
                break
            logger.info("스크롤 후 다음 항목 대기: %s (%d/%d)", target_label, no_new, _MAX_NO_NEW)
            _scroll_by(driver, 450)
            time.sleep(random.uniform(0.3, 0.5))
            continue

        no_new = 0
        _scroll_by(driver, 500)
        time.sleep(random.uniform(0.3, 0.5))

    if not all_rows and not skipped_existing:
        _push_shop_change_alert("parse_failures", target_label, "zero parsed rows")
    logger.info(
        "변경이력 추출 완료: %s -> 신규 %d건 / 기존스킵 %d건",
        target_label,
        len(all_rows),
        len(skipped_existing),
    )
    return all_rows


def _save_csv(rows: list[dict], brand: str, store: str) -> Path:
    """Override earlier implementation to preserve same-timestamp multi-row changes."""
    ym = pendulum.now(KST).format("YYYY-MM")
    stem = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "shop_change"

    new_df = pd.DataFrame(rows, columns=CSV_COLUMNS).astype(str)
    new_df["_sig"] = new_df.apply(lambda row: str(_row_signature(row)), axis=1)

    existing = read_table(stem)
    if existing is not None:
        for col in CSV_COLUMNS:
            if col not in existing.columns:
                existing[col] = ""
        existing = existing[CSV_COLUMNS].astype(str)
        existing["_sig"] = existing.apply(lambda row: str(_row_signature(row)), axis=1)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["_sig"], keep="last")
        combined = combined.drop(columns=["_sig"])
    else:
        combined = new_df.drop(columns=["_sig"])

    out_path = write_table(combined, stem)
    logger.info(
        "변경이력 병합 저장 완료: brand=%s store=%s -> %s (%d건)",
        brand, store, out_path, len(combined),
    )
    return out_path
