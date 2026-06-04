"""배민 주문내역 수집 파이프라인.

수집 흐름:
  account_id/password → 매장별 독립 Chrome 세션
    → orders/history 이동
    → 가게 필터 선택 (store_id) → 날짜 필터 어제 설정
    → 전 페이지 수집 (행별 개별 expand/extract/collapse → 페이지네이션)
    → CSV 저장 (upsert by 주문번호)
    → Chrome quit (팝업 메모리 해제)

저장 경로:
  analytics/baemin_macro/orders/
    brand={brand}/store={store}/ym={YYYY-MM}/orders_{YYYY-MM}.csv
"""

import logging
import random
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    human_click,
    launch_browser,
    login_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_ORDERS_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

ORDERS_URL = "https://self.baemin.com/orders/history"

_CRASH_KEYWORDS = (
    "Remote end closed", "Connection aborted", "RemoteDisconnected",
    "Connection refused", "Max retries exceeded", "NewConnectionError",
    "invalid session id", "chrome not reachable", "disconnected",
    "tab crashed", "session deleted", "Timed out receiving message from renderer",
)

def _is_crash(exc: Exception) -> bool:
    s = str(exc)
    return any(k in s for k in _CRASH_KEYWORDS)


class OrdersCollectionInterrupted(RuntimeError):
    """Chrome died mid-collection; partial rows must not be saved as complete."""
_TABLE_ROW_CSS = "tr.Table_b_r4ax_1dwbr4on[data-index]"
_MAX_PAGES = 50
_PAGE_TRANSITION_TIMEOUT = 15
_MAX_VALIDATION_RETRY = 2
_ORDER_COLLECTION_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# TotalSummary 읽기 / 검증 / 재수집
# ---------------------------------------------------------------------------

def _read_total_summary(driver) -> dict | None:
    """현재 필터 조건의 TotalSummary(건수, 금액)를 읽는다.

    필터가 완전히 적용된 후 호출해야 정확한 값을 반환한다.
    DOM: <span class="TotalSummary-module__SysK"><b ...>56</b>건</span>
    """
    return driver.execute_script(
        """
        const spans = document.querySelectorAll('span.TotalSummary-module__SysK');
        let count = null, amount = null;
        for (const span of spans) {
            const b = span.querySelector('b.TotalSummary-module__jikm');
            if (!b) continue;
            const val = parseInt(b.textContent.replace(/,/g, ''));
            const fullText = span.textContent.trim();
            if (fullText.endsWith('건')) count = val;
            else if (fullText.endsWith('원')) amount = val;
        }
        return (count !== null) ? {count, amount} : null;
        """
    )


def _validate_collected(rows: list[dict], expected: dict | None) -> dict:
    """수집 rows와 TotalSummary 기댓값을 비교한다.

    Returns:
        matched: True/False/None(TotalSummary 없음)
        actual_count: unique 주문번호 수
        actual_amount: 결제금액 합계 (isFirst row에만 값 있음)
    """
    actual_count = len({r["주문번호"] for r in rows if r.get("주문번호")})
    def _sum_amount(column: str) -> int:
        total = 0
        for r in rows:
            raw = r.get(column, "")
            if raw and str(raw).strip():
                try:
                    total += int(str(raw).replace(",", "").strip())
                except (ValueError, TypeError):
                    pass
        return total

    amount_candidates = {
        "결제금액": _sum_amount("결제금액"),
        "총결제금액": _sum_amount("총결제금액"),
        "상품금액": _sum_amount("상품금액"),
    }
    actual_amount = amount_candidates["결제금액"]
    amount_source = "결제금액"

    if expected is None:
        return {
            "matched": None,
            "actual_count": actual_count, "actual_amount": actual_amount,
            "expected_count": None, "expected_amount": None,
            "amount_source": amount_source,
            "amount_candidates": amount_candidates,
        }
    expected_amount = expected.get("amount") or 0
    for source, amount in amount_candidates.items():
        if amount == expected_amount:
            actual_amount = amount
            amount_source = source
            break
    matched = (
        actual_count == expected["count"]
        and actual_amount == expected_amount
    )
    return {
        "matched": matched,
        "actual_count": actual_count, "actual_amount": actual_amount,
        "expected_count": expected["count"], "expected_amount": expected.get("amount"),
        "amount_source": amount_source,
        "amount_candidates": amount_candidates,
    }


def _collect_with_retry_on_mismatch(
    driver,
    store_info: dict,
    status_label: str,
    filter_fn,
    max_retry: int = _MAX_VALIDATION_RETRY,
) -> tuple[list[dict], dict]:
    """TotalSummary 검증 포함 수집. 불일치 시 필터 재적용 후 재수집.

    filter_fn(driver) → bool: 페이지 재이동 후 필터를 재적용하는 callable.

    Returns:
        (rows, validation_result)
        validation_result 포함 키: status, matched, actual_count, actual_amount,
                                    expected_count, expected_amount, retried
    """
    store = store_info.get("store", "?")
    rows: list[dict] = []
    vr: dict = {}

    for attempt in range(max_retry + 1):
        try:
            expected = _read_total_summary(driver)
            rows = _collect_all_pages(driver, store_info)
        except Exception as e:
            if _is_crash(e):
                raise
            raise
        vr = _validate_collected(rows, expected)
        vr["status"] = status_label
        vr["store"] = store
        vr["retried"] = attempt

        if vr["matched"] is not False:
            if vr["matched"] is True:
                logger.info(
                    "합계 일치 [%s][%s]: %d건/%s원 source=%s",
                    store, status_label, vr["actual_count"], vr["actual_amount"],
                    vr.get("amount_source", ""),
                )
            break

        logger.warning(
            "합계 불일치 [%s][%s] 수집=%d건/%d원 기대=%s건/%s원 source=%s candidates=%s (재시도 %d/%d)",
            store, status_label,
            vr["actual_count"], vr["actual_amount"],
            vr["expected_count"], vr["expected_amount"],
            vr.get("amount_source", ""), vr.get("amount_candidates", {}),
            attempt + 1, max_retry,
        )

        if attempt >= max_retry:
            break

        try:
            driver.get(ORDERS_URL)
            if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                logger.warning("재수집 페이지 로드 실패, 중단: %s", store)
                break
            if not filter_fn(driver):
                logger.warning("재수집 필터 적용 실패, 중단: %s", store)
                break
        except Exception as e:
            if _is_crash(e):
                raise
            logger.warning("재수집 준비 실패, 중단: %s / %s", store, e)
            break
        time.sleep(2.0)

    return rows, vr


# ---------------------------------------------------------------------------
# 공개 함수
# ---------------------------------------------------------------------------

def collect_orders_for_driver(
    driver,
    store_info: dict,
    target_date: str | None = None,
) -> dict:
    """기존 Chrome 세션으로 단일 매장의 주문내역을 수집한다.

    combined.py의 per-store 루프에서 로그인 없이 호출.
    TotalSummary 검증 포함: 불일치 시 최대 _MAX_VALIDATION_RETRY 회 재수집.

    Returns:
        {"ok": bool, "validation": list[dict]}
        validation 항목: status, store, matched, actual_count, actual_amount,
                         expected_count, expected_amount, retried
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand = store_info["brand"]
    store = store_info["store"]
    validation: list[dict] = []
    ok = True

    logger.info("주문내역 수집 시작: %s (%s) / %s", store, store_id, target_date)

    try:
        driver.set_page_load_timeout(45)
        driver.get(ORDERS_URL)

        if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
            logger.warning("테이블 로드 실패, 건너뜀: %s", store)
            return {"ok": False, "validation": validation}

        if not _select_order_store(driver, store_id, store):
            logger.warning("가게 필터 선택 실패, 건너뜀: %s", store)
            return {"ok": False, "validation": validation}

        if not _set_date(driver, target_date):
            logger.warning("날짜 설정 실패, 건너뜀: %s", store)
            return {"ok": False, "validation": validation}

        time.sleep(2.0)

        # 배달완료 수집 + TotalSummary 검증
        def _filter_normal(d):
            return _select_order_store(d, store_id, store) and _set_date(d, target_date)

        rows, vr_normal = _collect_with_retry_on_mismatch(
            driver, store_info, "배달완료", _filter_normal,
        )
        validation.append(vr_normal)

        if vr_normal.get("matched") is False:
            logger.warning("검증 불일치로 정상 주문 저장 생략: %s / %s", store, target_date)
            ok = False
        elif rows:
            saved = _save_orders_csv(rows, brand, store, target_date)
            logger.info("저장 완료(정상): brand=%s store=%s → %s (%d행)", brand, store, saved, len(rows))
        else:
            logger.info("정상 주문 없음: %s / %s", store, target_date)

        # 주문취소 2차 수집 + TotalSummary 검증
        logger.info("주문취소 수집 시작: %s", store)
        driver.get(ORDERS_URL)

        if wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
            if _setup_cancel_filter(driver, store_id, store, target_date):
                time.sleep(2.0)

                def _filter_cancelled(d):
                    return (
                        _select_order_store(d, store_id, store)
                        and _select_status_cancelled(d)
                        and _set_date(d, target_date)
                    )

                cancelled_rows, vr_cancelled = _collect_with_retry_on_mismatch(
                    driver, store_info, "주문취소", _filter_cancelled,
                )
                validation.append(vr_cancelled)

                if vr_cancelled.get("matched") is False:
                    logger.warning("검증 불일치로 주문취소 저장 생략: %s / %s", store, target_date)
                    ok = False
                elif cancelled_rows:
                    saved_c = _save_orders_csv(cancelled_rows, brand, store, target_date)
                    logger.info(
                        "저장 완료(취소): brand=%s store=%s → %s (%d행)",
                        brand, store, saved_c, len(cancelled_rows),
                    )
                else:
                    logger.info("주문취소 없음: %s / %s", store, target_date)
            else:
                logger.warning("주문취소 필터 설정 실패, 건너뜀: %s", store)
        else:
            logger.warning("주문취소 수집 페이지 로드 실패: %s", store)

        return {"ok": ok, "validation": validation}

    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("주문내역 수집 실패 (%s): %s", store, e)
        return {"ok": False, "validation": validation}


def collect_orders_for_account(
    account_id: str,
    password: str,
    store_list: list[dict],
    target_date: str | None = None,
) -> dict:
    """매장별 독립 Chrome 세션으로 주문내역을 수집한다.

    팝업 클릭(즉시할인 파트너부담/배민지원) 후 Chrome을 종료해 메모리를 완전 해제.
    매장마다 새 Chrome 세션 → login → collect → quit.

    Returns:
        {"failed": list[dict], "validation": list[dict]}
        failed: 수집 실패한 store_info 리스트
        validation: 매장별 TotalSummary 검증 결과
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed_stores: list[dict] = []
    validation: list[dict] = []

    for store_info in store_list:
        store_id = store_info["store_id"]
        brand = store_info["brand"]
        store = store_info["store"]

        logger.info("주문내역 수집 시작: %s (%s) / %s", store, store_id, target_date)

        succeeded = False
        for attempt in range(_ORDER_COLLECTION_ATTEMPTS):
            driver = None
            try:
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, password):
                    logger.warning("로그인 실패: %s / %s", account_id, store)
                    break

                driver.set_page_load_timeout(45)
                driver.get(ORDERS_URL)

                if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                    logger.warning("테이블 로드 실패, 건너뜀: %s", store)
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                if not _select_order_store(driver, store_id, store):
                    logger.warning("가게 필터 선택 실패, 건너뜀: %s", store)
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        continue
                    break

                if not _set_date(driver, target_date):
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        logger.warning("날짜 설정 실패, Chrome 재시작 후 재시도: %s", store)
                        continue
                    logger.warning("날짜 설정 최종 실패, 건너뜀: %s", store)
                    break

                time.sleep(2.0)

                # 배달완료 수집 + TotalSummary 검증
                _sid, _sn = store_id, store  # closure 캡처용
                _td = target_date  # closure 캡처용

                def _filter_normal(d):
                    return _select_order_store(d, _sid, _sn) and _set_date(d, _td)

                rows, vr_normal = _collect_with_retry_on_mismatch(
                    driver, store_info, "배달완료", _filter_normal,
                )
                validation.append(vr_normal)

                if vr_normal.get("matched") is False:
                    logger.warning("검증 불일치로 정상 주문 저장 생략: %s / %s", store, target_date)
                    if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                        validation.pop()
                        continue
                    break
                elif rows:
                    saved = _save_orders_csv(rows, brand, store, target_date)
                    logger.info(
                        "저장 완료(정상): brand=%s store=%s → %s (%d행)", brand, store, saved, len(rows)
                    )
                else:
                    logger.info("정상 주문 없음: %s / %s", store, target_date)

                # ── 주문취소 2차 수집 + TotalSummary 검증 ──
                logger.info("주문취소 수집 시작: %s", store)
                driver.get(ORDERS_URL)

                if wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                    if _setup_cancel_filter(driver, store_id, store, target_date):
                        time.sleep(2.0)

                        def _filter_cancelled(d):
                            return (
                                _select_order_store(d, _sid, _sn)
                                and _select_status_cancelled(d)
                                and _set_date(d, _td)
                            )

                        cancelled_rows, vr_cancelled = _collect_with_retry_on_mismatch(
                            driver, store_info, "주문취소", _filter_cancelled,
                        )
                        validation.append(vr_cancelled)

                        if vr_cancelled.get("matched") is False:
                            logger.warning("검증 불일치로 주문취소 저장 생략: %s / %s", store, target_date)
                            if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                                validation.pop()
                                continue
                            break
                        elif cancelled_rows:
                            saved_c = _save_orders_csv(cancelled_rows, brand, store, target_date)
                            logger.info(
                                "저장 완료(취소): brand=%s store=%s → %s (%d행)",
                                brand, store, saved_c, len(cancelled_rows),
                            )
                        else:
                            logger.info("주문취소 없음: %s / %s", store, target_date)
                    else:
                        logger.warning("주문취소 필터 설정 실패, 건너뜀: %s", store)
                else:
                    logger.warning("주문취소 수집 페이지 로드 실패: %s", store)

                succeeded = True
                break

            except Exception as e:
                logger.warning(
                    "매장 수집 실패 (%s) attempt=%d/%d: %s",
                    store, attempt + 1, _ORDER_COLLECTION_ATTEMPTS, e,
                )
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            if attempt < _ORDER_COLLECTION_ATTEMPTS - 1:
                time.sleep(random.uniform(3.0, 5.0))

        if not succeeded:
            failed_stores.append(store_info)

        time.sleep(random.uniform(1.5, 3.0))

    return {"failed": failed_stores, "validation": validation}


# ---------------------------------------------------------------------------
# 가게 필터 선택
# ---------------------------------------------------------------------------

def _select_order_store(driver, store_id: str, store_name: str) -> bool:
    """orders/history 가게 필터에서 매장을 선택하고 적용한다.

    XPath text() 매칭은 중첩 span 구조에서 실패하므로 JS badge 탐색으로 클릭.
    Atelier Select 컴포넌트라 Selenium Select() 대신 JS change 이벤트를 사용한다.
    """
    try:
        # 1. 가게 필터 버튼 클릭 — JS로 badge closest(button) 탐색 (XPath보다 안정적)
        badge_text_found = driver.execute_script(
            """
            // 가게 필터 버튼 = Filter 클래스 버튼 중 Badge 포함한 것
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => b.className.includes('Filter'));
            const storeFilterBtn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
            if (storeFilterBtn) {
                const badge = storeFilterBtn.querySelector('.Badge_b_r4ax_19agxiso');
                storeFilterBtn.click();
                return badge?.textContent.trim() || 'clicked';
            }
            // fallback: Badge 포함 버튼 직접 탐색
            const badges = document.querySelectorAll('.Badge_b_r4ax_19agxiso');
            for (const badge of badges) {
                const t = badge.textContent.trim();
                if (t.includes('가게') || t.includes('전체') || t.includes('음식')) {
                    const btn = badge.closest('button') || badge.closest('[role="button"]');
                    if (btn) { btn.click(); return t; }
                }
            }
            return false;
            """
        )
        if not badge_text_found:
            logger.warning("가게 필터 badge 미발견: %s (DOM에 .Badge_b_r4ax_19agxiso 없음)", store_name)
            return False
        logger.info("가게 필터 버튼 클릭: badge=%s", badge_text_found)
        # 2. select 요소 찾아서 JS로 값 설정 + React change 이벤트 dispatch
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script(
                """
                return [...document.querySelectorAll('select')]
                    .some(sel => [...sel.options].some(o => o.value === arguments[0]));
                """,
                store_id,
            )
        )
        select_result = driver.execute_script(
            """
            const target = arguments[0];
            const selects = [...document.querySelectorAll('select')];
            const sel = selects.find(s => [...s.options].some(o => o.value === target));
            if (!sel) {
                return {
                    ok: false,
                    options: selects.flatMap(s => [...s.options].map(o => `${o.value}:${o.textContent.trim()}`))
                };
            }
            sel.value = target;
            sel.dispatchEvent(new Event('change', {bubbles: true}));
            sel.dispatchEvent(new Event('input', {bubbles: true}));
            return {
                ok: sel.value === target,
                value: sel.value,
                text: sel.selectedOptions?.[0]?.textContent?.trim() || ''
            };
            """,
            store_id,
        )
        if not select_result or not select_result.get("ok"):
            logger.warning(
                "가게 select 값 설정 실패: %s store_id=%s result=%s",
                store_name, store_id, select_result,
            )
            return False
        logger.info(
            "가게 select 값 설정: %s store_id=%s option=%s",
            store_name, store_id, select_result.get("text", ""),
        )
        time.sleep(0.3)

        # 3. 적용 버튼 클릭 — JS fallback 포함
        apply_clicked = driver.execute_script(
            """
            const btns = document.querySelectorAll('button');
            for (const btn of btns) {
                if (btn.textContent.trim() === '적용' && !btn.disabled) {
                    btn.click(); return true;
                }
            }
            return false;
            """
        )
        if not apply_clicked:
            logger.warning("가게 필터 적용 버튼 미발견: %s", store_name)
            return False
        # badge가 가게 전체→매장명으로 바뀔 때까지 대기 (고정 2.0s 제거)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: "가게 전체" not in (d.execute_script(
                    """
                    const filterBtns = [...document.querySelectorAll('button')]
                        .filter(b => b.className.includes('Filter'));
                    const btn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
                    return btn?.querySelector('.Badge_b_r4ax_19agxiso')?.textContent.trim() || '';
                    """
                ) or "")
            )
        except TimeoutException:
            pass
        time.sleep(0.5)  # FilterContainer popup 닫힘 animation 완료 대기

        # 4. 검증: Badge 포함 Filter 버튼(가게 필터)의 badge 텍스트 확인
        current_badge = driver.execute_script(
            """
            // 가게 필터 버튼 = Badge 포함 Filter 버튼
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => b.className.includes('Filter'));
            const storeBtn = filterBtns.find(b => b.querySelector('.Badge_b_r4ax_19agxiso'));
            const badge = storeBtn?.querySelector('.Badge_b_r4ax_19agxiso');
            return badge ? badge.textContent.trim() : '';
            """
        ) or ""
        if "가게 전체" in current_badge:
            logger.warning("가게 선택 미반영 (still 전체): %s → store_id=%s", store_name, store_id)
            return False

        logger.info("가게 필터 선택 완료: %s → badge=%s", store_name, current_badge)
        return True

    except TimeoutException as e:
        logger.warning("가게 필터 선택 오류 (%s): %s", store_name, e)
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("가게 필터 선택 오류 (%s): %s", store_name, e)
        return False


# ---------------------------------------------------------------------------
# 날짜 필터
# ---------------------------------------------------------------------------

def _set_date_specific(driver, target_date: str) -> bool:
    """DefaultDateFilter 달력에서 target_date 하루를 선택하고 적용한다.

    흐름:
        1. DefaultDateFilter-module 트리거 버튼 클릭 (달력 오픈)
        2. 달력 헤더에서 표시 월 확인 → 필요하면 이전달(<) 버튼으로 이동
        3. aria-label="{day}일" 버튼 시작일 클릭
        4. 같은 버튼 종료일 클릭 (1일 범위)
        5. 적용 버튼 클릭
    """
    import re as _re

    try:
        dt = pendulum.parse(target_date, tz=KST)
        target_year = dt.year
        target_month = dt.month
        target_day = dt.day
        day_label = f"{target_day}일"

        # 1. DefaultDateFilter 트리거 클릭
        clicked = driver.execute_script(
            """
            // data-atelier-component="DatePicker.Trigger" 또는 DefaultDateFilter 내 버튼
            let btn = document.querySelector('[data-atelier-component="DatePicker.Trigger"]');
            if (!btn) {
                const wrap = document.querySelector('[class*="DefaultDateFilter"]');
                btn = wrap && wrap.querySelector('button');
            }
            if (btn) { btn.click(); return true; }
            return false;
            """
        )
        if not clicked:
            logger.warning("DefaultDateFilter 트리거 버튼 미발견")
            return False

        # 2. 달력 헤더 확인 후 목표 월로 이동
        def _get_calendar_ym(d):
            """달력 헤더에서 (year, month) 반환. 실패 시 None."""
            text = d.execute_script(
                """
                const h = document.querySelector('[class*="DatePicker"][class*="Header"], [class*="CalendarHeader"]');
                return h ? h.textContent : null;
                """
            )
            if not text:
                return None
            m = _re.search(r"(\d{4})[^\d]+(\d{1,2})", text)
            return (int(m.group(1)), int(m.group(2))) if m else None

        # 달력이 열릴 때까지 대기
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script(
                "return !!document.querySelector('[aria-label$=\"일\"]');"
            )
        )

        # 최대 24번(2년치) 이전달 이동
        for _ in range(24):
            ym = _get_calendar_ym(driver)
            if ym and (ym[0], ym[1]) == (target_year, target_month):
                break
            if ym and (ym[0] * 12 + ym[1]) > (target_year * 12 + target_month):
                # 이전달 버튼 클릭
                driver.execute_script(
                    """
                    const btns = [...document.querySelectorAll('button')];
                    const prev = btns.find(b => b.getAttribute('aria-label') === '이전달'
                        || b.textContent.trim() === '<'
                        || b.className.includes('prev') || b.className.includes('Prev'));
                    if (prev) prev.click();
                    """
                )
                time.sleep(0.3)
            else:
                break  # 헤더 파싱 실패 시 현재 달로 진행

        # 3. 시작일 클릭
        clicked_start = driver.execute_script(
            f"""
            const btns = [...document.querySelectorAll('button[aria-label="{day_label}"]')];
            if (btns.length > 0) {{ btns[0].click(); return true; }}
            return false;
            """
        )
        if not clicked_start:
            logger.warning("달력 시작일 버튼 미발견: %s", day_label)
            return False

        time.sleep(0.2)

        # 4. 종료일 클릭 (같은 날 → 1일 범위)
        driver.execute_script(
            f"""
            const btns = [...document.querySelectorAll('button[aria-label="{day_label}"]')];
            if (btns.length > 0) btns[btns.length - 1].click();
            """
        )
        time.sleep(0.2)

        # 5. 적용 버튼 클릭
        apply_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-atelier-component='Button'][.//span[text()='적용']]")
            )
        )
        human_click(driver, apply_btn)

        # 검증: 날짜 텍스트에 target_day 포함 확인
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script(
                    f"const t = document.querySelector('[data-atelier-component=\"DatePicker.Trigger\"]');"
                    f"return t && t.textContent.includes('{target_day}');"
                )
            )
        except TimeoutException:
            pass

        logger.info("날짜 필터 설정 완료: %s", target_date)
        return True

    except TimeoutException as e:
        logger.warning("날짜 필터(specific) 설정 오류: %s", e)
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("날짜 필터(specific) 설정 오류: %s", e)
        return False


def _set_date(driver, target_date: str) -> bool:
    """target_date에 맞는 날짜 필터 함수를 선택해 호출한다.

    어제이면 _set_date_yesterday(radio 방식), 그 외는 _set_date_specific(달력 방식).
    """
    yesterday = pendulum.yesterday(KST).format("YYYY-MM-DD")
    if target_date == yesterday:
        return _set_date_yesterday(driver)
    return _set_date_specific(driver, target_date)


def _set_date_yesterday(driver) -> bool:
    """날짜 필터를 '일 → 어제'로 설정하고 적용한다."""
    try:
        # 1. 날짜 필터 버튼 클릭 — Badge 없는 Filter 버튼이 날짜 필터
        #    (가게 필터=Badge 포함, 날짜 필터=Badge 없음, 날짜 텍스트/p 포함)
        clicked = driver.execute_script(
            """
            const filterBtns = [...document.querySelectorAll('button')]
                .filter(b => b.className.includes('Filter'));
            // 날짜 필터 = Badge 없는 Filter 버튼
            const dateFilterBtn = filterBtns.find(b => !b.querySelector('.Badge_b_r4ax_19agxiso'));
            if (dateFilterBtn) { dateFilterBtn.click(); return 'date_filter_btn'; }
            // fallback: p 또는 날짜 패턴 포함 버튼
            for (const btn of document.querySelectorAll('button')) {
                const t = btn.textContent;
                if (t.includes('날짜') || t.match(/\\d{4}\\.\\s*\\d{2}\\.\\s*\\d{2}/)) {
                    if (!btn.querySelector('.Badge_b_r4ax_19agxiso')) { btn.click(); return 'text_btn'; }
                }
            }
            return false;
            """
        )
        if not clicked:
            logger.warning("날짜 필터 버튼 미발견 (filterBtns 없음)")
            return False
        logger.info("날짜 필터 버튼 클릭: %s", clicked)

        # 2. "일" 라디오 — 팝업 열릴 때까지 대기 후 클릭 (sleep 제거)
        daily_radio = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "input[type='radio'][value='dailyWeekly']")
            )
        )
        driver.execute_script("arguments[0].click();", daily_radio)

        # 3. "어제" 레이블 — dailyWeekly 선택 후 옵션 나타날 때까지 대기
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script(
                """
                for (const lbl of document.querySelectorAll('label')) {
                    if (lbl.textContent.trim() === '어제') return true;
                }
                return false;
                """
            )
        )
        driver.execute_script(
            """
            for (const lbl of document.querySelectorAll('label')) {
                if (lbl.textContent.trim() === '어제') { lbl.click(); return; }
            }
            """
        )
        time.sleep(0.2)

        # 4. 적용 버튼 클릭
        apply_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-atelier-component='Button'][.//span[text()='적용']]")
            )
        )
        human_click(driver, apply_btn)
        # 어제 배지 나타날 때까지 대기 (고정 2.0s 제거)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script(
                    "for (const b of document.querySelectorAll('button')) { if (b.textContent.includes('어제')) return true; } return false;"
                )
            )
        except TimeoutException:
            pass

        # 5. 검증: 어제 배지 확인
        confirmed = driver.execute_script(
            """
            const btns = document.querySelectorAll('button');
            for (const btn of btns) {
                if (btn.textContent.includes('어제')) return true;
            }
            return false;
            """
        )
        if not confirmed:
            logger.warning("날짜 '어제' 설정 검증 실패")
            return False

        logger.info("날짜 필터 설정 완료: 어제")
        return True

    except TimeoutException as e:
        logger.warning("날짜 필터 설정 오류: %s", e)
        return False
    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("날짜 필터 설정 오류: %s", e)
        return False


# ---------------------------------------------------------------------------
# 주문취소 status 필터
# ---------------------------------------------------------------------------

def _select_status_cancelled(driver) -> bool:
    """통합 필터 팝업(FilterContainer-module__ccrG)을 열고 주문취소(CANCELLED)를 선택한다.

    배민 orders 페이지 구조: 가게·상태·결제방법·광고서비스가 하나의 FilterContainer 버튼으로 묶여 있음.
    가게 필터(_select_order_store)가 이미 같은 버튼을 사용하므로, 상태 변경도 같은 버튼을 재클릭.
    CANCELLED 라디오는 input[readonly] → label.click()으로만 React 상태 변경.
    """
    try:
        # 1. 통합 필터 버튼 클릭 (FilterContainer-module__ccrG)
        opened = driver.execute_script(
            """
            // 날짜 필터(FilterContainer-module__vSPY) 제외한 나머지 FilterContainer 버튼
            const btns = [...document.querySelectorAll('button[class*="FilterContainer"]')];
            const combined = btns.find(b => !b.className.includes('vSPY'));
            if (combined) { combined.click(); return combined.textContent.trim().slice(0, 50); }
            return false;
            """
        )
        if not opened:
            logger.warning("통합 필터 버튼(FilterContainer-module__ccrG) 미발견")
            return False
        logger.info("통합 필터 버튼 클릭: %s", opened[:40])
        # CANCELLED 라디오 나타날 때까지 대기 (고정 1.0s 제거)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'input[name="status"][value="CANCELLED"]')
            )
        )

        # 2. CANCELLED 라디오 → label 클릭 (input[readonly] 우회)
        result = driver.execute_script(
            """
            const radio = document.querySelector('input[name="status"][value="CANCELLED"]');
            if (!radio) return 'not_found';
            const lbl = document.querySelector('label[for="' + radio.id + '"]');
            if (lbl) { lbl.click(); return 'label_clicked'; }
            const parent = radio.closest('[data-checked]') || radio.parentElement;
            if (parent) { parent.click(); return 'parent_clicked'; }
            radio.click();
            return 'radio_fallback';
            """
        )
        if result == "not_found":
            logger.warning("주문취소 radio 미발견 (팝업 안에 input[name='status'][value='CANCELLED'] 없음)")
            return False
        logger.info("주문취소 라디오 클릭: %s", result)
        time.sleep(0.5)

        # 3. 적용 버튼 JS 클릭 (headless: ActionChains viewport 이탈 → 클릭 미등록 방지)
        applied = driver.execute_script(
            """
            const btn = [...document.querySelectorAll('button')]
                .find(b => b.innerText.trim() === '적용' || b.textContent.trim() === '적용');
            if (!btn) return false;
            btn.click();
            return true;
            """
        )
        if not applied:
            logger.warning("주문취소 필터 적용 버튼 미발견")
            return False

        # 4. 팝업 닫힘 대기 — wait_for_page 금지
        #    wait_for_page는 timeout 시 driver.refresh() 호출 → CANCELLED 필터 소실
        #    → 배달완료 데이터가 주문취소 슬롯에 저장되는 오수집 발생
        #    취소건 0건도 유효: 팝업이 닫히면 필터 적용 완료
        try:
            WebDriverWait(driver, 20).until(
                EC.invisibility_of_element_located(
                    (By.CSS_SELECTOR, 'input[name="status"][value="CANCELLED"]')
                )
            )
        except TimeoutException:
            logger.warning("주문취소 적용 후 팝업 닫힘 미확인 (20s 초과): 필터 미적용으로 간주")
            return False

        time.sleep(1.0)  # React 상태 업데이트 + 데이터 리로드 대기
        logger.info("주문취소 status 필터 선택 완료")
        return True

    except Exception as e:
        if _is_crash(e):
            raise
        logger.warning("주문취소 status 선택 오류: %s", e)
        return False


def _setup_cancel_filter(driver, store_id: str, store: str, target_date: str) -> bool:
    """주문취소 필터(가게+CANCELLED+날짜)를 설정한다. 1차 실패 시 대시보드 경유 후 1회 재시도.

    배민 SPA 상태가 꼬이면 통합 필터 팝업 설정이 간헐 실패한다(CLAUDE.md gotcha).
    실패 시 대시보드를 경유해 SPA 상태를 초기화하고 orders 페이지를 다시 로드한 뒤 재시도한다.
    드라이버 크래시는 그대로 전파(상위 복구 로직이 처리).
    """
    def _apply() -> bool:
        return (
            _select_order_store(driver, store_id, store)
            and _select_status_cancelled(driver)
            and _set_date(driver, target_date)
        )

    if _apply():
        return True

    logger.info("주문취소 필터 1차 실패 → 대시보드 경유 후 재시도: %s", store)
    try:
        driver.get("https://self.baemin.com/")
        time.sleep(2.0)
        driver.get(ORDERS_URL)
        if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
            return False
    except Exception as e:
        if _is_crash(e):
            raise
        return False

    return _apply()


# ---------------------------------------------------------------------------
# 페이지 순회 수집
# ---------------------------------------------------------------------------

def _collect_all_pages(driver, store_info: dict) -> list[dict]:
    """현재 필터 상태에서 전 페이지 주문내역을 수집한다.

    모든 행을 한꺼번에 펼치면 Chrome OOM crash 발생 → 행별 개별 expand/extract/collapse.
    """
    store = f"{store_info['brand']} {store_info['store']}"

    try:
        row_count = driver.execute_script(
            f"return document.querySelectorAll('{_TABLE_ROW_CSS}').length;"
        ) or 0
    except Exception as e:
        if _is_crash(e):
            raise OrdersCollectionInterrupted(f"Chrome died before row count: {e}") from e
        raise
    if row_count == 0:
        logger.info("주문 없음 (테이블 행 0): %s", store)
        return []

    all_rows: list[dict] = []
    iso = pendulum.now(KST).isoformat()

    for page_num in range(1, _MAX_PAGES + 1):
        logger.info("%d페이지 수집 중: %s", page_num, store)

        # data-index 목록 수집 (expand/collapse 중에도 안정적)
        try:
            indices = driver.execute_script(
                "return [...document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]')]"
                ".map(r => r.getAttribute('data-index'));"
            ) or []
        except Exception as e:
            if _is_crash(e):
                raise OrdersCollectionInterrupted(
                    f"Chrome died while reading row indices page={page_num}: {e}"
                ) from e
            raise

        page_rows: list[dict] = []
        for idx in indices:
            try:
                # 1. 행 펼치기
                driver.execute_script(_EXPAND_ROW_JS, idx)
                time.sleep(0.3)

                # 2. 단일 행 추출
                row_data = driver.execute_script(_EXTRACT_SINGLE_ROW_JS, idx, store, iso) or []

                # 3. 즉시할인 팝업 (Chrome 죽으면 팝업만 skip, 데이터는 유지)
                if row_data and row_data[0].get("즉시할인"):
                    order_num = row_data[0].get("주문번호", "")
                    try:
                        detail = _get_discount_popup_detail(driver, order_num)
                        if detail:
                            row_data[0]["즉시할인_파트너부담"] = detail.get("partner", "")
                            row_data[0]["즉시할인_배민지원"] = detail.get("baemin", "")
                    except Exception as popup_err:
                        if _is_crash(popup_err):
                            raise OrdersCollectionInterrupted(
                                f"Chrome died in discount popup order={order_num}: {popup_err}"
                            ) from popup_err
                        logger.warning("팝업 클릭 실패 - 할인 상세만 비우고 행 데이터 유지: %s", popup_err)
                        page_rows.extend(row_data)
                        continue

                # 4. 행 접기
                driver.execute_script(_COLLAPSE_ROW_JS, idx)
                page_rows.extend(row_data)
                time.sleep(0.1)

            except Exception as row_err:
                if _is_crash(row_err) or isinstance(row_err, OrdersCollectionInterrupted):
                    raise OrdersCollectionInterrupted(
                        f"Chrome died while processing row idx={idx}: {row_err}"
                    ) from row_err
                logger.warning("행 처리 실패 (Chrome OOM?): idx=%s err=%s", idx, row_err)
                continue

        all_rows.extend(page_rows)
        logger.info(
            "%d페이지: %d행 수집 (누계 %d행)", page_num, len(page_rows), len(all_rows)
        )

        try:
            prev_first_id = _get_first_order_id(driver)
            has_next = _click_next_page(driver, page_num)
        except Exception as e:
            if _is_crash(e):
                raise OrdersCollectionInterrupted(
                    f"Chrome died before page transition page={page_num}: {e}"
                ) from e
            raise
        if not has_next:
            logger.info("마지막 페이지 도달: %s (%d페이지)", store, page_num)
            break

        if not _wait_for_page_transition(driver, prev_first_id):
            logger.warning(
                "%d→%d 페이지 전환 타임아웃, 수집 종료: %s", page_num, page_num + 1, store
            )
            break

        time.sleep(random.uniform(0.5, 0.8))

    return all_rows


def _get_discount_popup_detail(driver, order_num: str) -> dict | None:
    """주문번호로 discount element를 찾아 클릭 → 팝업 읽기 → 닫기.

    익스텐션: discountAmountEl.click() → _randomDelay(400,600) →
              popup.querySelector → ul lastUl → li 순회 → document.body.click()
    """
    # 1. 해당 주문번호 행의 discount element 클릭
    clicked = driver.execute_script(
        """
        function cleanNum(text, status) {
            return text.replace(status || '', '').trim();
        }
        const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
        for (const row of rows) {
            const cell = row.querySelector('td[data-td-index="1"]');
            const badge = cell?.querySelector('.Badge_b_r4ax_19agxiso');
            const num = cleanNum(cell?.textContent.trim() || '', badge?.textContent.trim() || '');
            if (num !== arguments[0]) continue;

            let sib = row.nextElementSibling;
            while (sib?.tagName === 'TR') {
                const container = sib.querySelector('.InstantDiscountDetailPageSheet-module__u8LB');
                if (container) {
                    const el = container.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
                    if (el && el.textContent.trim()) { el.click(); return true; }
                }
                sib = sib.nextElementSibling;
            }
        }
        return false;
        """,
        order_num,
    )

    if not clicked:
        return None

    # 2. 팝업 로드 대기 (익스텐션 _randomDelay(400, 600) 동일)
    time.sleep(random.uniform(0.4, 0.6))

    # 3. 팝업 읽기 → body 클릭으로 닫기
    result = driver.execute_script(
        r"""
        const popup = document.querySelector('.InstantDiscountDetailPageSheet-module__IbXh');
        if (!popup) { document.body.click(); return null; }

        const allUls = popup.querySelectorAll('ul.InstantDiscountDetailPageSheet-module__Go_F');
        const lastUl = allUls[allUls.length - 1];
        if (!lastUl) { document.body.click(); return null; }

        let partner = '', baemin = '';
        for (const li of lastUl.querySelectorAll('li')) {
            const labelText = li.textContent || '';
            const valueSpan = li.querySelector('.TextListItem_b_r4ax_n197m77 div span');
            const value = (valueSpan?.textContent || '').replace(/[,원\s]/g, '').trim();
            if (labelText.includes('파트너 부담')) partner = value;
            else if (labelText.includes('배민 지원')) baemin = value;
        }

        document.body.click();
        return {partner, baemin};
        """
    )

    return result


def _get_first_order_id(driver) -> str:
    return driver.execute_script(
        """
        const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index]');
        return row?.querySelector('td[data-td-index="1"]')?.textContent.trim() || '';
        """
    ) or ""


def _click_next_page(driver, current_page: int) -> bool:
    return driver.execute_script(
        """
        const next = arguments[0] + 1;
        const btns = document.querySelectorAll(
            'ul.Pagination_b_r4ax_pb5p4v5 button.Pagination_b_r4ax_pb5p4vb'
        );
        for (const btn of btns) {
            if (parseInt(btn.textContent.trim()) === next) { btn.click(); return true; }
        }
        return false;
        """,
        current_page,
    )


def _wait_for_page_transition(driver, prev_first_id: str) -> bool:
    start = time.time()
    while time.time() - start < _PAGE_TRANSITION_TIMEOUT:
        time.sleep(0.3)
        curr = _get_first_order_id(driver)
        if curr and curr != prev_first_id:
            return True
    return False


# ---------------------------------------------------------------------------
# JS 상수: 행별 expand / collapse / 단일 행 추출
# ---------------------------------------------------------------------------

# arguments[0]: data-index 문자열
_EXPAND_ROW_JS = r"""
const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + arguments[0] + '"]');
const svg = row?.querySelector('td[data-td-index="0"] svg');
if (svg && svg.getAttribute('aria-label') === '컨텐츠 펼치기') {
    svg.closest('td')?.click();
}
"""

# arguments[0]: data-index 문자열
_COLLAPSE_ROW_JS = r"""
const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + arguments[0] + '"]');
const svg = row?.querySelector('td[data-td-index="0"] svg');
if (svg && svg.getAttribute('aria-label') === '컨텐츠 접기') {
    svg.closest('td')?.click();
}
"""

# arguments[0]: data-index, arguments[1]: store_name, arguments[2]: iso
# 반환: [] 또는 [{...}, ...] (옵션별 분리)
_EXTRACT_SINGLE_ROW_JS = r"""
const dataIdx = arguments[0];
const storeName = arguments[1];
const iso = arguments[2];

function cleanPrice(text) {
    if (!text) return '';
    return text.replace(/[,원\s()]/g, '').trim();
}

const row = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index="' + dataIdx + '"]');
if (!row) return [];

const base = {
    collected_at: iso, store_name: storeName,
    주문상태: '', 주문번호: '', 주문시각: '', 광고상품: '', 캠페인ID: '',
    결제타입: '', 수령방법: '', 결제금액: '',
    상품금액: '', 즉시할인: '', 즉시할인_파트너부담: '', 즉시할인_배민지원: '',
    배민부담_쿠폰할인: '', 총결제금액: '',
    주문중개: '', 고객할인비용: '', 배달: '', 그외: '', 부가세: '',
    만나서결제금액: '', 입금예정금액: ''
};

for (const cell of row.querySelectorAll('td')) {
    const idx = cell.getAttribute('data-td-index');
    const text = cell.textContent.trim();
    if (idx === '1') {
        const badge = cell.querySelector('.Badge_b_r4ax_19agxiso, [data-atelier-component="Badge"] span');
        base.주문상태 = badge?.textContent.trim() || '';
        base.주문번호 = text.replace(base.주문상태, '').trim();
    } else if (idx === '2') { base.주문시각 = text.replace(/\s+/g, ' '); }
    else if (idx === '3') { base.광고상품 = text; }
    else if (idx === '4') { base.캠페인ID = text; }
    else if (idx === '6') { base.결제타입 = text; }
    else if (idx === '7') { base.수령방법 = text; }
    else if (idx === '8') { base.결제금액 = cleanPrice(text); }
}

// 상세 섹션 탐색 (다음 형제 TR)
let detailSection = null;
let sibling = row.nextElementSibling;
while (sibling?.tagName === 'TR') {
    const sec = sibling.querySelector('td[colspan="9"] .DetailInfo-module__pZYe');
    if (sec) { detailSection = sec; break; }
    sibling = sibling.nextElementSibling;
}

if (!detailSection) {
    return [{...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: ''}];
}

// 정산정보 / 주문정보 섹션 분리
const allSections = detailSection.querySelectorAll('section.DetailInfo-module__Sopx');
let orderSection = null, settleSection = null;
for (const sec of allSections) {
    const hdr = sec.querySelector('.DetailInfo-module__bKQt')?.textContent || '';
    if (hdr.includes('정산정보')) settleSection = sec;
    else if (hdr.includes('주문정보')) orderSection = sec;
}
if (!orderSection && allSections.length > 0) orderSection = allSections[0];

// 정산정보 추출
if (settleSection) {
    for (const item of settleSection.querySelectorAll('.FieldItem-module__gYJs')) {
        const label = item.querySelector('.FieldItem-module__YCcw')?.textContent.trim() || '';
        const value = cleanPrice(item.querySelector('.FieldItem-module__rb57')?.textContent || '');
        if (label.includes('(A)')) base.주문중개 = value;
        else if (label.includes('(B)')) base.배달 = value;
        else if (label.includes('(C)')) base.그외 = value;
        else if (label.includes('(D)')) base.부가세 = value;
        else if (label.includes('(E)') || label.includes('만나서결제금액')) base.만나서결제금액 = value;
        else if (label.includes('입금예정금액')) base.입금예정금액 = value;
    }
    for (const li of settleSection.querySelectorAll('.SettleContent-module__Bji5 li')) {
        if (li.textContent.includes('고객할인비용')) {
            base.고객할인비용 = cleanPrice(
                li.closest('p')?.querySelector('.SettleContent-module__E2ID')?.textContent || ''
            );
            break;
        }
    }
}

// 즉시할인 총액
const discountEl = detailSection.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
if (discountEl) base.즉시할인 = cleanPrice(discountEl.textContent);

// 배민부담 쿠폰할인
const couponEl = detailSection.querySelector('.CouponDiscount-module__rTI9');
if (couponEl) {
    const parentLi = couponEl.closest('li');
    if (parentLi) {
        const valEl = parentLi.querySelector(
            '.TextListItem_b_r4ax_n197m77 span, .TextListItem_b_r4ax_n197m76 p'
        );
        base.배민부담_쿠폰할인 = cleanPrice(valEl?.textContent || '');
    }
}

// 총결제금액
const totalEl = orderSection?.querySelector('.DetailInfo-module__PmTR .FieldItem-module__LyiN');
if (totalEl) base.총결제금액 = cleanPrice(totalEl.textContent);

// 메뉴 블록 추출 (옵션별 행 분리)
const menuContainer = orderSection?.querySelector('.DetailInfo-module__j9yH');
const allMenuData = [];

if (menuContainer) {
    for (const menuBlock of menuContainer.querySelectorAll('.DetailInfo-module__pC_2')) {
        const menuInfoEl = menuBlock.querySelector('.DetailInfo-module__nV94');
        const menuName = menuInfoEl?.querySelector('span:first-child')?.textContent.trim() || '';
        const menuQty = menuInfoEl?.querySelector('.DetailInfo-module__QGJz')
            ?.textContent.replace(/[^\d]/g, '') || '1';
        const menuPrice = cleanPrice(menuBlock.querySelector('.FieldItem-module__rb57')?.textContent || '');

        const options = [];
        const optContainer = menuBlock.nextElementSibling;
        if (optContainer?.classList.contains('DetailInfo-module__J1rX')) {
            for (const optDiv of optContainer.querySelectorAll('.DetailInfo-module__n2Ro')) {
                const spans = optDiv.querySelectorAll('span');
                const optName = spans[0]?.textContent.trim() || '';
                let optPrice = '';
                const priceSpan = spans[1];
                if (priceSpan) {
                    const origEl = priceSpan.querySelector('.DetailInfo-module__t8S5');
                    if (origEl) {
                        const clone = priceSpan.cloneNode(true);
                        clone.querySelector('.DetailInfo-module__t8S5')?.remove();
                        optPrice = cleanPrice(clone.textContent);
                    } else {
                        optPrice = cleanPrice(priceSpan.textContent);
                    }
                }
                if (optName) options.push({name: optName, price: optPrice});
            }
        }
        if (options.length === 0) options.push({name: menuName, price: menuPrice});
        if (menuName) allMenuData.push({menuName, menuQty, menuPrice, options});
    }
}

const totalMenuPrice = allMenuData.reduce((s, m) => s + (parseInt(m.menuPrice) || 0), 0);
if (totalMenuPrice > 0) base.상품금액 = totalMenuPrice.toString();

if (allMenuData.length === 0) {
    return [{...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: ''}];
}

const orderSummary = allMenuData[0].menuName +
    (allMenuData.length > 1 ? ` 외 ${allMenuData.length - 1}건` : '');

const result = [];
let isFirst = true;
for (const menu of allMenuData) {
    for (let i = 0; i < menu.options.length; i++) {
        const opt = menu.options[i];
        const optName = (i === 0 && opt.name === '기본') ? menu.menuName : opt.name;
        result.push({
            ...base,
            주문내역: orderSummary, 주문수량: menu.menuQty,
            결제금액: isFirst ? base.결제금액 : '',
            상품금액: isFirst ? base.상품금액 : '',
            즉시할인: isFirst ? base.즉시할인 : '',
            즉시할인_파트너부담: '',
            즉시할인_배민지원: '',
            배민부담_쿠폰할인: isFirst ? base.배민부담_쿠폰할인 : '',
            총결제금액: isFirst ? base.총결제금액 : '',
            주문옵션상세: optName, 주문옵션금액: opt.price,
            주문중개: isFirst ? base.주문중개 : '',
            고객할인비용: isFirst ? base.고객할인비용 : '',
            배달: isFirst ? base.배달 : '',
            그외: isFirst ? base.그외 : '',
            부가세: isFirst ? base.부가세 : '',
            만나서결제금액: isFirst ? base.만나서결제금액 : '',
            입금예정금액: isFirst ? base.입금예정금액 : '',
        });
        isFirst = false;
    }
}
return result;
"""


# ---------------------------------------------------------------------------
# CSV 저장 (upsert by 주문번호)
# ---------------------------------------------------------------------------

_COLUMNS = [
    "collected_at", "store_name",
    "주문상태", "주문번호", "주문시각", "광고상품", "캠페인ID",
    "주문내역", "주문수량", "결제타입", "수령방법",
    "결제금액", "상품금액", "즉시할인", "즉시할인_파트너부담", "즉시할인_배민지원",
    "배민부담_쿠폰할인", "총결제금액",
    "주문옵션상세", "주문옵션금액",
    "주문중개", "고객할인비용", "배달", "그외", "부가세",
    "만나서결제금액", "입금예정금액",
]


def _save_orders_csv(rows: list[dict], brand: str, store: str, target_date: str) -> Path:
    """월별 파일로 upsert 저장. 같은 주문번호의 기존 행은 전부 교체."""
    ym = target_date[:7]
    out_dir = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"orders_{ym}.csv"

    new_df = pd.DataFrame(rows, columns=_COLUMNS).astype(str)
    new_order_ids = set(new_df["주문번호"].unique())

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        existing = existing[~existing["주문번호"].isin(new_order_ids)]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("저장 완료: %s (%d행)", out_path, len(combined))
    return out_path
