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
_TABLE_ROW_CSS = "tr.Table_b_r4ax_1dwbr4on[data-index]"
_MAX_PAGES = 50
_PAGE_TRANSITION_TIMEOUT = 15


# ---------------------------------------------------------------------------
# 공개 함수
# ---------------------------------------------------------------------------

def collect_orders_for_account(
    account_id: str,
    password: str,
    store_list: list[dict],
    target_date: str | None = None,
) -> None:
    """매장별 독립 Chrome 세션으로 주문내역을 수집한다.

    팝업 클릭(즉시할인 파트너부담/배민지원) 후 Chrome을 종료해 메모리를 완전 해제.
    매장마다 새 Chrome 세션 → login → collect → quit.
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    for store_info in store_list:
        store_id = store_info["store_id"]
        brand = store_info["brand"]
        store = store_info["store"]

        logger.info("주문내역 수집 시작: %s (%s) / %s", store, store_id, target_date)
        driver = None
        try:
            # 매장마다 새 Chrome 실행 (팝업 메모리 누적 방지)
            driver = launch_browser(account_id)

            if not login_baemin(driver, account_id, password):
                logger.warning("로그인 실패: %s / %s", account_id, store)
                continue

            driver.set_page_load_timeout(45)
            driver.get(ORDERS_URL)

            if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
                logger.warning("테이블 로드 실패, 건너뜀: %s", store)
                continue

            if not _select_order_store(driver, store_id, store):
                logger.warning("가게 필터 선택 실패, 건너뜀: %s", store)
                continue

            if not _set_date_yesterday(driver):
                logger.warning("날짜 설정 실패, 건너뜀: %s", store)
                continue

            time.sleep(2.0)

            rows = _collect_all_pages(driver, store_info)

            if rows:
                saved = _save_orders_csv(rows, brand, store, target_date)
                logger.info(
                    "저장 완료: brand=%s store=%s → %s (%d행)", brand, store, saved, len(rows)
                )
            else:
                logger.info("주문내역 없음: %s / %s", store, target_date)

        except Exception as e:
            logger.warning("매장 수집 실패 (%s): %s", store, e)
        finally:
            if driver:
                try:
                    driver.quit()  # 팝업 메모리 해제
                except Exception:
                    pass

        time.sleep(random.uniform(1.5, 3.0))


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
        time.sleep(1.0)

        # 2. select 요소 찾아서 JS로 값 설정 + React change 이벤트 dispatch
        sel = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, "div.form-control-wrap select")
            )
        )
        driver.execute_script(
            """
            const sel = arguments[0];
            sel.value = arguments[1];
            sel.dispatchEvent(new Event('change', {bubbles: true}));
            sel.dispatchEvent(new Event('input', {bubbles: true}));
            """,
            sel,
            store_id,
        )
        time.sleep(0.5)

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
        time.sleep(2.0)

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

    except (TimeoutException, Exception) as e:
        logger.warning("가게 필터 선택 오류 (%s): %s", store_name, e)
        return False


# ---------------------------------------------------------------------------
# 날짜 필터 (어제)
# ---------------------------------------------------------------------------

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
        time.sleep(0.8)

        # 2. "일" 라디오 클릭 (value="dailyWeekly")
        daily_radio = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "input[type='radio'][value='dailyWeekly']")
            )
        )
        driver.execute_script("arguments[0].click();", daily_radio)
        time.sleep(0.5)

        # 3. "어제" 레이블 클릭 (value=""는 고유하지 않으므로 텍스트 기반 탐색)
        driver.execute_script(
            """
            const labels = document.querySelectorAll('label');
            for (const lbl of labels) {
                if (lbl.textContent.trim() === '어제') { lbl.click(); return; }
            }
            """
        )
        time.sleep(0.5)

        # 4. 적용 버튼 클릭
        apply_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-atelier-component='Button'][.//span[text()='적용']]")
            )
        )
        human_click(driver, apply_btn)
        time.sleep(2.0)

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

    except (TimeoutException, Exception) as e:
        logger.warning("날짜 필터 설정 오류: %s", e)
        return False


# ---------------------------------------------------------------------------
# 페이지 순회 수집
# ---------------------------------------------------------------------------

def _collect_all_pages(driver, store_info: dict) -> list[dict]:
    """현재 필터 상태에서 전 페이지 주문내역을 수집한다.

    모든 행을 한꺼번에 펼치면 Chrome OOM crash 발생 → 행별 개별 expand/extract/collapse.
    """
    store = store_info["store"]

    row_count = driver.execute_script(
        f"return document.querySelectorAll('{_TABLE_ROW_CSS}').length;"
    ) or 0
    if row_count == 0:
        logger.info("주문 없음 (테이블 행 0): %s", store)
        return []

    all_rows: list[dict] = []
    iso = pendulum.now(KST).isoformat()

    for page_num in range(1, _MAX_PAGES + 1):
        logger.info("%d페이지 수집 중: %s", page_num, store)

        # data-index 목록 수집 (expand/collapse 중에도 안정적)
        indices = driver.execute_script(
            "return [...document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]')]"
            ".map(r => r.getAttribute('data-index'));"
        ) or []

        page_rows: list[dict] = []
        chrome_alive = True
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
                        logger.warning("팝업 클릭 실패 (Chrome OOM?) - 데이터는 유지: %s", popup_err)
                        page_rows.extend(row_data)
                        chrome_alive = False
                        break

                # 4. 행 접기
                driver.execute_script(_COLLAPSE_ROW_JS, idx)
                page_rows.extend(row_data)
                time.sleep(0.1)

            except Exception as row_err:
                logger.warning("행 처리 실패 (Chrome OOM?): idx=%s err=%s", idx, row_err)
                chrome_alive = False
                break

        all_rows.extend(page_rows)
        logger.info(
            "%d페이지: %d행 수집 (누계 %d행)", page_num, len(page_rows), len(all_rows)
        )

        if not chrome_alive:
            logger.warning("Chrome 세션 종료, 현재까지 수집 데이터 반환: %s (%d행)", store, len(all_rows))
            break

        prev_first_id = _get_first_order_id(driver)
        if not _click_next_page(driver, page_num):
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
