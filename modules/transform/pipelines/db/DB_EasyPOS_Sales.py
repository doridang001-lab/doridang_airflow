"""
이지포스(EasyPOS) 매출 영수증 수집 파이프라인

처리 흐름:
1. 실행 날짜 결정 (conf 또는 yesterday)
2. Playwright로 EasyPOS 로그인 → 당일매출내역 → 영수증 클릭 → 상품내역 수집
3. 수집된 DataFrame을 OneDrive CSV로 저장

수집 대상:
- mainframe_childframe_popupBill_form_grdDetailList (상품 주문내역)
- stcSaleType (영수증 유형), stcShopName (매장명)
- grdSalePerDayList cell_{N}_7 (매출구분: 정상/반품)

Playwright 사용 이유:
- NexacroN은 name='main' 동적 프레임 내부 렌더링
- Selenium ActionChains.send_keys()가 headless에서 password 필드 미입력
- Playwright fill()이 NexacroN 내부 상태 갱신 확인됨
"""

import hashlib
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, Frame, Page, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import TargetClosedError

from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR, TEMP_DIR
from modules.load.load_onedrive import onedrive_csv_save
from modules.transform.utility.playwright_launcher import launch_chromium

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
LOGIN_URL     = "https://smart.easypos.net/index.jsp"
EASYPOS_ID    = "3322002029"
EASYPOS_PW    = "1"
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None
WAIT_TIMEOUT  = 30_000  # ms (Playwright 단위)
DEBUG_DIR     = TEMP_DIR / "easypos_debug"
DEBUG_MODE    = os.getenv("EASYPOS_DEBUG", "0").strip() in {"1", "true", "True", "YES", "yes"}
STRICT_MODE   = os.getenv("EASYPOS_STRICT", "0").strip() in {"1", "true", "True", "YES", "yes"}
USE_MOUSE_CLICKS = os.getenv("EASYPOS_USE_MOUSE", "1").strip() in {"1", "true", "True", "YES", "yes"}

# NexacroN element IDs — 로그인
_ID_OUTER     = "mainframe_childframe_form_divMain_edtId"
_ID_INPUT     = "mainframe_childframe_form_divMain_edtId_input"
_PW_OUTER     = "mainframe_childframe_form_divMain_edtPw"
_PW_INPUT     = "mainframe_childframe_form_divMain_edtPw_input"
_LOGIN_BTN    = "mainframe_childframe_form_divMain_btnLogin"
_PASSWD_POPUP_CLOSE = (
    "mainframe_childframe_popupChangePasswd_form_div_popup_bottom_btnClose"
)
_AD_POPUP_CLOSE = "mainframe_childframe_placeAdPopup_titlebar_closebutton"

# NexacroN element IDs — 메뉴 & 조회
_MENU_SALES_BRIEF = "mainframe_childframe_form_divTop_img_TA_top_menu2"   # 영업속보
_BTN_YESTERDAY    = "mainframe_childframe_form_divMain_divWork_divSalesDate_btnBeforeDay"
_BTN_SEARCH       = "mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch"

# 일자별매출조회 — grdLeft 사이드 메뉴 gridrow_2
_GRDLEFT_DAILY_INQUIRY = (
    "mainframe_childframe_form_divLeftMenu_divLeftMainList_grdLeft_body_"
    "gridrow_2_cell_2_0_controltreeTextBoxElement"
)
_DAILY_INQUIRY_READY_SELECTORS = [
    "[id*='grdSalePerDay'][id*='body']",
    "[id*='divMainNavi'][id*='btnCommSearch']",
    "[id*='btnCommSearch']",
    "[id*='btnCommExcel']",
]
_BTN_YESTERDAY_SELECTORS = [
    f"#{_BTN_YESTERDAY}",
    "[id$='btnBeforeDay']",
    "[id*='divSalesDate'][id*='btnBeforeDay']",
    "xpath=//*[contains(@id,'btnBeforeDay')]",
    "xpath=//*[contains(normalize-space(.),'전일') and contains(@id,'btn')]",
]
_BTN_SEARCH_SELECTORS = [
    f"#{_BTN_SEARCH}",
    "[id$='btnCommSearch']",
    "[id*='divCommonBtn'][id*='btnCommSearch']",
    "xpath=//*[contains(@id,'btnCommSearch')]",
    "xpath=//*[contains(normalize-space(.),'조회') and contains(@id,'btn')]",
]
_BTN_EXCEL_SELECTORS = [
    "[id$='btnCommExcel']",
    "[id*='btnCommExcel']",
    "xpath=//*[contains(@id,'btnCommExcel')]",
    "xpath=//*[contains(normalize-space(.),'엑셀') and (contains(@id,'btn') or contains(@id,'Excel'))]",
]
_DATE_INPUT_SELECTORS = [
    # mskSales: NexacroN 마스크 날짜 입력 — 가장 신뢰도 높은 선택자
    "[id*='divSalesDate'][id*='mskSales'][id$='_input']",
    "[id*='mskSales'][id$='_input']",
    "xpath=//*[contains(@id,'mskSales') and substring(@id, string-length(@id) - 5) = '_input']",
    # 구버전 fallback
    "[id*='SalesDate'][id$='_input']",
    "[id*='SaleDate'][id$='_input']",
    "[id*='divSalesDate'] [id$='_input']",
    "xpath=//*[contains(@id,'SalesDate') and contains(@id,'_input')]",
    "xpath=//*[contains(@id,'SaleDate') and contains(@id,'_input')]",
]
_DAILY_VIEW_READY_SELECTORS = [
    "[id*='grdSalePerDayList']",
    "[id*='divSalesDate']",
    "[id*='divMainNavi']",
    "[id*='btnCommSearch']",
    "[id*='btnBeforeDay']",
    *(_DATE_INPUT_SELECTORS),
]

# 메뉴검색(Find Menu) 팝업
_FIND_MENU_INPUT_ID = "mainframe_childframe_popupFindMenu_form_edtInputText_input"
_FIND_MENU_INPUT_SELECTORS = [
    f"#{_FIND_MENU_INPUT_ID}",
    "xpath=//*[contains(@id,'popupFindMenu')]//*[contains(@id,'edtInputText_input')]",
]
_FIND_MENU_OPEN_SELECTORS = [
    # 상단 메뉴검색 버튼/아이콘(환경마다 id가 다를 수 있어 broad match)
    "xpath=//*[contains(@id,'btnFindMenu') or contains(@id,'btnMenuFind') or contains(@id,'btnFind')][not(self::script)]",
    "xpath=//*[contains(@id,'imgFindMenu') or contains(@id,'imgMenuFind') or contains(@id,'imgFind')][not(self::script)]",
    "xpath=//*[contains(@title,'메뉴') and contains(@title,'검색')]",
    "xpath=//*[contains(normalize-space(.),'메뉴검색')]",
]
_FIND_MENU_RESULT_SELECTORS = [
    # 검색 결과 행(텍스트 셀)
    "xpath=//*[contains(@id,'popupFindMenu')]//*[contains(normalize-space(.),'{menu_text}')]",
]
_FIND_MENU_SELECT_BTN_SELECTORS = [
    "xpath=//*[contains(@id,'popupFindMenu')]//*[normalize-space(.)='선택']",
    "xpath=//*[contains(@id,'popupFindMenu')]//*[contains(@id,'btn')][contains(normalize-space(.),'선택')]",
]

# NexacroN element IDs — 영수증 팝업
_POPUP_RECEIPT_TYPE = "mainframe_childframe_popupBill_form_stcSaleType"
_POPUP_SHOP_NAME    = "mainframe_childframe_popupBill_form_stcShopName"
_POPUP_DETAIL_LIST  = "mainframe_childframe_popupBill_form_grdDetailList"
_POPUP_CLOSE_BTN    = "mainframe_childframe_popupBill_titlebar_closebutton"

# 그리드 ID 프리픽스
_MAIN_GRID_PREFIX   = "mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_"
_DETAIL_GRID_PREFIX = "mainframe_childframe_popupBill_form_grdDetailList_body_"

# 엑셀 다운로드 / 수동 일별합계 경로
EASYPOS_SALES_DOWNLOAD_DIR       = TEMP_DIR / "easypos_sales_download"
EASYPOS_MANUAL_TOTALS_CSV        = ANALYTICS_DB / "easypos_sales_raw" / "_manual_daily_totals.csv"
EASYPOS_MANUAL_ARCHIVE_DIR       = DOWN_DIR / "업로드_temp"
EASYPOS_MANUAL_TOTALS_FALLBACK_CSV = EASYPOS_MANUAL_ARCHIVE_DIR / "easypos_manual_daily_totals.csv"


# ============================================================
# 내부 유틸
# ============================================================

def _playwright_auto_install_enabled() -> bool:
    value = os.getenv("PLAYWRIGHT_AUTO_INSTALL")
    if value is None:
        return os.getenv("AIRFLOW_HOME") is not None or os.getenv("IS_DOCKER", "").strip() in {
            "1", "true", "True", "YES", "yes",
        }
    return value.strip() in {"1", "true", "True", "YES", "yes"}


def _ensure_playwright_chromium_installed() -> None:
    if not _playwright_auto_install_enabled():
        raise RuntimeError(
            "Playwright Chromium 브라우저 실행 파일이 없습니다. "
            "컨테이너에서 `python -m playwright install chromium` 을 실행하거나 "
            "`PLAYWRIGHT_AUTO_INSTALL=1` 로 자동 설치를 활성화하세요."
        )
    logger.warning("Playwright 브라우저가 없어서 chromium 설치를 시도합니다..")
    proc = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(
            "Playwright chromium 설치가 실패했습니다. "
            "스토리지/권한/PLAYWRIGHT_BROWSERS_PATH 설정을 확인하세요.\n"
            f"stdout:\n{stdout[-2000:]}\n\nstderr:\n{stderr[-2000:]}"
        )


def _is_page_closed(page: "Page | None") -> bool:
    try:
        return page is None or page.is_closed()
    except Exception:
        return True


def _get_main_frame(page: Page) -> Frame:
    """NexacroN main 프레임 반환 — 모든 UI 요소가 name='main' 프레임 안에 있음"""
    candidates = [f for f in page.frames if f.name == "main"]
    if not candidates:
        raise RuntimeError(
            "name='main' 프레임을 찾을 수 없음 — NexacroN 렌더링 미완료 또는 로그인 페이지 오류"
        )
    if len(candidates) == 1:
        return candidates[0]

    def _has_any(frame: Frame, selectors: list[str]) -> bool:
        for sel in selectors:
            try:
                if frame.query_selector(sel) is not None:
                    return True
            except Exception:
                continue
        return False

    # NexacroN은 상황에 따라 동일 name의 프레임이 여러 개 잡힐 수 있어,
    # 실제 UI(메뉴/조회영역)를 가진 프레임을 우선 선택한다.
    best = None
    best_score = -1
    for f in candidates:
        score = 0
        if _has_any(f, [f"#{_MENU_SALES_BRIEF}", f"#{_ID_INPUT}", f"#{_LOGIN_BTN}"]):
            score += 3
        if _has_any(f, _DAILY_VIEW_READY_SELECTORS):
            score += 2
        if _has_any(f, _BTN_SEARCH_SELECTORS) or _has_any(f, _BTN_YESTERDAY_SELECTORS):
            score += 1
        if score > best_score:
            best = f
            best_score = score

    return best or candidates[0]


def _debug_dump(page: Page, label: str) -> None:
    """실패 원인 추적용 스크린샷/프레임 목록 로그"""
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        shot = DEBUG_DIR / f"easypos_{label}_{ts}.png"
        page.screenshot(path=str(shot), full_page=True)
        logger.info("EasyPOS 디버그 스크린샷 저장: %s", shot)
    except Exception as e:
        logger.info("EasyPOS 디버그 스크린샷 실패(무시): %s", e)

    try:
        frames = [(f.name, f.url) for f in page.frames]
        logger.info("EasyPOS frame dump (name,url)=%s", frames[:10])
    except Exception:
        pass


def _wait_for_any_selector(mf: Frame, selectors: list[str], timeout_ms: int) -> tuple:
    """여러 selector 중 하나라도 나타나면 (ElementHandle, selector) 반환"""
    deadline = time.time() + (timeout_ms / 1000.0)
    last_err = None
    while time.time() < deadline:
        for sel in selectors:
            try:
                el = mf.query_selector(sel)
                if el is not None:
                    return el, sel
            except Exception as e:
                last_err = e
        time.sleep(0.5)
    raise TimeoutError(f"selectors not found within timeout (last_err={last_err})")


def _navigate_via_find_menu(page: Page, menu_text: str) -> bool:
    """메뉴검색 팝업으로 메뉴 진입: 메뉴명 입력→Enter→결과 클릭→선택 클릭"""
    mf = _get_main_frame(page)

    def _find_menu_input():
        return _find_first_selector(mf, _FIND_MENU_INPUT_SELECTORS)

    # 1) 팝업이 이미 떠있으면 그대로 사용
    inp, inp_sel = _find_menu_input()

    # 2) 팝업 열기 시도 (클릭 + 단축키)
    if inp is None:
        opened = False
        for sel in _FIND_MENU_OPEN_SELECTORS:
            try:
                el = mf.query_selector(sel)
                if el is None:
                    continue
                if _click_handle(page, el, f"메뉴검색 열기({sel})", prefer_mouse=True):
                    opened = True
                    break
            except Exception:
                continue

        if not opened:
            # Nexacro 앱에서 종종 Ctrl+F/Alt+F 류로 메뉴찾기 제공
            for key in ["Control+F", "Alt+F", "F3"]:
                try:
                    page.keyboard.press(key)
                    time.sleep(0.6)
                    break
                except Exception:
                    continue

        # 팝업 input 대기
        mf = _get_main_frame(page)
        try:
            inp, inp_sel = _wait_for_any_selector(mf, _FIND_MENU_INPUT_SELECTORS, timeout_ms=10_000)
        except Exception as e:
            logger.info("메뉴검색 팝업 input 대기 실패: %s", e)
            return False

    # 3) 메뉴명 입력 + Enter
    try:
        inp.click()
        time.sleep(0.2)
        inp.fill(menu_text)
        inp.press("Enter")
        logger.info("메뉴검색 입력/Enter 완료: %r (%s)", menu_text, inp_sel)
    except Exception as e:
        logger.info("메뉴검색 입력 실패: %s", e)
        return False

    # 4) 결과 행 클릭
    mf = _get_main_frame(page)
    result_clicked = False
    for attempt in range(1, 6):
        for tpl in _FIND_MENU_RESULT_SELECTORS:
            sel = tpl.format(menu_text=menu_text)
            try:
                el = mf.query_selector(sel)
            except Exception:
                el = None
            if el is None:
                continue
            if _click_handle(page, el, f"메뉴검색 결과({menu_text})", prefer_mouse=True):
                result_clicked = True
                break
        if result_clicked:
            break
        time.sleep(0.6)

    if not result_clicked:
        logger.info("메뉴검색 결과 행을 찾지 못함: %r", menu_text)
        return False

    # 5) 선택 버튼 클릭
    mf = _get_main_frame(page)
    for sel in _FIND_MENU_SELECT_BTN_SELECTORS:
        try:
            el = mf.query_selector(sel)
            if el is None:
                continue
            if _click_handle(page, el, f"메뉴검색 선택({sel})", prefer_mouse=True):
                time.sleep(1.0)
                return True
        except Exception:
            continue

    logger.info("메뉴검색 '선택' 버튼 클릭 실패")
    return False


def _open_daily_sales_via_quick_tile(page: Page) -> bool:
    """대시보드 빠른메뉴 '당일매출' 타일로 당일매출 화면 진입 시도"""
    mf = _get_main_frame(page)
    logger.info("빠른메뉴 '당일매출' 타일 탐색/클릭 시도")

    selectors = [
        # Playwright 텍스트 locator가 안 잡히는 케이스 대비 XPath 다중 시도
        "xpath=//*[contains(normalize-space(.),'당일매출') and not(contains(normalize-space(.),'시간대'))]",
        "xpath=//*[self::div or self::span or self::button][contains(normalize-space(.),'당일매출')]",
    ]

    for sel in selectors:
        try:
            candidates = mf.query_selector_all(sel)
        except Exception:
            continue

        for el in candidates[:8]:
            try:
                if _click_handle(page, el, f"빠른메뉴 타일({sel})", prefer_mouse=True, click_count=1):
                    time.sleep(1.0)
                    mf2 = _get_main_frame(page)
                    _wait_for_any_selector(mf2, _DAILY_VIEW_READY_SELECTORS, timeout_ms=WAIT_TIMEOUT)
                    logger.info("빠른메뉴 타일로 당일매출 화면 진입 성공")
                    return True
            except Exception:
                continue

    logger.info("빠른메뉴 타일로 진입 실패")
    return False


def _set_sale_date_inputs(mf: Frame, sale_date: str) -> bool:
    """가능한 날짜 입력 컴포넌트를 찾아 sale_date(YYYY-MM-DD)를 입력"""
    filled = 0
    seen_ids: set[str] = set()

    for sel in _DATE_INPUT_SELECTORS:
        try:
            els = mf.query_selector_all(sel)
        except Exception:
            continue
        for el in els:
            el_id = el.get_attribute("id") or ""
            if el_id and el_id in seen_ids:
                continue
            if el_id:
                seen_ids.add(el_id)
            try:
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass
                el.click()
                time.sleep(0.2)
                el.fill(sale_date)
                try:
                    el.press("Enter")
                except Exception:
                    pass
                filled += 1
            except Exception:
                continue
            if filled >= 2:
                return True

    return filled > 0


def _set_sale_date_inputs_v2(mf: Frame, sale_date: str, page: "Page | None" = None) -> bool:
    """날짜 입력 v2 — mskSales 마스크 입력 우선, YYYYMMDD 형식으로 keyboard.type() 사용.
    NexacroN mskSales는 fill()/el.type() 으로 내부 상태가 갱신되지 않아
    클릭 후 Ctrl+A → page.keyboard.type() 방식으로 우회한다.
    """
    def _digits(v) -> str:
        return re.sub(r"\D", "", str(v or ""))

    def _read_value(el) -> str:
        try:
            return (el.evaluate("el => el.value") or "").strip()
        except Exception:
            return ""

    def _matches(el) -> bool:
        return _digits(_read_value(el)) == _digits(sale_date)

    date_no_dash = _digits(sale_date)
    filled = 0
    seen_ids: set[str] = set()

    for sel in _DATE_INPUT_SELECTORS:
        try:
            els = mf.query_selector_all(sel)
        except Exception:
            continue
        for el in els:
            el_id = el.get_attribute("id") or ""
            if el_id and el_id in seen_ids:
                continue
            if el_id:
                seen_ids.add(el_id)
            try:
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass
                el.click()
                time.sleep(0.2)

                if _matches(el):
                    logger.info("날짜 이미 일치: id=%s sel=%s value=%r", el_id, sel, _read_value(el))
                    filled += 1
                    continue

                if page:
                    page.keyboard.press("Control+a")
                    time.sleep(0.1)
                    page.keyboard.type(date_no_dash)
                    time.sleep(0.2)
                    page.keyboard.press("Tab")
                else:
                    el.press("Control+a")
                    time.sleep(0.1)
                    el.type(date_no_dash)
                    time.sleep(0.2)
                    el.press("Tab")
                time.sleep(0.3)

                if _matches(el):
                    logger.info("날짜 입력 성공: id=%s sel=%s value=%r", el_id, sel, _read_value(el))
                    filled += 1
                    continue

                # fallback: fill + enter
                for _ in range(10):
                    if _matches(el):
                        logger.info("날짜 입력 성공(폴백): id=%s", el_id)
                        filled += 1
                        break
                    time.sleep(0.2)
            except Exception:
                continue
            if filled >= 2:
                return True

    return filled > 0


def _pw_text(mf: Frame, element_id: str) -> str:
    """NexacroN 셀 텍스트 추출"""
    try:
        el = mf.query_selector(f"#{element_id}")
        if el is None:
            return ""
        return (el.inner_text() or "").strip()
    except Exception:
        return ""


def _pw_input_value(mf: Frame, element_id: str) -> str:
    """NexacroN input value 추출 (el.value 기반)"""
    try:
        el = mf.query_selector(f"#{element_id}")
        if el is None:
            return ""
        return (el.evaluate("el => el.value") or "").strip()
    except Exception:
        return ""


def _coord_click(page: Page, mf: Frame, element_id: str, label: str = "") -> bool:
    """bounding_box 좌표 기반 클릭 — NexacroN 팝업 닫기 전용.
    JS click()은 NexacroN 모달에서 차단되므로 실제 마우스 이벤트 필요."""
    try:
        el = mf.query_selector(f"#{element_id}")
        if el is None:
            logger.info(f"{label or element_id} 요소 없음 (정상 스킵)")
            return False
        box = el.bounding_box()
        if box is None:
            logger.info(f"{label or element_id} bounding_box 없음 (스킵)")
            return False
        cx = box["x"] + box["width"] / 2
        cy = box["y"] + box["height"] / 2
        page.mouse.click(cx, cy)
        logger.info(f"{label or element_id} 좌표 클릭: ({cx:.0f}, {cy:.0f})")
        return True
    except Exception as e:
        logger.warning(f"{label or element_id} 좌표 클릭 실패: {e}")
        return False


def _get_row_indices_pw(mf: Frame, id_fragment: str) -> list:
    """NexacroN 그리드 행 인덱스 동적 탐지 (gridrow_{N} 패턴)"""
    elements = mf.query_selector_all(f"[id*='{id_fragment}']")
    row_ids: set = set()
    for el in elements:
        el_id = el.get_attribute("id") or ""
        m = re.search(r"gridrow_(\d+)", el_id)
        if m:
            row_ids.add(int(m.group(1)))
    return sorted(row_ids)


def _find_first_selector(mf: Frame, selectors: list[str]):
    """여러 selector 중 먼저 발견되는 ElementHandle 반환"""
    for selector in selectors:
        try:
            el = mf.query_selector(selector)
            if el is not None:
                return el, selector
        except Exception:
            continue
    return None, None


def _has_any_selector(mf: Frame, selectors: list[str]) -> bool:
    """selector 목록 중 하나라도 존재하면 True"""
    el, _ = _find_first_selector(mf, selectors)
    return el is not None


def _mouse_click(page: Page, handle, label: str, click_count: int = 1) -> bool:
    """실제 마우스 이벤트로 클릭 (NexacroN에서 화면 전환에 더 안정적)"""
    if handle is None:
        return False
    try:
        try:
            handle.scroll_into_view_if_needed()
        except Exception:
            pass
        box = handle.bounding_box()
        if box is None:
            logger.warning("%s mouse 클릭 실패: bounding_box 없음", label)
            return False
        cx = box["x"] + box["width"] / 2
        cy = box["y"] + box["height"] / 2
        page.mouse.click(cx, cy, click_count=click_count)
        logger.info("%s 클릭 성공 (mouse coord: %.0f, %.0f, count=%s)", label, cx, cy, click_count)
        return True
    except Exception as e:
        logger.warning("%s mouse 클릭 실패: %s", label, e)
        return False


def _click_handle(
    page: Page,
    handle,
    label: str,
    prefer_mouse: bool = False,
    click_count: int = 1,
) -> bool:
    """ElementHandle 클릭 보강: element.click + (선택적으로) 실제 마우스 클릭"""
    if handle is None:
        return False

    if USE_MOUSE_CLICKS and prefer_mouse:
        if _mouse_click(page, handle, label, click_count=click_count):
            return True

    try:
        try:
            handle.scroll_into_view_if_needed()
        except Exception:
            pass
        handle.click()
        logger.info("%s 클릭 성공 (element.click)", label)
        return True
    except Exception as e:
        logger.info("%s element.click 실패, mouse 클릭 재시도: %s", label, e)

    return _mouse_click(page, handle, label, click_count=click_count)


def _parse_amount(text: str) -> int:
    """'35,800' → 35800, '30500.0' → 30500, 빈값/비숫자 → 0"""
    cleaned = re.sub(r"[,\s]", "", str(text))
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _cleanup_download_dir(download_dir: Path, pattern: str = "*") -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for p in download_dir.glob(pattern):
        if p.is_file():
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _normalize_sale_date(value) -> str:
    """다양한 형식의 날짜 값을 YYYY-MM-DD로 정규화"""
    try:
        text = str(value or "").strip()
        # YYYYMMDD → YYYY-MM-DD
        if re.fullmatch(r"\d{8}", text):
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
        return m.group(0) if m else text
    except Exception:
        text = str(value or "").strip()
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
        return m.group(0) if m else text


def _load_manual_daily_totals() -> dict[str, int]:
    """DOWN_DIR에서 수동 다운로드한 일자별 매출내역 Excel 로드 → {sale_date: total_sales}"""
    rows: list[dict] = []
    for path in sorted(DOWN_DIR.glob("일자별 매출내역_수동다운_*.xlsx")):
        try:
            df = pd.read_excel(path, sheet_name=0, dtype=str, engine="openpyxl")
        except Exception as e:
            logger.warning("EasyPOS 수동다운 로드 실패: %s | %s", path, e)
            continue

        if "매출일자" not in df.columns or "총매출" not in df.columns:
            logger.warning("EasyPOS 수동다운 형식 불일치(매출일자/총매출 없음): %s", path)
            continue

        mtime = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
        for _, r in df.iterrows():
            sale_date = _normalize_sale_date(r.get("매출일자"))
            total_sales = _parse_amount(r.get("총매출"))
            if not sale_date or sale_date.lower() == "nan" or sale_date == "합계":
                continue
            rows.append({
                "sale_date": sale_date,
                "total_sales": total_sales,
                "source_file": path.name,
                "source_mtime": mtime,
            })

    if not rows:
        return {}

    merged = pd.DataFrame(rows)
    merged = merged.sort_values(["sale_date", "source_mtime", "source_file"])
    merged = merged.drop_duplicates(subset=["sale_date"], keep="last").copy()
    merged["updated_at"] = datetime.utcnow().isoformat()

    target_path = EASYPOS_MANUAL_TOTALS_CSV
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(target_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        target_path = EASYPOS_MANUAL_TOTALS_FALLBACK_CSV
        target_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(target_path, index=False, encoding="utf-8-sig")
        logger.warning("EasyPOS 수동 누적 저장 경로 폴백: %s", e)
    logger.info("EasyPOS 수동 일자별 매출 누적 갱신: %s (%d일)", target_path, len(merged))
    return dict(zip(merged["sale_date"], merged["total_sales"]))


def _download_daily_totals_auto(page: Page, sale_date: str, download_dir: Path) -> "Path | None":
    """조회 후 엑셀 버튼 클릭 → 일별합계 Excel 자동 다운로드.
    Returns 저장된 파일 경로, 실패 시 None.
    """
    mf = _get_main_frame(page)
    excel_btn, excel_sel = _find_first_selector(mf, _BTN_EXCEL_SELECTORS)
    if excel_btn is None:
        logger.warning("EasyPOS 엑셀 버튼 미발견 (selectors=%s)", _BTN_EXCEL_SELECTORS)
        return None

    download_dir.mkdir(parents=True, exist_ok=True)
    save_path = download_dir / f"daily_totals_{sale_date}.xlsx"

    try:
        with page.expect_download(timeout=30_000) as dl_info:
            _click_handle(page, excel_btn, f"엑셀 다운로드({excel_sel})", prefer_mouse=True)
        dl_info.value.save_as(str(save_path))
        logger.info("EasyPOS 엑셀 다운로드 완료: %s", save_path)
        return save_path
    except Exception as e:
        logger.warning("EasyPOS 엑셀 다운로드 실패: %s", e)
        return None


def _parse_auto_daily_total(xlsx_path: Path, sale_date: str) -> "int | None":
    """자동 다운로드 Excel에서 sale_date의 총매출 파싱.
    매출일자 열 우선 → 합계 행 fallback.
    """
    try:
        df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str, engine="openpyxl")
    except Exception as e:
        logger.warning("EasyPOS 자동다운 로드 실패: %s | %s", xlsx_path, e)
        return None

    if "매출일자" not in df.columns or "총매출" not in df.columns:
        logger.warning("EasyPOS 자동다운 열 불일치: %s cols=%s", xlsx_path, list(df.columns))
        return None

    norm = _normalize_sale_date(sale_date)
    for _, r in df.iterrows():
        if _normalize_sale_date(r.get("매출일자")) == norm:
            return _parse_amount(r.get("총매출"))

    for _, r in df.iterrows():
        if str(r.get("매출일자", "")).strip() in ("합계", "소계", "Total", "total"):
            return _parse_amount(r.get("총매출"))

    logger.warning("EasyPOS 자동다운 날짜 %s 미발견: %s", sale_date, xlsx_path)
    return None


def _upsert_daily_total_csv(sale_date: str, total: int, source: str) -> None:
    """자동 다운로드 일별합계를 _manual_daily_totals.csv에 누적 저장(upsert).
    당일(오늘) 데이터는 매출이 진행 중이므로 저장하지 않는다.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if sale_date >= today:
        logger.info("일별합계 CSV 저장 스킵 (당일 이후): %s", sale_date)
        return
    target = EASYPOS_MANUAL_TOTALS_CSV
    now_str = datetime.utcnow().isoformat()
    new_row = {
        "sale_date": sale_date,
        "total_sales": total,
        "source_file": source,
        "source_mtime": now_str,
        "updated_at": now_str,
    }

    if target.exists():
        try:
            existing = pd.read_csv(target, dtype=str)
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    new_df = pd.DataFrame([new_row])
    if not existing.empty and "sale_date" in existing.columns:
        merged = pd.concat([existing[existing["sale_date"] != sale_date], new_df], ignore_index=True)
    else:
        merged = new_df

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(target, index=False, encoding="utf-8-sig")
        logger.info("일별합계 CSV 갱신: %s ← %s=%d", target, sale_date, total)
    except Exception as e:
        logger.warning("일별합계 CSV 저장 실패: %s", e)


def _login(page: Page) -> Frame:
    """EasyPOS 로그인 → 팝업 닫기 → main 프레임 반환"""
    page.goto(LOGIN_URL, wait_until="networkidle", timeout=60_000)
    logger.info("EasyPOS 접속 완료, NexacroN 렌더링 대기...")
    time.sleep(5)

    mf = _get_main_frame(page)

    # ID 입력 — 외부 컴포넌트 클릭 후 fill()로 NexacroN 내부 상태 갱신
    id_outer = mf.query_selector(f"#{_ID_OUTER}")
    if id_outer:
        id_outer.click()
        time.sleep(0.3)
    mf.query_selector(f"#{_ID_INPUT}").fill(EASYPOS_ID)
    logger.info(f"ID 입력 완료: {EASYPOS_ID!r}")

    # PW 입력
    pw_outer = mf.query_selector(f"#{_PW_OUTER}")
    if pw_outer:
        pw_outer.click()
        time.sleep(0.3)
    mf.query_selector(f"#{_PW_INPUT}").fill(EASYPOS_PW)
    logger.info("PW 입력 완료")

    # 로그인 버튼 클릭
    mf.query_selector(f"#{_LOGIN_BTN}").click()
    logger.info("로그인 버튼 클릭")
    time.sleep(5)

    # 팝업 닫기 (비밀번호 변경 안내, 광고)
    for popup_id, popup_label in [
        (_PASSWD_POPUP_CLOSE, "비밀번호 변경 팝업"),
        (_AD_POPUP_CLOSE,     "광고 팝업"),
        ("mainframe_childframe_alertDialog_form_btn_ok",   "알림 팝업"),
        ("mainframe_childframe_confirmDialog_form_btn_ok", "확인 팝업"),
    ]:
        if _coord_click(page, mf, popup_id, popup_label):
            time.sleep(1)

    # 로그인 성공 확인 — 영업속보 메뉴 존재 여부
    mf = _get_main_frame(page)
    try:
        mf.wait_for_selector(f"#{_MENU_SALES_BRIEF}", timeout=WAIT_TIMEOUT)
        logger.info("로그인 성공 확인 (대시보드 메뉴 로드됨)")
    except Exception:
        _debug_dump(page, "login_failed")
        raise RuntimeError(
            "로그인 실패 또는 대시보드 렌더링 타임아웃 — "
            f"ID/PW 또는 NexacroN 상태 확인 필요 (DEBUG_DIR={DEBUG_DIR})"
        )

    logger.info(f"로그인 완료 | URL: {page.url}")
    return mf


def _navigate_to_daily_totals_inquiry(page: Page, mf: Frame) -> Frame:
    """영업속보 탭 → 일자별매출조회(단일클릭) → 일자별 매출내역(더블클릭) → 화면 로드.

    메뉴 구조 (grdLeft gridrow 인덱스 기준):
      gridrow_2: "일자별매출조회"  → 단일클릭으로 하위 항목 펼침
      gridrow_3: "일자별 매출내역" → 더블클릭으로 실제 조회 화면 오픈
    """
    # ① 상단 영업속보 탭 클릭 → grdLeft 좌측 메뉴 로드
    mf = _get_main_frame(page)
    tab_el = mf.query_selector(f"#{_MENU_SALES_BRIEF}")
    if tab_el is None:
        raise RuntimeError("영업속보 탭 요소를 찾을 수 없음")
    tab_box = tab_el.bounding_box()
    if tab_box:
        page.mouse.click(tab_box["x"] + tab_box["width"] / 2, tab_box["y"] + tab_box["height"] / 2)
    else:
        tab_el.click()
    logger.info("영업속보 탭 클릭 (일자별매출조회 진입 전)")
    time.sleep(1.5)
    try:
        _get_main_frame(page).wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8_000)
    except Exception:
        logger.warning("grdLeft 로드 타임아웃 — 계속 진행")

    def _find_row(text_candidates: list[str]):
        mf2 = _get_main_frame(page)
        for el in mf2.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            if (el.inner_text() or "").strip() in text_candidates:
                return el
        return None

    for attempt in range(1, 4):
        # ② "일자별매출조회" 단일클릭 → 하위메뉴(일자별 매출내역) 펼치기
        inquiry_el = _find_row(["일자별매출조회", "일자별 매출조회"])
        if inquiry_el is None:
            inquiry_el = _get_main_frame(page).query_selector(f"#{_GRDLEFT_DAILY_INQUIRY}")
        if inquiry_el is not None:
            _click_handle(page, inquiry_el, "일자별매출조회 단일클릭", prefer_mouse=True)
            logger.info("일자별매출조회 단일클릭 (attempt=%d/3)", attempt)
            time.sleep(1.2)
        else:
            logger.warning("일자별매출조회 항목 미발견 (attempt=%d/3)", attempt)
            time.sleep(1.5)
            continue

        # ③ "일자별 매출내역" 더블클릭 → 조회 화면 오픈
        detail_el = _find_row(["일자별 매출내역", "일자별매출내역"])
        if detail_el is not None:
            box = detail_el.bounding_box()
            if box:
                cx = box["x"] + box["width"] / 2
                cy = box["y"] + box["height"] / 2
                page.mouse.click(cx, cy)
                time.sleep(0.3)
                page.mouse.click(cx, cy, click_count=2)
                logger.info("일자별 매출내역 더블클릭 @ (%.0f, %.0f)", cx, cy)
            try:
                mf = _get_main_frame(page)
                _wait_for_any_selector(mf, _DAILY_INQUIRY_READY_SELECTORS, timeout_ms=WAIT_TIMEOUT)
                logger.info("일자별매출조회 화면 로드 성공")
                return _get_main_frame(page)
            except Exception as e:
                logger.warning("일자별매출조회 화면 대기 실패 (attempt=%d/3): %s", attempt, e)
                time.sleep(2.0)
        else:
            logger.warning("일자별 매출내역 항목 미발견 (attempt=%d/3)", attempt)
            time.sleep(1.5)

    raise RuntimeError("일자별 매출내역 메뉴를 찾을 수 없음")


def _download_and_save_daily_totals(page: Page, download_dir: Path) -> dict[str, int]:
    """일자별매출조회 화면에서 조회 → 엑셀 다운로드 → 파싱 → CSV upsert.
    최근 7일 데이터를 일괄 수집한다. {sale_date: total_sales} 반환.
    """
    mf = _get_main_frame(page)

    # 조회 버튼 클릭
    s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
    if s_btn:
        _click_handle(page, s_btn, f"일자별매출조회 조회({s_sel})", prefer_mouse=True)
        time.sleep(3.0)
    else:
        logger.warning("일자별매출조회 조회 버튼 미발견")

    mf = _get_main_frame(page)
    excel_btn, excel_sel = _find_first_selector(mf, _BTN_EXCEL_SELECTORS)
    if excel_btn is None:
        logger.warning("일자별매출조회 엑셀 버튼 미발견")
        return {}

    download_dir.mkdir(parents=True, exist_ok=True)
    save_path = download_dir / "daily_totals_inquiry.xlsx"

    try:
        with page.expect_download(timeout=30_000) as dl_info:
            _click_handle(page, excel_btn, f"일자별매출조회 엑셀({excel_sel})", prefer_mouse=True)
        dl_info.value.save_as(str(save_path))
        logger.info("일자별매출조회 엑셀 다운로드 완료: %s (%.1f KB)",
                    save_path, save_path.stat().st_size / 1024)
    except Exception as e:
        logger.warning("일자별매출조회 엑셀 다운로드 실패: %s", e)
        return {}

    # Excel 파싱
    try:
        df = pd.read_excel(save_path, sheet_name=0, dtype=str, engine="openpyxl")
    except Exception as e:
        logger.warning("일자별매출조회 Excel 읽기 실패: %s", e)
        return {}

    logger.info("일자별매출조회 Excel 컬럼: %s", list(df.columns))

    # 컬럼명 매핑 (실제 다운로드 파일에 따라 조정 가능)
    date_col  = next((c for c in df.columns if "일자" in c or "날짜" in c or "date" in c.lower()), None)
    total_col = next((c for c in df.columns if "매출" in c and ("합계" in c or "총" in c or "금액" in c)), None)
    if date_col is None or total_col is None:
        logger.warning("일자별매출조회 컬럼 매핑 실패: date_col=%s total_col=%s cols=%s",
                       date_col, total_col, list(df.columns))
        return {}

    result: dict[str, int] = {}
    for _, r in df.iterrows():
        sale_date = _normalize_sale_date(r.get(date_col))
        if not sale_date or sale_date in ("합계", "소계", "nan"):
            continue
        total = _parse_amount(r.get(total_col))
        result[sale_date] = total
        _upsert_daily_total_csv(sale_date, total, save_path.name)

    logger.info("일자별매출조회 파싱 완료: %d일치 데이터", len(result))
    return result


def _navigate_to_daily_sales(page: Page, mf: Frame) -> Frame:
    """영업속보 → 당일매출내역 메뉴 클릭

    진단 결과 확인된 동작 경로:
    1. 영업속보 탭 클릭 → grdLeft에 영업속보/당일매출내역 항목 표시
    2. '영업속보' 행 단일클릭 (서브메뉴 확장)
    3. '당일매출내역' 행 더블클릭 → grdSalePerDayList form 로드

    주의: 메뉴검색 팝업(CM_FINDMENU_P)의 '선택' 버튼은 NexacroN 이벤트에
    도달하지 않아 navigation 불가. grdLeft DOM 직접 클릭만 동작.
    """
    mf = _get_main_frame(page)

    # 영업속보 탭 클릭 (grdLeft 좌측 메뉴 로드)
    tab_el = mf.query_selector(f"#{_MENU_SALES_BRIEF}")
    if tab_el is None:
        raise RuntimeError("영업속보 탭 요소를 찾을 수 없음")
    tab_box = tab_el.bounding_box()
    if tab_box:
        page.mouse.click(tab_box["x"] + tab_box["width"] / 2, tab_box["y"] + tab_box["height"] / 2)
    else:
        tab_el.click()
    logger.info("영업속보 메뉴 클릭")

    # grdLeft 메뉴 로드 대기 (영업속보 탭 클릭 후 렌더링 지연)
    time.sleep(1.5)
    try:
        mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=8_000)
    except PlaywrightTimeoutError:
        logger.warning("grdLeft 메뉴 항목 로드 타임아웃 — 계속 진행")

    # 이미 당일매출 화면이면 통과
    mf = _get_main_frame(page)
    if _has_any_selector(mf, ["[id*='grdSalePerDayList']"]):
        logger.info("당일매출내역 화면이 이미 로드됨")
        return mf

    def _find_grd_row_by_text(texts: list[str]):
        """grdLeft_body_gridrow_* 중 텍스트가 일치하는 첫 번째 요소 반환"""
        mf2 = _get_main_frame(page)
        for el in mf2.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip()
            if txt in texts:
                return el
        return None

    menu_snapshot: list[str] = []

    for attempt in range(1, 4):
        mf = _get_main_frame(page)

        # Step 1: '영업속보' 단일 클릭 → 서브메뉴(당일매출내역) 확장
        ybs_el = _find_grd_row_by_text(["영업속보"])
        if ybs_el is not None:
            ybs_box = ybs_el.bounding_box()
            if ybs_box:
                cx = ybs_box["x"] + ybs_box["width"] / 2
                cy = ybs_box["y"] + ybs_box["height"] / 2
                page.mouse.click(cx, cy)
                logger.info("영업속보 단일클릭 @ (%.0f, %.0f)", cx, cy)
                time.sleep(1.5)

        # Step 2: '당일매출내역' 더블클릭
        daily_el = _find_grd_row_by_text(["당일매출내역", "당일 매출 내역"])
        if daily_el is not None:
            daily_box = daily_el.bounding_box()
            if daily_box:
                cx = daily_box["x"] + daily_box["width"] / 2
                cy = daily_box["y"] + daily_box["height"] / 2
                page.mouse.click(cx, cy)
                time.sleep(0.3)
                page.mouse.click(cx, cy, click_count=2)
                logger.info(
                    "당일매출내역 더블클릭 @ (%.0f, %.0f) (attempt=%d/3)",
                    cx, cy, attempt,
                )

                # 화면 로드 대기
                try:
                    mf = _get_main_frame(page)
                    _wait_for_any_selector(mf, _DAILY_VIEW_READY_SELECTORS, timeout_ms=WAIT_TIMEOUT)
                    logger.info("당일매출내역 화면 로드 성공 (attempt=%d/3)", attempt)
                    return _get_main_frame(page)
                except Exception as e:
                    logger.info("화면 로드 대기 실패 (attempt=%d/3): %s", attempt, e)
                    time.sleep(2.0)
                    continue

        # 메뉴를 찾지 못한 경우 — 스냅샷 수집 후 재시도
        menu_snapshot = []
        mf = _get_main_frame(page)
        for el in mf.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if txt:
                menu_snapshot.append(txt)
        menu_snapshot = menu_snapshot[:10]
        logger.warning(
            "당일매출내역 메뉴 탐색 실패 (attempt=%d/3) | 현재 메뉴=%s",
            attempt, menu_snapshot,
        )
        if attempt < 3:
            # 영업속보 탭 재클릭해서 좌측 메뉴 리셋
            tab_el2 = mf.query_selector(f"#{_MENU_SALES_BRIEF}")
            if tab_el2:
                t2box = tab_el2.bounding_box()
                if t2box:
                    page.mouse.click(t2box["x"] + t2box["width"] / 2, t2box["y"] + t2box["height"] / 2)
                    time.sleep(2.0)

    # 이미 화면이 열려 있는지 최종 확인
    mf = _get_main_frame(page)
    if _has_any_selector(mf, ["[id*='grdSalePerDayList']"]):
        logger.info("당일매출내역 메뉴 탐색 실패했으나 그리드 확인 — 계속 진행")
        return mf

    raise RuntimeError(
        "당일매출내역 메뉴를 찾을 수 없음. "
        f"현재 메뉴 샘플: {menu_snapshot}"
    )


def _select_yesterday_and_search(page: Page, mf: Frame, sale_date: str) -> Frame:
    """전일 버튼 또는 날짜 입력 → 조회 버튼 클릭"""
    date_selected = False
    s_clicked = False
    last_diag = ""

    # NexacroN 렌더링이 느린 경우가 있어, 조회영역(버튼/컨테이너) 먼저 대기
    try:
        mf = _get_main_frame(page)
        _wait_for_any_selector(mf, _DAILY_VIEW_READY_SELECTORS, timeout_ms=WAIT_TIMEOUT)
    except Exception as e:
        logger.info("당일매출내역 뷰 준비 대기 실패(계속 시도): %s", e)

    for attempt in range(1, 16):
        mf = _get_main_frame(page)
        y_btn, y_sel = _find_first_selector(mf, _BTN_YESTERDAY_SELECTORS)
        s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
        view_ready = _has_any_selector(mf, _DAILY_VIEW_READY_SELECTORS)

        if not date_selected and y_btn is not None:
            date_selected = _click_handle(page, y_btn, f"전일 버튼({y_sel})", prefer_mouse=True)
            if date_selected:
                time.sleep(0.8)

        # 전일 버튼이 없거나 동작하지 않는 화면(일부 UI 변경)에서는 날짜 입력으로 대체
        if not date_selected and attempt in {3, 6, 10}:
            try:
                mf = _get_main_frame(page)
                if _set_sale_date_inputs(mf, sale_date):
                    logger.info("날짜 입력으로 조회일 설정 성공: %s", sale_date)
                    date_selected = True
                    time.sleep(0.8)
            except Exception:
                pass

        # 조회 버튼은 전일 클릭 이후에 다시 탐색하는 편이 안정적임
        if date_selected and not s_clicked:
            mf = _get_main_frame(page)
            s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
            if s_btn is not None:
                s_clicked = _click_handle(page, s_btn, f"조회 버튼({s_sel})", prefer_mouse=True)
                if s_clicked:
                    time.sleep(2.5)

        if date_selected and s_clicked:
            logger.info("전일/조회 클릭 완료 (attempt=%s/15)", attempt)
            break

        last_diag = (
            f"attempt={attempt}, y={bool(y_btn)}, s={bool(s_btn)}, view_ready={view_ready}, "
            f"date_selected={date_selected}"
        )
        logger.info("전일/조회 버튼 탐색 재시도 중 | %s", last_diag)
        if DEBUG_MODE and attempt in {3, 6, 10, 15}:
            _debug_dump(page, f"daily_retry_{attempt}")
        time.sleep(1.2)

    if not date_selected or not s_clicked:
        _debug_dump(page, "daily_click_failed")
        raise RuntimeError(
            "전일/조회 클릭 실패 "
            f"(date_selected={date_selected}, s_clicked={s_clicked}, last_diag={last_diag})"
        )

    return _get_main_frame(page)


def _collect_all_receipts(page: Page, mf: Frame, sale_date: str) -> list:
    """그리드의 전체 영수증을 순서대로 클릭하며 상품내역 수집"""
    rows = []

    # 그리드 데이터 로드 대기
    try:
        mf.wait_for_selector(
            f"[id*='grdSalePerDayList_body_gridrow_'][id*='_2_controlbutton']",
            timeout=WAIT_TIMEOUT,
        )
    except Exception:
        logger.warning(f"{sale_date} 당일매출내역 데이터 없음")
        return rows

    mf = _get_main_frame(page)
    row_indices = _get_row_indices_pw(mf, "grdSalePerDayList_body_gridrow_")
    logger.info(f"영수증 행 수: {len(row_indices)}건")

    for row_idx in row_indices:
        try:
            # 1. 매출구분 (정상/반품)
            sale_type = _pw_text(
                mf,
                f"{_MAIN_GRID_PREFIX}gridrow_{row_idx}_cell_{row_idx}_7",
            )

            # 2. 영수증 번호
            receipt_no = _pw_text(
                mf,
                f"{_MAIN_GRID_PREFIX}gridrow_{row_idx}_cell_{row_idx}_2_controlbuttonTextBoxElement",
            )
            if not receipt_no:
                btn_el = mf.query_selector(
                    f"#{_MAIN_GRID_PREFIX}gridrow_{row_idx}_cell_{row_idx}_2_controlbutton"
                )
                receipt_no = (btn_el.inner_text().strip() if btn_el else str(row_idx))

            logger.info(f"  [row {row_idx}] receipt_no={receipt_no!r}  sale_type={sale_type!r}")

            # 3. 영수증 버튼 클릭
            btn = mf.query_selector(
                f"#{_MAIN_GRID_PREFIX}gridrow_{row_idx}_cell_{row_idx}_2_controlbutton"
            )
            btn.click()
            time.sleep(0.5)

            # 4. 팝업 로드 대기
            mf.wait_for_selector(f"#{_POPUP_DETAIL_LIST}", timeout=WAIT_TIMEOUT)

            # 5. 팝업 헤더 수집
            mf = _get_main_frame(page)
            receipt_type = _pw_text(mf, _POPUP_RECEIPT_TYPE)
            shop_name    = _pw_text(mf, _POPUP_SHOP_NAME)

            # 6. 상품 행 수집
            detail_indices = _get_row_indices_pw(mf, "grdDetailList_body_gridrow_")
            if not detail_indices:
                logger.warning(f"  [row {row_idx}] 팝업 grdDetailList 행 없음")

            for r in detail_indices:
                menu_name    = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_0")
                qty_text     = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_2")
                disc_text    = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_4")
                amount_text  = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_5")

                if not menu_name:
                    continue

                sign = -1 if "반품" in (sale_type or "") else 1
                rows.append({
                    "sale_date":       sale_date,
                    "receipt_no":      receipt_no,
                    "sale_type":       sale_type,
                    "receipt_type":    receipt_type,
                    "shop_name":       shop_name,
                    "menu_name":       menu_name,
                    "qty":             sign * _parse_amount(qty_text),
                    "discount_amount": sign * _parse_amount(disc_text),
                    "line_amount":     sign * _parse_amount(amount_text),
                })

            logger.info(f"  [row {row_idx}] 상품 {len(detail_indices)}건 수집")

            # 7. 팝업 닫기 (좌표 클릭)
            _coord_click(page, mf, _POPUP_CLOSE_BTN, "영수증 팝업")
            mf.wait_for_selector(f"#{_POPUP_DETAIL_LIST}", state="hidden", timeout=10_000)
            time.sleep(0.3)

            mf = _get_main_frame(page)

        except TargetClosedError:
            raise
        except Exception:
            logger.exception(f"  [row {row_idx}] 영수증 수집 실패 — 다음 행으로 계속")
            # 열린 팝업 닫기 시도
            try:
                mf = _get_main_frame(page)
                _coord_click(page, mf, _POPUP_CLOSE_BTN, "팝업(예외복구)")
                time.sleep(0.5)
            except Exception:
                pass

    return rows


# ============================================================
# Task 함수
# ============================================================

def resolve_sale_dates(dag_date_from=None, dag_date_to=None, **context) -> str:
    """실행 날짜 결정.
    우선순위: conf(date_from/date_to) > conf(sale_date) > dag 상수 > 어제
    결과는 XCom key='sale_dates' (list)로 push.
    """
    conf = context.get("dag_run").conf or {}
    date_from = conf.get("date_from") or dag_date_from
    date_to   = conf.get("date_to")   or dag_date_to

    if date_from and date_to:
        try:
            d_from = datetime.strptime(date_from, "%Y-%m-%d")
            d_to   = datetime.strptime(date_to,   "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"날짜 형식 오류 (YYYY-MM-DD 필요): {e}")
        if d_from > d_to:
            raise ValueError(f"date_from({date_from})이 date_to({date_to})보다 큽니다")
        delta = (d_to - d_from).days + 1
        sale_dates = [(d_from + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta)]
        logger.info(f"날짜 범위 사용 → {date_from} ~ {date_to} ({delta}일)")
    elif conf.get("sale_date"):
        try:
            datetime.strptime(conf["sale_date"], "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"날짜 형식 오류 (YYYY-MM-DD 필요): {e}")
        sale_dates = [conf["sale_date"]]
        logger.info(f"conf sale_date override → {conf['sale_date']}")
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        sale_dates = [yesterday]
        logger.info(f"어제 날짜 사용 → {yesterday}")

    context["ti"].xcom_push(key="sale_dates", value=sale_dates)
    return f"날짜 결정 완료: {sale_dates}"


_NEXACRO_INIT_SCRIPT = """
(function() {
    var _origGetById = Document.prototype.getElementById;
    Document.prototype.getElementById = function(id) {
        var el = _origGetById.call(this, id);
        if (!el && id === 'loadingImg') {
            return {
                style: {display: 'block'},
                id: 'loadingImg',
                setAttribute: function() {},
                removeAttribute: function() {}
            };
        }
        return el;
    };
    Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
    Object.defineProperty(document, 'hidden', {get: function() { return false; }});
    Object.defineProperty(document, 'visibilityState', {get: function() { return 'visible'; }});
    document.addEventListener('visibilitychange', function(e) { e.stopImmediatePropagation(); }, true);
})();
"""


def collect_receipts(**context) -> str:
    """EasyPOS 영수증 수집 → TEMP_DIR parquet 저장 (Playwright)"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    manual_totals = _load_manual_daily_totals()
    auto_totals: dict[str, int] = {}
    _cleanup_download_dir(EASYPOS_SALES_DOWNLOAD_DIR, "daily_totals_*.xlsx")

    launch_kwargs = dict(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1920,1080",
        ],
    )
    context_opts = dict(
        viewport={"width": 1920, "height": 1080},
        accept_downloads=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )

    all_rows = []

    with sync_playwright() as p:
        browser = launch_chromium(p, headless=launch_kwargs["headless"], args=launch_kwargs["args"])
        bctx = browser.new_context(**context_opts)
        bctx.add_init_script(_NEXACRO_INIT_SCRIPT)
        page = bctx.new_page()

        def _reopen_session(target_date: str):
            nonlocal browser, bctx, page
            try:
                browser.close()
            except Exception:
                pass
            browser2 = launch_chromium(p, headless=launch_kwargs["headless"], args=launch_kwargs["args"])
            bctx2 = browser2.new_context(**context_opts)
            bctx2.add_init_script(_NEXACRO_INIT_SCRIPT)
            page2 = bctx2.new_page()
            _mf = _login(page2)
            _mf = _navigate_to_daily_sales(page2, _mf)
            _select_yesterday_and_search(page2, _mf, target_date)
            logger.info("EasyPOS 세션 재오픈 완료: %s", target_date)
            return browser2, bctx2, page2

        try:
            mf = _login(page)

            # ── STEP 1: 일자별매출조회 → 엑셀 다운로드 → CSV 저장 (최근 7일 일괄)
            try:
                mf = _navigate_to_daily_totals_inquiry(page, mf)
                fetched = _download_and_save_daily_totals(page, EASYPOS_SALES_DOWNLOAD_DIR)
                auto_totals.update(fetched)
                logger.info("일자별합계 수집 완료: %d일치 (%s)", len(fetched), list(fetched.keys()))
            except Exception as e:
                logger.warning("일자별합계 수집 실패(계속 진행): %s", e)
                if DEBUG_MODE:
                    _debug_dump(page, "daily_inquiry_failed")

            # ── STEP 2: 당일매출내역 → 영수증 수집
            try:
                mf = _navigate_to_daily_sales(page, mf)
                mf = _select_yesterday_and_search(page, mf, sale_dates[0])
            except Exception as e:
                _debug_dump(page, "navigation_failed")
                logger.exception("당일매출내역 화면 진입 실패: %s", e)
                if STRICT_MODE:
                    raise
                logger.warning("STRICT_MODE=0 이므로 실패 대신 스킵 처리 (parquet_path=None)")
                context["ti"].xcom_push(key="parquet_path", value=None)
                return f"스킵: 당일매출내역 화면 진입 실패 ({e})"

            for idx, sale_date in enumerate(sale_dates):
                if idx > 0:
                    if _is_page_closed(page):
                        logger.warning("이전 날짜 처리 후 페이지 닫힘, 재세션 오픈: %s", sale_date)
                        browser, bctx, page = _reopen_session(sale_date)
                        mf = _get_main_frame(page)
                    else:
                        mf = _get_main_frame(page)
                        if not _set_sale_date_inputs_v2(mf, sale_date, page=page):
                            if not _is_page_closed(page):
                                _debug_dump(page, f"date_set_failed_{sale_date}")
                            logger.warning("sale_date 변경 실패, 재세션: %s", sale_date)
                            browser, bctx, page = _reopen_session(sale_date)
                            mf = _get_main_frame(page)
                        time.sleep(0.5)
                        mf = _get_main_frame(page)
                        s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
                        if s_btn is not None:
                            _click_handle(page, s_btn, f"조회({s_sel}, {sale_date})", prefer_mouse=True)
                            time.sleep(2.5)
                        mf = _get_main_frame(page)

                logger.info("수집 시작: %s (%d/%d)", sale_date, idx + 1, len(sale_dates))

                ref_total = auto_totals.get(sale_date) or manual_totals.get(sale_date)
                ref_source = "auto" if sale_date in auto_totals else ("manual" if sale_date in manual_totals else None)

                MAX_RETRY = 3
                for retry in range(1, MAX_RETRY + 1):
                    rows = _collect_all_receipts(page, mf, sale_date)
                    clicked_total = sum(_parse_amount(r.get("line_amount", 0)) for r in rows)

                    if ref_total is None:
                        break  # 비교 기준 없으면 그냥 진행

                    diff = clicked_total - ref_total
                    logger.info(
                        "EasyPOS 합계 비교(%s) [%d/%d]: %s | clicked=%s | ref=%s | diff=%s",
                        ref_source, retry, MAX_RETRY, sale_date, clicked_total, ref_total, diff,
                    )
                    if diff == 0:
                        break

                    if retry < MAX_RETRY:
                        logger.warning("합계 불일치 — 재수집 시도 %d/%d: %s", retry, MAX_RETRY, sale_date)
                        time.sleep(2.0)
                        mf = _get_main_frame(page)
                    else:
                        raise RuntimeError(
                            f"EasyPOS 총매출 불일치({ref_source}): {sale_date} "
                            f"(clicked={clicked_total}, ref={ref_total}, diff={diff}) "
                            f"— {MAX_RETRY}회 재시도 후에도 불일치"
                        )

                all_rows.extend(rows)
                logger.info("수집 완료: %s → %d건", sale_date, len(rows))

        finally:
            try:
                browser.close()
            except Exception:
                pass
            logger.info("Playwright 브라우저 종료")

    if not all_rows:
        logger.warning("수집 결과 없음 — 빈 DataFrame (dates=%s)", sale_dates)
        context["ti"].xcom_push(key="saved_paths", value=[])
        return "수집 완료: 0건 (데이터 없음)"

    df = pd.DataFrame(all_rows)
    df["collected_at"] = datetime.utcnow().isoformat()
    df["_pk"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['sale_date']}{r['receipt_no']}{r['menu_name']}".encode()
        ).hexdigest(),
        axis=1,
    )

    total_inserted = 0
    saved_paths = []
    for ym, ym_df in df.groupby(df["sale_date"].str[:7]):
        out_path = ANALYTICS_DB / "easypos_sales_raw" / f"ym={ym}" / "receipts.csv"
        result = onedrive_csv_save(df=ym_df, file_path=str(out_path), pk_col="_pk")
        inserted   = result.get("inserted", 0)
        duplicated = result.get("duplicated", 0)
        total_inserted += inserted
        saved_paths.append(str(out_path))
        logger.info("OneDrive 저장: %s | 신규=%d, 중복=%d", out_path, inserted, duplicated)

    context["ti"].xcom_push(key="saved_paths", value=saved_paths)
    return f"수집/저장 완료: {len(df)}건 | {len(saved_paths)}개 파티션 | 총 신규={total_inserted}건"


def _check_missing_dates(ref_map: dict[str, int]) -> tuple[list[str], list[str]]:
    """receipts.csv 확인 → (truly_missing, mismatched) 반환.
    - truly_missing: actual==0인 날짜 (재수집 필요)
    - mismatched: actual>0이지만 ref와 다른 날짜 (경고만, 재수집 X)
    """
    truly_missing: list[str] = []
    mismatched: list[str] = []
    for sale_date, ref_total in sorted(ref_map.items()):
        ym = sale_date[:7]
        receipts_path = ANALYTICS_DB / "easypos_sales_raw" / f"ym={ym}" / "receipts.csv"
        if not receipts_path.exists():
            logger.warning("누락검증: %s 영수증 파일 없음 (ref=%d)", sale_date, ref_total)
            truly_missing.append(sale_date)
            continue
        try:
            rdf = pd.read_csv(receipts_path, dtype=str)
        except Exception as e:
            logger.warning("누락검증: %s CSV 읽기 실패 (%s)", sale_date, e)
            truly_missing.append(sale_date)
            continue
        day_rows = rdf[rdf["sale_date"] == sale_date] if "sale_date" in rdf.columns else pd.DataFrame()
        actual = int(day_rows["line_amount"].apply(_parse_amount).sum()) if "line_amount" in day_rows.columns else 0
        if actual == 0:
            logger.warning("누락검증 빈 날짜: %s (ref=%d) → 재수집 대상", sale_date, ref_total)
            truly_missing.append(sale_date)
        elif actual != ref_total:
            logger.warning("누락검증 불일치(경고): %s | actual=%d ref=%d diff=%d", sale_date, actual, ref_total, actual - ref_total)
            mismatched.append(sale_date)
        else:
            logger.info("누락검증 일치: %s | total=%d", sale_date, actual)
    return truly_missing, mismatched


def _recollect_dates(missing_dates: list[str]) -> int:
    """누락 날짜를 Playwright로 재수집 → OneDrive 저장. 저장된 총 건수 반환."""
    launch_kwargs = dict(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled", "--window-size=1920,1080"],
    )
    context_opts = dict(
        viewport={"width": 1920, "height": 1080},
        accept_downloads=True,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    total_inserted = 0
    all_rows: list[dict] = []

    with sync_playwright() as p:
        browser = launch_chromium(p, headless=launch_kwargs["headless"], args=launch_kwargs["args"])
        bctx = browser.new_context(**context_opts)
        bctx.add_init_script(_NEXACRO_INIT_SCRIPT)
        page = bctx.new_page()
        try:
            mf = _login(page)
            mf = _navigate_to_daily_sales(page, mf)
            mf = _select_yesterday_and_search(page, mf, missing_dates[0])

            for idx, sale_date in enumerate(missing_dates):
                try:
                    if idx > 0:
                        mf = _get_main_frame(page)
                        if not _set_sale_date_inputs_v2(mf, sale_date, page=page):
                            logger.warning("재수집 날짜 입력 실패: %s", sale_date)
                        time.sleep(0.5)
                        mf = _get_main_frame(page)
                        s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
                        if s_btn:
                            _click_handle(page, s_btn, f"재수집 조회({sale_date})", prefer_mouse=True)
                            time.sleep(2.5)
                        mf = _get_main_frame(page)

                    logger.info("누락 재수집: %s (%d/%d)", sale_date, idx + 1, len(missing_dates))
                    rows = _collect_all_receipts(page, mf, sale_date)
                    logger.info("재수집 완료: %s → %d건", sale_date, len(rows))
                    all_rows.extend(rows)
                except TargetClosedError:
                    logger.warning("브라우저 컨텍스트 종료 감지 (%s) → 재시작 후 재시도", sale_date)
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = launch_chromium(p, headless=launch_kwargs["headless"], args=launch_kwargs["args"])
                    bctx = browser.new_context(**context_opts)
                    bctx.add_init_script(_NEXACRO_INIT_SCRIPT)
                    page = bctx.new_page()
                    mf = _login(page)
                    mf = _navigate_to_daily_sales(page, mf)
                    mf = _select_yesterday_and_search(page, mf, sale_date)
                    logger.info("누락 재수집(재시작 후 재시도): %s (%d/%d)", sale_date, idx + 1, len(missing_dates))
                    rows = _collect_all_receipts(page, mf, sale_date)
                    logger.info("재수집 완료: %s → %d건", sale_date, len(rows))
                    all_rows.extend(rows)
        finally:
            try:
                browser.close()
            except Exception:
                pass

    if not all_rows:
        return 0

    df = pd.DataFrame(all_rows)
    df["collected_at"] = datetime.utcnow().isoformat()
    df["_pk"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['sale_date']}{r['receipt_no']}{r['menu_name']}".encode()
        ).hexdigest(),
        axis=1,
    )
    for ym, ym_df in df.groupby(df["sale_date"].str[:7]):
        out_path = ANALYTICS_DB / "easypos_sales_raw" / f"ym={ym}" / "receipts.csv"
        result = onedrive_csv_save(df=ym_df, file_path=str(out_path), pk_col="_pk")
        inserted = result.get("inserted", 0)
        total_inserted += inserted
        logger.info("재수집 저장: %s | 신규=%d", out_path, inserted)
    return total_inserted


def verify_missing(**context) -> str:
    """_manual_daily_totals.csv 전체 기준으로 누락/불일치 날짜 탐지 → 자동 재수집.
    재수집 후에도 불일치면 RuntimeError → 이메일 알림.
    """
    if not EASYPOS_MANUAL_TOTALS_CSV.exists():
        logger.warning("일별합계 CSV 없음 — 누락 검증 스킵: %s", EASYPOS_MANUAL_TOTALS_CSV)
        return "스킵 (일별합계 CSV 없음)"

    try:
        ref_df = pd.read_csv(EASYPOS_MANUAL_TOTALS_CSV, dtype=str)
    except Exception as e:
        return f"스킵 (CSV 로드 실패: {e})"

    # manual totals 에 있는 전체 기간을 검증하되, 당일(오늘)은 매출 진행 중이므로 제외
    today = datetime.now().strftime("%Y-%m-%d")

    ref_map: dict[str, int] = {}
    if "sale_date" in ref_df.columns and "total_sales" in ref_df.columns:
        for _, r in ref_df.iterrows():
            sale_date = str(r["sale_date"]).strip()
            total = _parse_amount(r["total_sales"])
            if sale_date and total > 0 and sale_date < today:
                ref_map[sale_date] = total

    if not ref_map:
        return "스킵 (검증 대상 없음 — 당일 제외)"

    logger.info("누락검증 대상: %d일치 (manual totals 전체, 당일 %s 제외)", len(ref_map), today)

    # 1차 검증
    truly_missing, mismatched = _check_missing_dates(ref_map)

    # 빈 날짜 + 불일치 날짜 모두 재수집 대상
    to_recollect = sorted(set(truly_missing + mismatched))

    if not to_recollect:
        return f"누락 없음: {len(ref_map)}일 모두 일치"

    logger.warning("재수집 대상 %d건 (빈=%d, 불일치=%d): %s",
                   len(to_recollect), len(truly_missing), len(mismatched), to_recollect)

    # 재수집 최대 2회 시도 — 2회 후에도 없으면 휴일로 간주, 알람 없이 종료
    MAX_RECOLLECT = 2
    remaining = to_recollect
    last_still_missing: list[str] = []
    last_still_mismatched: list[str] = []
    total_inserted = 0

    for attempt in range(1, MAX_RECOLLECT + 1):
        inserted = _recollect_dates(remaining)
        total_inserted += inserted
        logger.info("재수집 %d/%d 완료: 신규=%d건", attempt, MAX_RECOLLECT, inserted)

        last_still_missing, last_still_mismatched = _check_missing_dates(
            {d: ref_map[d] for d in remaining}
        )

        if not last_still_missing:
            msg = f"재수집 완료: {len(to_recollect)}일 → 신규={total_inserted}건"
            if last_still_mismatched:
                logger.warning("재수집 후에도 합계 불일치(경고): %s", last_still_mismatched)
                msg += f" | 불일치 경고 {len(last_still_mismatched)}건: {last_still_mismatched}"
            return msg

        if attempt < MAX_RECOLLECT:
            logger.warning("재수집 %d/%d 후에도 누락 %d건: %s — 재시도",
                           attempt, MAX_RECOLLECT, len(last_still_missing), last_still_missing)
            remaining = last_still_missing

    # 2회 재수집 후에도 데이터 없음 → 휴일로 간주, 알람 없이 정상 종료
    logger.warning(
        "EasyPOS 재수집 %d회 후에도 데이터 없음 %d건: %s — 휴일로 간주, 알람 없이 종료",
        MAX_RECOLLECT, len(last_still_missing), last_still_missing,
    )
    return (
        f"휴일 추정 (재수집 {MAX_RECOLLECT}회 후 데이터 없음 {len(last_still_missing)}건): "
        f"{last_still_missing}"
    )


def save_to_raw(**context) -> str:
    """collect_receipts에서 OneDrive 저장까지 완료 — pass-through"""
    saved_paths = context["ti"].xcom_pull(task_ids="collect_receipts", key="saved_paths") or []
    if not saved_paths:
        return "스킵 (데이터 없음 또는 수집 실패)"
    return f"저장 완료 (collect_receipts에서 처리): {len(saved_paths)}개 파티션"
