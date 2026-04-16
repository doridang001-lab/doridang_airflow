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
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, Frame, Page

from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR
from modules.load.load_onedrive import onedrive_csv_save

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
LOGIN_URL     = "https://smart.easypos.net/index.jsp"
EASYPOS_ID    = "3322002029"
EASYPOS_PW    = "1"
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None
WAIT_TIMEOUT  = 30_000  # ms (Playwright 단위)

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
_BTN_YESTERDAY_SELECTORS = [
    f"#{_BTN_YESTERDAY}",
    "[id$='btnBeforeDay']",
    "[id*='divSalesDate'][id*='btnBeforeDay']",
    "xpath=//*[contains(@id,'btnBeforeDay')]",
]
_BTN_SEARCH_SELECTORS = [
    f"#{_BTN_SEARCH}",
    "[id$='btnCommSearch']",
    "[id*='divCommonBtn'][id*='btnCommSearch']",
    "xpath=//*[contains(@id,'btnCommSearch')]",
]
_DAILY_VIEW_READY_SELECTORS = [
    "[id*='grdSalePerDayList']",
    "[id*='divSalesDate']",
    "[id*='divMainNavi']",
    "[id*='btnCommSearch']",
]

# NexacroN element IDs — 영수증 팝업
_POPUP_RECEIPT_TYPE = "mainframe_childframe_popupBill_form_stcSaleType"
_POPUP_SHOP_NAME    = "mainframe_childframe_popupBill_form_stcShopName"
_POPUP_DETAIL_LIST  = "mainframe_childframe_popupBill_form_grdDetailList"
_POPUP_CLOSE_BTN    = "mainframe_childframe_popupBill_titlebar_closebutton"

# 그리드 ID 프리픽스
_MAIN_GRID_PREFIX   = "mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_"
_DETAIL_GRID_PREFIX = "mainframe_childframe_popupBill_form_grdDetailList_body_"


# ============================================================
# 내부 유틸
# ============================================================

def _get_main_frame(page: Page) -> Frame:
    """NexacroN main 프레임 반환 — 모든 UI 요소가 name='main' 프레임 안에 있음"""
    mf = next((f for f in page.frames if f.name == "main"), None)
    if mf is None:
        raise RuntimeError(
            "name='main' 프레임을 찾을 수 없음 — NexacroN 렌더링 미완료 또는 로그인 페이지 오류"
        )
    return mf


def _pw_text(mf: Frame, element_id: str) -> str:
    """NexacroN 셀 텍스트 추출"""
    try:
        el = mf.query_selector(f"#{element_id}")
        if el is None:
            return ""
        return (el.inner_text() or "").strip()
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


def _click_handle(page: Page, handle, label: str) -> bool:
    """ElementHandle 클릭 보강: 일반 클릭 실패 시 좌표 클릭으로 재시도"""
    if handle is None:
        return False
    try:
        handle.click()
        logger.info("%s 클릭 성공 (element.click)", label)
        return True
    except Exception as e:
        logger.info("%s element.click 실패, 좌표 클릭 재시도: %s", label, e)

    try:
        box = handle.bounding_box()
        if box is None:
            logger.warning("%s 좌표 클릭 실패: bounding_box 없음", label)
            return False
        cx = box["x"] + box["width"] / 2
        cy = box["y"] + box["height"] / 2
        page.mouse.click(cx, cy)
        logger.info("%s 클릭 성공 (coord: %.0f, %.0f)", label, cx, cy)
        return True
    except Exception as e:
        logger.warning("%s 좌표 클릭 실패: %s", label, e)
        return False


def _parse_amount(text: str) -> int:
    """'35,800' → 35800, 빈값/비숫자 → 0"""
    cleaned = re.sub(r"[,\s]", "", str(text))
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return 0


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
        # 디버그 스크린샷
        try:
            page.screenshot(path="/tmp/easypos_login_failed.png")
            logger.info("로그인 실패 스크린샷: /tmp/easypos_login_failed.png")
        except Exception:
            pass
        raise RuntimeError(
            "로그인 실패 또는 대시보드 렌더링 타임아웃 — "
            "ID/PW 또는 NexacroN 상태 확인 필요 (스크린샷: /tmp/easypos_login_failed.png)"
        )

    logger.info(f"로그인 완료 | URL: {page.url}")
    return mf


def _navigate_to_daily_sales(page: Page, mf: Frame) -> Frame:
    """영업속보 → 당일매출내역 메뉴 클릭"""
    mf = _get_main_frame(page)

    mf.query_selector(f"#{_MENU_SALES_BRIEF}").click()
    logger.info("영업속보 메뉴 클릭")

    item = None
    menu_snapshot: list[str] = []
    menu_selectors = [
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일매출내역')]",
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일 매출 내역')]",
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일매출')]",
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'매출내역')]",
        "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'당일') and contains(normalize-space(.),'매출')]",
    ]

    for attempt in range(1, 4):
        mf = _get_main_frame(page)

        # 이미 당일매출 화면이면 하위 메뉴 클릭 없이 통과
        y_btn, y_sel = _find_first_selector(mf, _BTN_YESTERDAY_SELECTORS)
        if y_btn is not None:
            logger.info("당일매출 화면이 이미 로드됨 (전일 버튼 확인)")
            return _get_main_frame(page)

        try:
            mf.wait_for_selector("[id*='grdLeft_body_gridrow_']", timeout=5_000)
        except Exception:
            logger.info(f"좌측 메뉴 로드 대기 중 (attempt={attempt}/3)")

        # 하위 메뉴가 접힌 경우가 있어 '영업일보'를 먼저 펼쳐본다.
        if item is None:
            day_menu = mf.query_selector(
                "xpath=//*[contains(@id,'grdLeft_body_gridrow_')][contains(normalize-space(.),'영업일보')]"
            )
            if day_menu is not None:
                try:
                    _click_handle(page, day_menu, "영업일보 메뉴")
                    time.sleep(0.6)
                    day_menu.dblclick()
                    time.sleep(0.6)
                    logger.info("영업일보 메뉴 확장 시도 완료")
                except Exception as e:
                    logger.info(f"영업일보 메뉴 확장 시도 실패(무시): {e}")

        for selector in menu_selectors:
            item = mf.query_selector(selector)
            if item is not None:
                break

        if item is not None:
            break

        # 실패 시 실제 로드된 좌측 메뉴 텍스트를 로그용으로 수집
        menu_snapshot = []
        for el in mf.query_selector_all("[id*='grdLeft_body_gridrow_']"):
            txt = (el.inner_text() or "").strip().replace("\n", " ")
            if txt:
                menu_snapshot.append(txt)
        menu_snapshot = menu_snapshot[:8]

        logger.warning(
            "당일매출내역 메뉴 탐색 실패 (attempt=%s/3) | 현재 메뉴 샘플=%s",
            attempt,
            menu_snapshot,
        )
        if attempt < 3:
            # 좌측 메뉴가 접힌 상태일 수 있어 상위 메뉴를 다시 눌러 펼친다.
            mf.query_selector(f"#{_MENU_SALES_BRIEF}").click()
            time.sleep(1.5)

    if item is None:
        # 하위 메뉴 탐색은 실패했지만 화면이 이미 열려있으면 진행 허용
        mf = _get_main_frame(page)
        y_btn, y_sel = _find_first_selector(mf, _BTN_YESTERDAY_SELECTORS)
        if y_btn is not None:
            logger.info("당일매출 메뉴 탐색 실패했으나 조회 영역이 확인되어 계속 진행")
            return _get_main_frame(page)

        raise RuntimeError(
            "'당일매출내역' 메뉴 항목을 찾을 수 없음 "
            f"(menu_sample={menu_snapshot})"
        )

    if not _click_handle(page, item, "당일매출내역 메뉴"):
        raise RuntimeError("당일매출내역 메뉴 클릭 실패")
    logger.info("당일매출내역 메뉴 클릭 완료")
    time.sleep(1.2)

    return _get_main_frame(page)


def _select_yesterday_and_search(page: Page, mf: Frame) -> Frame:
    """전일 버튼 → 조회 버튼 클릭"""
    y_clicked = False
    s_clicked = False
    last_diag = ""

    for attempt in range(1, 11):
        mf = _get_main_frame(page)
        y_btn, y_sel = _find_first_selector(mf, _BTN_YESTERDAY_SELECTORS)
        s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
        view_ready = _has_any_selector(mf, _DAILY_VIEW_READY_SELECTORS)

        if not y_clicked and y_btn is not None:
            y_clicked = _click_handle(page, y_btn, f"전일 버튼({y_sel})")
            if y_clicked:
                time.sleep(0.8)

        # 조회 버튼은 전일 클릭 이후에 다시 탐색하는 편이 안정적임
        if y_clicked and not s_clicked:
            mf = _get_main_frame(page)
            s_btn, s_sel = _find_first_selector(mf, _BTN_SEARCH_SELECTORS)
            if s_btn is not None:
                s_clicked = _click_handle(page, s_btn, f"조회 버튼({s_sel})")
                if s_clicked:
                    time.sleep(2.5)

        if y_clicked and s_clicked:
            logger.info("전일/조회 클릭 완료 (attempt=%s/10)", attempt)
            break

        last_diag = f"attempt={attempt}, y={bool(y_btn)}, s={bool(s_btn)}, view_ready={view_ready}"
        logger.info("전일/조회 버튼 탐색 재시도 중 | %s", last_diag)
        time.sleep(1.2)

    if not y_clicked or not s_clicked:
        raise RuntimeError(
            "전일/조회 클릭 실패 "
            f"(y_clicked={y_clicked}, s_clicked={s_clicked}, last_diag={last_diag})"
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
                qty_text     = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_1")
                disc_text    = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_2")
                amount_text  = _pw_text(mf, f"{_DETAIL_GRID_PREFIX}gridrow_{r}_cell_{r}_3")

                if not menu_name:
                    continue

                rows.append({
                    "sale_date":       sale_date,
                    "receipt_no":      receipt_no,
                    "sale_type":       sale_type,
                    "receipt_type":    receipt_type,
                    "shop_name":       shop_name,
                    "menu_name":       menu_name,
                    "qty":             _parse_amount(qty_text),
                    "discount_amount": _parse_amount(disc_text),
                    "line_amount":     _parse_amount(amount_text),
                })

            logger.info(f"  [row {row_idx}] 상품 {len(detail_indices)}건 수집")

            # 7. 팝업 닫기 (좌표 클릭)
            _coord_click(page, mf, _POPUP_CLOSE_BTN, "영수증 팝업")
            mf.wait_for_selector(f"#{_POPUP_DETAIL_LIST}", state="hidden", timeout=10_000)
            time.sleep(0.3)

            mf = _get_main_frame(page)

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

def resolve_sale_dates(**context) -> str:
    """실행 날짜 결정 (conf['sale_date'] 또는 어제)"""
    conf = context.get("dag_run").conf or {}
    sale_date = conf.get("sale_date")

    if sale_date:
        try:
            datetime.strptime(sale_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"날짜 형식 오류 (YYYY-MM-DD 필요): {e}")
        logger.info(f"conf override → {sale_date}")
    else:
        sale_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"어제 날짜 사용 → {sale_date}")

    context["ti"].xcom_push(key="sale_date", value=sale_date)
    return f"날짜 결정 완료: {sale_date}"


def collect_receipts(**context) -> str:
    """EasyPOS 영수증 수집 → TEMP_DIR parquet 저장 (Playwright)"""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_date")
    if not sale_date:
        raise ValueError("sale_date XCom 값이 없습니다.")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS_MODE,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page(viewport={"width": 1920, "height": 1080})
        try:
            mf = _login(page)
            mf = _navigate_to_daily_sales(page, mf)
            mf = _select_yesterday_and_search(page, mf)
            rows = _collect_all_receipts(page, mf, sale_date)
        finally:
            browser.close()
            logger.info("Playwright 브라우저 종료")

    if not rows:
        logger.warning(f"{sale_date} 수집 결과 없음 — 빈 DataFrame")
        context["ti"].xcom_push(key="parquet_path", value=None)
        return "수집 완료: 0건 (데이터 없음)"

    df = pd.DataFrame(rows)
    df["collected_at"] = datetime.utcnow().isoformat()
    df["_pk"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['sale_date']}{r['receipt_no']}{r['menu_name']}".encode()
        ).hexdigest(),
        axis=1,
    )

    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = TEMP_DIR / f"easypos_receipts_{sale_date}.parquet"
    df.to_parquet(parquet_path, index=False)

    context["ti"].xcom_push(key="parquet_path", value=str(parquet_path))
    logger.info(f"parquet 저장: {parquet_path} ({len(df)}행)")
    return f"수집 완료: {len(df)}건"


def save_to_raw(**context) -> str:
    """parquet → OneDrive CSV 저장"""
    parquet_path = context["ti"].xcom_pull(task_ids="collect_receipts", key="parquet_path")
    if not parquet_path:
        logger.warning("parquet_path 없음 — save_to_raw 스킵")
        return "스킵 (데이터 없음)"

    df = pd.read_parquet(parquet_path)
    if df.empty:
        return "스킵 (빈 DataFrame)"

    sale_date = str(df["sale_date"].iloc[0])
    ym = sale_date[:7]  # YYYY-MM

    out_path = ANALYTICS_DB / "easypos_sales_raw" / f"ym={ym}" / "receipts.csv"

    result = onedrive_csv_save(
        df=df,
        file_path=str(out_path),
        pk_col="_pk",
    )

    inserted   = result.get("inserted", 0)
    duplicated = result.get("duplicated", 0)
    logger.info(f"OneDrive 저장 완료: {out_path} | 신규={inserted}, 중복={duplicated}")
    return f"저장 완료: {out_path.name} | 신규={inserted}건"
