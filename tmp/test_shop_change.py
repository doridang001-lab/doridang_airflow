"""변경이력 수집 디버그 스크립트 — 도리당 역삼점 단일 매장.

실행 (WSL):
  docker cp C:/airflow/tmp/test_shop_change.py airflow-airflow-worker-1:/opt/airflow/tmp/test_shop_change.py
  docker exec airflow-airflow-worker-1 bash -c \
    "cd /opt/airflow && DISPLAY=:99 python tmp/test_shop_change.py 2>&1 | tail -150"
"""
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page
from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.transform.pipelines.db.DB_Beamin_03_shop_change import (
    SHOP_CHANGE_URL, SELECT_CSS, QUERY_BTN_CSS, ITEM_CSS,
    _get_page_options, _EXTRACT_JS,
)

STORE_NAME = "도리당 역삼점"
STORE_ID   = "14364235"  # 닭도리탕 전문 도리당 역삼점 (doriys 계정)

accounts = load_accounts([STORE_NAME], exact=True)
if not accounts:
    logger.error("계정 없음: %s", STORE_NAME)
    sys.exit(1)

account = accounts[0]
account_id = account["account_id"]

driver = launch_browser(account_id)
try:
    ok = login_baemin(driver, account_id, account["password"])
    logger.info("로그인: %s", ok)
    if not ok:
        sys.exit(1)

    driver.set_page_load_timeout(45)
    driver.get(SHOP_CHANGE_URL)
    logger.info("현재 URL: %s", driver.current_url)

    # ── STEP A: SELECT 선택자 검증 ────────────────────────────
    logger.info("=== STEP A: SELECT 선택자 ===")
    wait_for_page(driver, "body", timeout=10)
    time.sleep(3)

    # 현재 SELECT_CSS 테스트 (JS에 직접 interpolation하지 않고 argument 전달)
    found_select = driver.execute_script(
        "return document.querySelectorAll(arguments[0]).length;", SELECT_CSS
    )
    logger.info("SELECT_CSS(%s) 결과: %d개", SELECT_CSS, found_select or 0)

    # 모든 select 요소 목록
    all_selects = driver.execute_script(
        "return [...document.querySelectorAll('select')].map(s => ({cls: s.className, cnt: s.options.length}));"
    ) or []
    logger.info("페이지 내 select 요소 전체: %s", all_selects)

    # select 있으면 옵션 덤프
    if found_select:
        options_info = driver.execute_script(
            "const sel = document.querySelector(arguments[0]);"
            "return sel ? [...sel.options].map(o => ({val: o.value, txt: o.textContent.trim().slice(0,60)})) : [];",
            SELECT_CSS
        ) or []
        logger.info("SELECT 옵션 목록 (%d개):", len(options_info))
        for o in options_info[:20]:
            logger.info("  %s → %s", o["val"], o["txt"])

    # _get_page_options 호출
    page_opts = _get_page_options(driver)
    logger.info("_get_page_options 결과: %d개 → %s", len(page_opts), dict(list(page_opts.items())[:5]))

    # ── STEP B: 매장 선택 + 조회 버튼 ────────────────────────
    logger.info("=== STEP B: 매장 선택 + 조회 버튼 ===")

    # store_id 선택 시도
    select_ok = False
    if found_select:
        try:
            sel_elem = driver.find_element(By.CSS_SELECTOR, SELECT_CSS)
            Select(sel_elem).select_by_value(STORE_ID)
            time.sleep(1.0)
            logger.info("store_id=%s 선택 완료", STORE_ID)
            select_ok = True
        except Exception as e:
            logger.warning("select 선택 실패: %s", e)

    # QUERY_BTN_CSS 테스트
    found_btn = driver.execute_script(
        "return document.querySelectorAll(arguments[0]).length;", QUERY_BTN_CSS
    )
    logger.info("QUERY_BTN_CSS(%s) 결과: %d개", QUERY_BTN_CSS, found_btn or 0)

    # 모든 button 목록 (못 찾은 경우 대안 탐색)
    if not found_btn:
        all_btns = driver.execute_script(
            "return [...document.querySelectorAll('button')].map(b => ({txt: b.textContent.trim().slice(0,30), cls: b.className.slice(0,60)}));"
        ) or []
        logger.info("=== 모든 button 목록 ===")
        for b in all_btns:
            logger.info("  txt=%r cls=%s", b["txt"], b["cls"])

    # 조회 버튼 클릭 시도 (있으면)
    if found_btn and select_ok:
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, QUERY_BTN_CSS))
            )
            btn.click()
            logger.info("조회 버튼 클릭 완료")
            time.sleep(4.0)
        except Exception as e:
            logger.warning("조회 버튼 클릭 실패: %s", e)
    elif not select_ok:
        # select 없어도 조회 버튼 클릭 시도 (일부 페이지는 전체 조회)
        try:
            all_btns2 = driver.execute_script(
                "return [...document.querySelectorAll('button')].map(b => ({txt: b.textContent.trim(), cls: b.className}));"
            ) or []
            for b in all_btns2:
                if "조회" in b["txt"]:
                    logger.info("'조회' 텍스트 버튼 발견: cls=%s", b["cls"])
        except Exception:
            pass

    # ── STEP C: 항목 로드 대기 + DOM 덤프 ────────────────────
    logger.info("=== STEP C: 항목 로드 + DOM 덤프 ===")

    # ITEM_CSS 테스트
    item_count = driver.execute_script(
        "return document.querySelectorAll(arguments[0]).length;", ITEM_CSS
    ) or 0
    logger.info("ITEM_CSS(%s) 항목 수: %d", ITEM_CSS, item_count)

    # 대기
    if item_count == 0:
        try:
            WebDriverWait(driver, 20).until(
                lambda d: (d.execute_script(
                    "return document.querySelectorAll(arguments[0]).length;", ITEM_CSS
                ) or 0) > 0
            )
            item_count = driver.execute_script(
                "return document.querySelectorAll(arguments[0]).length;", ITEM_CSS
            ) or 0
            logger.info("대기 후 항목 수: %d", item_count)
        except TimeoutException:
            logger.warning("ITEM_CSS 20초 대기 타임아웃")

    # 첫 3개 항목 innerHTML 저장
    items_html = driver.execute_script(
        "const items = document.querySelectorAll(arguments[0]);"
        "return [...items].slice(0, 3).map(i => i.outerHTML).join('\\n\\n---\\n\\n');",
        ITEM_CSS
    ) or ""
    out_path = Path("/opt/airflow/tmp/shop_change_dom.html")
    out_path.write_text(f"<html><body>\n{items_html}\n</body></html>", encoding="utf-8")
    logger.info("DOM 저장: %s (%d chars)", out_path, len(items_html))

    # 현재 _EXTRACT_JS 실행
    raw = driver.execute_script(_EXTRACT_JS) or []
    logger.info("_EXTRACT_JS 결과: %d행 → %s", len(raw), raw[:3])

    # TARGET 필터 없이 모든 항목 title 출력
    all_titles = driver.execute_script(
        "const items = document.querySelectorAll(arguments[0]);"
        "return [...items].map(i => i.querySelector('h5')?.textContent.trim() || '(no-h5)').slice(0, 20);",
        ITEM_CSS
    ) or []
    logger.info("항목 title 목록 (h5): %s", all_titles)

    # ── STEP D: 후보 선택자 탐색 ──────────────────────────────
    logger.info("=== STEP D: 후보 선택자 탐색 ===")
    candidates = [
        "li[class*='List']", "li[class*='Item']", "ul > li",
        "li[class*='list']", "li[class*='item']",
        "[class*='HistoryItem']", "[class*='ListItem']",
        "[class*='history-item']", "[class*='change']",
    ]
    for css in candidates:
        cnt = driver.execute_script(
            "return document.querySelectorAll(arguments[0]).length;", css
        ) or 0
        if cnt:
            logger.info("  ✅ %s → %d개", css, cnt)
        else:
            logger.info("  ❌ %s → 0", css)

    # 날짜 요소 후보
    date_candidates = [
        "time[date]", "time[datetime]", "[class*='date']",
        "[class*='Date']", "time", "[class*='time']",
    ]
    logger.info("--- 날짜 요소 후보 ---")
    for css in date_candidates:
        cnt = driver.execute_script(
            "return document.querySelectorAll(arguments[0]).length;", css
        ) or 0
        if cnt:
            sample = driver.execute_script(
                "const el = document.querySelector(arguments[0]);"
                "return el ? {cls: el.className, txt: el.textContent.trim().slice(0,30), attrs: [...el.attributes].map(a=>a.name+'='+a.value).join(', ')} : null;",
                css
            )
            logger.info("  ✅ %s → %d개, sample=%s", css, cnt, sample)

    logger.info("=== 테스트 완료 ===")

finally:
    try:
        driver.quit()
    except Exception:
        pass
