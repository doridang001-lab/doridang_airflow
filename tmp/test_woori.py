"""우리가게 클릭 현황 수집 디버그 스크립트.

실행: python /opt/airflow/tmp/test_woori.py
"""
import logging
import sys
import time

sys.path.insert(0, "/opt/airflow")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page

STORE = "도리당 역삼점"
STORE_ID = "14364235"
YM = "2026-05"
URL = f"https://self.baemin.com/shops/{STORE_ID}/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth={YM}"
PATH = f"/shops/{STORE_ID}/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth={YM}"

accounts = load_accounts([STORE], exact=True)
if not accounts:
    logger.error("계정 없음: %s", STORE)
    sys.exit(1)

account = accounts[0]
account_id = account["account_id"]
logger.info("계정: %s", account_id)

driver = launch_browser(account_id)
try:
    # 1. 로그인
    ok = login_baemin(driver, account_id, account["password"])
    logger.info("로그인: %s", ok)
    if not ok:
        sys.exit(1)

    # 2. 대시보드 로드 확인
    ok = wait_for_page(driver, "select[class*='ShopSelect']", timeout=60)
    logger.info("대시보드 로드: %s / 현재 URL: %s", ok, driver.current_url)
    time.sleep(2)

    # ── 방법 A: driver.get() 직접 이동 ──
    logger.info("=== 방법A: driver.get(url) ===")
    try:
        driver.set_page_load_timeout(30)
        driver.get(URL)
    except Exception as e:
        logger.warning("get 타임아웃: %s", e)
    time.sleep(4)
    cnt = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    txt = driver.execute_script(
        "var rows=document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\");"
        "return rows.length>0 ? rows[0].innerText : 'NO_ROW';"
    )
    logger.info("방법A 즉시: tbody tr=%d, 첫행=%s", cnt, txt[:80])
    time.sleep(4)
    cnt2 = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    logger.info("방법A 8초후: tbody tr=%d", cnt2)

    # 현재 URL 확인
    logger.info("현재 URL: %s", driver.current_url)

    # ── 방법 B: 대시보드 → pushState ──
    logger.info("=== 방법B: 대시보드→pushState ===")
    try:
        driver.set_page_load_timeout(30)
        driver.get("https://self.baemin.com/")
    except Exception:
        pass
    ok = wait_for_page(driver, "select[class*='ShopSelect']", timeout=30)
    logger.info("대시보드 재로드: %s", ok)
    time.sleep(2)

    driver.execute_script(
        "window.history.pushState(null, '', arguments[0]);"
        "window.dispatchEvent(new PopStateEvent('popstate'));",
        PATH,
    )
    logger.info("pushState 완료. 현재 URL: %s", driver.current_url)
    time.sleep(4)
    cnt3 = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    logger.info("방법B 4초후: tbody tr=%d", cnt3)
    time.sleep(4)
    cnt4 = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    logger.info("방법B 8초후: tbody tr=%d", cnt4)

    # ── 방법 C: 대시보드 → window.location.href ──
    logger.info("=== 방법C: 대시보드→window.location.href ===")
    try:
        driver.set_page_load_timeout(30)
        driver.get("https://self.baemin.com/")
    except Exception:
        pass
    ok = wait_for_page(driver, "select[class*='ShopSelect']", timeout=30)
    logger.info("대시보드 재로드: %s", ok)
    time.sleep(2)

    driver.execute_script(f"window.location.href = '{URL}';")
    time.sleep(4)
    logger.info("현재 URL: %s", driver.current_url)
    cnt5 = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    logger.info("방법C 4초후: tbody tr=%d", cnt5)
    time.sleep(4)
    cnt6 = driver.execute_script(
        "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
    )
    txt6 = driver.execute_script(
        "var rows=document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\");"
        "return rows.length>0 ? rows[0].innerText : 'NO_ROW';"
    )
    logger.info("방법C 8초후: tbody tr=%d, 첫행=%s", cnt6, txt6[:80])

    # 페이지 소스 일부 저장
    src = driver.page_source
    with open("/opt/airflow/tmp/woori_debug_source.html", "w", encoding="utf-8") as f:
        f.write(src)
    logger.info("페이지 소스 저장: /opt/airflow/tmp/woori_debug_source.html (%d chars)", len(src))

finally:
    driver.quit()
    logger.info("완료")
