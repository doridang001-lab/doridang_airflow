"""combined 파이프라인 실제 흐름 재현: now → woori 순서로 실행해서 문제 재현"""
import sys, time, logging, re
sys.path.insert(0, "/opt/airflow")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger("woori_combined_debug")

import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.extract.croling_beamin import launch_browser, login_baemin, get_store_options, wait_for_page
from modules.transform.pipelines.db.DB_Beamin_01_now import collect_now_for_driver

KST = pendulum.timezone("Asia/Seoul")
_TABLE_CSS = "table[data-atelier-component='Table']"
_TABLE_TIMEOUT = 20
KNOWN_BRANDS = ["도리당", "나홀로"]

def get_dom_info(driver):
    return driver.execute_script("""
        var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
        if (!trs.length) return JSON.stringify({tr:0, url: location.href.slice(-80)});
        var cells = trs[0].querySelectorAll('td');
        var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
        var hasClass = !!(cells.length > 0 && cells[0].querySelector('.styles-module__x4Tk'));
        return JSON.stringify({tr: trs.length, cells: cells.length, val: val, hasClass: hasClass, url: location.href.slice(-80)});
    """)

def _has_rows(d):
    try:
        rows = d.find_elements(By.CSS_SELECTOR, f"{_TABLE_CSS} tbody tr")
        if not rows: return False
        cells = rows[0].find_elements(By.TAG_NAME, "td")
        if len(cells) < 7: return False
        try:
            val = (cells[0].find_element(By.CSS_SELECTOR, ".styles-module__x4Tk").get_attribute("innerText") or "").strip()
        except Exception:
            val = (cells[0].get_attribute("innerText") or cells[0].text or "").strip()
        has = bool(re.search(r"\d+\.\d+", val))
        if not has:
            logger.debug("_has_rows False: val=%r", val)
        return has
    except Exception as e:
        if "Connection refused" in str(e) or "Max retries" in str(e): raise
        return False

accounts = load_accounts(target_stores=["도리당 역삼점"], exact=True)
account = accounts[0]
account_id = account["account_id"]
logger.info("=== 계정: %s ===", account_id)

driver = None
try:
    driver = launch_browser(account_id)
    if not login_baemin(driver, account_id, account["password"]):
        logger.error("로그인 실패"); sys.exit(1)
    if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
        logger.error("대시보드 로드 실패"); sys.exit(1)

    raw_options = get_store_options(driver)
    store_list = []
    for opt in raw_options:
        text = opt.get("text", "")
        brand = next((b for b in KNOWN_BRANDS if b in text), None)
        if not brand: continue
        matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
        store = matches[-1] if matches else text[:20]
        store_list.append({"store_id": str(opt["store_id"]), "brand": brand, "store": store})

    logger.info("수집 대상: %s", store_list)

    # combined 파이프라인과 동일하게: 첫 번째 store에 대해 now → woori 순서 실행
    store_info = store_list[0]
    store_id = store_info["store_id"]
    ym = pendulum.now(KST).format("YYYY-MM")

    # === Step 1: now 수집 (combined 파이프라인과 동일) ===
    logger.info("=" * 60)
    logger.info("Step 1: collect_now_for_driver 실행 (store=%s)", store_info["store"])
    collect_now_for_driver(driver, account_id, [store_info])
    logger.info("Step 1 완료. 현재 URL: %s", driver.current_url)

    # === Step 2: 현재 코드(refresh 추가 버전) ===
    url = f"https://self.baemin.com/shops/{store_id}/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth={ym}"
    logger.info("=" * 60)
    logger.info("Step 2: woori URL 이동 (수정된 방식 - refresh 포함)")
    logger.info("URL: %s", url)

    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
    except Exception as e:
        logger.warning("get 타임아웃(계속): %s", e)

    logger.info("get 직후 URL: %s", driver.current_url)
    time.sleep(1.5)
    logger.info("1.5초 후 DOM: %s", get_dom_info(driver))

    try:
        driver.refresh()
        logger.info("refresh 완료")
    except Exception as e:
        logger.warning("refresh 예외: %s", e)

    logger.info("refresh 직후 URL: %s", driver.current_url)

    t0 = time.time()
    # 1초마다 DOM 상태 로깅하면서 대기
    for i in range(25):
        time.sleep(1)
        dom = get_dom_info(driver)
        elapsed = time.time() - t0
        logger.info("%.0fs: %s", elapsed, dom)
        if _has_rows(driver):
            logger.info("테이블 로드 성공! %.1fs", elapsed)
            table = driver.find_element(By.CSS_SELECTOR, _TABLE_CSS)
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            logger.info("행 수: %d", len(rows))
            break
    else:
        logger.warning("25초 경과해도 테이블 로드 실패")
        logger.warning("최종 DOM: %s", get_dom_info(driver))

    logger.info("=== 완료 ===")

finally:
    if driver:
        try: driver.quit()
        except Exception: pass
