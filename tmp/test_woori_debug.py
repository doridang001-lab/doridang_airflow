"""우가클 수집 직접 실행 디버그 스크립트.

단일 매장(도리당 역삼점) + 이번달 한 달만 수집해서 어느 단계에서 실패하는지 확인.
"""

import logging
import sys
import time
import random
import re

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("woori_debug")

sys.path.insert(0, "/opt/airflow")

import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.extract.croling_beamin import (
    launch_browser,
    login_baemin,
    get_store_options,
    wait_for_page,
)

KST = pendulum.timezone("Asia/Seoul")
_TABLE_CSS = "table[data-atelier-component='Table']"
_TABLE_TIMEOUT = 20

accounts = load_accounts(target_stores=["도리당 역삼점"], exact=True)
if not accounts:
    logger.error("계정 없음 - 종료")
    sys.exit(1)

account = accounts[0]
account_id = account["account_id"]
logger.info("=== 계정: %s ===", account_id)

driver = None
try:
    driver = launch_browser(account_id)
    if not login_baemin(driver, account_id, account["password"]):
        logger.error("로그인 실패")
        sys.exit(1)

    logger.info("로그인 성공")
    if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
        logger.error("대시보드 로드 실패")
        sys.exit(1)

    raw_options = get_store_options(driver)
    logger.info("매장 옵션: %s", raw_options)

    # 역삼점 store_id 추출
    KNOWN_BRANDS = ["도리당", "나홀로"]
    store_list = []
    for opt in raw_options:
        text = opt.get("text", "")
        brand = next((b for b in KNOWN_BRANDS if b in text), None)
        if not brand:
            continue
        matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
        store = matches[-1] if matches else text[:20]
        store_list.append({"store_id": str(opt["store_id"]), "brand": brand, "store": store})

    logger.info("수집 대상: %s", store_list)
    if not store_list:
        logger.error("수집 대상 매장 없음")
        sys.exit(1)

    store_info = store_list[0]
    store_id = store_info["store_id"]
    ym = pendulum.now(KST).format("YYYY-MM")
    url = (
        f"https://self.baemin.com/shops/{store_id}"
        f"/stat/marketing/woori-shop-click"
        f"?initialDateOption=MONTHLY&initialMonth={ym}"
    )

    logger.info("=== [시도 1] URL 직접 이동 후 4초 대기(기존 방식) ===")
    logger.info("이동: %s", url)
    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
    except Exception as e:
        logger.warning("이동 타임아웃(계속): %s", e)

    time.sleep(4.0)

    # DOM 상태 확인
    dom_info = driver.execute_script("""
        var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
        if (!trs.length) return JSON.stringify({tr:0, url: location.href});
        var cells = trs[0].querySelectorAll('td');
        var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
        var hasClass = !!(cells.length > 0 && cells[0].querySelector('.styles-module__x4Tk'));
        return JSON.stringify({tr: trs.length, cells: cells.length, val: val, hasClass: hasClass, url: location.href});
    """)
    logger.info("[시도 1] 4초 후 DOM: %s", dom_info)

    def _has_rows(d):
        try:
            rows = d.find_elements(By.CSS_SELECTOR, f"{_TABLE_CSS} tbody tr")
            if not rows:
                return False
            cells = rows[0].find_elements(By.TAG_NAME, "td")
            if len(cells) < 7:
                return False
            try:
                val = (cells[0].find_element(By.CSS_SELECTOR, ".styles-module__x4Tk").get_attribute("innerText") or "").strip()
            except Exception:
                val = (cells[0].get_attribute("innerText") or cells[0].text or "").strip()
            return bool(re.search(r"\d+\.\d+", val))
        except Exception as e:
            if "Connection refused" in str(e) or "Max retries" in str(e):
                raise
            return False

    logger.info("[시도 1] WebDriverWait %ds 시작...", _TABLE_TIMEOUT)
    t0 = time.time()
    try:
        WebDriverWait(driver, _TABLE_TIMEOUT).until(_has_rows)
        logger.info("[시도 1] 테이블 로드 성공! 소요: %.1fs", time.time() - t0)
    except TimeoutException:
        logger.warning("[시도 1] 타임아웃 %.1fs - 테이블 로드 실패", time.time() - t0)
        dom2 = driver.execute_script("""
            var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
            if (!trs.length) return JSON.stringify({tr:0, url: location.href});
            var cells = trs[0].querySelectorAll('td');
            var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
            var hasClass = !!(cells.length > 0 && cells[0].querySelector('.styles-module__x4Tk'));
            return JSON.stringify({tr: trs.length, cells: cells.length, val: val, hasClass: hasClass, url: location.href});
        """)
        logger.warning("[시도 1] 타임아웃 시 DOM: %s", dom2)

    logger.info("=== [시도 2] refresh 추가 방식 ===")
    # 대시보드로 이동
    try:
        driver.set_page_load_timeout(45)
        driver.get("https://self.baemin.com/")
    except Exception:
        pass
    time.sleep(2.0)

    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
    except Exception as e:
        logger.warning("이동 타임아웃(계속): %s", e)

    sleep_sec = random.uniform(1.5, 2.5)
    logger.info("[시도 2] %.1fs 대기 후 refresh 실행", sleep_sec)
    time.sleep(sleep_sec)

    dom_before_refresh = driver.execute_script("""
        var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
        if (!trs.length) return JSON.stringify({tr:0, url: location.href});
        var cells = trs[0].querySelectorAll('td');
        var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
        return JSON.stringify({tr: trs.length, val: val, url: location.href});
    """)
    logger.info("[시도 2] refresh 전 DOM: %s", dom_before_refresh)

    try:
        driver.refresh()
        logger.info("[시도 2] refresh 완료")
    except Exception as e:
        logger.warning("[시도 2] refresh 예외: %s", e)

    t0 = time.time()
    try:
        WebDriverWait(driver, _TABLE_TIMEOUT).until(_has_rows)
        logger.info("[시도 2] 테이블 로드 성공! 소요: %.1fs", time.time() - t0)

        table = driver.find_element(By.CSS_SELECTOR, _TABLE_CSS)
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        logger.info("[시도 2] 행 수: %d", len(rows))
        if rows:
            cells = rows[0].find_elements(By.TAG_NAME, "td")
            sample_val = cells[0].get_attribute("innerText") if cells else "N/A"
            logger.info("[시도 2] 첫 행 날짜 셀: %r", sample_val)
    except TimeoutException:
        logger.warning("[시도 2] 타임아웃 %.1fs", time.time() - t0)
        dom3 = driver.execute_script("""
            var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
            if (!trs.length) return JSON.stringify({tr:0, url: location.href});
            var cells = trs[0].querySelectorAll('td');
            var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
            return JSON.stringify({tr: trs.length, val: val, url: location.href});
        """)
        logger.warning("[시도 2] 타임아웃 시 DOM: %s", dom3)

    logger.info("=== 완료 ===")

finally:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
