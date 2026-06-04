"""orders 페이지 버튼 구조 디버그 — 상태 필터 위치 파악."""
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    ORDERS_URL, _TABLE_ROW_CSS, _select_order_store,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ACCOUNT_ID = "doriys"
PASSWORD   = "ever123@"
STORE_ID   = "14822058"
STORE_NAME = "역삼점"


def dump_buttons(driver, label=""):
    result = driver.execute_script("""
        return [...document.querySelectorAll('button')].map(b => ({
            cls: b.className.slice(0, 60),
            txt: b.textContent.trim().slice(0, 50),
            dis: b.disabled
        }));
    """)
    print(f"\n=== 버튼 목록 [{label}] ===")
    for i, b in enumerate(result):
        print(f"  [{i}] cls={b['cls'][:40]!r}  txt={b['txt']!r}  disabled={b['dis']}")


def dump_radios(driver, label=""):
    result = driver.execute_script("""
        return [...document.querySelectorAll('input[type=radio]')].map(r => ({
            name: r.name, value: r.value, id: r.id,
            readonly: r.hasAttribute('readonly'), checked: r.checked,
            visible: r.offsetParent !== null
        }));
    """)
    print(f"\n=== 라디오 목록 [{label}] ===")
    for r in result:
        print(f"  name={r['name']!r} value={r['value']!r} id={r['id']!r} readonly={r['readonly']} checked={r['checked']} visible={r['visible']}")


driver = None
try:
    driver = launch_browser(ACCOUNT_ID)
    login_baemin(driver, ACCOUNT_ID, PASSWORD)

    if urlparse(driver.current_url).hostname != "self.baemin.com":
        driver.set_page_load_timeout(45)
        driver.get("https://self.baemin.com/")

    driver.set_page_load_timeout(45)
    driver.get(ORDERS_URL)
    wait_for_page(driver, _TABLE_ROW_CSS, timeout=30)

    dump_buttons(driver, "orders 초기 로드 직후")
    dump_radios(driver, "orders 초기 로드 직후")

    # 가게 필터 선택
    _select_order_store(driver, STORE_ID, STORE_NAME)
    time.sleep(1.0)

    dump_buttons(driver, "가게 필터 적용 후")
    dump_radios(driver, "가게 필터 적용 후")

    # 각 Filter 버튼 순서대로 클릭해 상태 팝업이 뜨는지 확인
    filter_btns = driver.execute_script("""
        return [...document.querySelectorAll('button')]
            .filter(b => b.className.includes('Filter'))
            .map((b, i) => ({idx: i, txt: b.textContent.trim().slice(0,50), cls: b.className.slice(0,60)}));
    """)
    print(f"\n=== Filter 버튼 목록 ===")
    for b in filter_btns:
        print(f"  [{b['idx']}] {b['txt']!r}  cls={b['cls']!r}")

finally:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
