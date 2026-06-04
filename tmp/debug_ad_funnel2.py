"""광고 funnel 필터 클릭 후 라디오 구조 + 지표 추출 검증."""
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ACCOUNT_ID = "doriys"
PASSWORD   = "ever123@"
STORE_ID   = "14822058"
AD_URL     = f"https://self.baemin.com/shops/{STORE_ID}/stat/advertisement?initialDateOption=MONTH"

driver = None
try:
    driver = launch_browser(ACCOUNT_ID)
    login_baemin(driver, ACCOUNT_ID, PASSWORD)
    driver.set_page_load_timeout(45)
    driver.get(AD_URL)
    wait_for_page(driver, "button[class*='Filter-module']", timeout=30)
    time.sleep(1.0)

    # ── 지표 추출 (탭 버튼 + 값 span 매핑) ──
    metrics = driver.execute_script("""
        const METRIC_LABELS = ['노출수', '클릭수', '주문수', '주문금액'];
        const tabs = [...document.querySelectorAll('button[class*="Tab_b_r4ax"]')]
            .map(b => b.textContent.trim())
            .filter(t => METRIC_LABELS.includes(t));
        const vals = [...document.querySelectorAll('span[style*="margin"]')]
            .map(s => s.textContent.trim())
            .filter(v => /^[\\d,]+$/.test(v));
        const result = {};
        tabs.forEach((t, i) => { if (vals[i] !== undefined) result[t] = vals[i]; });
        return {tabs, vals, result};
    """)
    print(f"\n=== 지표 추출 (필터 적용 전) ===")
    print(f"  탭 순서: {metrics['tabs']}")
    print(f"  값 순서: {metrics['vals']}")
    print(f"  매핑: {metrics['result']}")

    # ── Filter 버튼 클릭 ──
    clicked = driver.execute_script("""
        const btns = [...document.querySelectorAll('button.Filter-module__lRdH')];
        if (btns[0]) { btns[0].click(); return btns[0].textContent.trim(); }
        return false;
    """)
    print(f"\n[Filter 버튼 클릭]: {clicked!r}")
    time.sleep(1.0)

    # ── 팝업 내 라디오 확인 ──
    radios = driver.execute_script("""
        return [...document.querySelectorAll('input[type=radio]')].map(r => ({
            name: r.name, value: r.value, id: r.id,
            readonly: r.hasAttribute('readonly'), checked: r.checked,
            visible: r.offsetParent !== null,
            labelText: (document.querySelector('label[for="'+r.id+'"]') || {}).textContent || ''
        }));
    """)
    print(f"\n=== 라디오 (Filter 버튼 클릭 후) ===")
    for r in radios:
        print(f"  name={r['name']!r:<40} value={r['value']!r:<15} readonly={r['readonly']}  label={r['labelText']!r}")

    # DAILY 라디오 클릭 시도
    daily_result = driver.execute_script("""
        const radio = document.querySelector('input[name="ad-stats-period-filter"][value="DAILY"]');
        if (!radio) return 'not_found: ad-stats-period-filter DAILY';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked: ' + lbl.textContent.trim(); }
        radio.click(); return 'radio_clicked';
    """)
    print(f"\n[DAILY 라디오]: {daily_result!r}")
    time.sleep(0.8)

    # DAILY 클릭 후 라디오 다시 확인
    radios2 = driver.execute_script("""
        return [...document.querySelectorAll('input[type=radio]')].map(r => ({
            name: r.name, value: r.value, id: r.id,
            readonly: r.hasAttribute('readonly'), checked: r.checked,
            visible: r.offsetParent !== null,
            labelText: (document.querySelector('label[for="'+r.id+'"]') || {}).textContent || ''
        }));
    """)
    print(f"\n=== 라디오 (DAILY 클릭 후) ===")
    for r in radios2:
        print(f"  name={r['name']!r:<40} value={r['value']!r:<15} readonly={r['readonly']}  label={r['labelText']!r:<10} checked={r['checked']}")

    # YESTERDAY 클릭
    yest_result = driver.execute_script("""
        const radio = document.querySelector('input[name="period"][value="YESTERDAY"]');
        if (!radio) return 'not_found: period YESTERDAY';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked: ' + lbl.textContent.trim(); }
        radio.click(); return 'radio_clicked';
    """)
    print(f"\n[YESTERDAY 라디오]: {yest_result!r}")
    time.sleep(0.5)

    # 적용 버튼 확인
    apply_btns = driver.execute_script("""
        return [...document.querySelectorAll('button[data-atelier-component="Button"]')]
            .filter(b => b.textContent.trim() === '적용')
            .map(b => ({ txt: b.textContent.trim(), disabled: b.disabled, cls: b.className.slice(0,60) }));
    """)
    print(f"\n=== 적용 버튼 ===")
    for b in apply_btns:
        print(f"  txt={b['txt']!r}  disabled={b['disabled']}")

    # 적용 클릭
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    apply_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[@data-atelier-component='Button'][.//span[text()='적용']]")
        )
    )
    apply_btn.click()
    print("\n[적용 클릭 완료]")
    time.sleep(2.5)

    # 적용 후 지표 재추출
    metrics2 = driver.execute_script("""
        const METRIC_LABELS = ['노출수', '클릭수', '주문수', '주문금액'];
        const tabs = [...document.querySelectorAll('button[class*="Tab_b_r4ax"]')]
            .map(b => b.textContent.trim())
            .filter(t => METRIC_LABELS.includes(t));
        const vals = [...document.querySelectorAll('span[style*="margin"]')]
            .map(s => s.textContent.trim())
            .filter(v => /^[\\d,]+$/.test(v));
        const result = {};
        tabs.forEach((t, i) => { if (vals[i] !== undefined) result[t] = vals[i]; });

        // Filter 버튼 텍스트 (날짜 확인)
        const filterBtns = [...document.querySelectorAll('button.Filter-module__lRdH')];
        const filterTexts = filterBtns.map(b => b.textContent.trim());
        return {tabs, vals, result, filterTexts};
    """)
    print(f"\n=== 지표 추출 (YESTERDAY 필터 적용 후) ===")
    print(f"  Filter 버튼 텍스트: {metrics2['filterTexts']}")
    print(f"  탭 순서: {metrics2['tabs']}")
    print(f"  값 순서: {metrics2['vals']}")
    print(f"  매핑: {metrics2['result']}")

finally:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
