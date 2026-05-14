import sys
sys.path.insert(0, '/opt/airflow')
import time
import subprocess
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = uc.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")

result = subprocess.run(["/usr/bin/google-chrome", "--version"], capture_output=True, text=True)
version_main = int(result.stdout.strip().split()[-1].split(".")[0])
print(f"Chrome: {version_main}")

driver = uc.Chrome(options=options, version_main=version_main)
try:
    driver.get("https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard")
    time.sleep(3)

    wait = WebDriverWait(driver, 10)
    id_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']")))
    pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")

    TOORDER_ID = "doridang15"
    TOORDER_PW = "ehfl5233!"

    # ActionChains 방식
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys

    def fill_field(el, value):
        ActionChains(driver).click(el).perform()
        time.sleep(0.2)
        ActionChains(driver).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).perform()
        time.sleep(0.1)
        ActionChains(driver).send_keys(Keys.DELETE).perform()
        time.sleep(0.1)
        ActionChains(driver).send_keys(value).perform()
        time.sleep(0.3)
        actual = driver.execute_script("return arguments[0].value;", el) or ""
        print(f"  ActionChains result: {actual!r}")
        return actual == value

    print("Filling ID...")
    fill_field(id_input, TOORDER_ID)

    print("Filling PW...")
    fill_field(pw_input, TOORDER_PW)

    id_val = driver.execute_script("return document.querySelector(\"input[name='id']\").value;")
    pw_val = driver.execute_script("return document.querySelector(\"input[name='password']\").value;")
    print(f"Before checkbox: ID={id_val!r} (len={len(id_val)}), PW len={len(pw_val or '')}")

    checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
    if not checkbox.is_selected():
        driver.execute_script("arguments[0].click();", checkbox)
    time.sleep(0.5)

    id_val2 = driver.execute_script("return document.querySelector(\"input[name='id']\").value;")
    pw_val2 = driver.execute_script("return document.querySelector(\"input[name='password']\").value;")
    print(f"After checkbox:  ID={id_val2!r} (len={len(id_val2)}), PW len={len(pw_val2 or '')}")

    submit = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    driver.execute_script("arguments[0].click();", submit)

    try:
        WebDriverWait(driver, 15).until(EC.url_contains("/dashboard"))
    except:
        pass

    final_url = driver.current_url
    print(f"Final URL: {final_url}")
    success = "/dashboard" in final_url and "/login" not in final_url and "/auth" not in final_url
    print(f"Login success: {success}")

    driver.save_screenshot("/opt/airflow/analytics/ai_daily_collection/_debug/test_login_result.png")

finally:
    driver.quit()
