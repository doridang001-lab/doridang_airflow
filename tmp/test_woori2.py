"""now 수집 후 우가클 이동 시뮬레이션 + CSS 클래스 디버그"""
import sys, time, logging
sys.path.insert(0, "/opt/airflow")
logging.basicConfig(level=logging.WARNING)

from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.extract.croling_beamin import (
    launch_browser, login_baemin, wait_for_page,
    select_store_by_id, wait_for_metrics_data,
)

accounts = load_accounts(["도리당 역삼점"], exact=True)
account = accounts[0]
account_id = account["account_id"]
print("계정:", account_id)

driver = launch_browser(account_id)
try:
    login_baemin(driver, account_id, account["password"])
    wait_for_page(driver, "select[class*='ShopSelect']", timeout=60)
    print("대시보드 로드. URL:", driver.current_url)

    # now 수집 시뮬레이션 (select_store_by_id + wait_for_metrics_data)
    select_store_by_id(driver, "14364235")
    wait_for_metrics_data(driver, timeout=45)
    print("now 수집 후 URL:", driver.current_url)

    # 우가클 URL로 이동
    url = "https://self.baemin.com/shops/14364235/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth=2026-05"
    driver.set_page_load_timeout(45)
    try:
        driver.get(url)
    except Exception as e:
        print("get 예외:", e)

    for i in [3, 8, 15]:
        time.sleep(i if i == 3 else 5)
        cnt = driver.execute_script(
            "return document.querySelectorAll(\"table[data-atelier-component='Table'] tbody tr\").length"
        )
        print(f"{i}초 후 tbody tr 수: {cnt}")
        if cnt > 0:
            break

    # CSS 클래스 확인
    sample = driver.execute_script("""
        var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
        if (!trs.length) return 'NO_TR';
        var td = trs[0].querySelector('td');
        if (!td) return 'NO_TD';
        var children = Array.from(td.querySelectorAll('*'));
        return JSON.stringify({
            tdText: td.innerText.slice(0, 50),
            childClasses: children.map(e => e.className).filter(Boolean).slice(0, 5)
        });
    """)
    print("첫번째 td:", sample)

    # 전체 URL 확인
    print("최종 URL:", driver.current_url)

    # 소스 저장
    with open("/opt/airflow/tmp/woori2_source.html", "w") as f:
        f.write(driver.page_source)
    print("소스 저장 완료")
finally:
    driver.quit()
