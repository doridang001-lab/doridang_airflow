"""단순 로그인 테스트 - isCompany 없이"""
from playwright.sync_api import sync_playwright
import json, time

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # 화면 보이게
    ctx = browser.new_context()
    page = ctx.new_page()

    cognito_requests = []

    def on_req(req):
        if req.resource_type in ("xhr", "fetch"):
            print(f"[REQ] {req.method} {req.url[:100]}")
            if req.post_data:
                print(f"  body: {req.post_data[:200]}")
            cognito_requests.append(req.url)

    def on_resp(resp):
        if resp.request.resource_type in ("xhr", "fetch"):
            print(f"[RESP] {resp.status} {resp.url[:100]}")

    page.on("request", on_req)
    page.on("response", on_resp)

    page.goto("https://ceo.toorder.co.kr/login", wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector('input[name="id"]', timeout=20000)
    print("페이지 로드 완료. URL:", page.url)

    # isCompany 없이 시도 (일반회원)
    page.fill('input[name="id"]', "doridang15")
    page.fill('input[name="password"]', "ehfl5233!")
    print("입력 완료 (isCompany 체크 안 함)")

    page.click('button[type="submit"]')
    print("로그인 클릭")

    time.sleep(10)
    print(f"최종 URL: {page.url}")

    browser.close()
