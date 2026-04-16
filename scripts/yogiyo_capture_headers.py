"""
요기요 CEO 공지 API 실제 요청 헤더 캡처 스크립트
- Playwright로 브라우저 실행
- ceo-api.yogiyo.co.kr/announcements/ 요청 가로채기
- Authorization, Cookie, x-csrftoken 등 헤더 전체 출력
"""
import json
import time
import logging
from playwright.sync_api import sync_playwright, Request, Response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_URL_KEYWORD = "announcements"
API_HOST = "ceo-api.yogiyo.co.kr"
PAGE_URL = "https://ceo.yogiyo.co.kr/announcement/list/"

captured_requests = []
captured_responses = []


def on_request(request: Request):
    url = request.url
    if API_HOST in url and TARGET_URL_KEYWORD in url:
        logger.info(f"[REQUEST CAPTURED] {request.method} {url}")
        headers = dict(request.headers)
        captured_requests.append({
            "method": request.method,
            "url": url,
            "headers": headers,
            "post_data": request.post_data,
        })
        print("\n" + "=" * 70)
        print(f"[CAPTURED REQUEST]")
        print(f"  Method : {request.method}")
        print(f"  URL    : {url}")
        print(f"\n  Headers:")
        for k, v in headers.items():
            print(f"    {k}: {v}")
        print("=" * 70 + "\n")


def on_response(response: Response):
    url = response.url
    if API_HOST in url and TARGET_URL_KEYWORD in url:
        logger.info(f"[RESPONSE CAPTURED] {response.status} {url}")
        resp_headers = dict(response.headers)
        print("\n" + "-" * 70)
        print(f"[CAPTURED RESPONSE]")
        print(f"  Status : {response.status}")
        print(f"  URL    : {url}")
        print(f"\n  Response Headers:")
        for k, v in resp_headers.items():
            print(f"    {k}: {v}")
        # 응답 본문 시도
        try:
            body = response.text()
            print(f"\n  Response Body (first 500 chars):")
            print(f"    {body[:500]}")
        except Exception as e:
            print(f"  Body read error: {e}")
        print("-" * 70 + "\n")
        captured_responses.append({
            "status": response.status,
            "url": url,
            "headers": resp_headers,
        })


def run():
    with sync_playwright() as p:
        logger.info("Chromium 브라우저 시작 (headless=False 로컬 디버그)")
        browser = p.chromium.launch(
            headless=False,  # 로컬 테스트: 브라우저 표시
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--start-maximized",
            ],
        )

        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
        )

        page = context.new_page()

        # 요청/응답 이벤트 리스너 등록
        page.on("request", on_request)
        page.on("response", on_response)

        logger.info(f"페이지 접속: {PAGE_URL}")
        try:
            page.goto(PAGE_URL, wait_until="networkidle", timeout=30000)
        except Exception as e:
            logger.warning(f"페이지 로드 타임아웃/오류 (계속 진행): {e}")

        # 추가 대기 - lazy load 대비
        logger.info("추가 3초 대기 (lazy load 대비)...")
        time.sleep(3)

        # 스크롤 시도 (리스트 로드 트리거)
        logger.info("페이지 스크롤 시도...")
        try:
            page.evaluate("window.scrollTo(0, 300)")
            time.sleep(1)
            page.evaluate("window.scrollTo(0, 600)")
            time.sleep(1)
        except Exception as e:
            logger.warning(f"스크롤 오류: {e}")

        # 전체 네트워크 요청 목록 (필터링 없이)
        logger.info("\n\n전체 캡처된 요청 중 API 관련 목록:")
        if not captured_requests:
            logger.warning("announcements API 요청이 캡처되지 않았습니다.")
            logger.info("모든 네트워크 요청을 다시 확인하려면 page.on 전체 덤프가 필요합니다.")

            # 전체 요청 덤프용 재시도
            all_requests = []

            def capture_all(req):
                all_requests.append(req.url)

            page.on("request", capture_all)

            # 페이지 새로고침
            logger.info("페이지 새로고침 후 전체 요청 덤프...")
            try:
                page.reload(wait_until="networkidle", timeout=20000)
            except Exception as e:
                logger.warning(f"리로드 타임아웃: {e}")

            time.sleep(3)

            print("\n[ALL CAPTURED URLS]")
            for url in all_requests:
                print(f"  {url}")

        # 결과 저장
        output_path = "C:/airflow/scripts/yogiyo_captured_headers.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {"requests": captured_requests, "responses": captured_responses},
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"캡처 결과 저장: {output_path}")

        browser.close()
        logger.info("브라우저 종료")


if __name__ == "__main__":
    run()
