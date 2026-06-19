"""Watch Coupang extension runner.html and retry failed/skipped rows."""

from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, NoSuchWindowException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

RUNNER_URL_SUFFIX = "/runner.html"
RUNNER_URL_FALLBACK = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
DEBUG_ADDRESS = "localhost:9222"
DEBUG_ENDPOINT = "http://127.0.0.1:9222"
MAX_RETRY = 5
POLL_SEC = 30
BATCH_TIMEOUT = 5400
TARGET_SETTLE_SEC = 5


def _http_json(url: str, method: str = "GET") -> Any:
    try:
        req = urllib.request.Request(url, method=method, data=b"" if method != "GET" else None)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as exc:
        raise RuntimeError(f"DevTools endpoint unreachable: {url}") from exc


def _fetch_tabs() -> list[dict]:
    return _http_json(f"{DEBUG_ENDPOINT}/json")


def _find_runner_url() -> str:
    for tab in _fetch_tabs():
        url = str(tab.get("url", ""))
        if url.startswith("chrome-extension://") and url.endswith(RUNNER_URL_SUFFIX):
            return url
    return RUNNER_URL_FALLBACK


def _open_runner_tab() -> None:
    runner_url = _find_runner_url()
    encoded = urllib.parse.quote(runner_url, safe="")
    logger.info("runner.html 탭 새로 열기: %s", runner_url)
    _http_json(f"{DEBUG_ENDPOINT}/json/new?{encoded}", method="PUT")


def connect_to_chrome() -> webdriver.Chrome:
    opts = Options()
    opts.debugger_address = DEBUG_ADDRESS
    return webdriver.Chrome(options=opts)


def find_runner_tab(driver: webdriver.Chrome) -> bool:
    for handle in driver.window_handles:
        try:
            driver.switch_to.window(handle)
            current_url = driver.current_url
        except WebDriverException:
            continue

        if current_url.startswith("chrome-extension://") and current_url.endswith(RUNNER_URL_SUFFIX):
            logger.info("runner.html 탭 감지: %s", current_url)
            return True

    return False


def ensure_runner_tab(driver: webdriver.Chrome) -> None:
    if find_runner_tab(driver):
        return

    _open_runner_tab()
    time.sleep(3)
    if not find_runner_tab(driver):
        driver.get(RUNNER_URL_FALLBACK)
        time.sleep(3)

    if not find_runner_tab(driver):
        raise RuntimeError("runner.html 탭을 찾거나 열 수 없습니다")


def wait_for_batch_done(driver: webdriver.Chrome, poll_sec: int, batch_timeout: int) -> None:
    deadline = time.time() + batch_timeout

    while time.time() < deadline:
        try:
            ensure_runner_tab(driver)
            running = driver.execute_script("return window.running === true")
            if not running:
                logger.info("배치 완료 감지")
                return
        except (NoSuchWindowException, WebDriverException) as exc:
            logger.warning("runner.html 상태 확인 실패, 재탐색: %s", exc)
            try:
                ensure_runner_tab(driver)
            except WebDriverException as retry_exc:
                logger.warning("runner.html 재탐색 실패: %s", retry_exc)
            time.sleep(5)

        time.sleep(poll_sec)

    logger.warning("배치 타임아웃 %ds 초과", batch_timeout)


def _row_index(row: Any) -> str | None:
    row_id = row.get_attribute("id")
    if not row_id or not row_id.startswith("row-"):
        return None
    return row_id.removeprefix("row-")


def _has_skipped_badge(row: Any) -> bool:
    for span in row.find_elements(By.CSS_SELECTOR, "span.badge"):
        cls = span.get_attribute("class") or ""
        text = span.text.strip()
        if "b-wait" in cls and text == "건너뜀":
            return True
    return False


def capture_initial_target_indices(driver: webdriver.Chrome, settle_sec: int) -> set[str]:
    ensure_runner_tab(driver)
    if settle_sec > 0:
        time.sleep(settle_sec)

    rows = driver.find_elements(By.CSS_SELECTOR, "tbody#rows tr")
    target_indices: list[str] = []
    skipped_indices: list[str] = []

    for row in rows:
        idx = _row_index(row)
        if idx is None:
            continue

        if _has_skipped_badge(row):
            skipped_indices.append(idx)
            continue

        target_indices.append(idx)

    logger.info(
        "최초 수집 대상 스냅샷: 대상 %d개, 초기 건너뜀 제외 %d개",
        len(target_indices),
        len(skipped_indices),
    )
    if skipped_indices:
        logger.info("초기 건너뜀 제외 행: %s", skipped_indices)

    return set(target_indices)


def find_failed_row_indices(driver: webdriver.Chrome, target_indices: set[str] | None = None) -> list[str]:
    ensure_runner_tab(driver)
    rows = driver.find_elements(By.CSS_SELECTOR, "tbody#rows tr")
    failed: list[str] = []

    for row in rows:
        idx = _row_index(row)
        if idx is None:
            continue

        if target_indices is not None and idx not in target_indices:
            continue

        for span in row.find_elements(By.CSS_SELECTOR, "span.badge"):
            cls = span.get_attribute("class") or ""
            text = span.text.strip()
            if "b-fail" in cls or "b-warn" in cls:
                failed.append(idx)
                break
            if "b-wait" in cls and text == "건너뜀":
                failed.append(idx)
                break

    return list(dict.fromkeys(failed))


def click_retry(driver: webdriver.Chrome, indices: list[str]) -> None:
    ensure_runner_tab(driver)

    for idx in indices:
        driver.find_element(By.ID, f"row-{idx}").click()
        time.sleep(0.3)

    try:
        driver.find_element(By.ID, "startBtn").click()
    except NoSuchElementException:
        logger.error("startBtn을 찾을 수 없어 재시도를 시작하지 못했습니다")
        raise

    logger.info("재시도 시작: %s", indices)


def run(args: argparse.Namespace) -> int:
    driver = connect_to_chrome()
    try:
        ensure_runner_tab(driver)
        target_indices = capture_initial_target_indices(driver, args.target_settle_sec)

        for attempt in range(args.max_retry + 1):
            logger.info("[%d/%d] 배치 완료 대기 중", attempt, args.max_retry)
            wait_for_batch_done(driver, args.poll_sec, args.batch_timeout)

            failed = find_failed_row_indices(driver, target_indices)
            logger.info("실패/건너뜀 행: %s", failed)

            if not failed:
                logger.info("모든 매장 완료")
                return 0

            if attempt >= args.max_retry:
                logger.warning("최대 재시도(%d) 도달, 미완료 행: %s", args.max_retry, failed)
                return 1

            click_retry(driver, failed)
            time.sleep(3)

        return 1
    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coupang extension runner.html 감시 및 실패/건너뜀 재시도")
    parser.add_argument("--max-retry", type=int, default=MAX_RETRY)
    parser.add_argument("--poll-sec", type=int, default=POLL_SEC)
    parser.add_argument("--batch-timeout", type=int, default=BATCH_TIMEOUT)
    parser.add_argument("--target-settle-sec", type=int, default=TARGET_SETTLE_SEC)
    return parser.parse_args()


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
