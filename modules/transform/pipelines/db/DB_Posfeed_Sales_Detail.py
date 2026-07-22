"""
Posfeed 주문 상세 크롤링 파이프라인

처리 흐름:
1. extract_order_codes: 어제 날짜 파티션에서 주문 코드 추출 (증분)
2. scrape_order_details: 상세 페이지 순회 크롤링 → 100건마다 OneDrive 중간 저장
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.analytics import read_analytics_partition
from modules.transform.utility.selenium_uc import configure_uc_data_path
from modules.load.load_onedrive import onedrive_csv_save

logger = logging.getLogger(__name__)


def _get_airflow_variable(key: str) -> str:
    try:
        from airflow.models import Variable

        return (Variable.get(key, default_var=None) or "").strip()
    except Exception:
        return ""


def _resolve_secret(key: str, default: str) -> str:
    return (os.getenv(key) or _get_airflow_variable(key) or default).strip()


# ============================================================
# 상수
# ============================================================
POSFEED_ID = _resolve_secret("POSFEED_ID", "siw2222@naver.com")
POSFEED_PW = _resolve_secret("POSFEED_PW", "ehfl8877!!")
LOGIN_URL   = "https://admin.posfeed.co.kr/#/login?redirect=%2Fdashboard"
ORDER_DETAIL_URL = "https://admin.posfeed.co.kr/#/order/edit/{code}"
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None

# 장시간 실행 시 Chrome/driver 누적으로 크래시가 나는 케이스가 있어
# 매 N개 매장마다 주기적으로 재시작해 안정성을 높인다. (0이면 비활성화)
_RESTART_DRIVER_EVERY_STORES = 10
_RESTART_DRIVER_EVERY_SECONDS = 45 * 60



# ============================================================
# 내부 유틸 - 브라우저 (다운로드 설정 없는 경량 버전)
# ============================================================
def _get_chrome_version() -> int | None:
    import subprocess
    candidates = [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["chromium-browser", "--version"],
        ["chromium", "--version"],
    ]
    for cmd in candidates:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            match = re.search(r"(\d+)\.", result.stdout.strip())
            if match:
                return int(match.group(1))
        except Exception:
            continue
    return None


def _kill_chrome_processes() -> None:
    """좀비 Chrome/ChromeDriver 프로세스를 정리해 다음 브라우저 실행을 안정화"""
    import subprocess
    targets = ["google-chrome", "chrome", "chromium", "chromium-browser", "undetected_chromedriver", "chromedriver"]
    for name in targets:
        try:
            subprocess.run(["pkill", "-9", "-f", name], capture_output=True, timeout=5)
        except Exception:
            pass
    time.sleep(1)


def _launch_browser() -> uc.Chrome:
    """다운로드 설정 없는 경량 Chrome 브라우저 실행"""
    _kill_chrome_processes()
    logger.info("브라우저 실행 (headless=%s)", HEADLESS_MODE)
    configure_uc_data_path()

    def _make_options() -> uc.ChromeOptions:
        options = uc.ChromeOptions()
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin and Path(chrome_bin).exists():
            options.binary_location = chrome_bin
        if HEADLESS_MODE:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        return options

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        if chrome_version:
            kwargs["version_main"] = chrome_version
        driver = uc.Chrome(**kwargs)
        driver.set_window_size(1920, 1080)
        # Avoid getting stuck in a long page load; if the site freezes, we want a Selenium-level timeout
        # (so Airflow execution_timeout is used as a last resort, not the primary breaker).
        try:
            driver.set_page_load_timeout(int(os.getenv("POSFEED_PAGELOAD_TIMEOUT_SEC", "45")))
            driver.set_script_timeout(int(os.getenv("POSFEED_SCRIPT_TIMEOUT_SEC", "45")))
        except Exception as timeout_err:
            logger.warning("WebDriver timeout 설정 실패(무시): %s", timeout_err)
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning("버전 불일치 → %d 으로 재시도", detected)
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            driver.set_window_size(1920, 1080)
            try:
                driver.set_page_load_timeout(int(os.getenv("POSFEED_PAGELOAD_TIMEOUT_SEC", "45")))
                driver.set_script_timeout(int(os.getenv("POSFEED_SCRIPT_TIMEOUT_SEC", "45")))
            except Exception as timeout_err:
                logger.warning("WebDriver timeout 설정 실패(무시): %s", timeout_err)
            return driver
        # DNS/네트워크 오류: 기존에 패치된 바이너리를 직접 지정하여 오프라인 재시도
        if "No address associated with hostname" in str(e) or "URLError" in type(e).__name__:
            _known_paths = [
                "/tmp/undetected_chromedriver/undetected_chromedriver",
                "/root/.local/share/undetected_chromedriver/undetected_chromedriver",
            ]
            for _p in _known_paths:
                if Path(_p).exists():
                    logger.warning("DNS 오류 → 캐시 드라이버 재사용: %s", _p)
                    driver = uc.Chrome(options=_make_options(), version_main=chrome_version, driver_executable_path=_p)
                    driver.set_window_size(1920, 1080)
                    try:
                        driver.set_page_load_timeout(int(os.getenv("POSFEED_PAGELOAD_TIMEOUT_SEC", "45")))
                        driver.set_script_timeout(int(os.getenv("POSFEED_SCRIPT_TIMEOUT_SEC", "45")))
                    except Exception:
                        pass
                    logger.info("브라우저 실행 성공 (오프라인 재시도)")
                    return driver
        raise


def _login(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """Posfeed 로그인"""
    driver.get(LOGIN_URL)
    time.sleep(2)
    id_input = wait.until(EC.presence_of_element_located((By.NAME, "username")))
    id_input.clear()
    id_input.send_keys(POSFEED_ID)
    pw_input = driver.find_element(By.NAME, "password")
    pw_input.clear()
    pw_input.send_keys(POSFEED_PW)
    pw_input.send_keys(Keys.RETURN)
    wait.until(lambda d: "#/login" not in d.current_url)
    _dismiss_change_password_notice(driver)
    time.sleep(2)
    logger.info("Posfeed 로그인 완료 | URL: %s", driver.current_url)


def _dismiss_change_password_notice(driver: uc.Chrome) -> None:
    """비밀번호 변경 안내 페이지에서 '다음에 (30일간 보지않기)'를 클릭해 작업을 계속한다."""
    try:
        btn_wait = WebDriverWait(driver, 5)
        btn = btn_wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[.//span[contains(normalize-space(), '다음에')]]",
                )
            )
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.7)
        logger.info("비밀번호 변경 안내 팝업: '다음에 (30일간 보지않기)' 클릭 완료")
    except Exception:
        # 해당 안내가 없거나 이미 닫힌 경우는 정상 흐름으로 간주
        pass


# ============================================================
# 내부 유틸 - 크롤링
# ============================================================
def _clean_price(val: str) -> str:
    """'20,800원' → '20800'"""
    return re.sub(r"[^\d]", "", val)


def _extract_int(text: str) -> str:
    """문자열에서 첫 번째 정수만 추출(없으면 '')"""
    if text is None:
        return ""
    match = re.search(r"\d+", str(text).replace(",", ""))
    return match.group(0) if match else ""


def _extract_qty(text: str) -> str:
    """수량 셀에서만 안전하게 정수 추출(예: 1, 1개, x1)."""
    if text is None:
        return ""
    normalized = str(text).strip().replace(",", "")
    if not normalized or normalized.lower() == "nan":
        return ""
    match = re.fullmatch(r"(?:x\s*)?(\d+)(?:\s*개)?", normalized, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _looks_like_option_row(name: str) -> bool:
    """0원 옵션/제거요청/추가 문구처럼 수집해야 하는 보조 행인지 판별."""
    normalized = str(name).strip()
    if not normalized:
        return False
    option_keywords = (
        "빼주세요",
        "제외",
        "추가",
        "선택",
        "변경",
        "토핑",
        "사리",
        "소스",
        "단무지",
    )
    return (
        normalized.startswith(("+", "-"))
        or "(X)" in normalized
        or any(keyword in normalized for keyword in option_keywords)
    )


def _extract_delivery_fee_text(driver: uc.Chrome) -> str:
    """주문 상세 화면에서 배달비 텍스트를 추출한다."""
    return (driver.execute_script("""
        function readCellText(cell) {
            if (!cell) return '';
            return (cell.innerText || cell.textContent || '').trim();
        }

        const labelNodes = Array.from(document.querySelectorAll('td, th, .cell, span'));
        for (const node of labelNodes) {
            if (readCellText(node) !== '배달비') continue;

            const cell = node.closest('td, th');
            if (!cell) continue;

            let sibling = cell.nextElementSibling;
            while (sibling) {
                const text = readCellText(sibling);
                if (text && text !== '-') {
                    return text;
                }
                sibling = sibling.nextElementSibling;
            }
        }

        return '';
    """) or "").strip()


def _scrape_one_order_non_empty_rows(
    driver: uc.Chrome,
    wait: WebDriverWait,
    order_code: str,
    *,
    max_attempts: int = 3,
) -> tuple[list[dict], bool, uc.Chrome, WebDriverWait]:
    """상세 수집을 '빈 상세/타임아웃'에 더 강하게 재시도해서 non-empty rows만 반환.

    Returns:
      - non_empty_rows: 실제 상품 행(비어있지 않은 행)만
      - empty_detected: 빈 상세(placeholder)까지는 관측되었는지 여부
      - driver/wait: 재시작이 발생할 수 있으므로 최신 핸들 반환
    """
    empty_detected = False
    sleep_steps = (0.8, 1.5, 2.5)

    for attempt in range(max_attempts):
        try:
            rows = _scrape_one_order(driver, wait, order_code)
            if rows:
                non_empty_rows = [r for r in rows if not _is_empty_detail_row(r)]
                if non_empty_rows:
                    return non_empty_rows, empty_detected, driver, wait
                empty_detected = True
        except Exception:
            # 아래 recovery 로직에서 복구 시도
            pass

        if attempt >= max_attempts - 1:
            break

        # recovery + backoff
        try:
            if not _is_driver_alive(driver):
                driver, wait = _restart_driver_with_login(driver)
            elif "#/login" in driver.current_url:
                _login(driver, wait)
            else:
                # 간헐적 빈 상세는 refresh만으로도 복구되는 경우가 많음
                driver.refresh()
        except Exception:
            try:
                driver, wait = _restart_driver_with_login(driver)
            except Exception:
                pass

        time.sleep(sleep_steps[min(attempt, len(sleep_steps) - 1)])

    return [], empty_detected, driver, wait


def _scrape_one_order(driver: uc.Chrome, wait: WebDriverWait, order_code: str) -> list[dict] | None:
    """주문 상세 페이지 1건 크롤링"""
    url = ORDER_DETAIL_URL.format(code=order_code)
    driver.get(url)

    # 세션 만료 감지 → 재로그인 후 재시도 (1회)
    if "#/login" in driver.current_url:
        logger.warning("세션 만료 감지 | 코드: %s | 재로그인 시도", order_code)
        _login(driver, wait)
        driver.get(url)

    try:
        # SPA 라우팅/렌더가 느리면 이전 주문 화면의 table이 남아있는 상태에서 파싱될 수 있다.
        # 주문코드가 페이지 텍스트에 반영될 때까지 대기해 '이전 주문 데이터' 혼입을 방지한다.
        wait.until(lambda d: str(order_code) in (d.execute_script("return document.body && document.body.innerText") or ""))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".el-table")))
        # 상품 정보 테이블 row가 생기거나(데이터) empty-text가 채워질 때(진짜 빈 상세)까지 추가로 대기
        def _table_ready(d):
            state = d.execute_script("""
                function findProductTable() {
                    const tables = Array.from(document.querySelectorAll('.el-table'));
                    for (const t of tables) {
                        // fixed 컬럼(좌/우) 영역은 제외하고 메인 header만 본다.
                        const headers = Array.from(t.querySelectorAll(':scope > .el-table__header-wrapper th .cell'))
                          .map(el => (el.innerText || '').trim())
                          .filter(Boolean);
                        const hasQty = headers.some(h => h.includes('수량'));
                        const hasName = headers.some(h => h.includes('상품') || h.includes('품목'));
                        const hasPrice = headers.some(h => h.includes('단품') || h.includes('가격') || h.includes('금액'));
                        if (hasQty && hasName && hasPrice) return t;
                    }
                    return null;
                }

                const table = findProductTable();
                if (!table) return { rowCount: 0, emptyText: '', tableFound: false };
                const rows = table.querySelectorAll(':scope > .el-table__body-wrapper tbody tr');
                const emptyTextEl = table.querySelector(':scope > .el-table__body-wrapper .el-table__empty-text');
                const emptyText = emptyTextEl ? (emptyTextEl.innerText || '').trim() : '';
                return { rowCount: rows.length, emptyText: emptyText };
            """) or {}
            if (state.get("rowCount", 0) or state.get("emptyText")):
                return state
            return False

        state = wait.until(_table_ready)
    except TimeoutException:
        logger.warning("테이블 로드 타임아웃 | 코드: %s", order_code)
        return None

    table_rows = driver.execute_script("""
        function findProductTable() {
            const tables = Array.from(document.querySelectorAll('.el-table'));
            for (const t of tables) {
                const headers = Array.from(t.querySelectorAll(':scope > .el-table__header-wrapper th .cell'))
                  .map(el => (el.innerText || '').trim())
                  .filter(Boolean);
                const hasQty = headers.some(h => h.includes('수량'));
                const hasName = headers.some(h => h.includes('상품') || h.includes('품목'));
                const hasPrice = headers.some(h => h.includes('단품') || h.includes('가격') || h.includes('금액'));
                if (hasQty && hasName && hasPrice) return t;
            }
            return null;
        }

        const table = findProductTable();
        if (!table) return { headers: [], rows: [] };

        const headers = Array.from(table.querySelectorAll(':scope > .el-table__header-wrapper th .cell'))
          .map(el => (el.innerText || '').trim());

        // 중요: td .cell 과 td 를 같이 잡으면 중복으로 들어가 컬럼 인덱스가 깨진다.
        // 또한 fixed 컬럼(좌/우) body를 포함하면 부분 컬럼만 파싱돼 인덱스가 깨진다.
        const rows = Array.from(table.querySelectorAll(':scope > .el-table__body-wrapper tbody tr')).map(row => {
          const tds = row.querySelectorAll(':scope > td');
          return Array.from(tds).map(td => (td.innerText || '').trim());
        });

        return { headers, rows };
    """) or []

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = []
    delivery_fee_text = _extract_delivery_fee_text(driver)

    headers = [h.strip() for h in (table_rows.get("headers") or []) if str(h).strip()]
    rows = table_rows.get("rows") or []

    def _find_col_index(candidates: list[str]) -> int | None:
        for i, h in enumerate(headers):
            for c in candidates:
                if c in h:
                    return i
        return None

    idx_name = _find_col_index(["상품", "품목"])
    idx_qty = _find_col_index(["수량"])
    idx_unit = _find_col_index(["단품", "단가", "가격"])
    idx_total = _find_col_index(["합계", "금액", "총액"])

    # 헤더가 비정상/공란이면 기본 포맷(상품/수량/단품/합계) 가정 (테이블 형태에 따라 4/5열 모두 대응)
    if idx_name is None:
        idx_name = 0

    for tds in rows:
        if not isinstance(tds, list) or len(tds) < 2:
            continue

        name_raw = tds[idx_name] if idx_name < len(tds) else ""

        if idx_qty is not None and idx_qty < len(tds):
            qty_raw = tds[idx_qty]
        else:
            qty_raw = tds[1] if len(tds) > 1 else ""

        if idx_unit is not None and idx_unit < len(tds):
            unit_raw = tds[idx_unit]
        else:
            unit_raw = tds[2] if len(tds) > 2 else ""

        if idx_total is not None and idx_total < len(tds):
            total_raw = tds[idx_total]
        else:
            # 컬럼 수가 5개면 합계는 보통 마지막(4), 4개면 마지막(3)
            total_raw = tds[4] if len(tds) > 4 else (tds[3] if len(tds) > 3 else "")

        name = name_raw
        name_stripped = str(name).strip()
        is_option_row = _looks_like_option_row(name_stripped)

        qty = _extract_qty(qty_raw)
        if not qty:
            if is_option_row:
                qty = "1"
            else:  # 수량이 없으면 비상품 행 (배달시간 등) → skip
                continue

        unit_price = _clean_price(unit_raw)
        total_price = _clean_price(total_raw)

        # 옵션(대부분 '+'로 시작)은 0원인데 UI에서 '-' 로 보이거나 공란으로 내려오는 케이스가 있다.
        # - 둘 다 비어있으면: 옵션이면 0으로 채우고, 아니면 비상품 행으로 간주해 skip
        # - 한쪽만 비어있으면: 가능한 범위에서 보정(합계=단품*수량 등)
        if not unit_price and not total_price:
            if is_option_row:
                unit_price = "0"
                total_price = "0"
            else:
                continue
        elif unit_price and not total_price:
            try:
                total_price = str(int(unit_price) * int(qty))
            except Exception:
                pass
        elif total_price and not unit_price:
            try:
                q = int(qty)
                t = int(total_price)
                if q > 0 and t % q == 0:
                    unit_price = str(t // q)
            except Exception:
                pass

        if name and name[0] in ('+', '-', '=', '@'):
            name = "'" + name  # Excel 수식 자동변환 방지
        result.append({
            "주문코드":  str(order_code),
            "상품명":   name,
            "수량":     qty,
            "단품가격": unit_price,
            "합계":     total_price,
            "collected_at": collected_at,
        })

    # 상품 행이 없어도 주문코드 기록 보존
    # - 진짜 빈 상세(EmptyText가 있는 경우): placeholder 반환 → 상위에서 빈 상세 재수집 대상으로 관리
    # - EmptyText가 없는데 상품 행이 안 잡히는 경우: Vue 로딩 지연/파싱 이슈일 수 있어 None 반환(재시도 유도)
    if not result and state.get("emptyText"):
        result.append({
            "주문코드":  str(order_code),
            "상품명":   "",
            "수량":     "",
            "단품가격": "",
            "합계":     "",
            "collected_at": collected_at,
        })
    elif not result:
        logger.warning("상품 행 파싱 실패(재시도 유도) | 코드: %s", order_code)
        return None

    delivery_fee = _clean_price(delivery_fee_text)
    if delivery_fee_text and delivery_fee:
        result.append({
            "주문코드": str(order_code),
            "상품명": "배달비",
            "수량": "1",
            "단품가격": delivery_fee,
            "합계": delivery_fee,
            "collected_at": collected_at,
        })

    return result


def _is_empty_detail_row(row: dict) -> bool:
    """상세 행이 실질적으로 비어있는지 판별"""
    return (
        str(row.get("상품명", "")).strip() == ""
        and str(row.get("수량", "")).strip() == ""
        and str(row.get("단품가격", "")).strip() == ""
        and str(row.get("합계", "")).strip() == ""
    )


def _is_driver_alive(driver: uc.Chrome) -> bool:
    """WebDriver 세션 유효성 확인"""
    try:
        driver.execute_script("return 1")
        return True
    except Exception:
        return False


def _restart_driver_with_login(driver: uc.Chrome | None) -> tuple[uc.Chrome, WebDriverWait]:
    """브라우저를 재생성하고 로그인까지 완료한 새 세션 반환"""
    if driver is not None:
        try:
            driver.quit()
        except Exception:
            pass

    new_driver = _launch_browser()
    new_wait = WebDriverWait(new_driver, 15)
    _login(new_driver, new_wait)
    logger.info("브라우저 재생성 및 재로그인 완료")
    return new_driver, new_wait


def _append_to_partition(rows: list[dict]) -> None:
    """수집 결과를 brand/store/ym 파티션별 CSV로 누적 저장."""
    df = pd.DataFrame(rows)
    # _pk는 Excel escape(') 제거 후 원본 텍스트 기준으로 생성
    df["_pk"] = df["주문코드"].astype(str) + "_" + df["상품명"].str.lstrip("'").astype(str)
    # ym: 주문 등록날짜(order_date) 기준 — 없으면 collected_at 폴백
    if "order_date" in df.columns and df["order_date"].notna().any():
        df["ym"] = pd.to_datetime(df["order_date"], errors="coerce").dt.strftime("%Y-%m")
        fallback = df["ym"].isna()
        if fallback.any():
            df.loc[fallback, "ym"] = pd.to_datetime(df.loc[fallback, "collected_at"], errors="coerce").dt.strftime("%Y-%m")
    else:
        df["ym"] = pd.to_datetime(df["collected_at"], errors="coerce").dt.strftime("%Y-%m")
    base = ANALYTICS_DB / "posfeed_sales_detail"

    for (brand, store, ym), part_df in df.groupby(["brand", "store", "ym"]):
        partition_path = base / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_order_item.csv"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        save_result = onedrive_csv_save(
            df=part_df.reset_index(drop=True),
            file_path=partition_path,
            pk_col="_pk",
            timestamp_col="collected_at",
            add_timestamp=False,
            if_exists="append",
        )
        logger.info(
            "저장: %s | inserted=%s duplicated=%s",
            partition_path,
            save_result.get("inserted"),
            save_result.get("duplicated"),
        )


# ============================================================
# Public Task 함수
# ============================================================
def _parse_collect_mode(collect_mode: str) -> tuple[str, list[str]]:
    """collect_mode를 파싱하여 (mode_type, date_list) 반환.
    mode_type: "yesterday" | "backfill_missing" | "date_range"
    date_list: date_range 모드일 때 YYYY-MM-DD 문자열 리스트
    """
    collect_mode = collect_mode or "yesterday"
    if collect_mode in ("yesterday", "backfill_missing"):
        return collect_mode, []

    # 날짜 범위: "2026-03-01~2026-03-02" 또는 단일 날짜: "2026-03-01"
    if "~" in collect_mode:
        start_str, end_str = collect_mode.split("~", 1)
        start = datetime.strptime(start_str.strip(), "%Y-%m-%d")
        end = datetime.strptime(end_str.strip(), "%Y-%m-%d")
    else:
        start = end = datetime.strptime(collect_mode.strip(), "%Y-%m-%d")

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return "date_range", dates


def _load_source_df(collect_mode: str) -> pd.DataFrame:
    """collect_mode에 따라 posfeed_sales 파티션 로드(파일명: posfeed_orders.csv)"""
    sales_root = ANALYTICS_DB / "posfeed_sales"
    mode_type, date_list = _parse_collect_mode(collect_mode)

    if mode_type == "yesterday":
        # posfeed_sales 전체에서 가장 최신 등록날짜 자동 감지
        try:
            df_all = read_analytics_partition(sales_root, filename="posfeed_orders.csv")
        except FileNotFoundError:
            raise AirflowSkipException("posfeed_sales 파티션 없음 (전체)")
        if df_all.empty or "등록날짜" not in df_all.columns:
            raise AirflowSkipException("posfeed_orders 데이터 없음 또는 등록날짜 컬럼 없음")
        data_date = df_all["등록날짜"].dropna().max()
        df = df_all[df_all["등록날짜"] == data_date]
        logger.info("모드: yesterday(최신일자) | data_date: %s | 건수: %d", data_date, len(df))

    elif mode_type == "backfill_missing":
        try:
            df = read_analytics_partition(sales_root, filename="posfeed_orders.csv")  # 전체 ym
        except FileNotFoundError:
            raise AirflowSkipException("posfeed_sales 파티션 없음 (전체)")
        logger.info("모드: backfill_missing | 전체 건수: %d", len(df))

    else:  # date_range
        yms = sorted({d[:7] for d in date_list})
        parts = []
        for ym in yms:
            try:
                part = read_analytics_partition(sales_root, ym=ym, filename="posfeed_orders.csv")
                parts.append(part)
            except FileNotFoundError:
                logger.warning("파티션 없음 | ym=%s", ym)
        if not parts:
            raise AirflowSkipException(f"posfeed_sales 파티션 없음 | {collect_mode}")
        df = pd.concat(parts, ignore_index=True)
        df = df[df["등록날짜"].isin(date_list)]
        logger.info("모드: %s | 날짜 %d개 | 건수: %d", collect_mode, len(date_list), len(df))
        if df.empty:
            raise AirflowSkipException(f"해당 날짜 데이터 없음 | {collect_mode}")

    return df


def _load_collected_set(ym_filter: str | None) -> set[str]:
    """이미 수집된 주문코드 set 반환. ym_filter=None이면 전체 스캔."""
    detail_base = ANALYTICS_DB / "posfeed_sales_detail"
    pattern = (
        f"brand=*/store=*/ym={ym_filter}/posfeed_order_item.csv"
        if ym_filter
        else "brand=*/store=*/ym=*/posfeed_order_item.csv"
    )
    collected: set[str] = set()
    for csv_path in detail_base.glob(pattern):
        try:
            existing = pd.read_csv(csv_path, usecols=["주문코드"], dtype=str)
            collected.update(existing["주문코드"].dropna().astype(str).tolist())
        except Exception:
            pass
    return collected


def extract_order_codes(collect_mode: str = "yesterday", force_rescrape: bool = False, **context) -> str:
    """collect_mode에 따라 수집 대상 주문 코드를 매장별로 그룹화하여 XCom에 저장"""
    # dag_run.conf로도 강제 재수집 가능
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("force_rescrape"):
        force_rescrape = True

    df = _load_source_df(collect_mode)

    df["주문 코드"] = df["주문 코드"].dropna().astype(str)

    # 모드별 기수집 코드 제외 범위 결정
    mode_type, date_list = _parse_collect_mode(collect_mode)
    if mode_type == "yesterday":
        # df에서 실제 감지된 최신 등록날짜의 ym 사용
        latest_date = df["등록날짜"].dropna().max()
        ym_filter = str(latest_date)[:7] if latest_date else None
    elif mode_type == "date_range" and len({d[:7] for d in date_list}) == 1:
        ym_filter = date_list[0][:7]  # 단일 ym이면 한정 스캔
    else:
        ym_filter = None  # 전체 스캔

    if force_rescrape:
        collected_set: set[str] = set()
        logger.info("force_rescrape=True: 기수집 필터 비활성화")
    else:
        collected_set = _load_collected_set(ym_filter)
        logger.info("기수집 코드: %d건", len(collected_set))

    # 날짜 → 매장 순으로 코드 그룹화 (기간 수집 시 순차 진행 가시성 강화)
    if "등록날짜" not in df.columns:
        df["등록날짜"] = ""
    df["등록날짜"] = df["등록날짜"].fillna("").astype(str)
    processing_dates = sorted([d for d in df["등록날짜"].unique().tolist() if d]) or [""]

    stores = []
    for date_key in processing_dates:
        day_df = df[df["등록날짜"] == date_key] if date_key else df
        if day_df.empty:
            continue
        logger.info("날짜 처리 시작: %s | 대상건수: %d", date_key or "(날짜없음)", len(day_df))

        for (brand, store), grp in day_df.groupby(["brand", "store"]):
            codes = [c for c in grp["주문 코드"].unique().tolist() if c not in collected_set]
            if codes:
                stores.append({
                    "date": date_key,
                    "brand": brand,
                    "store": store,
                    "codes": codes,
                })
            logger.info(
                "날짜: %s | 매장: %s | 전체: %d건 / 신규: %d건",
                date_key or "(날짜없음)",
                store,
                len(grp),
                len(codes),
            )

    # ── integrity_failed_yms XCom 처리 (check_monthly에서 무결성 실패 ym 전달) ──
    ti = context.get("ti")
    if ti:
        failed_yms = ti.xcom_pull(task_ids="check_monthly_collection", key="integrity_failed_yms") or []
        if failed_yms:
            logger.info("[무결성 재수집] 대상 ym=%s | 전체 스캔으로 누락 코드 추출", failed_yms)
            full_integrity_collected = set() if force_rescrape else _load_collected_set(None)
            sales_root = ANALYTICS_DB / "posfeed_sales"
            for ym in failed_yms:
                for csv_path in sales_root.glob(f"brand=*/store=*/ym={ym}/posfeed_orders.csv"):
                    parts_map = {p.split("=")[0]: p.split("=")[1] for p in csv_path.parts if "=" in p}
                    brand_val = parts_map.get("brand", "")
                    store_val = parts_map.get("store", "")
                    try:
                        df_ym = pd.read_csv(csv_path, dtype=str)
                        if "주문 코드" not in df_ym.columns:
                            continue
                        if "등록날짜" not in df_ym.columns:
                            df_ym["등록날짜"] = ""
                        df_ym["등록날짜"] = df_ym["등록날짜"].fillna("").astype(str)
                        for date_key, grp in df_ym.groupby("등록날짜"):
                            new_codes = [
                                c for c in grp["주문 코드"].dropna().astype(str).unique().tolist()
                                if c not in full_integrity_collected
                            ]
                            if not new_codes:
                                continue
                            existing = next(
                                (s for s in stores if s["date"] == date_key and s["brand"] == brand_val and s["store"] == store_val),
                                None,
                            )
                            if existing:
                                existing["codes"] = list(set(existing["codes"]) | set(new_codes))
                            else:
                                stores.append({"date": date_key, "brand": brand_val, "store": store_val, "codes": new_codes})
                            logger.info("[무결성 재수집] ym=%s %s/%s %s | 신규 코드 %d건", ym, brand_val, store_val, date_key, len(new_codes))
                    except Exception as exc:
                        logger.warning("[무결성 재수집] 파일 읽기 실패: %s | %s", csv_path, exc)

    # ── manual_backfill XCom 처리 (ingest_manual_files가 등록한 신규 매장 전체 기간 수집) ──
    ti = context.get("ti")
    if ti:
        manual_payload = ti.xcom_pull(task_ids="ingest_manual_files", key="manual_backfill") or {}
        backfill_items = manual_payload.get("backfill", [])
        if backfill_items:
            # detail이 없는 매장이므로 전체 ym 스캔으로 collected_set 재구성
            full_collected_set = set() if force_rescrape else _load_collected_set(None)
            sales_root = ANALYTICS_DB / "posfeed_sales"
            for item in backfill_items:
                brand, store = item["brand"], item["store"]
                from_ym = str(item["from"])[:7]
                to_ym = str(item["to"])[:7]
                # from_ym ~ to_ym 월 목록 생성
                yms: list[str] = []
                cur = datetime.strptime(from_ym, "%Y-%m")
                end_ym = datetime.strptime(to_ym, "%Y-%m")
                while cur <= end_ym:
                    yms.append(cur.strftime("%Y-%m"))
                    cur = cur.replace(month=cur.month + 1) if cur.month < 12 else cur.replace(year=cur.year + 1, month=1)
                logger.info("backfill 대상: %s/%s | ym %s ~ %s (%d개월)", brand, store, from_ym, to_ym, len(yms))
                for ym in yms:
                    part_path = sales_root / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "posfeed_orders.csv"
                    if not part_path.exists():
                        continue
                    try:
                        part_df = pd.read_csv(part_path, dtype=str)
                    except Exception as exc:
                        logger.warning("backfill 파티션 읽기 실패: %s | %s", part_path, exc)
                        continue
                    if "주문 코드" not in part_df.columns:
                        continue
                    if "등록날짜" not in part_df.columns:
                        part_df["등록날짜"] = ""
                    part_df["등록날짜"] = part_df["등록날짜"].fillna("").astype(str)
                    for date_key, grp in part_df.groupby("등록날짜"):
                        new_codes = [
                            c for c in grp["주문 코드"].dropna().astype(str).unique().tolist()
                            if c not in full_collected_set
                        ]
                        if not new_codes:
                            continue
                        existing = next(
                            (s for s in stores if s["date"] == date_key and s["brand"] == brand and s["store"] == store),
                            None,
                        )
                        if existing:
                            existing["codes"] = list(set(existing["codes"]) | set(new_codes))
                        else:
                            stores.append({"date": str(date_key), "brand": brand, "store": store, "codes": new_codes})
                        logger.info("backfill 추가: %s/%s | %s | %d건", brand, store, date_key, len(new_codes))

    total_new = sum(len(s["codes"]) for s in stores)
    logger.info("수집 대상: %d개 매장 / %d건 | 모드: %s", len(stores), total_new, collect_mode)

    if not stores:
        raise AirflowSkipException("수집할 신규 주문 코드 없음")

    context["ti"].xcom_push(
        key="order_codes",
        value={"stores": stores},
    )
    return f"[{collect_mode}] 신규 {total_new}건 / {len(stores)}개 매장 추출 완료"


def check_undetailed_orders(**context) -> str:
    """posfeed_sales 전체를 스캔해 detail이 없는 주문코드를 extract_order_codes XCom에 병합·재push."""
    sales_root = ANALYTICS_DB / "posfeed_sales"
    detail_root = ANALYTICS_DB / "posfeed_sales_detail"

    if not sales_root.exists():
        raise AirflowSkipException("posfeed_sales 파티션 없음")

    # detail 전체 수집 코드
    full_collected_set = _load_collected_set(None)
    logger.info("기수집 detail 코드: %d건", len(full_collected_set))

    # extract_order_codes XCom 읽기 (skipped이면 None)
    ti = context["ti"]
    prior = ti.xcom_pull(task_ids="extract_order_codes", key="order_codes") or {}
    stores: list[dict] = list(prior.get("stores", []))
    existing_keys = {(s["date"], s["brand"], s["store"]) for s in stores}

    added_codes = 0
    for brand_dir in sorted(sales_root.glob("brand=*")):
        brand = brand_dir.name[len("brand="):]
        for store_dir in sorted(brand_dir.glob("store=*")):
            store = store_dir.name[len("store="):]
            logger.info("매장 스캔: %s/%s", brand, store)
            for ym_dir in sorted(store_dir.glob("ym=*")):
                ym = ym_dir.name[len("ym="):]
                part_path = ym_dir / "posfeed_orders.csv"
                if not part_path.exists():
                    continue
                try:
                    part_df = pd.read_csv(part_path, dtype=str)
                except Exception as exc:
                    logger.warning("파티션 읽기 실패: %s | %s", part_path, exc)
                    continue
                if "주문 코드" not in part_df.columns:
                    continue
                if "등록날짜" not in part_df.columns:
                    part_df["등록날짜"] = ""
                part_df["등록날짜"] = part_df["등록날짜"].fillna("").astype(str)
                for date_key, grp in part_df.groupby("등록날짜"):
                    new_codes = [
                        c for c in grp["주문 코드"].dropna().astype(str).unique().tolist()
                        if c not in full_collected_set
                    ]
                    if not new_codes:
                        continue
                    key = (str(date_key), brand, store)
                    if key in existing_keys:
                        for s in stores:
                            if (s["date"], s["brand"], s["store"]) == key:
                                s["codes"] = list(set(s["codes"]) | set(new_codes))
                    else:
                        stores.append({"date": str(date_key), "brand": brand, "store": store, "codes": new_codes})
                        existing_keys.add(key)
                    added_codes += len(new_codes)
                    logger.info("추가: %s/%s/%s | %d건", brand, store, date_key, len(new_codes))

    total_new = sum(len(s["codes"]) for s in stores)
    if not stores:
        raise AirflowSkipException("수집할 신규 주문 코드 없음 (extract + cross-check 통합)")

    ti.xcom_push(key="order_codes", value={"stores": stores})
    return f"cross-check 완료 | detail 미보유 추가 {added_codes}건 | 총 {total_new}건 / {len(stores)}개 매장"


def scrape_order_details(**context) -> str:
    """Posfeed 주문 상세 페이지를 매장별 순차 크롤링하여 OneDrive에 저장"""
    payload = context["ti"].xcom_pull(task_ids="check_undetailed_orders", key="order_codes")
    if not payload:
        raise ValueError("check_undetailed_orders XCom 'order_codes' 값이 없습니다.")

    stores: list = payload["stores"]
    total_stores = len(stores)
    total_codes = sum(len(s["codes"]) for s in stores)
    logger.info("크롤링 시작 | %d개 매장 | 총 %d건", total_stores, total_codes)

    driver = _launch_browser()
    wait = WebDriverWait(driver, 15)

    total_success = 0
    total_failed = 0
    # 매장별 실패 코드 누적 (재시도용): {(brand, store, collect_date): [code, ...]}
    all_failed: dict[tuple[str, str, str], list[str]] = {}
    # 매장별 빈 상세(재수집 전용) 코드 누적
    all_empty: dict[tuple[str, str, str], list[str]] = {}
    # 전체 브라우저 재생성 횟수 — 너무 많으면 Airflow 재시도로 넘김
    _total_restarts = 0
    _MAX_TOTAL_RESTARTS = max(300, total_stores // _RESTART_DRIVER_EVERY_STORES + 50)
    _last_driver_restart_monotonic = time.monotonic()

    def _safe_restart(drv, wt, reason: str):
        nonlocal _total_restarts, _last_driver_restart_monotonic
        _total_restarts += 1
        if _total_restarts > _MAX_TOTAL_RESTARTS:
            raise RuntimeError(
                f"브라우저 재생성 {_MAX_TOTAL_RESTARTS}회 초과 ({reason}) — Airflow 재시도로 넘김"
            )
        logger.warning("브라우저 재생성 #%d | 사유: %s", _total_restarts, reason)
        new_driver, new_wait = _restart_driver_with_login(drv)
        _last_driver_restart_monotonic = time.monotonic()
        return new_driver, new_wait

    try:
        _login(driver, wait)

        for store_idx, store_info in enumerate(stores):
            collect_date = str(store_info.get("date", "") or "")
            brand = store_info["brand"]
            store = store_info["store"]
            codes = store_info["codes"]
            logger.info(
                "=== 날짜: %s | 매장 시작: %s | %d건 ===",
                collect_date or "(날짜없음)",
                store,
                len(codes),
            )

            results = []
            failed_codes = []
            consecutive_failures = 0

            for i, code in enumerate(codes):
                elapsed_since_restart = time.monotonic() - _last_driver_restart_monotonic
                if (
                    _RESTART_DRIVER_EVERY_SECONDS > 0
                    and elapsed_since_restart >= _RESTART_DRIVER_EVERY_SECONDS
                ):
                    logger.info(
                        "시간 기준 WebDriver 재시작 | 경과: %d초 | 매장: %s | %d/%d",
                        int(elapsed_since_restart),
                        store,
                        i,
                        len(codes),
                    )
                    driver, wait = _safe_restart(driver, wait, "elapsed_time")
                try:
                    non_empty_rows, empty_detected, driver, wait = _scrape_one_order_non_empty_rows(
                        driver, wait, code, max_attempts=3
                    )
                    if not non_empty_rows:
                        if empty_detected:
                            logger.warning("빈 상세 감지 | 코드: %s | 최종 재수집 대상 추가", code)
                            all_empty.setdefault((brand, store, collect_date), []).append(code)
                        else:
                            logger.warning("상세 수집 결과 없음 | 코드: %s", code)
                        failed_codes.append(code)
                        consecutive_failures += 1
                        continue

                    for row in non_empty_rows:
                        row["brand"] = brand
                        row["store"] = store
                        row["order_date"] = collect_date  # 주문 등록날짜 (ym 파티션 기준)
                    results.extend(non_empty_rows)
                    consecutive_failures = 0
                except Exception as e:
                    logger.warning("실패 | 코드: %s | %s", code, e)
                    failed_codes.append(code)
                    consecutive_failures += 1

                    # WebDriver 세션이 죽었으면 즉시 재생성 (consecutive 상관없이)
                    if isinstance(e, WebDriverException) or not _is_driver_alive(driver):
                        logger.warning("WebDriver 연결 끊김 감지 - 즉시 재생성")
                        try:
                            driver, wait = _safe_restart(driver, wait, "WebDriverException")
                            consecutive_failures = 0
                        except Exception as restart_err:
                            logger.error("즉시 재생성 실패: %s", restart_err)
                            consecutive_failures = 0  # tight loop 방지: 어차피 다음 코드도 실패할 것
                    elif consecutive_failures % 5 == 0:
                        # 5건마다 한 번씩만 재로그인 시도 (매 건마다 재시도 방지)
                        logger.warning("연속 %d건 실패 - 재로그인 시도", consecutive_failures)
                        try:
                            if not _is_driver_alive(driver):
                                driver, wait = _safe_restart(driver, wait, f"연속{consecutive_failures}실패")
                            else:
                                _login(driver, wait)
                            consecutive_failures = 0
                        except Exception as login_err:
                            logger.error("재로그인 실패: %s", login_err)
                            consecutive_failures = 0  # tight loop 방지

                # 100건마다 중간 저장
                if (i + 1) % 100 == 0 and results:
                    _append_to_partition(results)
                    logger.info("체크포인트 | %s | %d/%d", store, i + 1, len(codes))
                    results.clear()

            # 매장 완료 후 저장
            if results:
                _append_to_partition(results)

            success = len(codes) - len(failed_codes)
            total_success += success
            total_failed += len(failed_codes)
            done = store_idx + 1
            logger.info(
                "=== 날짜: %s | 매장 완료: %s | 성공: %d / 실패: %d | 진행: %d/%d (%.1f%%) ===",
                collect_date or "(날짜없음)",
                store,
                success,
                len(failed_codes),
                done,
                total_stores,
                done / total_stores * 100,
            )
            if failed_codes:
                logger.warning("실패 코드: %s", failed_codes[:10])
                all_failed[(brand, store, collect_date)] = failed_codes

            # 장시간 동작 안정성을 위해 주기적으로 driver 재시작
            if (
                _RESTART_DRIVER_EVERY_STORES > 0
                and done % _RESTART_DRIVER_EVERY_STORES == 0
                and done < total_stores
            ):
                logger.info("주기적 WebDriver 재시작: %d/%d 매장 처리 완료", done, total_stores)
                try:
                    driver, wait = _safe_restart(driver, wait, f"주기적재시작(매장{done})")
                except Exception as restart_err:
                    logger.warning("주기적 WebDriver 재시작 실패(계속 진행): %s", restart_err)

        # ── 전체 매장 완료 후 실패 코드 일괄 재시도 ──────────────────────────────
        if all_failed:
            retry_total = sum(len(v) for v in all_failed.values())
            logger.info("재시도 시작 | %d개 매장 / %d건", len(all_failed), retry_total)
            retry_success = 0
            retry_failed = 0
            for (brand, store, collect_date), codes in all_failed.items():
                results = []
                for code in codes:
                    try:
                        non_empty_rows, empty_detected, driver, wait = _scrape_one_order_non_empty_rows(
                            driver, wait, code, max_attempts=3
                        )
                        if not non_empty_rows:
                            retry_failed += 1
                            if empty_detected:
                                all_empty.setdefault((brand, store, collect_date), []).append(code)
                            continue

                        for row in non_empty_rows:
                            row["brand"] = brand
                            row["store"] = store
                            row["order_date"] = collect_date
                        results.extend(non_empty_rows)

                        retry_success += 1
                        total_success += 1
                        total_failed -= 1

                        key = (brand, store)
                        if key in all_empty:
                            all_empty[key] = [c for c in all_empty[key] if c != code]
                            if not all_empty[key]:
                                del all_empty[key]
                    except Exception as e:
                        logger.warning("재시도 실패 | %s | %s | %s", store, code, e)
                        retry_failed += 1
                if results:
                    _append_to_partition(results)
            logger.info("재시도 완료 | 성공: %d / 실패: %d", retry_success, retry_failed)

        # ── 마지막 단계: 빈 상세 행 전용 재수집 (항상 수행) ───────────────────────
        if all_empty:
            empty_total = sum(len(v) for v in all_empty.values())
            logger.info("빈 상세 재수집 시작 | %d개 매장 / %d건", len(all_empty), empty_total)
            empty_fixed = 0
            empty_still = 0

            for (brand, store, collect_date), codes in all_empty.items():
                results = []
                # 중복 제거 + 순서 유지
                unique_codes = list(dict.fromkeys(codes))

                for code in unique_codes:
                    non_empty_rows, _empty_detected, driver, wait = _scrape_one_order_non_empty_rows(
                        driver, wait, code, max_attempts=3
                    )
                    if non_empty_rows:
                        for row in non_empty_rows:
                            row["brand"] = brand
                            row["store"] = store
                            row["order_date"] = collect_date
                        results.extend(non_empty_rows)
                        empty_fixed += 1
                        total_success += 1
                        total_failed = max(0, total_failed - 1)
                    else:
                        empty_still += 1

                if results:
                    _append_to_partition(results)

            logger.info("빈 상세 재수집 완료 | 복구: %d / 미복구: %d", empty_fixed, empty_still)

    finally:
        try:
            driver.quit()
        except Exception as e:
            logger.warning("WebDriver 종료 중 예외(무시): %s", e)
        logger.info("WebDriver 종료")

    result = f"크롤링 완료 | 성공: {total_success}건 / 실패: {total_failed}건"
    logger.info(result)
    return result


def scrape_missing_order_details(collect_mode: str = "yesterday", **context) -> str:
    """scrape_order_details 이후 결과를 기준으로, extract_order_codes에서 추출된 코드 중
    미수집된 코드만 다시 수집해서 추가 저장한다."""
    payload = context["ti"].xcom_pull(task_ids="check_undetailed_orders", key="order_codes")
    if not payload:
        raise AirflowSkipException(
            "No XCom 'order_codes' from check_undetailed_orders (likely skipped: no missing codes)"
        )

    extracted_stores: list = payload["stores"]
    if not extracted_stores:
        raise AirflowSkipException("No missing codes (extract_order_codes stores is empty)")
    extracted_total = sum(len(s["codes"]) for s in extracted_stores)
    logger.info("미수집 재수집 체크 시작 | 추출 코드: %d건", extracted_total)

    # extract_order_codes와 동일한 ym_filter 계산 로직 유지
    df = _load_source_df(collect_mode)
    mode_type, date_list = _parse_collect_mode(collect_mode)
    if mode_type == "yesterday":
        latest_date = df["등록날짜"].dropna().max()
        ym_filter = str(latest_date)[:7] if latest_date else None
    elif mode_type == "date_range" and len({d[:7] for d in date_list}) == 1:
        ym_filter = date_list[0][:7]
    else:
        ym_filter = None

    payload_months = {
        str(store_info.get("date", "")).strip()[:7]
        for store_info in extracted_stores
        if str(store_info.get("date", "")).strip()
    }
    payload_months.discard("")
    if payload_months and (ym_filter is None or ym_filter not in payload_months):
        ym_filter = next(iter(payload_months)) if len(payload_months) == 1 else None

    collected_set = _load_collected_set(ym_filter)
    logger.info("현재까지 수집된 코드: %d건 (ym_filter=%s)", len(collected_set), ym_filter or "*")

    missing_stores = []
    missing_total = 0
    for store_info in extracted_stores:
        brand = store_info["brand"]
        store = store_info["store"]
        codes = store_info["codes"]
        missing_codes = [c for c in codes if c not in collected_set]
        if missing_codes:
            missing_total += len(missing_codes)
            missing_stores.append({
                "date": store_info.get("date", ""),
                "brand": brand,
                "store": store,
                "codes": missing_codes,
            })

    if not missing_stores:
        raise AirflowSkipException("미수집 코드 없음 (추출 코드 모두 수집 완료)")

    logger.warning("미수집 코드 재수집 시작 | 매장: %d개 | 코드: %d건", len(missing_stores), missing_total)

    class _TiShim:
        def __init__(self, shim_payload: dict):
            self._payload = shim_payload

        def xcom_pull(self, *args, **kwargs):
            return self._payload

    # 기존 scrape_order_details 로직을 그대로 재사용 (입력 stores만 missing으로 교체)
    return scrape_order_details(ti=_TiShim({"stores": missing_stores}))


def extract_today_order_codes(**context) -> str:
    """Today DAG의 sale_date 기준으로 주문 상세 대상 코드를 추출한다."""
    ti = context.get("ti")
    sale_date = ti.xcom_pull(task_ids="resolve_today", key="sale_date") if ti else None
    if not sale_date:
        conf = (getattr(context.get("dag_run"), "conf", None) or {})
        sale_date = conf.get("sale_date")
    if not sale_date:
        sale_date = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")

    return extract_order_codes(collect_mode=str(sale_date), **context)


def scrape_today_details(**context) -> str:
    """Today DAG의 extract_order_codes 결과만 상세 크롤링한다."""
    payload = context["ti"].xcom_pull(task_ids="extract_order_codes", key="order_codes")
    if not payload or not payload.get("stores"):
        raise AirflowSkipException("당일 신규 주문 코드 없음")

    class _TiShim:
        def __init__(self, shim_payload: dict):
            self._payload = shim_payload

        def xcom_pull(self, *args, **kwargs):
            return self._payload

    return scrape_order_details(ti=_TiShim(payload))
