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
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.analytics import read_analytics_partition
from modules.load.load_onedrive import onedrive_csv_save

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
POSFEED_ID = "siw2222@naver.com"
POSFEED_PW = "ehfl8877!!"
LOGIN_URL   = "https://admin.posfeed.co.kr/#/login?redirect=%2Fdashboard"
ORDER_DETAIL_URL = "https://admin.posfeed.co.kr/#/order/edit/{code}"
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None

# 수집 모드
# "yesterday"        : posfeed_sales 최신 등록날짜 기준 자동 감지 (기본, 매일 스케줄 실행)
# "backfill_missing" : posfeed_sales 전체 파티션 스캔 → 상세 누락 주문 전부 수집
# "2026-03-01"       : 특정 날짜 단건
# "2026-03-01~2026-03-02" : 날짜 범위
COLLECT_MODE = "yesterday"
# COLLECT_MODE = "2026-03-23"


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


def _launch_browser() -> uc.Chrome:
    """다운로드 설정 없는 경량 Chrome 브라우저 실행"""
    logger.info("브라우저 실행 (headless=%s)", HEADLESS_MODE)

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
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning("버전 불일치 → %d 으로 재시도", detected)
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            driver.set_window_size(1920, 1080)
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
    time.sleep(2)
    logger.info("Posfeed 로그인 완료 | URL: %s", driver.current_url)


# ============================================================
# 내부 유틸 - 크롤링
# ============================================================
def _clean_price(val: str) -> str:
    """'20,800원' → '20800'"""
    return re.sub(r"[^\d]", "", val)


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
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".el-table__body-wrapper")))
        time.sleep(1)  # Vue 재렌더링 완료 대기
    except TimeoutException:
        logger.warning("테이블 로드 타임아웃 | 코드: %s", order_code)
        return None

    # 상품 테이블만 추출: 수량(tds[1])이 정수인 행만 포함 (배달시간 테이블 행 혼입 방지)
    table_data = driver.execute_script("""
        const rows = document.querySelectorAll('.el-table__body tbody tr');
        return Array.from(rows).map(row => {
            const tds = row.querySelectorAll('td');
            return Array.from(tds).map(td => td.innerText.trim());
        });
    """) or []

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = []

    for tds in table_data:
        if len(tds) < 5:
            continue
        qty = tds[1].strip()
        if not qty.isdigit():  # 수량이 정수가 아니면 비상품 행 (배달시간 등) → skip
            continue
        name = tds[0]
        if name and name[0] in ('+', '-', '=', '@'):
            name = "'" + name  # Excel 수식 자동변환 방지
        result.append({
            "주문코드":  str(order_code),
            "상품명":   name,
            "수량":     qty,
            "단품가격": _clean_price(tds[2]),
            "합계":     _clean_price(tds[4]),
            "collected_at": collected_at,
        })

    # 상품 행이 없어도 주문코드 기록 보존
    if not result:
        result.append({
            "주문코드":  str(order_code),
            "상품명":   "",
            "수량":     "",
            "단품가격": "",
            "합계":     "",
            "collected_at": collected_at,
        })

    return result


def _append_to_partition(rows: list[dict]) -> None:
    """수집 결과를 brand/store/ym 파티션별로 분리하여 누적 저장.
    ym은 collected_at 기준으로 파생 (backfill 모드에서도 정확한 파티션 분류)."""
    df = pd.DataFrame(rows)
    # _pk는 Excel escape(') 제거 후 원본 텍스트 기준으로 생성
    df["_pk"] = df["주문코드"].astype(str) + "_" + df["상품명"].str.lstrip("'").astype(str)
    # ym: posfeed_sales의 등록날짜 기준이 없으므로 주문코드 수집 시각(collected_at) 기준 사용
    df["ym"] = pd.to_datetime(df["collected_at"], errors="coerce").dt.strftime("%Y-%m")
    base = ANALYTICS_DB / "posfeed_sales_detail"

    for (brand, store, ym), part_df in df.groupby(["brand", "store", "ym"]):
        partition_path = base / f"brand={brand}" / f"store={store}" / f"ym={ym}" / "data.csv"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        onedrive_csv_save(
            df=part_df.reset_index(drop=True),
            file_path=partition_path,
            pk_col="_pk",
            timestamp_col="collected_at",
            add_timestamp=False,
            if_exists="append",
        )
        logger.info("저장: %s | %d행", partition_path, len(part_df))


# ============================================================
# Public Task 함수
# ============================================================
def _parse_collect_mode() -> tuple[str, list[str]]:
    """COLLECT_MODE를 파싱하여 (mode_type, date_list) 반환.
    mode_type: "yesterday" | "backfill_missing" | "date_range"
    date_list: date_range 모드일 때 YYYY-MM-DD 문자열 리스트
    """
    if COLLECT_MODE in ("yesterday", "backfill_missing"):
        return COLLECT_MODE, []

    # 날짜 범위: "2026-03-01~2026-03-02" 또는 단일 날짜: "2026-03-01"
    if "~" in COLLECT_MODE:
        start_str, end_str = COLLECT_MODE.split("~", 1)
        start = datetime.strptime(start_str.strip(), "%Y-%m-%d")
        end = datetime.strptime(end_str.strip(), "%Y-%m-%d")
    else:
        start = end = datetime.strptime(COLLECT_MODE.strip(), "%Y-%m-%d")

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return "date_range", dates


def _load_source_df() -> pd.DataFrame:
    """COLLECT_MODE에 따라 posfeed_sales 파티션 로드"""
    sales_root = ANALYTICS_DB / "posfeed_sales"
    mode_type, date_list = _parse_collect_mode()

    if mode_type == "yesterday":
        # posfeed_sales 전체에서 가장 최신 등록날짜 자동 감지
        try:
            df_all = read_analytics_partition(sales_root)
        except FileNotFoundError:
            raise AirflowSkipException("posfeed_sales 파티션 없음 (전체)")
        if df_all.empty or "등록날짜" not in df_all.columns:
            raise AirflowSkipException("posfeed_sales 데이터 없음 또는 등록날짜 컬럼 없음")
        data_date = df_all["등록날짜"].dropna().max()
        df = df_all[df_all["등록날짜"] == data_date]
        logger.info("모드: yesterday(최신일자) | data_date: %s | 건수: %d", data_date, len(df))

    elif mode_type == "backfill_missing":
        try:
            df = read_analytics_partition(sales_root)  # 전체 ym
        except FileNotFoundError:
            raise AirflowSkipException("posfeed_sales 파티션 없음 (전체)")
        logger.info("모드: backfill_missing | 전체 건수: %d", len(df))

    else:  # date_range
        yms = sorted({d[:7] for d in date_list})
        parts = []
        for ym in yms:
            try:
                part = read_analytics_partition(sales_root, ym=ym)
                parts.append(part)
            except FileNotFoundError:
                logger.warning("파티션 없음 | ym=%s", ym)
        if not parts:
            raise AirflowSkipException(f"posfeed_sales 파티션 없음 | {COLLECT_MODE}")
        df = pd.concat(parts, ignore_index=True)
        df = df[df["등록날짜"].isin(date_list)]
        logger.info("모드: %s | 날짜 %d개 | 건수: %d", COLLECT_MODE, len(date_list), len(df))
        if df.empty:
            raise AirflowSkipException(f"해당 날짜 데이터 없음 | {COLLECT_MODE}")

    return df


def _load_collected_set(ym_filter: str | None) -> set[str]:
    """이미 수집된 주문코드 set 반환. ym_filter=None이면 전체 스캔."""
    detail_base = ANALYTICS_DB / "posfeed_sales_detail"
    pattern = f"brand=*/store=*/ym={ym_filter}/data.csv" if ym_filter else "brand=*/store=*/ym=*/data.csv"
    collected: set[str] = set()
    for csv_path in detail_base.glob(pattern):
        try:
            existing = pd.read_csv(csv_path, usecols=["주문코드"], dtype=str)
            collected.update(existing["주문코드"].tolist())
        except Exception:
            pass
    return collected


def extract_order_codes(**context) -> str:
    """COLLECT_MODE에 따라 수집 대상 주문 코드를 매장별로 그룹화하여 XCom에 저장"""
    df = _load_source_df()

    df["주문 코드"] = df["주문 코드"].dropna().astype(str)

    # 모드별 기수집 코드 제외 범위 결정
    mode_type, date_list = _parse_collect_mode()
    if mode_type == "yesterday":
        # df에서 실제 감지된 최신 등록날짜의 ym 사용
        latest_date = df["등록날짜"].dropna().max()
        ym_filter = str(latest_date)[:7] if latest_date else None
    elif mode_type == "date_range" and len({d[:7] for d in date_list}) == 1:
        ym_filter = date_list[0][:7]  # 단일 ym이면 한정 스캔
    else:
        ym_filter = None  # 전체 스캔

    collected_set = _load_collected_set(ym_filter)
    logger.info("기수집 코드: %d건", len(collected_set))

    # 매장별 코드 그룹화 (순차 처리용)
    stores = []
    for (brand, store), grp in df.groupby(["brand", "store"]):
        codes = [c for c in grp["주문 코드"].unique().tolist() if c not in collected_set]
        if codes:
            stores.append({"brand": brand, "store": store, "codes": codes})
        logger.info("매장: %s | 전체: %d건 / 신규: %d건", store, len(grp), len(codes))

    total_new = sum(len(s["codes"]) for s in stores)
    logger.info("수집 대상: %d개 매장 / %d건 | 모드: %s", len(stores), total_new, COLLECT_MODE)

    if not stores:
        raise AirflowSkipException("수집할 신규 주문 코드 없음")

    context["ti"].xcom_push(
        key="order_codes",
        value={"stores": stores},
    )
    return f"[{COLLECT_MODE}] 신규 {total_new}건 / {len(stores)}개 매장 추출 완료"


def scrape_order_details(**context) -> str:
    """Posfeed 주문 상세 페이지를 매장별 순차 크롤링하여 OneDrive에 저장"""
    payload = context["ti"].xcom_pull(task_ids="extract_order_codes", key="order_codes")
    if not payload:
        raise ValueError("extract_order_codes XCom 'order_codes' 값이 없습니다.")

    stores: list = payload["stores"]
    total_stores = len(stores)
    total_codes = sum(len(s["codes"]) for s in stores)
    logger.info("크롤링 시작 | %d개 매장 | 총 %d건 | 모드: %s", total_stores, total_codes, COLLECT_MODE)

    driver = _launch_browser()
    wait = WebDriverWait(driver, 15)

    total_success = 0
    total_failed = 0
    # 매장별 실패 코드 누적 (재시도용): {(brand, store): [code, ...]}
    all_failed: dict[tuple[str, str], list[str]] = {}

    try:
        _login(driver, wait)

        for store_idx, store_info in enumerate(stores):
            brand = store_info["brand"]
            store = store_info["store"]
            codes = store_info["codes"]
            logger.info("=== 매장 시작: %s | %d건 ===", store, len(codes))

            results = []
            failed_codes = []
            consecutive_failures = 0

            for i, code in enumerate(codes):
                try:
                    rows = _scrape_one_order(driver, wait, code)
                    if rows:
                        for row in rows:
                            row["brand"] = brand
                            row["store"] = store
                        results.extend(rows)
                    consecutive_failures = 0
                except Exception as e:
                    logger.warning("실패 | 코드: %s | %s", code, e)
                    failed_codes.append(code)
                    consecutive_failures += 1
                    if consecutive_failures >= 5:
                        logger.warning("연속 5건 실패 - 재로그인 시도")
                        try:
                            _login(driver, wait)
                            consecutive_failures = 0
                        except Exception as login_err:
                            logger.error("재로그인 실패: %s", login_err)

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
                "=== 매장 완료: %s | 성공: %d / 실패: %d | 진행: %d/%d (%.1f%%) ===",
                store, success, len(failed_codes), done, total_stores, done / total_stores * 100,
            )
            if failed_codes:
                logger.warning("실패 코드: %s", failed_codes[:10])
                all_failed[(brand, store)] = failed_codes

        # ── 전체 매장 완료 후 실패 코드 일괄 재시도 ──────────────────────────────
        if all_failed:
            retry_total = sum(len(v) for v in all_failed.values())
            logger.info("재시도 시작 | %d개 매장 / %d건", len(all_failed), retry_total)
            retry_success = 0
            retry_failed = 0
            for (brand, store), codes in all_failed.items():
                results = []
                for code in codes:
                    try:
                        rows = _scrape_one_order(driver, wait, code)
                        if rows:
                            for row in rows:
                                row["brand"] = brand
                                row["store"] = store
                            results.extend(rows)
                        retry_success += 1
                        total_success += 1
                        total_failed -= 1
                    except Exception as e:
                        logger.warning("재시도 실패 | %s | %s | %s", store, code, e)
                        retry_failed += 1
                if results:
                    _append_to_partition(results)
            logger.info("재시도 완료 | 성공: %d / 실패: %d", retry_success, retry_failed)

    finally:
        driver.quit()
        logger.info("WebDriver 종료")

    result = f"크롤링 완료 | 성공: {total_success}건 / 실패: {total_failed}건 | 모드: {COLLECT_MODE}"
    logger.info(result)
    return result
