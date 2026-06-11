from __future__ import annotations

import logging
import os
import random
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pendulum
import undetected_chromedriver as uc
from airflow.exceptions import AirflowException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR
from modules.transform.utility.selenium_uc import configure_uc_data_path

logger = logging.getLogger(__name__)

LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
VOC_URL = "https://ceo.toorder.co.kr/dashboard/review-status/review-voc-analysis"
DEFAULT_TOORDER_ID = "doridang15"
MAX_DATE_ATTEMPTS = 5
BACKOFF_SECONDS = (5, 10, 20, 20, 20)
DOWNLOAD_WAIT_SECONDS = 120


class StageError(RuntimeError):
    def __init__(self, stage: str, error: Exception | str):
        self.stage = stage
        self.original_error = error
        super().__init__(str(error))


def _log(account_id: str, target_date: Optional[str], stage: str, message: str) -> None:
    prefix = f"[TOORDER_VOC][{account_id}][{stage}]"
    if target_date:
        prefix = f"{prefix}[{target_date}]"
    logger.info("%s %s", prefix, message)


def _get_airflow_variable(key: str) -> str:
    try:
        from airflow.models import Variable
        value = Variable.get(key, default_var=None)
        return (value or "").strip()
    except Exception:
        return ""


def _resolve_toorder_id() -> str:
    for key in ("TOORDER_VOC_ACCOUNT_ID", "TOORDER_VOC_ID", "TOORDER_DELIVERY_ACCOUNT_ID", "TOORDER_DELIVERY_ID"):
        value = (os.getenv(key) or _get_airflow_variable(key)).strip()
        if value:
            return value
    return DEFAULT_TOORDER_ID


def _resolve_toorder_password(account_id: str) -> str:
    specific_key = f"TOORDER_PW_{str(account_id).upper()}"
    keys = (
        "TOORDER_VOC_ACCOUNT_PW",
        "TOORDER_VOC_PW",
        "TOORDER_DELIVERY_ACCOUNT_PW",
        "TOORDER_DELIVERY_PW",
        specific_key,
        "TOORDER_PW",
    )
    for key in keys:
        value = (os.getenv(key) or _get_airflow_variable(key)).strip()
        if value:
            return value
    return ""


def _resolve_account_df() -> pd.DataFrame:
    account_id = _resolve_toorder_id()
    password = _resolve_toorder_password(account_id)
    if not password:
        raise AirflowException(
            f"Missing ToOrder password for account `{account_id}`. "
            f"Set `TOORDER_PW_{account_id.upper()}` or `TOORDER_VOC_ACCOUNT_PW`."
        )
    return pd.DataFrame([{"channel": "toorder", "id": account_id, "pw": password}])


def _parse_date_str(date_str: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise AirflowException(f"`{field_name}` must be YYYY-MM-DD format: {date_str}") from exc


def _resolve_output_dir() -> Path:
    override = os.getenv("TOORDER_VOC_OUTPUT_DIR")
    if override:
        return Path(override)
    return ANALYTICS_DB / "toorder_review"



def _generate_date_list(lookback_days=None, conf: dict | None = None) -> tuple[str, list[str]]:
    """conf > lookback_days 순서로 수집 날짜 목록 결정."""
    conf = conf or {}
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    if start_date or end_date:
        if not start_date or not end_date:
            raise AirflowException("conf 수동 실행 시 start_date + end_date 모두 필요.")
        start_dt = _parse_date_str(str(start_date), "start_date")
        end_dt = _parse_date_str(str(end_date), "end_date")
        if start_dt > end_dt:
            raise AirflowException(f"start_date > end_date: {start_dt.date()} > {end_dt.date()}")
        mode = "manual_range"
    else:
        kst = pendulum.timezone("Asia/Seoul")
        end_dt = datetime.combine(pendulum.now(kst).subtract(days=1).date(), datetime.min.time())

        if lookback_days is None:
            start_dt = end_dt
        elif isinstance(lookback_days, int):
            start_dt = end_dt - timedelta(days=max(lookback_days, 1) - 1)
        elif "~" in str(lookback_days):
            s, e = str(lookback_days).split("~", 1)
            start_dt = _parse_date_str(s.strip(), "LOOKBACK_DAYS[start]")
            end_dt = _parse_date_str(e.strip(), "LOOKBACK_DAYS[end]")
        else:
            start_dt = _parse_date_str(str(lookback_days), "LOOKBACK_DAYS")
        mode = "scheduled"

    date_list: list[str] = []
    cursor = start_dt
    while cursor <= end_dt:
        date_list.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return mode, date_list


# ── 브라우저 유틸 ────────────────────────────────────────────────────────

def _get_chrome_version() -> Optional[int]:
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    try:
        result = subprocess.run([chrome_bin, "--version"], capture_output=True, text=True, timeout=5, check=False)
        match = re.search(r"(\d+)\.", result.stdout or "")
        if match:
            return int(match.group(1))
    except Exception:
        return None
    return None


def _launch_browser(download_dir: Path):
    options = uc.ChromeOptions()
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    if Path(chrome_bin).exists():
        options.binary_location = chrome_bin
    if os.getenv("AIRFLOW_HOME"):
        options.add_argument("--headless=new")
    for arg in ("--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"):
        options.add_argument(arg)
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    configure_uc_data_path()
    chrome_version = _get_chrome_version()
    if chrome_version:
        return uc.Chrome(options=options, version_main=chrome_version)
    return uc.Chrome(options=options)


def _human_type(element, text: str) -> None:
    element.clear()
    time.sleep(0.2)
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    time.sleep(0.3)


def _wait_for_login_form(driver, timeout: int = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready = driver.execute_script("return !!document.querySelector('input[name=\"id\"]');")
        if ready:
            return
        time.sleep(0.5)
    raise RuntimeError("login form did not load")


def _do_login(driver, account_id: str, password: str) -> None:
    _log(account_id, None, "login", "open login page")
    driver.get(LOGIN_URL)
    _wait_for_login_form(driver)

    wait = WebDriverWait(driver, 10)
    id_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']")))
    _human_type(id_input, account_id)

    pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
    _human_type(pw_input, password)

    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        driver.execute_script("arguments[0].click();", checkbox)
    except NoSuchElementException:
        pass

    submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    driver.execute_script("arguments[0].click();", submit_btn)
    time.sleep(3.0)

    current_url = driver.current_url
    if "/dashboard" not in current_url and "/auth" in current_url:
        raise RuntimeError(f"login failed: {current_url}")


def _navigate_to_voc_page(driver, account_id: str, target_date: str) -> None:
    _log(account_id, target_date, "navigate_voc", "open voc page")
    driver.get(VOC_URL)
    time.sleep(3.0)
    if "review-voc-analysis" not in driver.current_url:
        raise RuntimeError(f"unexpected voc url: {driver.current_url}")


def _set_date(driver, account_id: str, target_date: str) -> None:
    _log(account_id, target_date, "set_date", "input target date")
    short_date = target_date[2:]
    wait = WebDriverWait(driver, 10)
    date_input = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input.MuiInputBase-input[placeholder='YY-MM-DD']")
        )
    )
    driver.execute_script("arguments[0].click();", date_input)
    time.sleep(0.5)
    date_input.send_keys(Keys.CONTROL + "a")
    time.sleep(0.2)
    date_input.send_keys(Keys.DELETE)
    time.sleep(0.2)
    _human_type(date_input, short_date)
    date_input.send_keys(Keys.RETURN)
    time.sleep(1.5)


def _click_search(driver, account_id: str, target_date: str) -> None:
    _log(account_id, target_date, "click_search", "click search")
    selectors = [
        (By.CSS_SELECTOR, "div.css-i9gxme > button"),
        (By.CSS_SELECTOR, "button.css-slx3eq"),
        (By.XPATH, "//div[contains(@class,'css-i9gxme')]/button"),
    ]
    wait = WebDriverWait(driver, 10)
    search_button = None
    for by, selector in selectors:
        try:
            candidate = wait.until(EC.presence_of_element_located((by, selector)))
            if candidate.is_displayed():
                search_button = candidate
                break
        except Exception:
            continue
    if search_button is None:
        raise RuntimeError("search button not found")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", search_button)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", search_button)
    time.sleep(random.uniform(3.0, 5.0))


def _wait_for_download(driver, account_id: str, target_date: str, download_dir: Path) -> Path:
    _log(account_id, target_date, "download", "click export and wait for xlsx")
    before_files = set(download_dir.glob("*"))
    wait = WebDriverWait(driver, 10)
    export_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., '전체 토픽 내보내기')]"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", export_button)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", export_button)
    time.sleep(1.5)

    elapsed = 0
    while elapsed < DOWNLOAD_WAIT_SECONDS:
        time.sleep(1.0)
        elapsed += 1
        buttons = driver.find_elements(By.XPATH, "//button[contains(., '전체 토픽 내보내기')]")
        if buttons and buttons[0].get_attribute("disabled") is None:
            break

    time.sleep(2.0)
    after_files = set(download_dir.glob("*"))
    candidates = sorted(
        (
            path for path in (after_files - before_files)
            if path.is_file() and not path.name.endswith(".crdownload")
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise RuntimeError("downloaded file not found")
    return candidates[0]


def _read_voc_excel(excel_path: Path, account_id: str, target_date: str) -> pd.DataFrame:
    _log(account_id, target_date, "parse_excel", f"read {excel_path.name}")
    df = pd.read_excel(excel_path, header=6)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    if "secondary_category" not in df.columns:
        df["secondary_category"] = ""
    else:
        df["secondary_category"] = df["secondary_category"].fillna("")
    df["source_file"] = excel_path.name
    df["target_date"] = target_date
    df["collected_at"] = pendulum.now("Asia/Seoul").to_datetime_string()
    df["account_id"] = account_id
    return df


def _is_webdriver_connection_refused(exc: Exception) -> bool:
    message = str(exc) or ""
    needles = (
        "Failed to establish a new connection",
        "Connection refused",
        "Max retries exceeded with url",
        "HTTPConnectionPool(host='localhost'",
        "NewConnectionError",
    )
    return any(needle in message for needle in needles)


def _collect_account_session(
    account_id: str,
    password: str,
    dates_to_collect: list[str],
    attempt: int,
    temp_dir: Path,
) -> dict[str, Any]:
    """로그인 1회 후 dates_to_collect 전체 수집. 반환: {date -> df | Exception}"""
    shared_dl_dir = temp_dir / f"attempt_{attempt}"
    shared_dl_dir.mkdir(parents=True, exist_ok=True)
    driver = None
    results: dict[str, Any] = {}
    try:
        _log(account_id, None, "launch_browser", f"attempt={attempt} dates={len(dates_to_collect)}")
        try:
            driver = _launch_browser(shared_dl_dir)
        except Exception as exc:
            raise StageError("launch_browser", exc) from exc
        try:
            _do_login(driver, account_id, password)
        except Exception as exc:
            raise StageError("login", exc) from exc

        for target_date in dates_to_collect:
            try:
                _navigate_to_voc_page(driver, account_id, target_date)
                _set_date(driver, account_id, target_date)
                _click_search(driver, account_id, target_date)
                excel_path = _wait_for_download(driver, account_id, target_date, shared_dl_dir)
                df = _read_voc_excel(excel_path, account_id, target_date)
                excel_path.unlink(missing_ok=True)
                results[target_date] = df
            except Exception as exc:
                logger.warning("[TOORDER_VOC][%s][%s] date failed: %s", account_id, target_date, exc)
                results[target_date] = exc

        return results
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ── Task callables ────────────────────────────────────────────────────────

def t1_prepare(lookback_days=None, **context: Any) -> str:
    """날짜 계산 + 계정 확인 + 이미 저장된 날짜 skip → XCom."""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run and dag_run.conf else {}

    mode, date_list = _generate_date_list(lookback_days=lookback_days, conf=conf)
    account_df = _resolve_account_df()
    account_records = account_df.to_dict("records")

    output_dir = _resolve_output_dir()
    dates_to_collect = []
    skipped_dates = []
    for target_date in date_list:
        expected_path = output_dir / f"toorder_voc_{target_date.replace('-', '')}.parquet"
        if expected_path.exists():
            skipped_dates.append(target_date)
            logger.info("[TOORDER_VOC] skip %s — already saved", target_date)
        else:
            dates_to_collect.append(target_date)

    ti.xcom_push(key="date_list", value=date_list)
    ti.xcom_push(key="dates_to_collect", value=dates_to_collect)
    ti.xcom_push(key="skipped_dates", value=skipped_dates)
    ti.xcom_push(key="account_records", value=account_records)
    ti.xcom_push(key="output_dir", value=str(output_dir))
    ti.xcom_push(key="mode", value=mode)

    logger.info("[TOORDER_VOC] mode=%s total=%d skip=%d collect=%d", mode, len(date_list), len(skipped_dates), len(dates_to_collect))
    return f"mode={mode} total={len(date_list)} skip={len(skipped_dates)} collect={len(dates_to_collect)}"


def t2_collect(**context: Any) -> str:
    """브라우저 세션 — 로그인 1회 후 날짜별 다운로드·파싱 → TEMP_DIR 임시 parquet 저장."""
    ti = context["ti"]
    dates_to_collect: list[str] = ti.xcom_pull(task_ids="t1_prepare", key="dates_to_collect") or []
    account_records: list[dict] = ti.xcom_pull(task_ids="t1_prepare", key="account_records") or []

    if not dates_to_collect:
        logger.info("[TOORDER_VOC] 수집할 날짜 없음 — skip")
        ti.xcom_push(key="temp_parquet_map", value={})
        ti.xcom_push(key="failed_dates", value=[])
        return "수집 대상 없음"

    run_id = (context.get("run_id") or "manual").replace(":", "_").replace("+", "_")
    session_temp_dir = TEMP_DIR / "toorder_voc_downloads" / run_id
    session_temp_dir.mkdir(parents=True, exist_ok=True)

    temp_parquet_map: dict[str, str] = {}
    failed_dates: list[str] = []
    failure_details: list[dict] = []

    for account in account_records:
        account_id = str(account["id"])
        password = str(account["pw"])

        remaining = list(dates_to_collect)
        for attempt in range(1, MAX_DATE_ATTEMPTS + 1):
            if not remaining:
                break
            try:
                session_results = _collect_account_session(
                    account_id, password, remaining, attempt, session_temp_dir
                )
            except StageError as exc:
                detail = {"account_id": account_id, "attempt": attempt, "stage": exc.stage, "error": str(exc.original_error)}
                failure_details.append(detail)
                logger.warning("[TOORDER_VOC] session crashed: %s", detail)
                if attempt < MAX_DATE_ATTEMPTS:
                    time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])
                else:
                    failed_dates.extend(remaining)
                    remaining = []
                continue

            still_failed = []
            for target_date, result in session_results.items():
                if isinstance(result, Exception):
                    detail = {"account_id": account_id, "target_date": target_date, "attempt": attempt, "error": str(result)}
                    failure_details.append(detail)
                    still_failed.append(target_date)
                else:
                    # DataFrame → 임시 parquet 저장
                    tmp_path = session_temp_dir / f"tmp_{target_date.replace('-', '')}.parquet"
                    result.to_parquet(tmp_path, index=False)
                    temp_parquet_map[target_date] = str(tmp_path)

            remaining = still_failed
            if remaining and attempt < MAX_DATE_ATTEMPTS:
                logger.info("[TOORDER_VOC] retry %d dates (attempt %d+1)", len(remaining), attempt)
                time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])

        failed_dates.extend(remaining)

    ti.xcom_push(key="temp_parquet_map", value=temp_parquet_map)
    ti.xcom_push(key="failed_dates", value=failed_dates)
    ti.xcom_push(key="failure_details", value=failure_details)

    logger.info("[TOORDER_VOC] collected=%d failed=%d", len(temp_parquet_map), len(failed_dates))

    if failed_dates:
        raise AirflowException(f"수집 실패 날짜: {failed_dates}")

    return f"collected={len(temp_parquet_map)}"


def t3_save(**context: Any) -> str:
    """임시 parquet → 최종 output_dir 이동 + 정리."""
    ti = context["ti"]
    temp_parquet_map: dict[str, str] = ti.xcom_pull(task_ids="t2_collect", key="temp_parquet_map") or {}
    output_dir = Path(ti.xcom_pull(task_ids="t1_prepare", key="output_dir"))

    if not temp_parquet_map:
        logger.info("[TOORDER_VOC] 저장할 parquet 없음")
        ti.xcom_push(key="saved_parquet_paths", value=[])
        return "저장 대상 없음"

    output_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: list[str] = []

    for target_date, tmp_path_str in temp_parquet_map.items():
        tmp_path = Path(tmp_path_str)
        final_path = output_dir / f"toorder_voc_{target_date.replace('-', '')}.parquet"
        shutil.move(str(tmp_path), str(final_path))
        saved_paths.append(str(final_path))
        logger.info("[TOORDER_VOC] saved %s → %s", target_date, final_path)

    ti.xcom_push(key="saved_parquet_paths", value=saved_paths)
    ti.xcom_push(key="toorder_voc_paths", value=saved_paths)

    logger.info("[TOORDER_VOC] 최종 저장 완료: %d개", len(saved_paths))
    return f"saved={len(saved_paths)}"
