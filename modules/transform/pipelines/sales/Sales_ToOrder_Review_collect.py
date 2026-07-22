from __future__ import annotations

import logging
import os
import random
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pendulum
import undetected_chromedriver as uc
from airflow.exceptions import AirflowException, AirflowFailException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.account import get_default_account, get_pw
from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR
from modules.transform.utility.selenium_uc import launch_uc_chrome

logger = logging.getLogger(__name__)

LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
VOC_URL = "https://ceo.toorder.co.kr/dashboard/review-status/review-voc-analysis"
DEFAULT_TOORDER_ID, DEFAULT_TOORDER_PW = get_default_account("toorder")
MAX_DATE_ATTEMPTS = 5
BACKOFF_SECONDS = (5, 10, 20, 20, 20)
DOWNLOAD_WAIT_SECONDS = 120
DOWNLOAD_EXTENSIONS = (".xlsx", ".xls", ".csv")
DOWNLOAD_IN_PROGRESS_EXTENSIONS = (".crdownload", ".part", ".tmp")
TOPIC_REQUIRED_MESSAGE = "토픽을 선택해주세요."
MANUAL_REVIEW_DOWNLOAD_DIR = Path(
    os.getenv(
        "TOORDER_REVIEW_MANUAL_DIR",
        "/opt/airflow/manual_download" if os.getenv("AIRFLOW_HOME") else "E:/d_down",
    )
)


class StageError(RuntimeError):
    def __init__(self, stage: str, error: Exception | str):
        self.stage = stage
        self.original_error = error
        super().__init__(str(error))


class CredentialError(RuntimeError):
    pass


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
        if value and get_pw("toorder", value):
            return value
        if value:
            logger.warning("자동화 연결 대상이 아닌 ToOrder 계정 무시: key=%s account_id=%s", key, value)
    return DEFAULT_TOORDER_ID


def _resolve_toorder_password(account_id: str) -> str:
    account_pw = get_pw("toorder", account_id)
    if account_pw:
        return account_pw
    return ""


def _resolve_account_df() -> pd.DataFrame:
    account_id = _resolve_toorder_id()
    password = _resolve_toorder_password(account_id)
    if not password:
        raise AirflowException(
            f"Missing automation-linked ToOrder password for account `{account_id}`. "
            "`sales_employee.csv` must contain a 토더 row with 비고='자동화 연결'."
        )
    return pd.DataFrame([{"channel": "toorder", "id": account_id, "pw": password}])


def _parse_date_str(date_str: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise AirflowException(f"`{field_name}` must be YYYY-MM-DD format: {date_str}") from exc


def _parse_month_str(month_str: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(month_str, "%Y-%m")
    except ValueError as exc:
        raise AirflowException(f"`{field_name}` must be YYYY-MM format: {month_str}") from exc


def _add_months(dt: datetime, months: int) -> datetime:
    month_index = dt.year * 12 + (dt.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return datetime(year, month, 1)


def _month_end(dt: datetime) -> datetime:
    return _add_months(dt, 1) - timedelta(days=1)


def _select_order_date_series(df: pd.DataFrame) -> pd.Series | None:
    candidates = ("작성일자", "target_date")
    for column_name in candidates:
        if column_name in df.columns:
            return df[column_name].astype(str).str[:10]
    return None


REVIEW_LOSS_KEY_COLUMNS = ["작성일자", "매장명", "채널", "작성자", "별점", "주문메뉴", "리뷰내용"]


def _normalize_review_key_frame(df: pd.DataFrame, drop_duplicates: bool = True) -> pd.DataFrame:
    missing_columns = [column for column in REVIEW_LOSS_KEY_COLUMNS if column not in df.columns]
    if missing_columns:
        raise AirflowException(f"missing review key columns: {missing_columns}")

    out = df[REVIEW_LOSS_KEY_COLUMNS].copy()
    for column in REVIEW_LOSS_KEY_COLUMNS:
        out[column] = out[column].fillna("").astype(str).str.strip()
    out["작성일자"] = out["작성일자"].str[:10]
    star = pd.to_numeric(out["별점"], errors="coerce")
    out.loc[star.notna(), "별점"] = star[star.notna()].map(lambda value: f"{value:.1f}")
    if drop_duplicates:
        out = out.drop_duplicates()
    return out.reset_index(drop=True)


def _review_key_values(df: pd.DataFrame, drop_duplicates: bool = True) -> pd.Series:
    normalized = _normalize_review_key_frame(df, drop_duplicates=drop_duplicates)
    return normalized.astype(str).agg("\x1f".join, axis=1)


def _find_manual_review_lookup_file(start_date: str, end_date: str) -> Path | None:
    if not MANUAL_REVIEW_DOWNLOAD_DIR.exists():
        return None
    patterns = [
        f"리뷰조회{start_date}-{end_date}*.xlsx",
        f"리뷰조회{start_date}-{end_date}*.xls",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(MANUAL_REVIEW_DOWNLOAD_DIR.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _supplement_from_manual_review_lookup(
    df: pd.DataFrame,
    account_id: str,
    window: dict[str, str],
) -> pd.DataFrame:
    start_date = window["start_date"]
    end_date = window["end_date"]
    manual_path = _find_manual_review_lookup_file(start_date, end_date)
    if manual_path is None:
        logger.info("[TOORDER_VOC][supplement] manual 리뷰조회 file not found: %s~%s", start_date, end_date)
        return df

    manual_df = pd.read_excel(manual_path, header=6)
    manual_df = manual_df.dropna(how="all")
    manual_df = manual_df.dropna(axis=1, how="all")
    manual_df.columns = [str(column).strip() for column in manual_df.columns]
    missing_columns = [column for column in REVIEW_LOSS_KEY_COLUMNS if column not in manual_df.columns]
    if missing_columns:
        logger.warning("[TOORDER_VOC][supplement] skip %s: missing columns=%s", manual_path.name, missing_columns)
        return df

    manual_dates = manual_df["작성일자"].fillna("").astype(str).str[:10]
    manual_df = manual_df[manual_dates.between(start_date, end_date)].copy()
    manual_df = manual_df[manual_df["주문메뉴"].fillna("").astype(str).str.strip().ne("")].copy()
    if manual_df.empty:
        logger.info("[TOORDER_VOC][supplement] skip %s: no rows in range", manual_path.name)
        return df

    existing_key_set = set(_review_key_values(df))
    manual_row_keys = _review_key_values(manual_df, drop_duplicates=False)
    missing_rows = manual_df.loc[(~manual_row_keys.isin(existing_key_set)).to_numpy()].copy()
    if missing_rows.empty:
        logger.info("[TOORDER_VOC][supplement] %s: no missing review rows", manual_path.name)
        return df

    append_rows = pd.DataFrame(columns=df.columns)
    for column in df.columns:
        if column in missing_rows.columns:
            append_rows[column] = missing_rows[column]
        else:
            append_rows[column] = pd.NA

    append_rows["source_file"] = manual_path.name
    append_rows["target_month"] = window["month"]
    append_rows["collection_start_date"] = start_date
    append_rows["collection_end_date"] = end_date
    append_rows["collected_at"] = pendulum.now("Asia/Seoul").to_datetime_string()
    append_rows["account_id"] = account_id
    if "secondary_category" in append_rows.columns:
        append_rows["secondary_category"] = append_rows["secondary_category"].fillna("")

    logger.warning(
        "[TOORDER_VOC][supplement] %s: append missing review rows=%d",
        manual_path.name,
        len(append_rows),
    )
    return pd.concat([df, append_rows], ignore_index=True)


def _validate_same_month_no_review_loss(new_path: Path, existing_path: Path) -> dict[str, Any]:
    """같은 월 재수집 파일이 기존 리뷰 고유키를 잃으면 덮어쓰기를 중단한다."""
    if not existing_path.exists():
        return {
            "ok": True,
            "existing_review_count": 0,
            "new_review_count": 0,
            "missing_count": 0,
            "missing_samples": [],
        }

    existing_keys = _normalize_review_key_frame(pd.read_parquet(existing_path))
    new_keys = _normalize_review_key_frame(pd.read_parquet(new_path))

    merged = existing_keys.merge(new_keys, on=REVIEW_LOSS_KEY_COLUMNS, how="left", indicator=True)
    missing = merged[merged["_merge"] == "left_only"][REVIEW_LOSS_KEY_COLUMNS]
    missing_samples = missing.head(10).to_dict("records")
    return {
        "ok": missing.empty,
        "existing_review_count": int(len(existing_keys)),
        "new_review_count": int(len(new_keys)),
        "missing_count": int(len(missing)),
        "missing_samples": missing_samples,
    }


def _preserve_missing_existing_reviews(new_path: Path, existing_path: Path) -> dict[str, Any]:
    """같은 월 재수집본에서 빠진 기존 리뷰 행을 새 snapshot에 보존 병합한다."""
    if not existing_path.exists():
        return {
            "ok": True,
            "existing_review_count": 0,
            "new_review_count": 0,
            "missing_count": 0,
            "preserved_count": 0,
            "missing_samples": [],
        }

    existing_df = pd.read_parquet(existing_path)
    new_df = pd.read_parquet(new_path)
    existing_keys = _normalize_review_key_frame(existing_df)
    new_keys = _normalize_review_key_frame(new_df)

    merged = existing_keys.merge(new_keys, on=REVIEW_LOSS_KEY_COLUMNS, how="left", indicator=True)
    missing_keys = merged[merged["_merge"] == "left_only"][REVIEW_LOSS_KEY_COLUMNS]
    missing_samples = missing_keys.head(10).to_dict("records")
    if missing_keys.empty:
        return {
            "ok": True,
            "existing_review_count": int(len(existing_keys)),
            "new_review_count": int(len(new_keys)),
            "missing_count": 0,
            "preserved_count": 0,
            "missing_samples": [],
        }

    existing_key_rows = _normalize_review_key_frame(existing_df, drop_duplicates=False)
    existing_key_rows["__row_index"] = existing_df.index
    missing_existing_indices = existing_key_rows.merge(
        missing_keys,
        on=REVIEW_LOSS_KEY_COLUMNS,
        how="inner",
    )["__row_index"]
    missing_existing_rows = existing_df.loc[missing_existing_indices].copy()
    preserved_df = pd.concat([new_df, missing_existing_rows], ignore_index=True)
    preserved_df.to_parquet(new_path, index=False)
    return {
        "ok": True,
        "existing_review_count": int(len(existing_keys)),
        "new_review_count": int(len(new_keys)),
        "missing_count": int(len(missing_keys)),
        "preserved_count": int(len(missing_existing_rows)),
        "missing_samples": missing_samples,
    }


def _replace_parquet_from_temp(tmp_path: Path, final_path: Path) -> None:
    """서로 다른 mount 간에도 metadata 복사 권한 문제 없이 parquet를 교체한다."""
    incoming_path = final_path.with_name(f".{final_path.name}.incoming")
    try:
        shutil.copyfile(tmp_path, incoming_path)
        os.replace(incoming_path, final_path)
        tmp_path.unlink()
    finally:
        incoming_path.unlink(missing_ok=True)


def _validate_downloaded_range(
    df: pd.DataFrame,
    excel_path: Path,
    start_date: str,
    end_date: str,
) -> None:
    """요청 기간과 다른 범위의 엑셀을 내려받은 경우 저장 전에 실패시킨다."""
    filename_dates = re.findall(r"\d{4}-\d{2}-\d{2}", excel_path.name)
    if len(filename_dates) >= 2 and (filename_dates[0] != start_date or filename_dates[-1] != end_date):
        raise RuntimeError(
            f"download range mismatch: target={start_date}~{end_date}, file={excel_path.name}"
        )
    if len(filename_dates) == 1 and not (start_date <= filename_dates[0] <= end_date):
        raise RuntimeError(
            f"download date out of range: target={start_date}~{end_date}, file={excel_path.name}"
        )

    if "작성일자" not in df.columns:
        raise RuntimeError(f"missing 작성일자 column in {excel_path.name}")

    written_dates = pd.to_datetime(df["작성일자"], errors="coerce").dt.strftime("%Y-%m-%d").dropna()
    if written_dates.empty:
        raise RuntimeError(f"missing valid 작성일자 values in {excel_path.name}")
    min_written_date = written_dates.min()
    max_written_date = written_dates.max()
    if min_written_date < start_date or max_written_date > end_date:
        raise RuntimeError(
            f"download content out of range: target={start_date}~{end_date}, "
            f"min 작성일자={min_written_date}, max 작성일자={max_written_date}, file={excel_path.name}"
        )


def _validate_snapshot(new_path: Path, prev_path: Path, drop_tol: float = 0.05) -> dict[str, Any]:
    """Validate saved snapshot vs previous snapshot by per-date row count drop."""
    new_df = pd.read_parquet(new_path)
    new_date_series = _select_order_date_series(new_df)
    if new_date_series is None:
        raise AirflowException(f"missing date column in {new_path.name}")

    if not prev_path.exists():
        return {
            "ok": True,
            "suspicious": [],
            "total_new": int(len(new_df)),
            "total_prev": 0,
            "total_shrink": False,
        }

    prev_df = pd.read_parquet(prev_path)
    prev_date_series = _select_order_date_series(prev_df)
    if prev_date_series is None:
        raise AirflowException(f"missing date column in {prev_path.name}")

    new_counts = new_date_series.value_counts()
    prev_counts = prev_date_series.value_counts()

    suspicious: list[tuple[str, int, int]] = []
    for target_date in prev_counts.index.intersection(new_counts.index):
        prev_count = int(prev_counts[target_date])
        new_count = int(new_counts[target_date])
        if prev_count > 0 and new_count < prev_count * (1 - drop_tol):
            suspicious.append((target_date, prev_count, new_count))

    total_shrink = len(new_df) < len(prev_df)
    ok = not suspicious
    return {
        "ok": ok,
        "suspicious": suspicious,
        "total_new": int(len(new_df)),
        "total_prev": int(len(prev_df)),
        "total_shrink": total_shrink,
    }


def _resolve_output_dir() -> Path:
    override = os.getenv("TOORDER_VOC_OUTPUT_DIR")
    if override:
        return Path(override)
    return ANALYTICS_DB / "toorder_review"


def _generate_month_windows(lookback_months: int = 6, conf: dict | None = None) -> tuple[str, list[dict[str, str]]]:
    """conf > lookback_months 우선순위로 월 단위 수집 기간을 결정한다."""
    conf = conf or {}
    start_month = conf.get("start_month")
    end_month = conf.get("end_month")
    month = conf.get("month")
    kst = pendulum.timezone("Asia/Seoul")
    yesterday = datetime.combine(pendulum.now(kst).subtract(days=1).date(), datetime.min.time())
    current_month = datetime(yesterday.year, yesterday.month, 1)

    if month:
        start_dt = _parse_month_str(str(month), "month")
        end_dt = start_dt
        mode = "manual_month"
    elif start_month or end_month:
        if not start_month or not end_month:
            raise AirflowException("conf 수동 실행 시 start_month + end_month 모두 필요합니다.")
        start_dt = _parse_month_str(str(start_month), "start_month")
        end_dt = _parse_month_str(str(end_month), "end_month")
        if start_dt > end_dt:
            raise AirflowException(f"start_month > end_month: {start_dt:%Y-%m} > {end_dt:%Y-%m}")
        mode = "manual_month_range"
    else:
        months = max(int(lookback_months or 1), 1)
        start_dt = _add_months(current_month, -(months - 1))
        end_dt = current_month
        mode = "scheduled"

    month_windows: list[dict[str, str]] = []
    cursor = start_dt
    while cursor <= end_dt:
        window_start = cursor
        window_end = _month_end(cursor)
        if cursor.year == yesterday.year and cursor.month == yesterday.month:
            window_end = min(window_end, yesterday)
        if window_end >= window_start:
            month_windows.append(
                {
                    "month": cursor.strftime("%Y-%m"),
                    "start_date": window_start.strftime("%Y-%m-%d"),
                    "end_date": window_end.strftime("%Y-%m-%d"),
                }
            )
        cursor = _add_months(cursor, 1)
    return mode, month_windows


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


# 브라우저 유틸

def _launch_browser(download_dir: Path):
    # Host-mounted temp roots can make Chrome 148 tabs crash under UC.
    # Keep ToOrder's browser scratch space inside the container shm by default.
    os.environ.setdefault("AIRFLOW_CHROME_TMP_DIR", "/dev/shm/chrome_tmp")

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
    return launch_uc_chrome(
        options,
        account_id="toorder_voc",
        chrome_bin=chrome_bin,
        log_fn=lambda message: _log("toorder_voc", None, "browser", message),
    )


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


def _extract_login_error_text(driver) -> str:
    selectors = (
        "[role='alert']",
        ".MuiAlert-message",
        ".ant-message-notice-content",
        ".toast-message",
        ".error",
        ".text-red-500",
        ".text-danger",
    )
    for selector in selectors:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, selector):
                text = (el.text or el.get_attribute("textContent") or "").strip()
                if text:
                    return text
        except Exception:
            continue

    page = driver.page_source or ""
    if "아이디 또는 비밀번호" in page and "잘못" in page:
        return "아이디 또는 비밀번호가 잘못 입력 되었습니다."
    if "비밀번호가 잘못" in page or "잘못 입력" in page:
        return "아이디 또는 비밀번호 오류"
    return ""


def _wait_login_result(driver, timeout: int = 15) -> tuple[bool, str]:
    deadline = time.time() + timeout
    last_url = ""
    while time.time() < deadline:
        current_url = driver.current_url
        last_url = current_url
        if "/dashboard" in current_url or "/auth" not in current_url:
            return True, f"url={current_url}"

        try:
            has_login_input = bool(
                driver.execute_script(
                    "return !!document.querySelector('input[name=\"id\"], input[name=\"password\"]');"
                )
            )
        except Exception:
            has_login_input = True
        if not has_login_input:
            return True, f"login form hidden, url={current_url}"

        error_text = _extract_login_error_text(driver)
        if error_text:
            return False, f"login error: {error_text}"
        time.sleep(0.5)
    return False, f"timeout={timeout}s, last_url={last_url}"


def _save_voc_login_debug(driver, account_id: str, tag: str) -> str | None:
    try:
        debug_dir = TEMP_DIR / "toorder_voc_login_debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = debug_dir / f"{tag}_{ts}"
        png = base.with_suffix(".png")
        html = base.with_suffix(".html")
        driver.save_screenshot(str(png))
        html.write_text(driver.page_source or "", encoding="utf-8", errors="replace")
        logger.error("[TOORDER_VOC][%s] 로그인 실패 디버그 저장: %s", account_id, png)
        return str(png)
    except Exception as exc:
        logger.warning("[TOORDER_VOC][%s] 로그인 실패 디버그 저장 실패: %s", account_id, exc)
        return None


def _set_company_login(driver, account_id: str, is_company: bool) -> None:
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        try:
            checked = bool(driver.execute_script("return !!arguments[0].checked;", checkbox))
        except Exception:
            checked = checkbox.is_selected()
        if checked != is_company:
            driver.execute_script("arguments[0].click();", checkbox)
        _log(account_id, None, "login", f"isCompany={is_company}")
    except Exception:
        _log(account_id, None, "login", "isCompany checkbox not found")


def _do_login_once(driver, account_id: str, password: str, *, is_company: bool) -> None:
    _log(account_id, None, "login", "open login page")
    driver.get(LOGIN_URL)
    _wait_for_login_form(driver)

    wait = WebDriverWait(driver, 10)
    id_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']")))
    _human_type(id_input, account_id)

    pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
    _human_type(pw_input, password)

    _set_company_login(driver, account_id, is_company)

    try:
        pre_id = driver.execute_script("return document.querySelector(\"input[name='id']\").value;") or ""
        pre_pw = driver.execute_script("return document.querySelector(\"input[name='password']\").value;") or ""
        pre_company = driver.execute_script(
            "const el=document.querySelector(\"input[name='isCompany']\"); return el ? !!el.checked : null;"
        )
        _log(
            account_id,
            None,
            "login",
            f"submit ready: id={pre_id!r}, pw_len={len(pre_pw)}, isCompany={pre_company}",
        )
    except Exception as exc:
        _log(account_id, None, "login", f"submit ready diagnostic failed: {exc}")

    try:
        submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        driver.execute_script("arguments[0].click();", submit_btn)
    except Exception:
        pw_input.send_keys(Keys.RETURN)

    success, detail = _wait_login_result(driver, timeout=15)
    if success:
        _log(account_id, None, "login", f"success: {detail}")
        return

    current_url = driver.current_url
    debug_path = _save_voc_login_debug(driver, account_id, f"login_fail_company_{int(is_company)}")
    raise RuntimeError(
        "login failed: "
        f"account_id={account_id}, isCompany={is_company}, url={current_url}, "
        f"detail={detail}, debug={debug_path or 'unavailable'}"
    )


def _is_credential_login_error(message: str) -> bool:
    return (
        "아이디 또는 비밀번호" in message
        or "비밀번호가 잘못" in message
        or "잘못 입력" in message
        or "credential" in message.lower()
    )


def _do_login(driver, account_id: str, password: str) -> None:
    errors: list[str] = []
    for attempt, is_company in enumerate((True, False), start=1):
        try:
            _log(account_id, None, "login", f"attempt={attempt}/2 isCompany={is_company}")
            _do_login_once(driver, account_id, password, is_company=is_company)
            return
        except Exception as exc:
            message = str(exc)
            errors.append(message)
            _log(account_id, None, "login", f"attempt={attempt} failed: {message}")
            if attempt < 2:
                try:
                    driver.delete_all_cookies()
                except Exception:
                    pass
                time.sleep(1.0)

    combined = " | ".join(errors)
    if errors and all(_is_credential_login_error(message) for message in errors):
        raise CredentialError(combined)
    raise RuntimeError(combined)


def _navigate_to_voc_page(driver, account_id: str, target_date: str) -> None:
    _log(account_id, target_date, "navigate_voc", "open voc page")
    driver.get(VOC_URL)
    time.sleep(3.0)
    if "review-voc-analysis" not in driver.current_url:
        raise RuntimeError(f"unexpected voc url: {driver.current_url}")


def _set_date_range(driver, account_id: str, window: dict[str, str]) -> None:
    label = window["month"]
    start_date = window["start_date"]
    end_date = window["end_date"]
    _log(account_id, label, "set_date", f"input date range {start_date}~{end_date}")
    short_start = start_date[2:]
    short_end = end_date[2:]
    wait = WebDriverWait(driver, 10)
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input.MuiInputBase-input[placeholder='YY-MM-DD']")
        )
    )
    date_inputs = [
        el
        for el in driver.find_elements(By.CSS_SELECTOR, "input.MuiInputBase-input[placeholder='YY-MM-DD']")
        if el.is_displayed() and el.is_enabled()
    ]
    if len(date_inputs) < 2:
        raise RuntimeError(f"date range inputs not found: count={len(date_inputs)}")

    start_input = date_inputs[0]
    end_input = date_inputs[1]

    def _set_input_value(date_input, value: str) -> None:
        driver.execute_script("arguments[0].click();", date_input)
        time.sleep(0.3)
        date_input.send_keys(Keys.CONTROL + "a")
        time.sleep(0.1)
        date_input.send_keys(Keys.DELETE)
        time.sleep(0.1)
        date_input.send_keys(value)
        date_input.send_keys(Keys.RETURN)
        time.sleep(0.3)
        if value not in (date_input.get_attribute("value") or ""):
            driver.execute_script(
                """
                const el = arguments[0];
                const value = arguments[1];
                const setter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype,
                    'value'
                ).set;
                setter.call(el, value);
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                el.dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', bubbles: true}));
                el.dispatchEvent(new KeyboardEvent('keyup', {key: 'Enter', bubbles: true}));
                el.blur();
                """,
                date_input,
                value,
            )
            time.sleep(0.5)

    _set_input_value(start_input, short_start)
    _set_input_value(end_input, short_end)

    try:
        confirm_inputs = [
            el
            for el in driver.find_elements(By.CSS_SELECTOR, "input.MuiInputBase-inputSizeSmall")
            if el.is_displayed() and el.is_enabled()
        ]
        if confirm_inputs:
            driver.execute_script("arguments[0].click();", confirm_inputs[-1])
            time.sleep(0.2)
            confirm_inputs[-1].send_keys(Keys.RETURN)
            time.sleep(1.0)
    except Exception as exc:
        _log(account_id, label, "set_date", f"range confirm input skipped: {exc}")

    values = [el.get_attribute("value") for el in date_inputs]
    _log(account_id, label, "set_date", f"date input values={values}")
    if short_start not in (start_input.get_attribute("value") or "") or short_end not in (end_input.get_attribute("value") or ""):
        raise RuntimeError(
            f"date inputs did not accept target={short_start}~{short_end}; values={values}"
        )


def _contains_text(driver, message: str) -> bool:
    try:
        raw = (driver.page_source or "").replace(" ", "").replace("\n", "").replace("\r", "")
        normalized_message = message.replace(" ", "")
        return normalized_message in raw
    except Exception:
        return False


def _select_topic_row(driver, account_id: str) -> bool:
    """토픽 그리드에서 첫 토픽 행을 클릭해 선택을 시도."""
    row_selectors = [
        "div.MuiDataGrid-main [role='row'][data-id][aria-rowindex]",
        "div.MuiDataGrid-main div[role='row'][data-id]",
        "div.MuiDataGrid-virtualScroller [role='row'][data-id]",
        "[role='grid'] [role='row'][data-id]",
        "[role='rowgroup'] [role='row']",
        "[role='row'][aria-rowindex]",
        "[role='row']",
    ]
    seen = set()
    row_count = 0

    for selector in row_selectors:
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            continue

        for row in rows:
            row_key = row.id
            if row_key in seen:
                continue
            seen.add(row_key)
            row_count += 1
            try:
                if not row.is_displayed():
                    continue
            except Exception:
                continue

            row_id = (row.get_attribute("data-id") or "").strip()
            row_text = (row.text or "").strip()
            if row_id == "__total__":
                continue
            if TOPIC_REQUIRED_MESSAGE in row_text:
                continue
            if not row_text:
                continue

            targets = row.find_elements(
                By.CSS_SELECTOR,
                "button[role='checkbox'], [role='checkbox'], input[type='checkbox'], "
                "[aria-checked], .MuiCheckbox-root, div[role='cell']",
            )
            if targets:
                target = targets[0]
            else:
                cells = row.find_elements(By.CSS_SELECTOR, "div[role='cell']")
                if not cells:
                    target = row
                else:
                    target = cells[0]

            try:
                if not target.is_displayed():
                    continue
            except Exception:
                pass

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", target)
                _log(account_id, None, "topic", f"selected row id={row_id or '-'} text={row_text[:40]}")
                return True
            except Exception:
                continue

    _log(account_id, None, "topic", f"topic row candidate count={row_count}")
    return False


def _find_topic_grid_checkbox(driver) -> Any:
    checkbox_selectors = [
        "div.MuiDataGrid-main input[type='checkbox']",
        "div.MuiDataGrid-virtualScroller input[type='checkbox']",
        "input[type='checkbox'][aria-label*='선택']",
        "div.MuiDataGrid-main [role='checkbox']",
        "div.MuiDataGrid-virtualScroller [role='checkbox']",
        "[role='checkbox'][aria-checked]",
        ".MuiCheckbox-root",
    ]
    checkbox_count = 0
    for selector in checkbox_selectors:
        try:
            checks = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            continue
        for checkbox in checks:
            checkbox_count += 1
            try:
                if checkbox.is_displayed():
                    return checkbox
                parent = checkbox.find_element(By.XPATH, "./ancestor::*[self::label or @role='checkbox' or contains(@class,'MuiCheckbox-root')][1]")
                if parent.is_displayed():
                    return parent
            except Exception:
                continue
    logger.info("[TOORDER_VOC][topic] checkbox candidate count=%d", checkbox_count)
    return None


def _ensure_topic_selected(driver, account_id: str, target_date: str) -> None:
    if not _contains_text(driver, TOPIC_REQUIRED_MESSAGE):
        return

    _log(account_id, target_date, "topic", "topic required message detected")

    for wait_tick in range(10):
        if _select_topic_row(driver, account_id) or _find_topic_grid_checkbox(driver) is not None:
            break
        time.sleep(0.5)
        _log(account_id, target_date, "topic", f"wait topic candidates {wait_tick + 1}/10")

    for attempt in range(1, 5):
        _log(account_id, target_date, "topic", f"recover attempt {attempt}")

        if _select_topic_row(driver, account_id):
            # 선택 직후 그리드가 갱신되기까지 대기
            time.sleep(1.5)
            if not _contains_text(driver, TOPIC_REQUIRED_MESSAGE):
                _log(account_id, target_date, "topic", "topic selected")
                return

            # 토픽선택 뒤에도 메시지가 남아있으면 검색을 한 번 더 실행
            try:
                _click_search_button(driver)
            except Exception:
                pass

            time.sleep(1.5)
            if not _contains_text(driver, TOPIC_REQUIRED_MESSAGE):
                _log(account_id, target_date, "topic", "topic selected")
                return

        checkbox = _find_topic_grid_checkbox(driver)
        if checkbox is not None:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", checkbox)
                driver.execute_script("arguments[0].click();", checkbox)
                time.sleep(1.5)
                try:
                    _click_search_button(driver)
                except Exception:
                    pass
                time.sleep(1.5)
                if not _contains_text(driver, TOPIC_REQUIRED_MESSAGE):
                    _log(account_id, target_date, "topic", "topic checkbox selected")
                    return
            except Exception:
                pass

    raise RuntimeError("topic selection recovery failed")


def _click_search_button(driver) -> None:
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
    time.sleep(1.0)


def _click_search(driver, account_id: str, target_date: str) -> None:
    _log(account_id, target_date, "click_search", "click search")
    _click_search_button(driver)
    _ensure_topic_selected(driver, account_id, target_date)
    time.sleep(random.uniform(3.0, 5.0))


def _wait_for_download(driver, account_id: str, target_date: str, download_dir: Path) -> Path:
    _log(account_id, target_date, "download", "click export and wait for xlsx")
    _ensure_topic_selected(driver, account_id, target_date)
    start_ts = time.time()
    before_files = set(download_dir.glob("*"))

    def _normalize(text: str) -> str:
        return "".join((text or "").split())

    export_keywords = (
        "전체토픽내보내기",
        "전체데이터내보내기",
        "내보내기",
        "엑셀",
        "download",
        "export",
        "다운로드",
        "xlsx",
    )

    candidates = []
    for candidate in driver.find_elements(By.CSS_SELECTOR, "button"):
        if not candidate.is_displayed():
            continue
        raw_text = candidate.text or ""
        compact = _normalize(raw_text)
        if not compact:
            continue
        compact_lower = compact.lower()
        if any(keyword in compact for keyword in export_keywords):
            candidates.append((candidate, raw_text, 0))
            continue
        aria = (candidate.get_attribute("aria-label") or "").lower()
        title = (candidate.get_attribute("title") or "").lower()
        if any(keyword in aria for keyword in ("내보내기", "download", "export", "엑셀", "다운로드", "xlsx")):
            candidates.append((candidate, raw_text, 1))
            continue
        if any(keyword in title for keyword in ("내보내기", "download", "export", "엑셀", "다운로드", "xlsx")):
            candidates.append((candidate, raw_text, 1))

    export_button = None
    exact_match = None
    for candidate, raw_text, _prio in candidates:
        compact = _normalize(raw_text)
        if "토픽" in compact and "내보내기" in raw_text:
            exact_match = candidate
            break
    if exact_match is not None:
        export_button = exact_match
    elif candidates:
        sorted_candidates = sorted(candidates, key=lambda item: item[2])
        export_button = sorted_candidates[0][0]

    if export_button is None:
        raise RuntimeError("export button not found")

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", export_button)
    time.sleep(0.5)
    try:
        export_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", export_button)

    before_names = {path.name for path in before_files}
    deadline = time.time() + DOWNLOAD_WAIT_SECONDS
    downloaded_path: Path | None = None
    stable_ticks = 0
    last_size = None
    while time.time() < deadline:
        time.sleep(1.0)
        now_files = [path for path in download_dir.iterdir() if path.is_file()]
        in_progress = [
            path.name
            for path in now_files
            if any(path.name.endswith(ext) for ext in DOWNLOAD_IN_PROGRESS_EXTENSIONS)
        ]

        candidates = []
        for path in now_files:
            if path.name in before_names:
                continue
            if path.suffix.lower() not in DOWNLOAD_EXTENSIONS:
                continue
            if path.name.endswith(".crdownload"):
                continue
            if path.stat().st_mtime < start_ts - 1:
                continue
            candidates.append(path)

        if candidates:
            newest = sorted(candidates, key=lambda path: path.stat().st_mtime, reverse=True)[0]
            size = newest.stat().st_size
            if size > 0 and in_progress == []:
                if downloaded_path == newest and last_size == size:
                    stable_ticks += 1
                    if stable_ticks >= 3:
                        return newest
                else:
                    downloaded_path = newest
                    last_size = size
                    stable_ticks = 1
                continue
            downloaded_path = newest
            last_size = size
            stable_ticks = 0
            continue

        stable_ticks = 0
        downloaded_path = None

    context_files = [path.name for path in sorted(download_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)[:5]]
    raise RuntimeError(
        f"downloaded xlsx not found. seen in_progress={in_progress} latest_files={context_files}"
    )


def _read_voc_excel(excel_path: Path, account_id: str, window: dict[str, str]) -> pd.DataFrame:
    label = window["month"]
    start_date = window["start_date"]
    end_date = window["end_date"]
    _log(account_id, label, "parse_excel", f"read {excel_path.name}")
    df = pd.read_excel(excel_path, header=6)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    _validate_downloaded_range(df, excel_path, start_date, end_date)
    if "secondary_category" not in df.columns:
        df["secondary_category"] = ""
    else:
        df["secondary_category"] = df["secondary_category"].fillna("")
    df["source_file"] = excel_path.name
    df["target_month"] = label
    df["collection_start_date"] = start_date
    df["collection_end_date"] = end_date
    df["collected_at"] = pendulum.now("Asia/Seoul").to_datetime_string()
    df["account_id"] = account_id
    df = _supplement_from_manual_review_lookup(df, account_id, window)
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
    windows_to_collect: list[dict[str, str]],
    attempt: int,
    temp_dir: Path,
) -> dict[str, Any]:
    """단일 계정 기준 windows_to_collect 전체 수집. 반환: {month -> df | Exception}"""
    shared_dl_dir = temp_dir / f"attempt_{attempt}"
    shared_dl_dir.mkdir(parents=True, exist_ok=True)
    driver = None
    results: dict[str, Any] = {}
    try:
        _log(account_id, None, "launch_browser", f"attempt={attempt} months={len(windows_to_collect)}")
        try:
            driver = _launch_browser(shared_dl_dir)
        except Exception as exc:
            raise StageError("launch_browser", exc) from exc
        try:
            _do_login(driver, account_id, password)
        except CredentialError as exc:
            raise StageError("login_credential", exc) from exc
        except Exception as exc:
            raise StageError("login", exc) from exc

        for window in windows_to_collect:
            label = window["month"]
            try:
                _navigate_to_voc_page(driver, account_id, label)
                _set_date_range(driver, account_id, window)
                _click_search(driver, account_id, label)
                excel_path = _wait_for_download(driver, account_id, label, shared_dl_dir)
                df = _read_voc_excel(excel_path, account_id, window)
                excel_path.unlink(missing_ok=True)
                results[label] = df
            except Exception as exc:
                logger.warning("[TOORDER_VOC][%s][%s] month failed: %s", account_id, label, exc)
                debug_png = shared_dl_dir / f"{label}_attempt{attempt}.png"
                debug_html = shared_dl_dir / f"{label}_attempt{attempt}.html"
                try:
                    driver.save_screenshot(str(debug_png))
                    debug_html.write_text(driver.page_source, encoding="utf-8")
                except Exception:
                    pass
                results[label] = exc

        return results
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# Task callables

def t1_prepare(lookback_months: int = 6, **context: Any) -> str:
    """수집 대상 계산 + 스킵 목록 계산 + XCom에 수집 대상/스킵 정보를 저장."""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run and dag_run.conf else {}
    force_recollect = True if "force_recollect" not in conf else _truthy(conf.get("force_recollect"))

    mode, month_windows = _generate_month_windows(lookback_months=lookback_months, conf=conf)
    account_df = _resolve_account_df()
    account_records = account_df.to_dict("records")

    output_dir = _resolve_output_dir()
    windows_to_collect = []
    skipped_months = []
    for window in month_windows:
        month = window["month"]
        expected_path = output_dir / f"toorder_voc_{month.replace('-', '')}.parquet"
        if expected_path.exists() and not force_recollect:
            skipped_months.append(month)
            logger.info("[TOORDER_VOC] %s 이미 저장되어 있어 skip", month)
        else:
            windows_to_collect.append(window)

    ti.xcom_push(key="month_windows", value=month_windows)
    ti.xcom_push(key="windows_to_collect", value=windows_to_collect)
    ti.xcom_push(key="skipped_months", value=skipped_months)
    ti.xcom_push(key="account_records", value=account_records)
    ti.xcom_push(key="output_dir", value=str(output_dir))
    ti.xcom_push(key="mode", value=mode)
    ti.xcom_push(key="force_recollect", value=force_recollect)

    logger.info("[TOORDER_VOC] mode=%s total=%d skip=%d collect=%d force_recollect=%s", mode, len(month_windows), len(skipped_months), len(windows_to_collect), force_recollect)
    return f"mode={mode} total={len(month_windows)} skip={len(skipped_months)} collect={len(windows_to_collect)}"


def t2_collect(**context: Any) -> str:
    """실제 수집 월을 실행하고 TEMP_DIR에 parquet 임시 파일을 저장."""
    ti = context["ti"]
    windows_to_collect: list[dict[str, str]] = ti.xcom_pull(task_ids="t1_prepare", key="windows_to_collect") or []
    account_records: list[dict] = ti.xcom_pull(task_ids="t1_prepare", key="account_records") or []

    if not windows_to_collect:
        logger.info("[TOORDER_VOC] 수집 대상이 없어 skip")
        ti.xcom_push(key="temp_parquet_map", value={})
        ti.xcom_push(key="failed_months", value=[])
        return "수집 대상 없음"

    run_id = (context.get("run_id") or "manual").replace(":", "_").replace("+", "_")
    session_temp_dir = TEMP_DIR / "toorder_voc_downloads" / run_id
    session_temp_dir.mkdir(parents=True, exist_ok=True)

    temp_parquet_map: dict[str, str] = {}
    failed_months: list[str] = []
    failure_details: list[dict] = []
    credential_failure = False

    for account in account_records:
        account_id = str(account["id"])
        password = str(account["pw"])

        remaining = list(windows_to_collect)
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
                if exc.stage == "login_credential":
                    credential_failure = True
                    logger.error("[TOORDER_VOC] 자격증명 오류 -> 재시도 중단(계정 잠금 방지): %s", account_id)
                    failed_months.extend(window["month"] for window in remaining)
                    remaining = []
                    break
                if attempt < MAX_DATE_ATTEMPTS:
                    time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])
                else:
                    failed_months.extend(window["month"] for window in remaining)
                    remaining = []
                continue

            still_failed = []
            remaining_by_month = {window["month"]: window for window in remaining}
            for month, result in session_results.items():
                if isinstance(result, Exception):
                    detail = {"account_id": account_id, "month": month, "attempt": attempt, "error": str(result)}
                    failure_details.append(detail)
                    if month in remaining_by_month:
                        still_failed.append(remaining_by_month[month])
                else:
                    tmp_path = session_temp_dir / f"tmp_{month.replace('-', '')}.parquet"
                    result.to_parquet(tmp_path, index=False)
                    temp_parquet_map[month] = str(tmp_path)

            remaining = still_failed
            if remaining and attempt < MAX_DATE_ATTEMPTS:
                logger.info("[TOORDER_VOC] retry %d months (attempt %d+1)", len(remaining), attempt)
                time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])

        failed_months.extend(window["month"] for window in remaining)

    ti.xcom_push(key="temp_parquet_map", value=temp_parquet_map)
    ti.xcom_push(key="failed_months", value=failed_months)
    ti.xcom_push(key="failure_details", value=failure_details)

    logger.info("[TOORDER_VOC] collected=%d failed=%d", len(temp_parquet_map), len(failed_months))

    if failed_months:
        failure_preview = failure_details[-10:]
        logger.warning(
            "[TOORDER_VOC] 수집 실패 월 %d건: %s → 다음 실행(LOOKBACK_MONTHS)에서 자동 재시도",
            len(failed_months),
            failed_months,
        )
        if credential_failure:
            raise AirflowFailException(
                "[TOORDER_VOC] 자격증명 오류로 재시도 없이 실패 처리합니다. "
                f"failed_months={failed_months}; failure_details={failure_preview}"
            )
        raise AirflowException(
            "[TOORDER_VOC] 수집 실패 월이 남아 DAG를 실패 처리합니다. "
            f"failed_months={failed_months}; failure_details={failure_preview}"
        )

    return f"collected={len(temp_parquet_map)}"


def t3_save(**context: Any) -> str:
    """Move temp parquet to final output dir."""
    ti = context["ti"]
    temp_parquet_map: dict[str, str] = ti.xcom_pull(task_ids="t2_collect", key="temp_parquet_map") or {}
    output_dir = Path(ti.xcom_pull(task_ids="t1_prepare", key="output_dir"))

    if not temp_parquet_map:
        logger.info("[TOORDER_VOC] no parquet to save")
        ti.xcom_push(key="saved_parquet_paths", value=[])
        return "no saved parquet"

    output_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: list[str] = []

    for month, tmp_path_str in temp_parquet_map.items():
        tmp_path = Path(tmp_path_str)
        final_path = output_dir / f"toorder_voc_{month.replace('-', '')}.parquet"
        if not tmp_path.exists():
            if final_path.exists():
                logger.warning(
                    "[TOORDER_VOC] temp parquet missing but final exists; treating as already saved: %s => %s",
                    tmp_path,
                    final_path,
                )
                saved_paths.append(str(final_path))
                continue
            raise AirflowException(f"[TOORDER_VOC] temp parquet missing: {tmp_path}")

        same_month_validation = _preserve_missing_existing_reviews(tmp_path, final_path)
        if same_month_validation["missing_count"]:
            logger.warning(
                "[TOORDER_VOC][same_month_guard] %s preserved missing existing reviews: missing_count=%s preserved_count=%s samples=%s",
                month,
                same_month_validation["missing_count"],
                same_month_validation["preserved_count"],
                same_month_validation["missing_samples"],
            )
        _replace_parquet_from_temp(tmp_path, final_path)
        saved_paths.append(str(final_path))
        logger.info("[TOORDER_VOC] saved %s => %s", month, final_path)

    ti.xcom_push(key="saved_parquet_paths", value=saved_paths)
    ti.xcom_push(key="toorder_voc_paths", value=saved_paths)

    logger.info("[TOORDER_VOC] save done: %d files", len(saved_paths))
    return f"saved={len(saved_paths)}"


def t4_validate(**context: Any) -> str:
    """Validate snapshots after save by comparing each month with previous month snapshot."""
    ti = context["ti"]
    saved_parquet_paths: list[str] = ti.xcom_pull(task_ids="t3_save", key="saved_parquet_paths") or []
    if not saved_parquet_paths:
        logger.info("[TOORDER_VOC] no snapshots to validate")
        return "no snapshots to validate"

    output_dir = Path(ti.xcom_pull(task_ids="t1_prepare", key="output_dir") or str(_resolve_output_dir()))
    month_file_re = re.compile(r"^toorder_voc_(\d{6})\.parquet$")
    validation_results: list[dict[str, Any]] = []
    failed = 0

    for new_path_str in saved_parquet_paths:
        new_path = Path(new_path_str)
        match = month_file_re.match(new_path.name)
        if not match:
            message = f"invalid filename format: {new_path.name}"
            logger.warning("[TOORDER_VOC][validate] %s", message)
            failed += 1
            validation_results.append({
                "path": new_path_str,
                "ok": False,
                "suspicious": [],
                "total_new": 0,
                "total_prev": 0,
                "total_shrink": False,
                "error": message,
            })
            continue

        target_month = datetime.strptime(match.group(1), "%Y%m")
        prev_path = output_dir / f"toorder_voc_{_add_months(target_month, -1).strftime('%Y%m')}.parquet"
        result = _validate_snapshot(new_path, prev_path)
        result.update({
            "path": str(new_path),
            "prev_path": str(prev_path),
        })
        validation_results.append(result)
        if not result["ok"]:
            failed += 1
            logger.warning(
                "[TOORDER_VOC][validate] failed: %s, suspicious=%s",
                new_path.name,
                result["suspicious"],
            )

    ti.xcom_push(key="snapshot_validation", value=validation_results)
    if failed:
        raise AirflowException(f"[TOORDER_VOC][validate] {failed} snapshot validation failed")

    logger.info("[TOORDER_VOC] validate done: %d files", len(validation_results))
    return f"validated={len(validation_results)}"
