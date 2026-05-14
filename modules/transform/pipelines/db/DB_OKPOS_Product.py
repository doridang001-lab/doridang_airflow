"""
OKPOS product export download/save pipeline.
"""

import logging
import os
import shutil
import time
from pathlib import Path
from zipfile import BadZipFile, ZipFile, is_zipfile

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.pipelines.db.DB_OKPOS_Sales import (  # noqa: E402
    DOWNLOAD_TIMEOUT,
    WAIT_TIMEOUT,
    _launch_browser,
    _login,
    _setup_download_dir,
    _wait_for_download,
)
from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR

logger = logging.getLogger(__name__)

OKPOS_PRODUCT_URL = "https://my.okpos.co.kr/asp/base/prod/search"
OKPOS_PRODUCT_FILE_NAME = "상품조회.xlsx"
OKPOS_PRODUCT_PER_PAGE = "5000"


def _cleanup_product_downloads(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(OKPOS_PRODUCT_FILE_NAME).stem
    allowed_suffixes = {".xlsx", ".crdownload", ".part", ".tmp"}
    for path in download_dir.glob(f"{stem}*"):
        try:
            if path.is_file() and path.suffix.lower() in allowed_suffixes:
                path.unlink(missing_ok=True)
        except Exception:
            continue


def _okpos_product_filename_matches(path: Path) -> bool:
    return Path(OKPOS_PRODUCT_FILE_NAME).stem in path.stem


def _validate_okpos_product_xlsx(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"OKPOS product xlsx not found: {path}")
    if not is_zipfile(path):
        raise ValueError(f"OKPOS product file is not a real Excel workbook or is corrupted: {path}")

    try:
        with ZipFile(path) as workbook_zip:
            if "[Content_Types].xml" not in workbook_zip.namelist():
                raise ValueError(f"OKPOS product file is not a real Excel workbook or is corrupted: {path}")
    except BadZipFile as exc:
        raise ValueError(f"OKPOS product file is not a real Excel workbook or is corrupted: {path}") from exc


def download_okpos_product(**context) -> str:
    download_dir = TEMP_DIR / "okpos_product_download"
    _cleanup_product_downloads(download_dir)

    driver = _launch_browser(download_dir=download_dir)
    try:
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        _setup_download_dir(driver, download_dir)
        _login(driver, wait)

        logger.info("Open OKPOS product page: %s", OKPOS_PRODUCT_URL)
        driver.get(OKPOS_PRODUCT_URL)
        time.sleep(2)

        per_page_el = wait.until(EC.presence_of_element_located((By.ID, "perPage")))
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input',  {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            per_page_el,
            OKPOS_PRODUCT_PER_PAGE,
        )

        search_btn = wait.until(EC.element_to_be_clickable((By.ID, "search_send")))
        driver.execute_script("arguments[0].click();", search_btn)
        time.sleep(2)

        existing_files = {path for path in download_dir.iterdir() if path.is_file()}
        export_btn = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[contains(@onclick,'exportSheet') or contains(.,'엑셀다운')]",
                )
            )
        )
        driver.execute_script("arguments[0].click();", export_btn)

        downloaded = _wait_for_download(
            download_dir,
            existing_files,
            timeout=DOWNLOAD_TIMEOUT,
            expected_suffixes={".xlsx"},
            filename_predicate=_okpos_product_filename_matches,
        )
        if downloaded is None:
            expected = download_dir / OKPOS_PRODUCT_FILE_NAME
            if expected.exists() and _okpos_product_filename_matches(expected):
                downloaded = expected
            else:
                raise TimeoutException(
                    f"OKPOS product download timed out after {DOWNLOAD_TIMEOUT}s. "
                    f"download_dir={download_dir}"
                )

        context["ti"].xcom_push(key="downloaded_path", value=str(downloaded))
        logger.info("OKPOS product downloaded: downloaded_file=%s", downloaded.name)
        return f"downloaded: {downloaded}"
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def save_okpos_product(**context) -> str:
    downloaded_path = context["ti"].xcom_pull(task_ids="download_okpos_product", key="downloaded_path")
    if not downloaded_path:
        raise ValueError("download_okpos_product XCom(downloaded_path) is empty.")

    src = Path(str(downloaded_path))
    if not src.exists():
        raise FileNotFoundError(f"Downloaded product file not found: {src}")

    _validate_okpos_product_xlsx(src)

    dest_dir = ANALYTICS_DB / "okpos_product"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / OKPOS_PRODUCT_FILE_NAME
    tmp = dest_dir / f"{dest.name}.tmp"

    try:
        shutil.copy2(src, tmp)
        _validate_okpos_product_xlsx(tmp)
        os.replace(tmp, dest)
        try:
            src.unlink(missing_ok=True)
        except Exception:
            pass
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    context["ti"].xcom_push(key="saved_path", value=str(dest))
    logger.info(
        "OKPOS product saved: downloaded_file=%s | saved_file=%s",
        src.name,
        dest.name,
    )
    return f"saved: {dest}"
