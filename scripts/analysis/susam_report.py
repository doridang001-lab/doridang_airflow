"""수삼 일별 리포트를 CSV로 저장하고 Flow 게시글 댓글에 올린다."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT
from modules.transform.utility.paths import LOCAL_DB, MART_DB

logger = logging.getLogger(__name__)

OUTPUT_DIR = MART_DB / "Flow"
STATE_PATH = LOCAL_DB / "flow_susam_uploaded.json"

REQUIRED_COLUMNS = {"sale_date", "store", "item_name", "qty", "total_price"}
REPORT_COLUMNS = ("order_date", "store", "item_name", "qty", "총매출액")


def parse_iso_date(value: str) -> date:
    """엄격한 YYYY-MM-DD 문자열을 date로 변환한다."""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"올바르지 않은 날짜입니다: {value}") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError(f"날짜 형식은 YYYY-MM-DD여야 합니다: {value}")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="수삼 일별 CSV 생성 및 Flow 댓글 표 텍스트 업로드")
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date", type=parse_iso_date, help="처리할 단일 날짜")
    date_group.add_argument("--start", type=parse_iso_date, help="처리 시작일")
    parser.add_argument("--end", type=parse_iso_date, help="처리 종료일")
    parser.add_argument("--no-upload", action="store_true", help="CSV만 생성")
    parser.add_argument("--force", action="store_true", help="업로드 완료 날짜도 다시 처리")
    parser.add_argument("--post-url", help="Flow 고정 게시글 detail URL")
    parser.add_argument("--user-data-dir", help="로그인된 Chrome user-data-dir")
    parser.add_argument("--profile", help="로그인된 Chrome profile-directory")
    parser.add_argument("--debugger-address", help="이미 실행 중인 Chrome debuggerAddress")
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.start is not None and args.end is None:
        parser.error("--start를 사용하면 --end도 필요합니다.")
    if args.start is None and args.end is not None:
        parser.error("--end는 --start와 함께 사용해야 합니다.")
    if args.start is not None and args.start > args.end:
        parser.error("--start는 --end보다 늦을 수 없습니다.")


def target_dates(args: argparse.Namespace) -> list[date]:
    if args.date is not None:
        return [args.date]
    days = (args.end - args.start).days
    return [args.start + timedelta(days=offset) for offset in range(days + 1)]


def _normalize_numeric(series: pd.Series, column: str) -> pd.Series:
    source = series.astype("string").str.replace(",", "", regex=False).str.strip()
    normalized = pd.to_numeric(source, errors="coerce")
    invalid_count = int((source.notna() & source.ne("") & normalized.isna()).sum())
    if invalid_count:
        logger.warning("숫자 변환 실패값을 0으로 처리 | column=%s count=%d", column, invalid_count)
    return normalized.fillna(0)


def build_report_frame(date_str: str) -> pd.DataFrame:
    """해당 날짜의 수삼 매출을 매장·상품별로 집계한다."""
    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
    source_path = UNIFIED_ROOT / f"unified_sales_{parsed_date:%y%m%d}.parquet"
    if not source_path.exists():
        logger.error("unified_sales parquet 없음 | date=%s path=%s", date_str, source_path)
        raise FileNotFoundError(source_path)

    source = pd.read_parquet(source_path)
    missing = sorted(REQUIRED_COLUMNS.difference(source.columns))
    if missing:
        raise ValueError(f"unified_sales 필수 컬럼 누락: {', '.join(missing)}")

    working = source.loc[:, ["sale_date", "store", "item_name", "qty", "total_price"]].copy()
    working["order_date"] = pd.to_datetime(working["sale_date"], errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    invalid_date_count = int(working["order_date"].isna().sum())
    if invalid_date_count:
        raise ValueError(f"sale_date 변환 실패: {invalid_date_count}건")
    working = working.loc[working["order_date"].eq(date_str)].copy()
    working["store"] = working["store"].astype("string").fillna("미지정").replace("", "미지정")
    working["item_name"] = (
        working["item_name"].astype("string").fillna("미지정").replace("", "미지정")
    )
    working = working.loc[working["item_name"].str.contains("수삼", na=False)].copy()
    working["qty"] = _normalize_numeric(working["qty"], "qty")
    working["total_price"] = _normalize_numeric(working["total_price"], "total_price")
    report = (
        working.groupby(["order_date", "store", "item_name"], as_index=False, dropna=False)
        .agg(qty=("qty", "sum"), 총매출액=("total_price", "sum"))
        .sort_values(
            ["order_date", "store", "총매출액", "item_name"],
            ascending=[True, True, False, True],
        )
        .reset_index(drop=True)
    )
    report = report.loc[:, REPORT_COLUMNS]
    logger.info(
        "수삼 일별 상품 매출 집계 완료 | date=%s source_rows=%d report_rows=%d",
        date_str,
        len(working),
        len(report),
    )
    return report


def save_csv(report: pd.DataFrame, date_str: str) -> Path:
    """수삼 리포트를 Excel 호환 UTF-8 CSV로 저장한다."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"Susam_{date_str}.csv"
    report.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("수삼 일별 CSV 저장 완료 | date=%s rows=%d path=%s", date_str, len(report), output_path)
    return output_path


def format_comment(report: pd.DataFrame, date_str: str) -> str:
    """수삼 리포트를 Flow 댓글용 파이프 구분 텍스트로 변환한다."""
    if report.empty:
        return f"{date_str} 수삼 판매 내역 없음"
    lines = [" | ".join(REPORT_COLUMNS)]
    lines.extend(" | ".join(str(value) for value in row) for row in report.itertuples(index=False, name=None))
    return "\n".join(lines)


def load_uploaded_dates(path: Path | None = None) -> set[str]:
    path = STATE_PATH if path is None else path
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Flow 업로드 상태파일을 읽을 수 없습니다: {path}") from exc
    if not isinstance(payload, list) or not all(isinstance(value, str) for value in payload):
        raise RuntimeError(f"Flow 업로드 상태파일 형식이 올바르지 않습니다: {path}")
    return set(payload)


def save_uploaded_dates(dates: set[str], path: Path | None = None) -> None:
    path = STATE_PATH if path is None else path
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(sorted(dates), handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _validate_flow_url(post_url: str) -> None:
    parsed = urlparse(post_url)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or hostname not in {"flow.team", "www.myflow.kr"}:
        raise ValueError("Flow 게시글 URL은 flow.team 또는 www.myflow.kr의 완전한 URL이어야 합니다.")


def _create_driver(
    user_data_dir: str | None,
    profile: str | None,
    debugger_address: str | None = None,
):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    if debugger_address:
        options.add_experimental_option("debuggerAddress", debugger_address)
    else:
        options.add_argument(f"--user-data-dir={Path(user_data_dir).expanduser().resolve()}")
        options.add_argument(f"--profile-directory={profile}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver_path = (os.getenv("FLOW_CHROMEDRIVER_PATH") or "").strip()
    if driver_path:
        from selenium.webdriver.chrome.service import Service

        configured_path = Path(driver_path)
        if not configured_path.is_file():
            version_match = re.search(r"\d+\.\d+\.\d+\.\d+", driver_path)
            if version_match is None:
                raise FileNotFoundError(f"설정된 ChromeDriver가 존재하지 않습니다: {driver_path}")
            logger.warning(
                "설정된 ChromeDriver 없음, 동일 버전 복구 | version=%s path=%s",
                version_match.group(0),
                driver_path,
            )
            from webdriver_manager.chrome import ChromeDriverManager

            driver_path = ChromeDriverManager(driver_version=version_match.group(0)).install()
        return webdriver.Chrome(service=Service(driver_path), options=options)

    try:
        return webdriver.Chrome(options=options)
    except Exception as exc:
        logger.warning("Selenium 기본 드라이버 초기화 실패, webdriver_manager로 재시도 | error=%r", exc)
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def _find_comment_input(driver, wait):
    from selenium.webdriver.common.by import By

    selector = (
        "textarea.js-remark-input, textarea[class*='remark'], textarea[class*='comment'], "
        "[contenteditable='true'][class*='remark'], [contenteditable='true'][class*='comment']"
    )

    def locate(current_driver):
        for element in current_driver.find_elements(By.CSS_SELECTOR, selector):
            if element.is_displayed() and element.is_enabled():
                return element
        return False

    return wait.until(locate)


def _normalize_newlines(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _copy_to_browser_clipboard(driver, value: str) -> None:
    """원격 Chrome의 브라우저 클립보드에 댓글 본문을 기록한다."""
    parsed = urlparse(driver.current_url or "")
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    if origin:
        try:
            driver.execute_cdp_cmd(
                "Browser.grantPermissions",
                {
                    "origin": origin,
                    "permissions": ["clipboardReadWrite", "clipboardSanitizedWrite"],
                },
            )
        except Exception as exc:
            logger.debug("Chrome 클립보드 권한 사전 부여 실패, 페이지 API로 계속 | error=%r", exc)

    result = driver.execute_async_script(
        """
        const value = arguments[0];
        const done = arguments[arguments.length - 1];
        if (!navigator.clipboard || !navigator.clipboard.writeText) {
            done({ok: false, error: 'Clipboard API unavailable'});
            return;
        }
        navigator.clipboard.writeText(value)
            .then(() => done({ok: true}))
            .catch((error) => done({ok: false, error: String(error)}));
        """,
        value,
    )
    if not isinstance(result, dict) or result.get("ok") is not True:
        error = result.get("error") if isinstance(result, dict) else repr(result)
        raise RuntimeError(f"Flow 댓글 브라우저 클립보드 기록 실패: {error}")


def _set_comment_text(driver, element, value: str) -> None:
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait

    _set_comment_text_with_events(driver, element, "")
    try:
        driver.execute_script("arguments[0].focus();", element)
        _copy_to_browser_clipboard(driver, value)
        driver.execute_script("arguments[0].focus();", element)
        element.send_keys(Keys.CONTROL, "v")
        WebDriverWait(driver, 5, poll_frequency=0.2).until(
            lambda current: _comment_input_text(current, element) == _normalize_newlines(value)
        )
        logger.info("Flow 댓글 Ctrl+V 반영 완료 | chars=%d", len(value))
        return
    except Exception as exc:
        actual = _comment_input_text(driver, element)
        logger.warning(
            "Flow 댓글 Ctrl+V 미반영, DOM 입력으로 전환 | expected_chars=%d actual_chars=%d error=%r",
            len(value),
            len(actual),
            exc,
        )

    _set_comment_text_with_events(driver, element, value)


def _set_comment_text_with_events(driver, element, value: str) -> None:
    """원격 Chrome 키 입력이 막힐 때 DOM 기본 setter와 이벤트로 본문을 반영한다."""
    driver.execute_script(
        """
        const element = arguments[0];
        const value = arguments[1];
        const tag = (element.tagName || '').toLowerCase();
        if (tag === 'textarea') {
            Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')
                .set.call(element, value);
        } else if (tag === 'input') {
            Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')
                .set.call(element, value);
        } else {
            element.textContent = value;
        }
        element.dispatchEvent(new InputEvent('input', {
            bubbles: true,
            inputType: 'insertText',
            data: value
        }));
        element.dispatchEvent(new Event('change', {bubbles: true}));
        element.focus();
        """,
        element,
        value,
    )


def _comment_input_text(driver, element) -> str:
    value = driver.execute_script(
        """
        const element = arguments[0];
        const tag = (element.tagName || '').toLowerCase();
        if (tag === 'textarea' || tag === 'input') return element.value || '';
        return element.innerText || element.textContent || '';
        """,
        element,
    )
    return _normalize_newlines(str(value or ""))


def _wait_for_comment_text(driver, element, expected: str, timeout: int = 30) -> None:
    from selenium.webdriver.support.ui import WebDriverWait

    normalized_expected = _normalize_newlines(expected)
    started = time.monotonic()
    logger.info("Flow 댓글 붙여넣기 반영 대기 시작 | expected_chars=%d", len(normalized_expected))
    try:
        WebDriverWait(driver, timeout).until(
            lambda current: _comment_input_text(current, element) == normalized_expected
        )
    except Exception:
        actual = _comment_input_text(driver, element)
        logger.error(
            "Flow 댓글 붙여넣기 반영 타임아웃 | expected_chars=%d actual_chars=%d timeout=%d",
            len(normalized_expected),
            len(actual),
            timeout,
        )
        raise
    logger.info(
        "Flow 댓글 붙여넣기 반영 완료 | chars=%d elapsed=%.2fs",
        len(normalized_expected),
        time.monotonic() - started,
    )


def _matching_comment_count(driver, date_label: str) -> int:
    return int(
        driver.execute_script(
            """
            const selectors = [
                '.js-remark-item', '.remark-item', 'li[class*="remark"]',
                'li[class*="comment"]', 'div[class*="comment-item"]'
            ];
            let items = [];
            for (const selector of selectors) {
                items = Array.from(document.querySelectorAll(selector));
                if (items.length) break;
            }
            return items.filter((item) =>
                (item.innerText || item.textContent || '').includes(arguments[0])
            ).length;
            """,
            date_label,
        )
    )


def _submit_comment_with_enter(driver, comment_input) -> None:
    from selenium.webdriver.common.keys import Keys

    driver.execute_script("arguments[0].focus();", comment_input)
    comment_input.send_keys(Keys.ENTER)
    logger.info("Flow 댓글 등록 Enter 전송 완료")


def _submit_comment_with_dom_enter(driver, comment_input) -> None:
    """원격 Chrome 키 입력이 막힐 때 DOM 키 이벤트로 Enter를 전달한다."""
    driver.execute_script(
        """
        const element = arguments[0];
        element.focus();
        for (const type of ['keydown', 'keypress', 'keyup']) {
            const event = new KeyboardEvent(type, {
                key: 'Enter',
                code: 'Enter',
                bubbles: true,
                cancelable: true
            });
            Object.defineProperty(event, 'keyCode', {get: () => 13});
            Object.defineProperty(event, 'which', {get: () => 13});
            element.dispatchEvent(event);
        }
        """,
        comment_input,
    )
    logger.info("Flow 댓글 등록 DOM Enter 전송 완료")


def _new_comment_appeared(driver, date_label: str, before_count: int, timeout: int) -> bool:
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.support.ui import WebDriverWait

    try:
        WebDriverWait(driver, timeout).until(
            lambda current: _matching_comment_count(current, date_label) > before_count
        )
        return True
    except TimeoutException:
        return False


def upload_to_flow(
    comment_text: str,
    date_label: str,
    post_url: str,
    user_data_dir: str | None,
    profile: str | None,
    debugger_address: str | None = None,
) -> None:
    """Flow 고정 게시글에 표 텍스트 댓글을 올리고 실제 DOM 반영을 확인한다."""
    _validate_flow_url(post_url)
    if not debugger_address:
        if not user_data_dir or not Path(user_data_dir).expanduser().is_dir():
            raise ValueError(f"Chrome user-data-dir이 존재하지 않습니다: {user_data_dir}")
        if not str(profile or "").strip():
            raise ValueError("Chrome profile-directory가 비어 있습니다.")

    driver = None
    try:
        driver = _create_driver(user_data_dir, profile, debugger_address)
        _upload_with_driver(driver, comment_text, date_label, post_url)
    except Exception:
        logger.exception("Flow 댓글 업로드 실패 | date=%s", date_label)
        raise
    finally:
        if driver is not None:
            driver.quit()


def _upload_with_driver(driver, comment_text: str, date_label: str, post_url: str) -> None:
    """열려 있는 단일 브라우저 세션에서 댓글 하나를 등록한다."""
    from selenium.webdriver.support.ui import WebDriverWait

    driver.set_window_size(1920, 1080)
    driver.get(post_url)
    wait = WebDriverWait(driver, 30)
    comment_input = _find_comment_input(driver, wait)
    current_url = (driver.current_url or "").lower()
    if "login" in current_url or "signin" in current_url:
        raise RuntimeError("Flow 로그인 세션이 유효하지 않습니다.")

    before_count = _matching_comment_count(driver, date_label)
    _set_comment_text(driver, comment_input, comment_text)
    _wait_for_comment_text(driver, comment_input, comment_text, timeout=30)
    _submit_comment_with_enter(driver, comment_input)
    if not _new_comment_appeared(driver, date_label, before_count, timeout=5):
        if _comment_input_text(driver, comment_input) == "":
            wait.until(lambda current: _matching_comment_count(current, date_label) > before_count)
        else:
            logger.warning("Flow 댓글 일반 Enter 미반영, DOM Enter로 전환 | date=%s", date_label)
            _submit_comment_with_dom_enter(driver, comment_input)
            wait.until(lambda current: _matching_comment_count(current, date_label) > before_count)
    logger.info("Flow 댓글 업로드 완료 | date=%s chars=%d", date_label, len(comment_text))


def upload_many_to_flow(
    reports: Sequence[tuple[str, str]],
    post_url: str,
    user_data_dir: str | None,
    profile: str | None,
    debugger_address: str | None = None,
) -> tuple[list[str], list[str]]:
    """브라우저 하나로 여러 날짜의 댓글을 입력 순서대로 등록한다."""
    _validate_flow_url(post_url)
    if not debugger_address:
        if not user_data_dir or not Path(user_data_dir).expanduser().is_dir():
            raise ValueError(f"Chrome user-data-dir이 존재하지 않습니다: {user_data_dir}")
        if not str(profile or "").strip():
            raise ValueError("Chrome profile-directory가 비어 있습니다.")
    uploaded: list[str] = []
    failed: list[str] = []
    driver = None
    try:
        driver = _create_driver(user_data_dir, profile, debugger_address)
        for index, (comment_text, date_label) in enumerate(reports):
            try:
                _upload_with_driver(driver, comment_text, date_label, post_url)
                uploaded.append(date_label)
            except Exception:
                try:
                    driver.execute_script("return 1")
                    driver_alive = True
                except Exception:
                    driver_alive = False

                if driver_alive:
                    logger.exception("Flow 댓글 업로드 실패 | date=%s", date_label)
                    failed.append(date_label)
                else:
                    logger.warning("ChromeDriver 연결 종료 감지, 새 세션으로 1회 재시도 | date=%s", date_label)
                    driver = _create_driver(user_data_dir, profile, debugger_address)
                    try:
                        _upload_with_driver(driver, comment_text, date_label, post_url)
                        uploaded.append(date_label)
                    except Exception:
                        logger.exception("Flow 댓글 업로드 재시도 실패 | date=%s", date_label)
                        failed.append(date_label)
            if index < len(reports) - 1:
                time.sleep(4)
    finally:
        if driver is not None:
            driver.quit()
    return uploaded, failed


def _upload_config(args: argparse.Namespace) -> tuple[str, str | None, str | None, str | None]:
    post_url = (args.post_url or os.getenv("FLOW_POST_URL") or "").strip()
    user_data_dir = (args.user_data_dir or os.getenv("CHROME_USER_DATA_DIR") or "").strip()
    profile = (args.profile or os.getenv("CHROME_PROFILE") or "").strip()
    debugger_address = (
        args.debugger_address or os.getenv("FLOW_CHROME_DEBUGGER") or ""
    ).strip()
    missing = []
    if not post_url:
        missing.append("FLOW_POST_URL/--post-url")
    if not debugger_address:
        if not user_data_dir:
            missing.append("CHROME_USER_DATA_DIR/--user-data-dir")
        if not profile:
            missing.append("CHROME_PROFILE/--profile")
    if missing:
        raise ValueError(f"Flow 업로드 설정 누락: {', '.join(missing)}")
    return post_url, user_data_dir or None, profile or None, debugger_address or None


def main(argv: Sequence[str] | None = None) -> dict[str, list[str]]:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    dates = target_dates(args)
    upload_config = None if args.no_upload else _upload_config(args)
    uploaded_dates = load_uploaded_dates()
    result: dict[str, list[str]] = {
        "generated": [],
        "uploaded": [],
        "skipped": [],
        "failed": [],
    }

    pending_reports: list[tuple[str, str]] = []
    for target_date in dates:
        date_str = target_date.isoformat()
        if not args.force and date_str in uploaded_dates:
            logger.info("이미 업로드된 날짜 스킵 | date=%s", date_str)
            result["skipped"].append(date_str)
        else:
            try:
                report = build_report_frame(date_str)
                save_csv(report, date_str)
                comment_text = format_comment(report, date_str)
                result["generated"].append(date_str)
                if upload_config is not None:
                    pending_reports.append((comment_text, date_str))
            except Exception:
                logger.exception("수삼 리포트 날짜 처리 실패 | date=%s", date_str)
                result["failed"].append(date_str)

    if upload_config is not None and pending_reports:
        if len(pending_reports) == 1:
            comment_text, date_str = pending_reports[0]
            try:
                upload_to_flow(comment_text, date_str, *upload_config)
                uploaded = [date_str]
                failed = []
            except Exception:
                logger.exception("수삼 리포트 날짜 처리 실패 | date=%s", date_str)
                uploaded = []
                failed = [date_str]
        else:
            uploaded, failed = upload_many_to_flow(pending_reports, *upload_config)
        result["uploaded"].extend(uploaded)
        result["failed"].extend(failed)
        if uploaded:
            uploaded_dates.update(uploaded)
            save_uploaded_dates(uploaded_dates)

    logger.info("일별 상품 매출 리포트 실행 요약 | %s", result)
    return result


def _run_cli() -> int:
    try:
        result = main()
    except Exception:
        logger.exception("수삼 리포트 실행 준비 실패")
        return 1
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    sys.exit(_run_cli())
