"""미수백 일·주·월 리포트를 생성하고 Flow 하위업무 본문을 갱신한다."""

from __future__ import annotations

import argparse
import html
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
from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

OUTPUT_DIR = MART_DB / "Flow"
REQUIRED_COLUMNS = {"sale_date", "store", "item_name", "qty", "total_price"}
NORMALIZED_COLUMNS = ("order_date", "store", "item_name", "qty", "total_price")
REPORT_COLUMNS = ("order_date", "store", "item_name", "qty", "총매출액")
WEEKLY_COLUMNS = ("order_date", "store", "qty", "총매출액")
MONTHLY_COLUMNS = ("month_week", "order_date", "store", "qty", "총매출액")

MEMO_SENTINEL = "📝 이벤트·활동 기록 (수기 입력)"
MEMO_SENTINEL_HTML = f"<h2>{MEMO_SENTINEL}</h2>"
EMPTY_MEMO_HTML = "<p><br></p>"


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
    parser = argparse.ArgumentParser(
        description="미수백 일·주·월 CSV/HTML 생성 및 Flow 하위업무 본문 갱신"
    )
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date", type=parse_iso_date, help="처리할 단일 날짜")
    date_group.add_argument("--start", type=parse_iso_date, help="처리 시작일")
    parser.add_argument("--end", type=parse_iso_date, help="처리 종료일")
    parser.add_argument("--no-upload", action="store_true", help="CSV와 HTML만 생성")
    parser.add_argument(
        "--force",
        action="store_true",
        help="기존 하위업무도 다시 편집한다(기본 upsert 동작과 호환)",
    )
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


def _empty_frame(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_source(source: pd.DataFrame, *, date_str: str | None = None) -> pd.DataFrame:
    missing = sorted(REQUIRED_COLUMNS.difference(source.columns))
    if missing:
        raise ValueError(f"unified_sales 필수 컬럼 누락: {', '.join(missing)}")

    working = source.loc[:, ["sale_date", "store", "item_name", "qty", "total_price"]].copy()
    parsed_dates = pd.to_datetime(working["sale_date"], errors="coerce")
    invalid_date_count = int(parsed_dates.isna().sum())
    if invalid_date_count:
        raise ValueError(f"sale_date 변환 실패: {invalid_date_count}건")
    working["order_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    if date_str is not None:
        working = working.loc[working["order_date"].eq(date_str)].copy()

    working["store"] = working["store"].astype("string").fillna("미지정").replace("", "미지정")
    working["item_name"] = (
        working["item_name"].astype("string").fillna("미지정").replace("", "미지정")
    )
    working = working.loc[working["item_name"].str.contains("수삼", na=False)].copy()
    working["qty"] = _normalize_numeric(working["qty"], "qty")
    working["total_price"] = _normalize_numeric(working["total_price"], "total_price")
    return working.loc[:, NORMALIZED_COLUMNS].reset_index(drop=True)


def _source_path(date_str: str) -> Path:
    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
    return UNIFIED_ROOT / f"unified_sales_{parsed_date:%y%m%d}.parquet"


def build_daily_frame(date_str: str) -> pd.DataFrame:
    """해당 날짜의 수삼 매출을 매장·상품별로 집계한다."""
    source_path = _source_path(date_str)
    if not source_path.exists():
        logger.error("unified_sales parquet 없음 | date=%s path=%s", date_str, source_path)
        raise FileNotFoundError(source_path)

    working = _normalize_source(pd.read_parquet(source_path), date_str=date_str)
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


def load_susam_range(dates: list[str]) -> pd.DataFrame:
    """여러 날짜의 UnifiedSales에서 수삼 행을 읽어 정규화한다."""
    frames: list[pd.DataFrame] = []
    for date_str in dict.fromkeys(dates):
        source_path = _source_path(date_str)
        if ".bak_" in source_path.name or not source_path.is_file():
            logger.info("기간 수삼 집계 원천 건너뜀 | date=%s path=%s", date_str, source_path)
            continue
        normalized = _normalize_source(pd.read_parquet(source_path), date_str=date_str)
        frames.append(normalized)
    if not frames:
        return _empty_frame(NORMALIZED_COLUMNS)
    return pd.concat(frames, ignore_index=True).loc[:, NORMALIZED_COLUMNS]


def build_weekly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame(WEEKLY_COLUMNS)
    weekly = (
        frame.groupby(["order_date", "store"], as_index=False, dropna=False)
        .agg(qty=("qty", "sum"), 총매출액=("total_price", "sum"))
        .sort_values(["order_date", "총매출액", "store"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    return weekly.loc[:, WEEKLY_COLUMNS]


def _month_week_map(ym: str) -> dict[tuple[int, int], str]:
    month_start = datetime.strptime(f"{ym}-01", "%Y-%m-%d").date()
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    current = month_start
    iso_weeks: list[tuple[int, int]] = []
    while current < next_month:
        iso = current.isocalendar()
        key = (iso.year, iso.week)
        if key not in iso_weeks:
            iso_weeks.append(key)
        current += timedelta(days=1)
    return {key: f"W{index}" for index, key in enumerate(iso_weeks, start=1)}


def month_week_label(value: str | date | datetime) -> str:
    parsed = (
        datetime.strptime(value, "%Y-%m-%d").date()
        if isinstance(value, str)
        else value.date()
        if isinstance(value, datetime)
        else value
    )
    iso = parsed.isocalendar()
    return _month_week_map(parsed.strftime("%Y-%m"))[(iso.year, iso.week)]


def build_monthly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame(MONTHLY_COLUMNS)
    working = frame.copy()
    working["month_week"] = working["order_date"].map(month_week_label)
    monthly = (
        working.groupby(["month_week", "order_date", "store"], as_index=False, dropna=False)
        .agg(qty=("qty", "sum"), 총매출액=("total_price", "sum"))
        .sort_values(
            ["month_week", "order_date", "총매출액", "store"],
            ascending=[True, True, False, True],
        )
        .reset_index(drop=True)
    )
    return monthly.loc[:, MONTHLY_COLUMNS]


def save_csv(report: pd.DataFrame, date_str: str) -> Path:
    """수삼 리포트를 Excel 호환 UTF-8 CSV로 저장한다."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"Susam_{date_str}.csv"
    report.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("수삼 일별 CSV 저장 완료 | date=%s rows=%d path=%s", date_str, len(report), output_path)
    return output_path


def _format_number(value) -> str:
    number = float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}".rstrip("0").rstrip(".")


def _table_html(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    header_html = "".join(f"<th>{html.escape(str(value))}</th>" for value in headers)
    if not rows:
        body_html = f'<tr><td colspan="{len(headers)}">판매 내역 없음</td></tr>'
    else:
        body_html = "".join(
            "<tr>"
            + "".join(f"<td>{html.escape(str(value))}</td>" for value in row)
            + "</tr>"
            for row in rows
        )
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>"


def build_daily_body_html(date_str: str, frame: pd.DataFrame) -> str:
    total_qty = frame["qty"].sum() if not frame.empty else 0
    total_sales = frame["총매출액"].sum() if not frame.empty else 0
    rows = [
        (
            row.store,
            row.item_name,
            _format_number(row.qty),
            _format_number(row.총매출액),
        )
        for row in frame.itertuples(index=False)
    ]
    return "".join(
        [
            f"<h1>{html.escape(date_str)} 미수백 일별 판매</h1>",
            f"<p>총 수량: {_format_number(total_qty)} / 총 매출: {_format_number(total_sales)}원</p>",
            _table_html(("매장", "상품", "수량", "매출"), rows),
        ]
    )


def build_weekly_body_html(year: int, week: int, frame: pd.DataFrame) -> str:
    start = date.fromisocalendar(year, week, 1)
    end = date.fromisocalendar(year, week, 7)
    total_qty = frame["qty"].sum() if not frame.empty else 0
    total_sales = frame["총매출액"].sum() if not frame.empty else 0
    by_date = (
        frame.groupby("order_date", as_index=False)
        .agg(qty=("qty", "sum"), 총매출액=("총매출액", "sum"))
        .sort_values("order_date")
        if not frame.empty
        else _empty_frame(("order_date", "qty", "총매출액"))
    )
    by_store = (
        frame.groupby("store", as_index=False)
        .agg(qty=("qty", "sum"), 총매출액=("총매출액", "sum"))
        .sort_values(["총매출액", "store"], ascending=[False, True])
        if not frame.empty
        else _empty_frame(("store", "qty", "총매출액"))
    )
    date_rows = [
        (row.order_date, _format_number(row.qty), _format_number(row.총매출액))
        for row in by_date.itertuples(index=False)
    ]
    store_rows = [
        (row.store, _format_number(row.qty), _format_number(row.총매출액))
        for row in by_store.itertuples(index=False)
    ]
    return "".join(
        [
            f"<h1>{year}-W{week:02d} 미수백 주간 현황</h1>",
            f"<p>집계기간: {start.isoformat()} ~ {end.isoformat()}</p>",
            f"<p>총 수량: {_format_number(total_qty)} / 총 매출: {_format_number(total_sales)}원</p>",
            "<h2>날짜별 현황</h2>",
            _table_html(("날짜", "수량", "매출"), date_rows),
            "<h2>매장별 현황</h2>",
            _table_html(("매장", "수량", "매출"), store_rows),
            MEMO_SENTINEL_HTML,
        ]
    )


def build_monthly_body_html(ym: str, frame: pd.DataFrame) -> str:
    datetime.strptime(ym, "%Y-%m")
    total_qty = frame["qty"].sum() if not frame.empty else 0
    total_sales = frame["총매출액"].sum() if not frame.empty else 0
    by_week = (
        frame.groupby("month_week", as_index=False)
        .agg(qty=("qty", "sum"), 총매출액=("총매출액", "sum"))
        .assign(_order=lambda value: value["month_week"].str.removeprefix("W").astype(int))
        .sort_values("_order")
        .drop(columns="_order")
        if not frame.empty
        else _empty_frame(("month_week", "qty", "총매출액"))
    )
    by_date = (
        frame.groupby("order_date", as_index=False)
        .agg(qty=("qty", "sum"), 총매출액=("총매출액", "sum"))
        .sort_values(["총매출액", "order_date"], ascending=[False, True])
        if not frame.empty
        else _empty_frame(("order_date", "qty", "총매출액"))
    )
    by_store = (
        frame.groupby("store", as_index=False)
        .agg(qty=("qty", "sum"), 총매출액=("총매출액", "sum"))
        .sort_values(["총매출액", "store"], ascending=[False, True])
        if not frame.empty
        else _empty_frame(("store", "qty", "총매출액"))
    )
    week_rows = [
        (row.month_week, _format_number(row.qty), _format_number(row.총매출액))
        for row in by_week.itertuples(index=False)
    ]
    date_rows = [
        (row.order_date, _format_number(row.qty), _format_number(row.총매출액))
        for row in by_date.itertuples(index=False)
    ]
    store_rows = [
        (row.store, _format_number(row.qty), _format_number(row.총매출액))
        for row in by_store.itertuples(index=False)
    ]
    return "".join(
        [
            f"<h1>{html.escape(ym)} 미수백 월간 현황</h1>",
            f"<p>총 수량: {_format_number(total_qty)} / 총 매출: {_format_number(total_sales)}원</p>",
            "<h2>주차별 현황</h2>",
            _table_html(("주차", "수량", "매출"), week_rows),
            "<h2>날짜별 TOP</h2>",
            _table_html(("날짜", "수량", "매출"), date_rows),
            "<h2>매장별 TOP</h2>",
            _table_html(("매장", "수량", "매출"), store_rows),
            MEMO_SENTINEL_HTML,
        ]
    )


def merge_preserving_memo(
    existing_html: str,
    new_auto_html: str,
    sentinel: str = MEMO_SENTINEL,
) -> str:
    """기존 sentinel 뒤의 수기 HTML을 보존하고 자동 영역만 교체한다."""
    for match in re.finditer(r"<h2\b[^>]*>(.*?)</h2>", existing_html or "", flags=re.I | re.S):
        visible = re.sub(r"<[^>]+>", "", match.group(1))
        visible = " ".join(html.unescape(visible).split())
        if visible == " ".join(sentinel.split()):
            return new_auto_html + existing_html[match.end() :]
    return new_auto_html + EMPTY_MEMO_HTML


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


def _normalized_title(value: str) -> str:
    return " ".join(str(value or "").split())


def _find_subtask_by_title(driver, title: str):
    from selenium.webdriver.common.by import By

    expected = _normalized_title(title)
    for element in driver.find_elements(By.CSS_SELECTOR, "p.subtask__tit--display"):
        actual = driver.execute_script(
            "return arguments[0].innerText || arguments[0].textContent || '';",
            element,
        )
        if _normalized_title(actual) == expected:
            return element
    return None


def _add_subtask(driver, title: str):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(driver, 30)
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, ".subtask-list, .post-list, .task-list, .js-add-subtask-button")
        )
    )
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    add_button = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "button.js-add-subtask-button"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", add_button)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", add_button)
    time.sleep(1)
    subtask_input = wait.until(
        lambda current: next(
            (
                element
                for element in current.find_elements(By.CSS_SELECTOR, "input.js-subtask-input")
                if not element.get_attribute("readonly")
            ),
            None,
        )
    )
    driver.execute_script(
        """
        const element = arguments[0], title = arguments[1];
        element.scrollIntoView({block: 'center'});
        element.focus();
        element.value = title;
        element.dispatchEvent(new Event('input', {bubbles: true}));
        element.dispatchEvent(new Event('change', {bubbles: true}));
        for (const type of ['keydown', 'keypress', 'keyup']) {
            element.dispatchEvent(new KeyboardEvent(type, {
                key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true
            }));
        }
        """,
        subtask_input,
        title,
    )
    created = wait.until(lambda current: _find_subtask_by_title(current, title))
    logger.info("Flow 하위업무 생성 완료 | title=%s", title)
    return created


def _open_subtask_editor(driver, title: str) -> None:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(driver, 30)
    target = wait.until(lambda current: _find_subtask_by_title(current, title))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
    driver.execute_script("arguments[0].click();", target)
    time.sleep(2)
    setting_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.js-setting-button.set-btn"))
    )
    driver.execute_script("arguments[0].click();", setting_button)
    time.sleep(1)
    modify_item = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "li.js-setting-item[data-code='modify']"))
    )
    driver.execute_script("arguments[0].click();", modify_item)
    time.sleep(2)

    def editor_ready(current):
        try:
            if current.execute_script(
                """
                if (typeof CKEDITOR === 'undefined' || !CKEDITOR.instances) return false;
                const keys = Object.keys(CKEDITOR.instances);
                return keys.length > 0 && CKEDITOR.instances[keys[keys.length - 1]].status === 'ready';
                """
            ):
                return True
        except Exception:
            pass
        return bool(current.find_elements(By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame"))

    wait.until(editor_ready)
    logger.info("Flow 하위업무 편집기 열기 완료 | title=%s", title)


def _read_editor_html(driver) -> str:
    from selenium.webdriver.common.by import By

    value = driver.execute_script(
        """
        if (typeof CKEDITOR === 'undefined' || !CKEDITOR.instances) return null;
        const keys = Object.keys(CKEDITOR.instances);
        if (!keys.length) return null;
        return CKEDITOR.instances[keys[keys.length - 1]].getData();
        """
    )
    if value is not None:
        return str(value)

    iframe = driver.find_element(By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")
    driver.switch_to.frame(iframe)
    try:
        return str(driver.execute_script("return document.body.innerHTML || '';") or "")
    finally:
        driver.switch_to.default_content()


def _set_editor_html(driver, body_html: str) -> None:
    from selenium.webdriver.common.by import By

    driver.set_script_timeout(30)
    updated = driver.execute_async_script(
        """
        const body = arguments[0];
        const done = arguments[arguments.length - 1];
        if (typeof CKEDITOR === 'undefined' || !CKEDITOR.instances) {
            done(false);
            return;
        }
        const keys = Object.keys(CKEDITOR.instances);
        if (!keys.length) {
            done(false);
            return;
        }
        const editor = CKEDITOR.instances[keys[keys.length - 1]];
        let completed = false;
        const finish = (value) => {
            if (completed) return;
            completed = true;
            done(value);
        };
        const timer = setTimeout(() => finish(false), 20000);
        try {
            editor.focus();
            editor.execCommand('selectAll');
            editor.insertHtml(body);
            editor.fire('change');
            editor.updateElement();
            setTimeout(() => {
                clearTimeout(timer);
                finish(true);
            }, 100);
        } catch (error) {
            clearTimeout(timer);
            finish(false);
        }
        """,
        body_html,
    )
    if updated:
        return

    iframe = driver.find_element(By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")
    driver.switch_to.frame(iframe)
    try:
        driver.execute_script(
            """
            const body = document.body;
            body.focus();
            const selection = window.getSelection();
            const range = document.createRange();
            range.selectNodeContents(body);
            selection.removeAllRanges();
            selection.addRange(range);
            const inserted = document.execCommand('insertHTML', false, arguments[0]);
            if (!inserted) body.innerHTML = arguments[0];
            body.dispatchEvent(new Event('input', {bubbles: true}));
            body.dispatchEvent(new Event('change', {bubbles: true}));
            """,
            body_html,
        )
    finally:
        driver.switch_to.default_content()


def _submit_editor(driver) -> None:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(driver, 30)
    submit_button = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "button.js-complete-btn.confirm"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_button)
    driver.execute_script("arguments[0].click();", submit_button)
    wait.until(
        lambda current: not any(
            element.is_displayed()
            for element in current.find_elements(
                By.CSS_SELECTOR, "button.js-complete-btn.confirm"
            )
        )
    )


def upsert_subtask(
    driver,
    title: str,
    auto_html: str,
    *,
    preserve_memo: bool,
) -> None:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    WebDriverWait(driver, 30).until(
        lambda current: bool(
            current.find_elements(By.CSS_SELECTOR, "button.js-add-subtask-button")
        )
    )
    existing = _find_subtask_by_title(driver, title)
    action = "수정" if existing is not None else "추가"
    if existing is None:
        _add_subtask(driver, title)
    _open_subtask_editor(driver, title)
    body_html = auto_html
    if preserve_memo:
        body_html = merge_preserving_memo(_read_editor_html(driver), auto_html)
    _set_editor_html(driver, body_html)
    _submit_editor(driver)
    logger.info(
        "Flow 하위업무 본문 upsert 완료 | action=%s title=%s replace=select_all "
        "preserve_memo=%s chars=%d",
        action,
        title,
        preserve_memo,
        len(body_html),
    )


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


def _date_range(start: date, end: date) -> list[str]:
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _week_dates(anchor: date) -> list[str]:
    start = anchor - timedelta(days=anchor.weekday())
    return _date_range(start, start + timedelta(days=6))


def _month_dates(anchor: date) -> list[str]:
    start = anchor.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return _date_range(start, next_month - timedelta(days=1))


def _is_driver_alive(driver) -> bool:
    try:
        driver.execute_script("return 1")
        return True
    except Exception:
        return False


def main(argv: Sequence[str] | None = None) -> dict[str, list[str]]:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    dates = target_dates(args)
    upload_config = None if args.no_upload else _upload_config(args)
    result: dict[str, list[str]] = {
        "generated": [],
        "uploaded": [],
        "skipped": [],
        "failed": [],
    }
    upload_items: list[tuple[str, str, bool]] = []

    for target_date in dates:
        date_str = target_date.isoformat()
        try:
            daily = build_daily_frame(date_str)
            save_csv(daily, date_str)
            daily_html = build_daily_body_html(date_str, daily)
            title = f"[데이터] {date_str} 미수백 일별 판매"
            upload_items.append((title, daily_html, False))
            result["generated"].append(date_str)
            logger.info("미수백 일별 HTML 생성 완료 | date=%s chars=%d", date_str, len(daily_html))
        except Exception:
            logger.exception("미수백 일별 리포트 생성 실패 | date=%s", date_str)
            result["failed"].append(date_str)

    anchor = dates[-1]
    iso = anchor.isocalendar()
    week_label = month_week_label(anchor)
    weekly_title = f"[집계-주] {anchor:%Y-%m} {week_label} 미수백 주간 현황"
    try:
        weekly_source = load_susam_range(_week_dates(anchor))
        weekly = build_weekly_frame(weekly_source)
        weekly_html = build_weekly_body_html(iso.year, iso.week, weekly)
        upload_items.append((weekly_title, weekly_html, True))
        logger.info("미수백 주간 HTML 생성 완료 | title=%s chars=%d", weekly_title, len(weekly_html))
    except Exception:
        logger.exception("미수백 주간 리포트 생성 실패 | title=%s", weekly_title)
        result["failed"].append(weekly_title)

    ym = anchor.strftime("%Y-%m")
    monthly_title = f"[집계-월] {ym} 미수백 월간 현황"
    try:
        monthly_source = load_susam_range(_month_dates(anchor))
        monthly = build_monthly_frame(monthly_source)
        monthly_html = build_monthly_body_html(ym, monthly)
        upload_items.append((monthly_title, monthly_html, True))
        logger.info("미수백 월간 HTML 생성 완료 | title=%s chars=%d", monthly_title, len(monthly_html))
    except Exception:
        logger.exception("미수백 월간 리포트 생성 실패 | title=%s", monthly_title)
        result["failed"].append(monthly_title)

    if upload_config is None:
        logger.info("미수백 Flow 업로드 생략 | generated=%d", len(upload_items))
        logger.info("미수백 리포트 실행 요약 | %s", result)
        return result

    post_url, user_data_dir, profile, debugger_address = upload_config
    _validate_flow_url(post_url)
    if not debugger_address:
        if not user_data_dir or not Path(user_data_dir).expanduser().is_dir():
            raise ValueError(f"Chrome user-data-dir이 존재하지 않습니다: {user_data_dir}")
        if not str(profile or "").strip():
            raise ValueError("Chrome profile-directory가 비어 있습니다.")
    if args.force:
        logger.info("--force 지정: 기존 하위업무를 포함해 모든 대상을 다시 편집합니다.")

    driver = None
    try:
        driver = _create_driver(user_data_dir, profile, debugger_address)
        driver.set_window_size(1920, 1080)
        driver.get(post_url)
        time.sleep(2)
        current_url = (driver.current_url or "").lower()
        if "login" in current_url or "signin" in current_url:
            raise RuntimeError("Flow 로그인 세션이 유효하지 않습니다.")

        for index, (title, body_html, preserve_memo) in enumerate(upload_items):
            try:
                upsert_subtask(
                    driver,
                    title,
                    body_html,
                    preserve_memo=preserve_memo,
                )
                result["uploaded"].append(title)
            except Exception:
                logger.exception("미수백 Flow 하위업무 갱신 실패 | title=%s", title)
                result["failed"].append(title)
                if not _is_driver_alive(driver):
                    remaining = [item[0] for item in upload_items[index + 1 :]]
                    result["failed"].extend(remaining)
                    break
            if index < len(upload_items) - 1:
                time.sleep(2)
    finally:
        if driver is not None:
            driver.quit()

    logger.info("미수백 리포트 실행 요약 | %s", result)
    return result


def _run_cli() -> int:
    try:
        result = main()
    except Exception:
        logger.exception("미수백 리포트 실행 준비 실패")
        return 1
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    sys.exit(_run_cli())
