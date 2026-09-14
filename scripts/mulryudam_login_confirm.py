"""Launch Mulryudam and open yesterday's order detail screen."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
    from pywinauto import Desktop, keyboard, mouse
    from pywinauto.timings import TimeoutError as PywinautoTimeoutError
    from openpyxl import Workbook
    import win32com.client
    import win32api
    import win32con
    import win32gui
    import win32process
except ImportError as exc:  # pragma: no cover - exercised only on host setup drift
    raise SystemExit(
        "pywinauto is required. Install dependencies with: "
        "C:\\airflow\\.venv\\Scripts\\python.exe -m pip install -r C:\\airflow\\requirements.txt"
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

try:
    from modules.transform.utility import notifier as _notifier

    def _get_env_telegram_creds() -> tuple[str, str]:
        return os.getenv("TELEGRAM_BOT_TOKEN", ""), os.getenv("TELEGRAM_CHAT_ID", "")

    _notifier._get_telegram_creds = _get_env_telegram_creds
    send_telegram = _notifier.send_telegram
except Exception:
    send_telegram = None

DEFAULT_SHORTCUT_PATH = Path(
    r"C:\ProgramData\Microsoft\Windows\Start Menu\Programs\물류담\물류담.lnk"
)
DEFAULT_POPUP_TITLE = "로그인"
DEFAULT_BUTTON_TITLE = "확인"
DEFAULT_TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 0.5
LOGIN_SETTLE_SECONDS = 8.0
MAIN_WINDOW_TITLE_PREFIX = "ENT-"
ORDER_WINDOW_TITLE = "주문확인"
_DIALOG_TITLE_HINTS = ("확인", "알림", "주의", "Information", "Warning", "Error")
SALES_BUTTON_REL = (227, 78)
ORDER_CONFIRM_BUTTON_REL = (115, 165)
ORDER_DETAILS_TAB_REL = (220, 44)
ORDER_DATE_FIELD_REL = (210, 96)
INCLUDE_AFTER_DATE_CHECKBOX_REL = (287, 88)
ORDER_DETAILS_QUERY_BUTTON_REL = (635, 94)
TO_EXCEL_BUTTON_REL = (868, 94)
KEY_PAUSE_SECONDS = 0.1
EXCEL_TABLE_START_ROW = 5
DEFAULT_PARQUET_DIR = Path(
    r"C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\Logistics_Dam"
)
ORDER_WINDOW_CLOSE_REL = (-24, 18)
SALES_MDI_CLOSE_FROM_MAIN_TOP_RIGHT = (-25, 116)
PROGRAM_EXIT_REL = (320, 78)
EXIT_CONFIRM_TITLE = "Confirm"
EXIT_CONFIRM_YES_REL = (62, -24)

logger = logging.getLogger(__name__)


class NoDataError(RuntimeError):
    """조회 결과가 0건이라 Excel 내보내기가 발생하지 않은 정상 상황."""


def _setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{datetime.now():%Y%m%d_%H%M%S}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_path


def _console_safe(text: object) -> str:
    value = str(text)
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def _launch_shortcut(shortcut_path: Path) -> None:
    if not shortcut_path.exists():
        raise FileNotFoundError(f"Mulryudam shortcut not found: {shortcut_path}")

    logger.info("launch shortcut: %s", shortcut_path)
    os.startfile(str(shortcut_path))  # type: ignore[attr-defined]


def _click_popup_button(
    popup_title: str,
    button_title: str,
    timeout_seconds: int,
) -> bool:
    desktop = Desktop(backend="uia")
    popup = desktop.window(title=popup_title)

    try:
        logger.info("wait popup title=%r timeout=%ss", popup_title, timeout_seconds)
        popup.wait("exists visible", timeout=timeout_seconds, retry_interval=POLL_INTERVAL_SECONDS)
    except PywinautoTimeoutError:
        logger.warning("popup not found within timeout: %s", popup_title)
        return False

    popup.set_focus()
    button = popup.child_window(title=button_title, control_type="Button")
    try:
        button.wait("exists enabled visible", timeout=10, retry_interval=POLL_INTERVAL_SECONDS)
    except PywinautoTimeoutError as exc:
        raise RuntimeError(f"button not ready: {button_title}") from exc

    logger.info("click button: %s", button_title)
    button.click_input()
    time.sleep(0.5)
    try:
        if popup.exists(timeout=1):
            logger.info("popup still visible after click; send Enter")
            popup.set_focus()
            keyboard.send_keys("{ENTER}")
            time.sleep(0.8)
    except Exception as exc:
        logger.info("popup post-click check skipped: %s", exc)
    return True


def _find_window_by_title_prefix(title_prefix: str, timeout_seconds: int):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        for window in Desktop(backend="uia").windows():
            if window.window_text().startswith(title_prefix) or (
                title_prefix == MAIN_WINDOW_TITLE_PREFIX and _is_mulryudam_main_window(window)
            ):
                return window
        time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"window not found by title prefix: {title_prefix}")


def _is_mulryudam_main_window(window) -> bool:
    title = window.window_text().strip()
    if title.startswith(MAIN_WINDOW_TITLE_PREFIX):
        return True
    if window.class_name() != "TfmMain":
        return False
    try:
        _, pid = win32process.GetWindowThreadProcessId(window.handle)
        process_path = _process_path(pid)
        if process_path and process_path.name.lower() == "mulryudam_ml_client.exe":
            return True
    except Exception:
        pass
    return title == "-"


def _process_path(pid: int) -> Path | None:
    handle = None
    try:
        handle = win32api.OpenProcess(win32con.PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        return Path(win32process.GetModuleFileNameEx(handle, 0))
    except Exception:
        return None
    finally:
        if handle:
            win32api.CloseHandle(handle)


def _window_process_path(window) -> Path | None:
    try:
        _, pid = win32process.GetWindowThreadProcessId(window.handle)
    except Exception:
        return None
    return _process_path(pid)


def _is_mulryudam_owned_window(window) -> bool:
    process_path = _window_process_path(window)
    return bool(process_path and process_path.name.lower() == "mulryudam_ml_client.exe")


def _find_window_by_title(title: str, timeout_seconds: int):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        for window in Desktop(backend="uia").windows():
            if window.window_text() == title:
                return window
        time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"window not found by title: {title}")


def _find_blocking_dialog():
    for window in Desktop(backend="uia").windows():
        if not _is_mulryudam_owned_window(window):
            continue
        title = window.window_text().strip()
        if not title or title.startswith(MAIN_WINDOW_TITLE_PREFIX):
            continue
        if title == ORDER_WINDOW_TITLE or len(title) > 30:
            continue
        if title in _DIALOG_TITLE_HINTS or window.class_name() in ("#32770", "TMessageForm"):
            texts = []
            try:
                for child in window.descendants():
                    text = child.window_text().strip()
                    if text and text not in texts:
                        texts.append(text)
            except Exception:
                pass
            return window, " | ".join(texts[:10])
    return None, ""


def _close_blocking_dialog(dialog) -> None:
    for button_title in ("확인", "OK"):
        try:
            button = dialog.child_window(title=button_title, control_type="Button")
            button.wait("exists enabled visible", timeout=2, retry_interval=POLL_INTERVAL_SECONDS)
            button.click_input()
            return
        except Exception:
            pass
    try:
        dialog.set_focus()
        keyboard.send_keys("{ENTER}")
    except Exception as exc:
        logger.warning("blocking dialog close failed: %s", exc)


def _click_relative(window, rel_x: int, rel_y: int, label: str, wait_seconds: float = 1.0) -> None:
    rect = window.rectangle()
    coords = (rect.left + rel_x, rect.top + rel_y)
    logger.info("click %s at %s", label, coords)
    window.set_focus()
    time.sleep(0.3)
    mouse.click(button="left", coords=coords)
    time.sleep(wait_seconds)


def _click_from_edges(
    window,
    rel_x_from_right: int,
    rel_y_from_top_or_bottom: int,
    label: str,
    from_bottom: bool = False,
    wait_seconds: float = 1.0,
) -> None:
    rect = window.rectangle()
    y = rect.bottom + rel_y_from_top_or_bottom if from_bottom else rect.top + rel_y_from_top_or_bottom
    coords = (rect.right + rel_x_from_right, y)
    logger.info("click %s at %s", label, coords)
    window.set_focus()
    time.sleep(0.3)
    mouse.click(button="left", coords=coords)
    time.sleep(wait_seconds)


def _ensure_include_after_date_checked(order_window) -> None:
    try:
        for child in order_window.descendants():
            if "이후날짜포함" not in child.window_text():
                continue
            toggle_state = child.get_toggle_state()
            logger.info("include after date checkbox state=%s", toggle_state)
            if toggle_state == 0:
                child.click_input()
                time.sleep(0.5)
            return
    except Exception as exc:
        logger.info("include after date checkbox UIA check skipped: %s", exc)

    _click_relative(
        order_window,
        *INCLUDE_AFTER_DATE_CHECKBOX_REL,
        label="이후날짜포함",
        wait_seconds=0.5,
    )


def _bring_to_front(window, label: str, maximize: bool = False) -> None:
    logger.info("bring to front: %s handle=%s rect=%s", label, window.handle, window.rectangle())
    if maximize:
        win32gui.ShowWindow(window.handle, win32con.SW_MAXIMIZE)
        time.sleep(0.8)
    else:
        win32gui.ShowWindow(window.handle, win32con.SW_RESTORE)
        time.sleep(0.3)
    win32gui.SetForegroundWindow(window.handle)
    window.set_focus()
    time.sleep(0.8)
    logger.info("front window ready: %s rect=%s", label, window.rectangle())


def _default_target_date() -> str:
    return (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")


def _normalize_target_date(target_date: str) -> str:
    digits = "".join(ch for ch in target_date if ch.isdigit())
    if len(digits) != 8:
        raise ValueError(f"target date must contain 8 digits: {target_date}")
    return digits


def _date_digits_to_iso(date_digits: str) -> str:
    return f"{date_digits[:4]}-{date_digits[4:6]}-{date_digits[6:8]}"


def _dedupe_headers(headers: list[object]) -> list[str]:
    result = []
    counts: dict[str, int] = {}
    for index, header in enumerate(headers, start=1):
        name = str(header).strip() if header not in (None, "") else f"column_{index}"
        counts[name] = counts.get(name, 0) + 1
        if counts[name] > 1:
            name = f"{name}_{counts[name]}"
        result.append(name)
    return result


def _get_excel_app():
    try:
        return win32com.client.GetActiveObject("Excel.Application")
    except Exception:
        return None


def _excel_workbook_keys(excel) -> set[tuple[str, str]]:
    if excel is None:
        return set()
    keys = set()
    for index in range(1, excel.Workbooks.Count + 1):
        workbook = excel.Workbooks(index)
        keys.add((str(workbook.Name), str(workbook.FullName)))
    return keys


def _is_temporary_export_workbook(workbook) -> bool:
    try:
        name = str(workbook.Name)
        path = str(workbook.Path)
        saved = bool(workbook.Saved)
    except Exception:
        return False
    return not path and not saved and (name.startswith("통합 문서") or name.startswith("Book"))


def _temporary_export_workbooks(excel, before_keys: set[tuple[str, str]] | None = None) -> list[object]:
    if excel is None:
        return []
    candidates = []
    for index in range(1, excel.Workbooks.Count + 1):
        workbook = excel.Workbooks(index)
        key = (str(workbook.Name), str(workbook.FullName))
        if before_keys is not None and key in before_keys and not _is_temporary_export_workbook(workbook):
            continue
        if _is_temporary_export_workbook(workbook):
            candidates.append(workbook)
    return candidates


def _single_temporary_export_workbook(
    before_keys: set[tuple[str, str]] | None,
    label: str,
):
    excel = _get_excel_app()
    candidates = _temporary_export_workbooks(excel, before_keys)
    if len(candidates) == 1:
        workbook = candidates[0]
        logger.info(
            "%s temporary Excel workbook found: name=%s full_name=%s",
            label,
            workbook.Name,
            workbook.FullName,
        )
        return workbook
    if len(candidates) > 1:
        logger.warning("%s temporary Excel workbook ambiguous: count=%s", label, len(candidates))
    return None


def _wait_for_export_workbook(
    before_keys: set[tuple[str, str]],
    timeout_seconds: int,
    target_date: str,
):
    deadline = time.time() + timeout_seconds
    fallback = None
    while time.time() < deadline:
        excel = _get_excel_app()
        if excel is not None:
            for index in range(1, excel.Workbooks.Count + 1):
                workbook = excel.Workbooks(index)
                key = (str(workbook.Name), str(workbook.FullName))
                if key not in before_keys and _is_temporary_export_workbook(workbook):
                    logger.info("new Excel workbook found: name=%s full_name=%s", key[0], key[1])
                    return workbook
                if key in before_keys and _is_temporary_export_workbook(workbook):
                    fallback = workbook

            if fallback is not None:
                logger.info(
                    "use unsaved Excel workbook fallback: name=%s full_name=%s",
                    fallback.Name,
                    fallback.FullName,
                )
                return fallback

        dialog, dialog_text = _find_blocking_dialog()
        if dialog is not None:
            logger.info(
                "blocking dialog: title=%s text=%s",
                dialog.window_text(),
                dialog_text,
            )
            _close_blocking_dialog(dialog)
            raise NoDataError(f"blocking dialog during export: {dialog_text or dialog.window_text()}")
        time.sleep(POLL_INTERVAL_SECONDS)

    if datetime.strptime(target_date, "%Y%m%d").weekday() == 6:
        raise NoDataError("no data (Sunday)")
    raise RuntimeError("export Excel workbook not found")


def _save_excel_table_from_row(
    workbook,
    target_date: str,
    output_dir: Path,
    parquet_dir: Path | None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    table_path = output_dir / f"mulryudam_order_details_{target_date}.xlsx"
    raw_path = output_dir / f"mulryudam_order_details_raw_{target_date}.xlsx"

    worksheet = workbook.Worksheets(1)
    used = worksheet.UsedRange
    first_col = used.Column
    last_row = used.Row + used.Rows.Count - 1
    last_col = first_col + used.Columns.Count - 1

    rows = []
    for row_index in range(EXCEL_TABLE_START_ROW, last_row + 1):
        row = [worksheet.Cells(row_index, col_index).Value for col_index in range(first_col, last_col + 1)]
        if any(value not in (None, "") for value in row):
            rows.append(row)

    if not rows:
        raise RuntimeError(f"no Excel table rows found from row {EXCEL_TABLE_START_ROW}")

    clean_workbook = Workbook()
    clean_sheet = clean_workbook.active
    clean_sheet.title = "order_details"
    for row in rows:
        clean_sheet.append(row)

    for col_cells in clean_sheet.columns:
        col_letter = col_cells[0].column_letter
        max_len = max((len(str(cell.value)) for cell in col_cells if cell.value is not None), default=8)
        clean_sheet.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)

    clean_workbook.save(table_path)
    workbook.SaveCopyAs(str(raw_path))
    logger.info("Excel table saved: %s rows=%s", table_path, len(rows))
    logger.info("Excel raw copy saved: %s", raw_path)
    if parquet_dir is not None:
        _save_parquet_table(rows, target_date, parquet_dir)
    return table_path


def _close_export_workbook(workbook) -> None:
    try:
        excel = workbook.Application
        logger.info("close export Excel workbook: %s", workbook.Name)
        workbook.Close(SaveChanges=False)
        if excel.Workbooks.Count == 0:
            logger.info("quit Excel application")
            excel.Quit()
    except Exception as exc:
        logger.warning("export Excel workbook close failed: %s", exc)


def _save_parquet_table(rows: list[list[object]], target_date: str, parquet_dir: Path) -> list[Path]:
    if len(rows) < 2:
        raise RuntimeError("not enough rows to save parquet table")

    headers = _dedupe_headers(rows[0])
    data_rows = rows[1:]
    df = pd.DataFrame(data_rows, columns=headers)
    df = df.dropna(how="all")

    parquet_dir.mkdir(parents=True, exist_ok=True)
    if "배송일자" in df.columns:
        delivery_dates = pd.to_datetime(df["배송일자"], errors="coerce").dt.strftime("%Y-%m-%d")
        df.insert(0, "sale_date", delivery_dates.fillna(_date_digits_to_iso(target_date)))
    else:
        df.insert(0, "sale_date", _date_digits_to_iso(target_date))

    paths = []
    for sale_date, group in df.groupby("sale_date", dropna=False):
        sale_date_text = str(sale_date)
        file_date = sale_date_text.replace("-", "")[2:]
        parquet_path = parquet_dir / f"Dam_order_details_{file_date}.parquet"
        group.to_parquet(parquet_path, index=False)
        logger.info(
            "Parquet table saved: %s rows=%s cols=%s",
            parquet_path,
            len(group),
            len(group.columns),
        )
        paths.append(parquet_path)
    return paths


def _close_mulryudam(timeout_seconds: int) -> None:
    logger.info("close Mulryudam windows")

    try:
        order_window = _find_window_by_title(ORDER_WINDOW_TITLE, 3)
        _click_from_edges(
            order_window,
            *ORDER_WINDOW_CLOSE_REL,
            label="주문확인 닫기",
            wait_seconds=1.0,
        )
    except Exception as exc:
        logger.info("order window close skipped: %s", exc)

    main_window = _find_window_by_title_prefix(MAIN_WINDOW_TITLE_PREFIX, timeout_seconds)
    _bring_to_front(main_window, label="물류담 본창", maximize=True)
    _click_from_edges(
        main_window,
        *SALES_MDI_CLOSE_FROM_MAIN_TOP_RIGHT,
        label="매출업무 닫기",
        wait_seconds=1.0,
    )
    _click_relative(main_window, *PROGRAM_EXIT_REL, label="프로그램 종료", wait_seconds=1.5)

    try:
        confirm = _find_window_by_title(EXIT_CONFIRM_TITLE, 5)
        _click_relative(
            confirm,
            EXIT_CONFIRM_YES_REL[0],
            confirm.rectangle().height() + EXIT_CONFIRM_YES_REL[1],
            label="종료 확인 Yes",
            wait_seconds=1.5,
        )
    except Exception as exc:
        logger.info("exit confirmation skipped: %s", exc)


def _force_kill_mulryudam() -> None:
    try:
        window = _find_window_by_title_prefix(MAIN_WINDOW_TITLE_PREFIX, 3)
    except RuntimeError:
        return
    _, pid = win32process.GetWindowThreadProcessId(window.handle)
    logger.warning("force kill Mulryudam pid=%s", pid)
    subprocess.run(["taskkill", "/PID", str(pid), "/F", "/T"], capture_output=True)


def _capture_failure_diagnostics(log_dir: Path) -> Path | None:
    screenshot_path = log_dir / f"fail_{datetime.now():%Y%m%d_%H%M%S}.png"
    try:
        from PIL import ImageGrab

        log_dir.mkdir(parents=True, exist_ok=True)
        ImageGrab.grab().save(screenshot_path)
        logger.info("failure screenshot saved: %s", screenshot_path)
    except Exception as exc:
        logger.warning("failure screenshot failed: %s", exc)
        screenshot_path = None

    try:
        for window in Desktop(backend="uia").windows():
            logger.info(
                "visible window: title=%r rect=%s",
                _console_safe(window.window_text()),
                window.rectangle(),
            )
    except Exception as exc:
        logger.warning("visible window dump failed: %s", exc)
    return screenshot_path


def _notify_failure(target_date: str, stage: str, exc: Exception, log_path: Path, screenshot_path: Path | None) -> None:
    if send_telegram is None:
        logger.warning("Telegram notifier unavailable; skip send")
        return
    lines = [
        "[DAG 실패] 물류담 주문내역 수집",
        f"대상일: {_date_digits_to_iso(target_date)}",
        f"단계: {stage}",
        f"오류: {exc}",
        f"로그: {log_path}",
    ]
    if screenshot_path is not None:
        lines.append(f"스크린샷: {screenshot_path}")
    send_telegram("\n".join(lines))


def _open_order_details_for_date(
    target_date: str,
    timeout_seconds: int,
    output_dir: Path,
    parquet_dir: Path | None,
    export_workbook_holder: dict[str, object] | None = None,
):
    date_digits = _normalize_target_date(target_date)
    main_window = _find_window_by_title_prefix(MAIN_WINDOW_TITLE_PREFIX, timeout_seconds)
    logger.info("main window found: %s", main_window.window_text())
    _bring_to_front(main_window, label="물류담 본창", maximize=True)

    _click_relative(main_window, *SALES_BUTTON_REL, label="매출업무", wait_seconds=4.0)
    _click_relative(main_window, *ORDER_CONFIRM_BUTTON_REL, label="주문확인", wait_seconds=3.0)

    order_window = _find_window_by_title(ORDER_WINDOW_TITLE, timeout_seconds)
    logger.info("order window found: %s", order_window.window_text())
    _bring_to_front(order_window, label="주문확인 창")

    _click_relative(order_window, *ORDER_DETAILS_TAB_REL, label="주문내역 탭", wait_seconds=1.5)
    _click_relative(order_window, *ORDER_DATE_FIELD_REL, label="매출일자", wait_seconds=0.8)

    logger.info("type target date like a human: %s", date_digits)
    keyboard.send_keys("^a")
    time.sleep(0.8)
    keyboard.send_keys("{BACKSPACE}")
    time.sleep(0.5)
    keyboard.send_keys(date_digits, pause=KEY_PAUSE_SECONDS)
    time.sleep(1.0)
    _ensure_include_after_date_checked(order_window)

    before_workbooks = _excel_workbook_keys(_get_excel_app())
    if export_workbook_holder is not None:
        export_workbook_holder["before_keys"] = before_workbooks
    _click_relative(
        order_window,
        *ORDER_DETAILS_QUERY_BUTTON_REL,
        label="주문내역 조회",
        wait_seconds=2.5,
    )
    _click_relative(order_window, *TO_EXCEL_BUTTON_REL, label="To Excel", wait_seconds=2.0)
    if export_workbook_holder is not None:
        export_workbook_holder["export_clicked"] = True
    export_workbook = _wait_for_export_workbook(before_workbooks, timeout_seconds, date_digits)
    if export_workbook_holder is not None:
        export_workbook_holder["workbook"] = export_workbook
    _save_excel_table_from_row(export_workbook, date_digits, output_dir, parquet_dir)
    if export_workbook_holder is not None:
        export_workbook_holder["saved"] = True
    return export_workbook


def run(
    shortcut_path: Path = DEFAULT_SHORTCUT_PATH,
    popup_title: str = DEFAULT_POPUP_TITLE,
    button_title: str = DEFAULT_BUTTON_TITLE,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    target_date: str | None = None,
    log_dir: Path = REPO_ROOT / ".tmp" / "mulryudam_login_confirm",
    parquet_dir: Path | None = DEFAULT_PARQUET_DIR,
) -> int:
    log_path = _setup_logging(log_dir)
    date_digits = _normalize_target_date(target_date or _default_target_date())
    export_workbook_holder: dict[str, object] = {}
    exit_code = 0
    stage = "시작"
    logger.info("log file: %s", log_path)
    logger.info(
        "Mulryudam order detail start: shortcut=%s popup_title=%r button_title=%r timeout=%s target_date=%s",
        shortcut_path,
        popup_title,
        button_title,
        timeout_seconds,
        date_digits,
    )

    try:
        stage = "기존 인스턴스 정리"
        try:
            stale = _find_window_by_title_prefix(MAIN_WINDOW_TITLE_PREFIX, 2)
            logger.warning("stale Mulryudam instance found: %s; closing", stale.window_text())
            try:
                _close_mulryudam(timeout_seconds)
            except Exception:
                _force_kill_mulryudam()
            time.sleep(3)
        except RuntimeError:
            pass

        stage = "로그인 팝업 확인"
        logger.info("precheck existing popup before launching")
        popup_clicked = _click_popup_button(
            popup_title=popup_title,
            button_title=button_title,
            timeout_seconds=2,
        )
        if popup_clicked:
            logger.info("existing Mulryudam popup confirmed")
        else:
            stage = "물류담 실행"
            _launch_shortcut(shortcut_path)
            stage = "로그인 팝업 확인"
            popup_clicked = _click_popup_button(
                popup_title=popup_title,
                button_title=button_title,
                timeout_seconds=timeout_seconds,
            )

        if popup_clicked:
            logger.info("Mulryudam login confirm completed")
            logger.info("wait after login confirm: %ss", LOGIN_SETTLE_SECONDS)
            time.sleep(LOGIN_SETTLE_SECONDS)
        else:
            logger.info("Mulryudam popup was not visible; continue with main window")

        stage = "주문내역/Excel 내보내기"
        _open_order_details_for_date(
            target_date=date_digits,
            timeout_seconds=timeout_seconds,
            output_dir=log_dir,
            parquet_dir=parquet_dir,
            export_workbook_holder=export_workbook_holder,
        )
    except NoDataError as exc:
        logger.info("no data for %s; graceful exit: %s", date_digits, exc)
        exit_code = 0
    except Exception as exc:
        logger.exception("Mulryudam order detail setup failed")
        screenshot_path = _capture_failure_diagnostics(log_dir)
        _notify_failure(date_digits, stage, exc, log_path, screenshot_path)
        exit_code = 1
    finally:
        export_workbook = export_workbook_holder.get("workbook")
        if export_workbook is None and export_workbook_holder.get("export_clicked"):
            try:
                export_workbook = _single_temporary_export_workbook(
                    export_workbook_holder.get("before_keys"),
                    label="cleanup",
                )
                if export_workbook is not None:
                    _save_excel_table_from_row(export_workbook, date_digits, log_dir, parquet_dir)
                    export_workbook_holder["workbook"] = export_workbook
                    export_workbook_holder["saved"] = True
            except Exception as exc:
                logger.warning("cleanup Excel workbook save failed: %s", exc)
        if export_workbook is not None:
            try:
                _close_export_workbook(export_workbook)
            except Exception as exc:
                logger.warning("export workbook cleanup failed: %s", exc)
        try:
            _find_window_by_title_prefix(MAIN_WINDOW_TITLE_PREFIX, 2)
            _close_mulryudam(timeout_seconds)
        except Exception as exc:
            logger.info("Mulryudam cleanup skipped or failed: %s", exc)
        try:
            _force_kill_mulryudam()
        except Exception as exc:
            logger.warning("Mulryudam force kill failed: %s", exc)

    if exit_code == 0:
        logger.info("Mulryudam order detail setup completed")
    return exit_code


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch Mulryudam and click the login confirmation popup."
    )
    parser.add_argument(
        "--shortcut-path",
        type=Path,
        default=DEFAULT_SHORTCUT_PATH,
        help=f"Mulryudam shortcut path (default: {DEFAULT_SHORTCUT_PATH})",
    )
    parser.add_argument(
        "--popup-title",
        default=DEFAULT_POPUP_TITLE,
        help=f"Login popup title (default: {DEFAULT_POPUP_TITLE})",
    )
    parser.add_argument(
        "--button-title",
        default=DEFAULT_BUTTON_TITLE,
        help=f"Confirmation button title (default: {DEFAULT_BUTTON_TITLE})",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Popup wait timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--target-date",
        default=None,
        help="Target sales date. Accepts YYYYMMDD or YYYY-MM-DD. Default: yesterday.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=REPO_ROOT / ".tmp" / "mulryudam_login_confirm",
        help="Log directory.",
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=DEFAULT_PARQUET_DIR,
        help=f"Daily parquet output root. Default: {DEFAULT_PARQUET_DIR}",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(
        run(
            shortcut_path=args.shortcut_path,
            popup_title=args.popup_title,
            button_title=args.button_title,
            timeout_seconds=args.timeout_seconds,
            target_date=args.target_date,
            log_dir=args.log_dir,
            parquet_dir=args.parquet_dir,
        )
    )
