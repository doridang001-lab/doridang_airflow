"""
Google Sheets load utility backed by googleapiclient.
"""

from __future__ import annotations

import logging
import random
import re
import socket
import time
from datetime import date, datetime
from http.client import RemoteDisconnected
from typing import List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    import ssl
except ImportError:  # pragma: no cover
    ssl = None

try:
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from requests.exceptions import Timeout as RequestsTimeout
except ImportError:  # pragma: no cover
    RequestsConnectionError = None
    RequestsTimeout = None


DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/service_account_key.json"
DEFAULT_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/17jloeN0cqpQrPAfcBaINjU4u02gMXgRfGfpvZ7uMj-E/edit?usp=sharing"
DEFAULT_SHEET_NAME = "시트1"

MODE_OVERWRITE = "overwrite"
MODE_APPEND = "append"
MODE_APPEND_UNIQUE = "append_unique"

ACTION_CLEAR = "clear"

TIMESTAMP_COLUMN = "uploaded_at"
KEY_SEPARATOR = "|"

SPREADSHEET_ID_PATTERN = r"/d/([a-zA-Z0-9_-]+)"
GSHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

NEW_SHEET_ROWS = 1000
NEW_SHEET_COLS = 50

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RETRY_MAX_ATTEMPTS = 5
RETRY_INITIAL_DELAY_SECONDS = 2
RETRY_MAX_DELAY_SECONDS = 30

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")


def _parse_spreadsheet_id(url_or_id: str) -> str:
    if "/d/" in url_or_id:
        match = re.search(SPREADSHEET_ID_PATTERN, url_or_id)
        if match:
            return match.group(1)
    return url_or_id


def _build_sheets_service(credentials_path: str):
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=GSHEET_SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def _get_http_status(exc: Exception) -> Optional[int]:
    status = getattr(getattr(exc, "resp", None), "status", None)
    if status is None:
        status = getattr(exc, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_retryable_sheets_error(exc: Exception) -> bool:
    if _get_http_status(exc) in RETRYABLE_STATUS_CODES:
        return True

    retryable_types = [TimeoutError, ConnectionError, socket.timeout, RemoteDisconnected, OSError]
    if ssl is not None:
        retryable_types.append(ssl.SSLError)
    if RequestsConnectionError is not None:
        retryable_types.append(RequestsConnectionError)
    if RequestsTimeout is not None:
        retryable_types.append(RequestsTimeout)

    return isinstance(exc, tuple(retryable_types))


def _execute_with_retry(request_factory, *, action: str, sheet_name: str):
    delay = RETRY_INITIAL_DELAY_SECONDS

    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return request_factory().execute()
        except Exception as exc:
            retryable = _is_retryable_sheets_error(exc)
            status = _get_http_status(exc)
            if not retryable or attempt >= RETRY_MAX_ATTEMPTS:
                raise

            jitter = random.uniform(0, min(1.0, delay / 2))
            sleep_seconds = min(RETRY_MAX_DELAY_SECONDS, delay + jitter)
            logger.warning(
                "Google Sheets transient failure; retrying action=%s sheet=%s attempt=%s/%s status=%s error=%r sleep=%.2fs",
                action,
                sheet_name,
                attempt,
                RETRY_MAX_ATTEMPTS,
                status,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            delay = min(RETRY_MAX_DELAY_SECONDS, delay * 2)


def _get_spreadsheet(service, spreadsheet_id: str) -> dict:
    return _execute_with_retry(
        lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id),
        action="get_spreadsheet",
        sheet_name=spreadsheet_id,
    )


def _get_sheet_props_by_title(spreadsheet: dict, sheet_title: str) -> Optional[dict]:
    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_title:
            return props
    return None


def _add_sheet_if_missing(service, spreadsheet_id: str, sheet_name: str) -> dict:
    spreadsheet = _get_spreadsheet(service, spreadsheet_id)
    props = _get_sheet_props_by_title(spreadsheet, sheet_name)
    if props:
        return props

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": sheet_name,
                        "gridProperties": {
                            "rowCount": NEW_SHEET_ROWS,
                            "columnCount": NEW_SHEET_COLS,
                        },
                    }
                }
            }
        ]
    }
    _execute_with_retry(
        lambda: service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body),
        action="add_sheet",
        sheet_name=sheet_name,
    )
    spreadsheet = _get_spreadsheet(service, spreadsheet_id)
    return _get_sheet_props_by_title(spreadsheet, sheet_name)


def _get_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
    spreadsheet = _get_spreadsheet(service, spreadsheet_id)
    props = _get_sheet_props_by_title(spreadsheet, sheet_name)
    return props.get("sheetId") if props else None


def _ensure_date_format(service, spreadsheet_id: str, sheet_name: str, column_index: int = 0, pattern: str = "yyyy-mm-dd"):
    sheet_id = _get_sheet_id(service, spreadsheet_id, sheet_name)
    if sheet_id is None:
        print(f"[warning] sheet id not found: {sheet_name}")
        return

    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": pattern,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]
    }
    _execute_with_retry(
        lambda: service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body),
        action="ensure_date_format",
        sheet_name=sheet_name,
    )


def _get_header(service, spreadsheet_id: str, sheet_name: str) -> List[str]:
    range_a1 = f"{sheet_name}!1:1"
    response = _execute_with_retry(
        lambda: service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_a1),
        action="get_header",
        sheet_name=sheet_name,
    )
    values = response.get("values", [])
    return values[0] if values else []


def _update_header_if_empty(service, spreadsheet_id: str, sheet_name: str, df_columns: List[str]):
    existing_header = _get_header(service, spreadsheet_id, sheet_name)
    if existing_header:
        return

    headers = df_columns + [TIMESTAMP_COLUMN]
    body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": [headers]}
    _execute_with_retry(
        lambda: service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body,
        ),
        action="update_header",
        sheet_name=sheet_name,
    )


def _add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out[TIMESTAMP_COLUMN] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    return out


def _df_to_rows(df: pd.DataFrame) -> List[List[str]]:
    def _fmt(value):
        if pd.isna(value):
            return ""
        if isinstance(value, float) and (value == float("inf") or value == float("-inf")):
            return ""
        if value == "":
            return ""
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d") if value.time() == pd.Timestamp(0).time() else value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            date_pattern = r"^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$"
            if re.match(date_pattern, value.strip()):
                return value.strip()
            try:
                if "." not in value and value.replace("-", "").replace("+", "").isdigit():
                    return int(value)
                return float(value)
            except (ValueError, AttributeError):
                return value
        if isinstance(value, (int, float)):
            return value
        return str(value)

    return df.map(_fmt).values.tolist()


def _execute_clear(service, spreadsheet_id: str, sheet_name: str) -> dict:
    print(f"clear: {sheet_name}")
    try:
        _execute_with_retry(
            lambda: service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                body={},
            ),
            action="clear_sheet",
            sheet_name=sheet_name,
        )
        print("done")
        return {"success": True}
    except Exception as exc:
        print(f"failed: {exc}")
        return {"success": False, "error": str(exc)}


def _execute_read(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    print(f"read: {sheet_name}")
    try:
        response = _execute_with_retry(
            lambda: service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
            ),
            action="read_sheet",
            sheet_name=sheet_name,
        )
        values = response.get("values", [])
        if not values:
            print("done: 0 rows")
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        print(f"done: {len(df)} rows")
        return df
    except Exception as exc:
        print(f"failed: {exc}")
        return pd.DataFrame()


def _execute_overwrite(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    df_w = _add_timestamp_column(df)

    print(f"[overwrite] clearing sheet before upload: {sheet_name}")
    try:
        _execute_with_retry(
            lambda: service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                body={},
            ),
            action="overwrite_clear",
            sheet_name=sheet_name,
        )
        print(f"[overwrite] cleared sheet: {sheet_name}")
    except Exception as exc:
        print(f"[warn] clear failed but upload will continue: {exc}")

    data = [df_w.columns.tolist()] + _df_to_rows(df_w)
    print(f"[upload debug] header: {data[0]}")
    print("[upload debug] first 3 rows:")
    for i, row in enumerate(data[1:4], 1):
        print(f"  row{i}: {row[:5]}...")
    print(f"[upload debug] total rows including header: {len(data)}")

    body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": data}
    result = _execute_with_retry(
        lambda: service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body,
        ),
        action="overwrite_update",
        sheet_name=sheet_name,
    )

    print(f"[API response] updatedCells: {result.get('updatedCells', 0)}, updatedRows: {result.get('updatedRows', 0)}")
    print(f"done (overwrite): {len(df_w)} rows")
    return {"success": True, "new": len(df_w)}


def _execute_append(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    df_w = _add_timestamp_column(df)
    body = {"majorDimension": "ROWS", "values": _df_to_rows(df_w)}
    _execute_with_retry(
        lambda: service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ),
        action="append_rows",
        sheet_name=sheet_name,
    )
    print(f"done (append): {len(df_w)} rows")
    return {"success": True, "new": len(df_w)}


def _create_composite_key(df: pd.DataFrame, key_columns: List[str]) -> pd.Series:
    return df[key_columns].astype(str).agg(KEY_SEPARATOR.join, axis=1)


def _resolve_key_columns(df: pd.DataFrame, primary_key: Union[str, List[str], None]) -> List[str]:
    if primary_key is None:
        return [column for column in df.columns if column != TIMESTAMP_COLUMN]
    return [primary_key] if isinstance(primary_key, str) else list(primary_key)


def _filter_new_rows(upload_df: pd.DataFrame, existing_df: pd.DataFrame, key_columns: List[str]) -> Tuple[pd.DataFrame, int]:
    has_existing = len(existing_df) > 0
    has_all_key_cols = all(column in existing_df.columns for column in key_columns)
    if not (has_existing and has_all_key_cols):
        return upload_df, 0

    existing_keys = _create_composite_key(existing_df, key_columns)
    upload_keys = _create_composite_key(upload_df, key_columns)
    is_new = ~upload_keys.isin(existing_keys)
    new_rows_df = upload_df[is_new]
    duplicate_count = len(upload_df) - len(new_rows_df)
    return new_rows_df, duplicate_count


def _execute_append_unique(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, primary_key: Union[str, List[str], None]) -> dict:
    existing_df = _execute_read(service, spreadsheet_id, sheet_name)
    df_w = _add_timestamp_column(df)

    key_cols = _resolve_key_columns(df, primary_key)
    new_rows_df, duplicate_count = _filter_new_rows(df_w, existing_df, key_cols)

    if len(existing_df) > 0 and len(new_rows_df) > 0:
        existing_cols = list(existing_df.columns)

        missing_cols = [column for column in existing_cols if column not in new_rows_df.columns]
        for column in missing_cols:
            new_rows_df[column] = ""

        extra_cols = [column for column in new_rows_df.columns if column not in existing_cols]
        if extra_cols:
            print(f"[warning] append columns missing from sheet and excluded: {extra_cols}")

        new_rows_df = new_rows_df[existing_cols]

    if len(new_rows_df) > 0:
        body = {"majorDimension": "ROWS", "values": _df_to_rows(new_rows_df)}
        _execute_with_retry(
            lambda: service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ),
            action="append_unique_rows",
            sheet_name=sheet_name,
        )

    print(f"done: new {len(new_rows_df)} rows, duplicate {duplicate_count} rows")
    return {"success": True, "new": len(new_rows_df), "duplicate": duplicate_count}


def save_to_gsheet(
    df: Optional[pd.DataFrame] = None,
    sheet_name: Optional[str] = None,
    mode: str = MODE_APPEND_UNIQUE,
    action: Optional[str] = None,
    primary_key: Union[str, List[str], None] = None,
    credentials_path: Optional[str] = None,
    url: Optional[str] = None,
) -> Union[dict, pd.DataFrame]:
    if isinstance(df, str) and sheet_name is None:
        sheet_name = df
        df = None

    sheet_name = sheet_name or DEFAULT_SHEET_NAME
    credentials_path = credentials_path or DEFAULT_CREDENTIALS_PATH
    spreadsheet_id = _parse_spreadsheet_id(url or DEFAULT_SPREADSHEET_URL)

    try:
        service = _build_sheets_service(credentials_path)
    except Exception as exc:
        print(f"auth failed: {exc}")
        return {"success": False, "error": str(exc)}

    try:
        _add_sheet_if_missing(service, spreadsheet_id, sheet_name)
        if df is not None and mode != MODE_OVERWRITE:
            _update_header_if_empty(service, spreadsheet_id, sheet_name, df.columns.tolist())
    except Exception as exc:
        print(f"sheet preparation failed: {exc}")
        return {"success": False, "error": str(exc)}

    if action == ACTION_CLEAR:
        return _execute_clear(service, spreadsheet_id, sheet_name)

    if df is None:
        return _execute_read(service, spreadsheet_id, sheet_name)

    print(f"upload: {sheet_name} ({len(df)} rows, mode={mode})")
    try:
        if mode == MODE_OVERWRITE:
            return _execute_overwrite(service, spreadsheet_id, sheet_name, df)
        if mode == MODE_APPEND:
            return _execute_append(service, spreadsheet_id, sheet_name, df)
        return _execute_append_unique(service, spreadsheet_id, sheet_name, df, primary_key)
    except Exception as exc:
        print(f"failed: {exc}")
        return {"success": False, "error": str(exc)}


def apply_date_format_to_column(
    credentials_path: str,
    url: str,
    sheet_name: str,
    column_index: int = 0,
    pattern: str = "yyyy-mm-dd",
):
    service = _build_sheets_service(credentials_path)
    spreadsheet_id = _parse_spreadsheet_id(url)
    sheet_id = _get_sheet_id(service, spreadsheet_id, sheet_name)
    if sheet_id is None:
        raise ValueError(f"sheet not found: {sheet_name}")

    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": pattern,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]
    }

    _execute_with_retry(
        lambda: service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body),
        action="apply_date_format",
        sheet_name=sheet_name,
    )
    print(f"date format applied: {sheet_name} col={column_index} pattern={pattern}")
