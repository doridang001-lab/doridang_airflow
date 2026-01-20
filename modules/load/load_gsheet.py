"""
구글시트 저장 함수 (Google Sheets API 직행 버전)
- gspread/Drive 미사용
- googleapiclient + service_account로 인증
"""

import re
from datetime import datetime
from typing import Optional, Union, List, Tuple
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===== 기본값 =====
# 반드시 '서비스 계정 JSON' 경로를 지정하세요.
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/service_account_key.json"

# URL 또는 ID 모두 허용 (아래에서 자동 파싱)
DEFAULT_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/17jloeN0cqpQrPAfcBaINjU4u02gMXgRfGfpvZ7uMj-E/edit?usp=sharing"
DEFAULT_SHEET_NAME = "시트1"

MODE_OVERWRITE = "overwrite"
MODE_APPEND = "append"
MODE_APPEND_UNIQUE = "append_unique"

ACTION_CLEAR = "clear"

TIMESTAMP_COLUMN = "uploaded_at"
KEY_SEPARATOR = "|"

SPREADSHEET_ID_PATTERN = r"/d/([a-zA-Z0-9_-]+)"

# Drive 경유 없음 → Sheets 스코프만 사용
GSHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

NEW_SHEET_ROWS = 1000
NEW_SHEET_COLS = 50


# =========================
# 유틸 & 인증
# =========================
def _parse_spreadsheet_id(url_or_id: str) -> str:
    """
    Google Sheets URL 또는 ID 문자열에서 ID를 추출
    """
    if "/d/" in url_or_id:
        m = re.search(SPREADSHEET_ID_PATTERN, url_or_id)
        if m:
            return m.group(1)
    return url_or_id


def _build_sheets_service(credentials_path: str):
    """
    서비스 계정 JSON으로 Sheets API 클라이언트 생성
    """
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=GSHEET_SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def _get_spreadsheet(service, spreadsheet_id: str) -> dict:
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def _get_sheet_props_by_title(spreadsheet: dict, sheet_title: str) -> Optional[dict]:
    for s in spreadsheet.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_title:
            return props
    return None


def _add_sheet_if_missing(service, spreadsheet_id: str, sheet_name: str) -> dict:
    """
    시트가 없으면 addSheet로 생성, 있으면 그대로 props 반환
    """
    ss = _get_spreadsheet(service, spreadsheet_id)
    props = _get_sheet_props_by_title(ss, sheet_name)
    if props:
        return props

    add_req = {
        "requests": [{
            "addSheet": {
                "properties": {
                    "title": sheet_name,
                    "gridProperties": {"rowCount": NEW_SHEET_ROWS, "columnCount": NEW_SHEET_COLS}
                }
            }
        }]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=add_req).execute()
    ss2 = _get_spreadsheet(service, spreadsheet_id)
    props2 = _get_sheet_props_by_title(ss2, sheet_name)
    print(f"새 시트 생성: {sheet_name}")
    return props2


def _get_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
    ss = _get_spreadsheet(service, spreadsheet_id)
    props = _get_sheet_props_by_title(ss, sheet_name)
    return props.get("sheetId") if props else None


def _ensure_date_format(service, spreadsheet_id: str, sheet_name: str, column_index: int = 0, pattern: str = "yyyy-mm-dd"):
    sheet_id = _get_sheet_id(service, spreadsheet_id, sheet_name)
    if sheet_id is None:
        print(f"[경고] 시트 ID를 찾을 수 없음: {sheet_name}")
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
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def _get_header(service, spreadsheet_id: str, sheet_name: str) -> List[str]:
    """
    첫 행(헤더) 가져오기. 없으면 빈 리스트
    """
    range_a1 = f"{sheet_name}!1:1"
    resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_a1).execute()
    values = resp.get("values", [])
    return values[0] if values else []


def _update_header_if_empty(service, spreadsheet_id: str, sheet_name: str, df_columns: List[str]):
    existing_header = _get_header(service, spreadsheet_id, sheet_name)
    if not existing_header:
        headers = df_columns + [TIMESTAMP_COLUMN]
        body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": [headers]}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body,
        ).execute()


def _add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out[TIMESTAMP_COLUMN] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return out


def _df_to_rows(df: pd.DataFrame) -> List[List[str]]:
    def _fmt(v):
        if pd.isna(v):
            return ""
        # 빈 문자열 그대로 반환
        if v == '':
            return ''
        # pandas Timestamp / datetime.date / datetime.datetime → ISO 날짜/시간
        if isinstance(v, pd.Timestamp):
            return v.strftime("%Y-%m-%d") if v.time() == pd.Timestamp(0).time() else v.strftime("%Y-%m-%d %H:%M:%S")
        from datetime import datetime as dt, date
        if isinstance(v, dt):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, date):
            return v.isoformat()
        # ⭐ 문자열 처리: 날짜 형식인지 먼저 확인
        if isinstance(v, str):
            # 날짜 형식 패턴 체크 (YYYY-MM-DD 또는 YYYY-MM-DD HH:MM:SS)
            import re
            date_pattern = r'^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$'
            if re.match(date_pattern, v.strip()):
                return v.strip()  # 날짜 문자열은 그대로 반환 (USER_ENTERED가 자동 변환)
            
            # 날짜가 아닌 경우 숫자로 변환 시도
            try:
                # 정수 확인
                if '.' not in v and v.replace('-', '').replace('+', '').isdigit():
                    return int(v)
                # 실수 확인
                float_val = float(v)
                return float_val
            except (ValueError, AttributeError):
                return v  # 문자열 그대로
        # 이미 숫자인 경우 그대로 반환
        if isinstance(v, (int, float)):
            return v
        return str(v)

    return df.map(_fmt).values.tolist()


# =========================
# 액션: 읽기/초기화
# =========================
def _execute_clear(service, spreadsheet_id: str, sheet_name: str) -> dict:
    """
    시트 전체 값 제거. 헤더도 제거되므로 필요시 다시 올려야 함.
    """
    print(f"초기화: {sheet_name}")
    clear_range = f"{sheet_name}"
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range,
            body={}
        ).execute()
        print("완료")
        return {"success": True}
    except Exception as e:
        print(f"실패: {str(e)}")
        return {"success": False, "error": str(e)}


def _execute_read(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    """
    시트 전체 읽기 → 첫 행을 헤더로 간주
    """
    print(f"읽기: {sheet_name}")
    try:
        range_a1 = f"{sheet_name}"
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_a1
        ).execute()
        values = resp.get("values", [])
        if not values:
            print("완료: 0건")
            return pd.DataFrame()
        header = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=header)
        print(f"완료: {len(df)}건")
        return df
    except Exception as e:
        print(f"실패: {str(e)}")
        return pd.DataFrame()


# =========================
# 쓰기: 덮어쓰기/추가/유니크 추가
# =========================
def _execute_overwrite(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    """
    헤더 + 데이터 전체 덮어쓰기
    """
    df_w = _add_timestamp_column(df)
    # 전체 지우기
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=sheet_name, body={}).execute()

    data = [df_w.columns.tolist()] + _df_to_rows(df_w)
    
    # ⭐ 디버깅: 실제 업로드되는 데이터 확인
    print(f"[디버그] 헤더: {data[0]}")
    print(f"[디버그] 첫 3행 샘플:")
    for i, row in enumerate(data[1:4], 1):
        print(f"  행{i}: {row[:5]}...")  # 첫 5개 컬럼만
    print(f"[디버그] 총 {len(data)}행 (헤더 포함)")
    
    body = {"range": f"{sheet_name}!A1", "majorDimension": "ROWS", "values": data}
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",  # ⭐ RAW → USER_ENTERED (숫자 자동 변환)
        body=body
    ).execute()
    
    print(f"[API 응답] updatedCells: {result.get('updatedCells', 0)}, updatedRows: {result.get('updatedRows', 0)}")
    print(f"완료 (덮어쓰기): {len(df_w)}건")
    return {"success": True, "new": len(df_w)}


def _execute_append(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame) -> dict:
    """
    데이터 행만 추가 (헤더는 유지). 중복 허용
    """
    df_w = _add_timestamp_column(df)
    values = _df_to_rows(df_w)
    body = {"majorDimension": "ROWS", "values": values}

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

    print(f"완료 (추가): {len(df_w)}건")
    return {"success": True, "new": len(df_w)}


def _create_composite_key(df: pd.DataFrame, key_columns: List[str]) -> pd.Series:
    return df[key_columns].astype(str).agg(KEY_SEPARATOR.join, axis=1)


def _resolve_key_columns(df: pd.DataFrame, primary_key: Union[str, List[str], None]) -> List[str]:
    if primary_key is None:
        return [c for c in df.columns if c != TIMESTAMP_COLUMN]
    return [primary_key] if isinstance(primary_key, str) else list(primary_key)


def _filter_new_rows(upload_df: pd.DataFrame, existing_df: pd.DataFrame, key_columns: List[str]) -> Tuple[pd.DataFrame, int]:
    has_existing = len(existing_df) > 0
    has_all_key_cols = all(c in existing_df.columns for c in key_columns)
    if not (has_existing and has_all_key_cols):
        return upload_df, 0

    existing_keys = _create_composite_key(existing_df, key_columns)
    upload_keys = _create_composite_key(upload_df, key_columns)

    is_new = ~upload_keys.isin(existing_keys)
    new_rows_df = upload_df[is_new]
    dup_cnt = len(upload_df) - len(new_rows_df)
    return new_rows_df, dup_cnt


def _execute_append_unique(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, primary_key: Union[str, List[str], None]) -> dict:
    """
    기존 데이터와 키 기준 비교하여 신규 행만 추가
    """
    # 기존 데이터 읽기
    existing_df = _execute_read(service, spreadsheet_id, sheet_name)
    df_w = _add_timestamp_column(df)

    key_cols = _resolve_key_columns(df, primary_key)
    new_rows_df, duplicate_count = _filter_new_rows(df_w, existing_df, key_cols)

    if len(new_rows_df) > 0:
        values = _df_to_rows(new_rows_df)
        body = {"majorDimension": "ROWS", "values": values}
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()

    print(f"완료: 신규 {len(new_rows_df)}건, 중복 {duplicate_count}건")
    return {"success": True, "new": len(new_rows_df), "duplicate": duplicate_count}


# =========================
# 공개 API
# =========================
def save_to_gsheet(
    df: Optional[pd.DataFrame] = None,
    sheet_name: Optional[str] = None,
    mode: str = MODE_APPEND_UNIQUE,
    action: Optional[str] = None,
    primary_key: Union[str, List[str], None] = None,
    credentials_path: Optional[str] = None,
    url: Optional[str] = None,
) -> Union[dict, pd.DataFrame]:
    """
    구글시트에 데이터 저장/읽기/초기화 (Sheets API 직행)
    - action == "clear": 시트 값 전체 삭제
    - df is None: 시트 읽기
    - mode: overwrite | append | append_unique
    """
    # 레거시 호환: df 위치에 sheet_name 문자열이 온 경우
    if isinstance(df, str) and sheet_name is None:
        sheet_name = df
        df = None

    sheet_name = sheet_name or DEFAULT_SHEET_NAME
    credentials_path = credentials_path or DEFAULT_CREDENTIALS_PATH
    spreadsheet_id = _parse_spreadsheet_id(url or DEFAULT_SPREADSHEET_URL)

    # 인증/서비스
    try:
        service = _build_sheets_service(credentials_path)
    except Exception as e:
        print(f"인증 실패: {str(e)}")
        return {"success": False, "error": str(e)}

    # 시트 존재 확인/생성 & 헤더 보장
    try:
        _add_sheet_if_missing(service, spreadsheet_id, sheet_name)
        if df is not None:
            _update_header_if_empty(service, spreadsheet_id, sheet_name, df.columns.tolist())
            # ⭐ 날짜 포맷 지정 제거 (Looker Studio 에러 방지)
            # order_daily는 문자열로 저장되므로 별도 포맷 지정 불필요
    except Exception as e:
        print(f"시트 준비 실패: {str(e)}")
        return {"success": False, "error": str(e)}

    # 액션 처리
    if action == ACTION_CLEAR:
        return _execute_clear(service, spreadsheet_id, sheet_name)

    # 읽기
    if df is None:
        return _execute_read(service, spreadsheet_id, sheet_name)

    # 쓰기
    print(f"업로드: {sheet_name} ({len(df)}건, {mode})")
    try:
        if mode == MODE_OVERWRITE:
            return _execute_overwrite(service, spreadsheet_id, sheet_name, df)
        elif mode == MODE_APPEND:
            return _execute_append(service, spreadsheet_id, sheet_name, df)
        else:
            return _execute_append_unique(service, spreadsheet_id, sheet_name, df, primary_key)
    except Exception as e:
        print(f"실패: {str(e)}")
        return {"success": False, "error": str(e)}


def apply_date_format_to_column(
    credentials_path: str,
    url: str,
    sheet_name: str,
    column_index: int = 0,
    pattern: str = "yyyy-mm-dd"
):
    """
    특정 열에 날짜 서식 적용
    
    Args:
        credentials_path: 서비스 계정 JSON 파일 경로
        url: 스프레드시트 URL 또는 ID
        sheet_name: 시트 이름
        column_index: 열 인덱스 (0=A열, 1=B열, ...)
        pattern: 날짜 패턴 (기본값: "yyyy-mm-dd")
    """
    service = _build_sheets_service(credentials_path)
    spreadsheet_id = _parse_spreadsheet_id(url)
    
    # 시트 ID 가져오기
    sheet_id = _get_sheet_id(service, spreadsheet_id, sheet_name)
    if sheet_id is None:
        raise ValueError(f"시트를 찾을 수 없음: {sheet_name}")
    
    # 날짜 서식 적용 요청
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
    
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    
    print(f"✅ {sheet_name}의 {chr(65+column_index)}열에 날짜 서식 적용 완료 (패턴: {pattern})")
