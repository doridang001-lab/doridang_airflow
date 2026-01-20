"""
구글시트 추출(읽기) 함수

사용법:
    # 구글 스프레드시트 또는 구글 드라이브 엑셀 파일 모두 지원
    df = extract_gsheet(
        url="https://docs.google.com/spreadsheets/d/파일ID/edit...",
        sheet_name="시트이름",  # 선택사항, 없으면 첫 번째 시트
        file_name="파일이름"    # 선택사항, 파일명 검증용
    )

주요 기능:
    - 구글 스프레드시트 읽기 (자동 감지)
    - 구글 드라이브 엑셀(.xlsx) 파일 읽기 (자동 전환)
    - 특정 워크시트(탭) 선택 가능
    - URL 또는 파일 ID 모두 지원

주의사항:
    - 서비스 계정을 파일 공유에 추가해야 함
    - Docker 환경에서는 /opt/airflow/config/ 경로 사용
    - 로컬 환경에서는 credentials_path 파라미터로 경로 지정
"""
import re
import io
from typing import Optional
from pathlib import Path

import pandas as pd


DEFAULT_CREDENTIALS_PATH = '/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json'

GSHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
]

SPREADSHEET_ID_PATTERN = r"/d/([a-zA-Z0-9_-]+)"
FIRST_WORKSHEET_INDEX = 0


def _get_gspread_modules():
    try:
        import gspread
        from google.oauth2 import service_account
        return gspread, service_account
    except ImportError as e:
        raise ImportError("pip install gspread google-auth 필요") from e


def _get_drive_api_modules():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        return service_account, build, MediaIoBaseDownload
    except ImportError as e:
        raise ImportError("pip install google-api-python-client google-auth 필요") from e


def _parse_spreadsheet_id_from_url(url: str) -> str:
    url_match = re.search(SPREADSHEET_ID_PATTERN, url)
    
    if not url_match:
        raise ValueError("올바른 구글시트 URL 형식이 아닙니다.")
    
    return url_match.group(1)


def _resolve_spreadsheet_id(
    spreadsheet_id: Optional[str],
    url: Optional[str],
) -> str:
    if spreadsheet_id:
        return spreadsheet_id
    
    if not url:
        raise ValueError("url 또는 spreadsheet_id 중 하나는 반드시 필요합니다.")
    
    return _parse_spreadsheet_id_from_url(url)


def _validate_credentials_path(credentials_path: str):
    if not Path(credentials_path).exists():
        raise FileNotFoundError(
            f"서비스 계정 JSON 파일이 존재하지 않습니다: {credentials_path}"
        )


def _create_gsheet_client(credentials_path: str):
    gspread, service_account = _get_gspread_modules()
    
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=GSHEET_SCOPES,
    )
    
    return gspread.authorize(creds)


def _validate_file_name(spreadsheet, expected_name: str):
    actual_name = spreadsheet.title
    is_name_mismatch = actual_name != expected_name
    
    if is_name_mismatch:
        raise ValueError(
            f"파일명 불일치: 전달='{expected_name}', 실제='{actual_name}'"
        )


def _read_worksheet_as_dataframe(spreadsheet, sheet_name: Optional[str] = None) -> pd.DataFrame:
    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)
    else:
        worksheet = spreadsheet.get_worksheet(FIRST_WORKSHEET_INDEX)
    return pd.DataFrame(worksheet.get_all_records())


def _extract_excel_from_gdrive(file_id: str, sheet_name: Optional[str], credentials_path: str) -> pd.DataFrame:
    """구글 드라이브에서 엑셀 파일을 다운로드하여 읽기"""
    service_account, build, MediaIoBaseDownload = _get_drive_api_modules()
    
    # 인증
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    
    # Drive API 클라이언트 생성
    service = build('drive', 'v3', credentials=creds)
    
    # 파일 다운로드
    request = service.files().get_media(fileId=file_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    file_buffer.seek(0)
    
    # 엑셀 파일 읽기
    if sheet_name:
        return pd.read_excel(file_buffer, sheet_name=sheet_name)
    else:
        return pd.read_excel(file_buffer)


def extract_gsheet(
    *,
    file_name: Optional[str] = None,
    sheet_name: Optional[str] = None,
    url: Optional[str] = None,
    spreadsheet_id: Optional[str] = None,
    credentials_path: Optional[str] = None,
) -> pd.DataFrame:
    """구글시트 또는 구글 드라이브 엑셀 파일에서 데이터를 DataFrame으로 반환
    
    Args:
        file_name: 스프레드시트 파일 이름 검증용 (선택)
        sheet_name: 워크시트(탭) 이름. 없으면 첫 번째 시트
        url: 구글시트 URL
        spreadsheet_id: 구글시트/파일 ID (url 대신 사용 가능)
        credentials_path: 서비스 계정 JSON 파일 경로
    
    Returns:
        DataFrame: 읽어온 데이터
    """
    credentials_path = credentials_path or DEFAULT_CREDENTIALS_PATH
    
    _validate_credentials_path(credentials_path)
    
    resolved_id = _resolve_spreadsheet_id(spreadsheet_id, url)
    
    # 먼저 구글 스프레드시트로 시도
    try:
        client = _create_gsheet_client(credentials_path)
        spreadsheet = client.open_by_key(resolved_id)
        
        if file_name:
            _validate_file_name(spreadsheet, file_name)
        
        return _read_worksheet_as_dataframe(spreadsheet, sheet_name)
    
    except Exception as gsheet_error:
        # 구글 스프레드시트 접근 실패 시, 구글 드라이브 엑셀 파일로 시도
        error_msg = str(gsheet_error)
        
        if "This operation is not supported for this document" in error_msg or "APIError" in error_msg:
            try:
                return _extract_excel_from_gdrive(resolved_id, sheet_name, credentials_path)
            except Exception as drive_error:
                raise RuntimeError(
                    f"구글시트 및 드라이브 엑셀 파일 읽기 모두 실패.\n"
                    f"구글시트 오류: {gsheet_error}\n"
                    f"드라이브 오류: {drive_error}"
                ) from drive_error
        else:
            raise RuntimeError(f"구글시트 접근 실패: {gsheet_error}") from gsheet_error