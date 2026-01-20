"""
로컬 파일 추출(읽기) 함수
"""
from pathlib import Path
from typing import List

import pandas as pd


BASE_DIR = Path("/opt/airflow/onedrive")

EXTENSION_CSV = ".csv"
EXTENSION_XLSX = ".xlsx"
EXTENSION_XLS = ".xls"
SUPPORTED_EXCEL_EXTENSIONS = {EXTENSION_XLSX, EXTENSION_XLS}

SELECT_LATEST = "latest"
SELECT_FIRST = "first"

DEFAULT_ENCODING = "utf-8-sig"
DEFAULT_SHEET_INDEX = 0

GLOB_WILDCARDS = ("*", "?")


def _is_glob_pattern(file_name: str) -> bool:
    return any(wildcard in file_name for wildcard in GLOB_WILDCARDS)


def _find_files_by_pattern(pattern: str) -> List[Path]:
    files = list(BASE_DIR.glob(pattern))
    
    if not files:
        raise FileNotFoundError(f"패턴에 맞는 파일이 없습니다: {pattern}")
    
    return files


def _select_file_from_list(files: List[Path], select: str) -> Path:
    if select == SELECT_LATEST:
        return max(files, key=lambda p: p.stat().st_mtime)
    return files[0]


def _resolve_file_path(file_name: str, select: str) -> Path:
    if _is_glob_pattern(file_name):
        files = _find_files_by_pattern(file_name)
        return _select_file_from_list(files, select)
    
    path = BASE_DIR / file_name
    
    if not path.exists():
        raise FileNotFoundError(f"파일이 존재하지 않습니다: {path}")
    
    return path


def _read_csv_file(path: Path, encoding: str) -> pd.DataFrame:
    return pd.read_csv(path, encoding=encoding)


def _read_excel_file(path: Path, sheet_name: str | int) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name)


def _read_file_as_dataframe(
    path: Path,
    sheet_name: str | int,
    encoding: str,
) -> pd.DataFrame:
    suffix = path.suffix.lower()
    
    if suffix == EXTENSION_CSV:
        return _read_csv_file(path, encoding)
    
    if suffix in SUPPORTED_EXCEL_EXTENSIONS:
        return _read_excel_file(path, sheet_name)
    
    raise ValueError(f"지원하지 않는 파일 형식입니다: {suffix}")


def _filter_columns(df: pd.DataFrame, columns: List[str] | None) -> pd.DataFrame:
    if columns is None:
        return df
    return df[columns]


def _delete_file(path: Path):
    try:
        path.unlink()
        print(f"파일 삭제 완료: {path}")
    except Exception as e:
        print(f"파일 삭제 실패: {path}, 오류: {e}")


def read_onedrive_file(
    file_name: str,
    sheet_name: str | int | None = DEFAULT_SHEET_INDEX,
    columns: List[str] | None = None,
    encoding: str = DEFAULT_ENCODING,
    select: str = SELECT_LATEST,
    delete_after: bool = False,
) -> pd.DataFrame:
    """로컬 파일(csv, xlsx)을 DataFrame으로 반환"""
    path = _resolve_file_path(file_name, select)
    
    df = _read_file_as_dataframe(path, sheet_name, encoding)
    df = _filter_columns(df, columns)
    
    if delete_after:
        _delete_file(path)
    
    return df