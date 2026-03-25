"""
OneDrive 업로드 통합 허브 모듈

기존 기능들을 중앙에서 관리:
- onedrive_csv_save() - 중복 제외 CSV 저장 (load_onedrive.py)
- load_to_onedrive_csv() - 다운로드 파일 병합 저장 (load_onedrive_csv.py)
- backup_to_onedrive() - 로컬 파일 백업 복사 (backup_to_onedrive.py)

사용 예시:
---------

from modules.transform.utility.onedrive import (
    save_to_onedrive_csv,
    merge_to_onedrive,
    backup_to_onedrive
)

# 1. 중복 제외 CSV 저장 (주문 데이터 등)
result = save_to_onedrive_csv(
    df=orders_df,
    file_path="C:/OneDrive/Doridang_DB/영업관리/매출/orders.csv",
    pk_col="order_id",
    timestamp_col="created_at",
    add_timestamp=True
)
print(f"신규: {result['inserted']}건, 중복: {result['duplicated']}건")

# 2. 다운로드 파일들을 합쳐서 OneDrive에 저장
result = merge_to_onedrive(
    df=download_df,
    onedrive_csv_path="C:/OneDrive/Doridang_DB/sales/toorder.csv",
    duplicate_subset=['주문번호', '주문일시']
)
print(f"저장 완료: {result['total_rows']}행")

# 3. 로컬 DB 파일을 OneDrive로 백업
result = backup_to_onedrive(
    local_file_path="C:/Local_DB/orders.csv",
    onedrive_file_path="C:/OneDrive/Doridang_DB/backup/orders.csv"
)
print(f"백업: {result['message']}")
"""

import logging
from pathlib import Path
from typing import Literal, Optional, List, Union

from modules.load.load_onedrive import onedrive_csv_save
from modules.load.load_onedrive_csv import load_to_onedrive_csv
from modules.load.backup_to_onedrive import backup_to_onedrive as _backup_to_onedrive
import pandas as pd

logger = logging.getLogger(__name__)


def save_to_onedrive_csv(
    df: pd.DataFrame,
    file_path: str,
    pk_col: str,
    timestamp_col: Optional[str] = None,
    add_timestamp: bool = False,
    if_exists: Literal["append", "replace"] = "append",
) -> dict:
    """
    중복 제외 후 OneDrive CSV에 저장 (주문, 매장 등 마스터 데이터용)

    Args:
        df: 저장할 DataFrame
        file_path: OneDrive CSV 경로
        pk_col: Primary key 컬럼명 (중복 체크 기준)
        timestamp_col: timestamp 컬럼명
        add_timestamp: timestamp 자동 추가 여부
        if_exists: "append" 또는 "replace"

    Returns:
        {"inserted": 신규 건수, "duplicated": 중복 건수, "total": 전체 건수}
    """
    logger.info(f"save_to_onedrive_csv 시작: {file_path}")

    result = onedrive_csv_save(
        df=df,
        file_path=file_path,
        pk_col=pk_col,
        timestamp_col=timestamp_col,
        add_timestamp=add_timestamp,
        if_exists=if_exists,
    )

    logger.info(f"저장 완료: 신규 {result['inserted']}건, 중복 {result['duplicated']}건")
    return result


def merge_to_onedrive(
    df: Optional[pd.DataFrame] = None,
    file_paths: Optional[List[str]] = None,
    onedrive_csv_path: str = None,
    target_date: Optional[str] = None,
    platform: str = "toorder",
    duplicate_subset: Optional[List[str]] = None,
    delete_local_files: bool = False,
) -> dict:
    """
    다운로드 파일들을 합쳐서 OneDrive CSV에 저장 (거래 데이터, 로그 등용)

    Args:
        df: 이미 로드된 DataFrame
        file_paths: 로드할 파일 경로 리스트
        onedrive_csv_path: OneDrive CSV 저장 경로
        target_date: 데이터 수집 날짜 (YYYY-MM-DD)
        platform: 플랫폼명 (기본값: "toorder")
        duplicate_subset: 중복 제거 기준 컬럼 리스트
        delete_local_files: 저장 후 로컬 파일 삭제 여부

    Returns:
        {"success": bool, "total_rows": int, "duplicates_removed": int, ...}
    """
    logger.info(f"merge_to_onedrive 시작: {onedrive_csv_path}")

    result = load_to_onedrive_csv(
        result_df=df,
        df=file_paths,
        onedrive_csv_path=onedrive_csv_path,
        target_date=target_date,
        platform=platform,
        duplicate_subset=duplicate_subset,
        delete_local_files=delete_local_files,
    )

    logger.info(f"병합 저장 완료: {result['total_rows']}행")
    return result


def backup_to_onedrive(
    local_file_path: Union[str, Path],
    onedrive_file_path: Union[str, Path],
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> dict:
    """
    로컬 DB 파일을 OneDrive로 백업 (주기적 자동 백업용)

    Args:
        local_file_path: 로컬 DB 파일 경로
        onedrive_file_path: OneDrive 대상 파일 경로
        max_retries: 최대 재시도 횟수
        retry_delay: 재시도 간 대기 시간(초)

    Returns:
        {"success": bool, "message": str, ...}
    """
    logger.info(f"backup_to_onedrive 시작: {local_file_path} → {onedrive_file_path}")

    result = _backup_to_onedrive(
        local_file_path=local_file_path,
        onedrive_file_path=onedrive_file_path,
        max_retries=max_retries,
        retry_delay=retry_delay,
    )

    if result.get("success"):
        logger.info(f"백업 완료: {result['message']}")
    else:
        logger.warning(f"백업 실패: {result['message']}")

    return result
