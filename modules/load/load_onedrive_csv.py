"""
OneDrive CSV 저장 모듈

============================================================================
사용법
============================================================================

from modules.load.load_onedrive_csv import load_to_onedrive_csv

# 기본 사용 (모든 컬럼 기준 중복 제거)
result = load_to_onedrive_csv(
    result_df=download_files_list,
    onedrive_csv_path="/path/to/file.csv",
    target_date="2025-12-30",
    platform="toorder",
    delete_local_files=False,
)

# 특정 컬럼 기준 중복 제거
result = load_to_onedrive_csv(
    result_df=download_files_list,
    onedrive_csv_path="/path/to/file.csv",
    target_date="2025-12-30",
    platform="toorder",
    duplicate_subset=['주문번호', '주문일시'],  # 이 컬럼들 기준으로만 중복 판단
    delete_local_files=False,
)

============================================================================
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Any


def load_to_onedrive_csv(
    result_df: Optional[pd.DataFrame] = None,
    df: Optional[List[str]] = None,
    onedrive_csv_path: str = None, #/opt/airflow/onedrive
    target_date: str = None,
    platform: str = "toorder",
    duplicate_subset: Optional[List[str]] = None,  # 추가
    delete_local_files: bool = False,
) -> Dict[str, Any]:
    """
    다운로드된 파일들을 OneDrive CSV로 저장
    
    Parameters:
        result_df: 이미 로드된 DataFrame (result_df와 df 중 하나만 전달)
        df: 로드할 파일 경로 리스트
        onedrive_csv_path: OneDrive CSV 저장 경로
        target_date: 데이터 수집 날짜 (format: "YYYY-MM-DD")
        platform: 플랫폼명 (기본값: "toorder")
        duplicate_subset: 중복 제거 기준 컬럼 리스트 (None이면 모든 컬럼 기준)
        delete_local_files: 저장 후 로컬 파일 삭제 여부
    
    Returns:
        저장 결과 딕셔너리
    """
    
    result = {
        "success": False,
        "total_rows": 0,
        "duplicates_removed": 0,
        "saved_path": None,
        "deleted_files": [],
        "message": "",
    }
    
    try:
        onedrive_path = Path(onedrive_csv_path)
        
        # 1. DataFrame 준비
        if result_df is not None:
            combined_df = result_df.copy()
        elif df is not None:
            combined_df = _load_files(df, target_date, platform)
        else:
            result["message"] = "result_df 또는 df 중 하나를 전달해야 합니다"
            return result
        
        if combined_df is None or combined_df.empty:
            result["message"] = "로드할 데이터가 없습니다"
            return result
        
        # 2. 기존 파일 병합
        if onedrive_path.exists():
            existing_df = pd.read_csv(onedrive_path)
            before_count = len(combined_df)
            
            combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
            
            # 중복 제거 (subset 파라미터 적용)
            # subset 컬럼이 실제로 존재하는지 확인
            if duplicate_subset:
                missing_cols = [col for col in duplicate_subset if col not in combined_df.columns]
                if missing_cols:
                    print(f"⚠️ 중복 제거 기준 컬럼 누락: {missing_cols}")
                    print(f"   사용 가능한 컬럼: {list(combined_df.columns)}")
                    # 존재하는 컬럼만 사용
                    valid_subset = [col for col in duplicate_subset if col in combined_df.columns]
                    if valid_subset:
                        duplicate_subset = valid_subset
                        print(f"   실제 사용 컬럼: {valid_subset}")
                    else:
                        duplicate_subset = None
                        print(f"   ➡️ 모든 컬럼 기준으로 중복 제거")
            
            combined_df.drop_duplicates(
                subset=duplicate_subset,  # None이면 모든 컬럼 기준
                keep='last', 
                inplace=True
            )
            
            duplicates = before_count + len(existing_df) - len(combined_df)
            result["duplicates_removed"] = duplicates
            
            # 중복 제거 기준 출력
            subset_info = f"기준: {duplicate_subset}" if duplicate_subset else "기준: 모든 컬럼"
            print(f"기존 {len(existing_df)}행 + 신규 {before_count}행 = 최종 {len(combined_df)}행")
            print(f"중복 {duplicates}개 제거 ({subset_info})")
        else:
            print(f"신규 파일 생성: {len(combined_df)}행")
        
        # 3. 파일 저장
        onedrive_path.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_csv(onedrive_path, index=False, encoding='utf-8-sig')
        
        result["success"] = True
        result["total_rows"] = len(combined_df)
        result["saved_path"] = str(onedrive_path)
        result["message"] = f"✅ 저장 완료: {len(combined_df)}행"
        
        print(result["message"])
        print(f"경로: {onedrive_path}")
        
        # 4. 로컬 파일 삭제 (옵션)
        if delete_local_files and df:
            deleted_files = []
            for file_path in df:
                try:
                    Path(file_path).unlink()
                    deleted_files.append(Path(file_path).name)
                    print(f"삭제: {Path(file_path).name}")
                except Exception as e:
                    print(f"삭제 실패 {Path(file_path).name}: {e}")
            
            result["deleted_files"] = deleted_files
        
        return result
    
    except Exception as e:
        result["message"] = f"❌ 저장 실패: {str(e)}"
        print(result["message"])
        import traceback
        traceback.print_exc()
        return result


def _load_files(
    df: List[str],
    target_date: str,
    platform: str,
) -> Optional[pd.DataFrame]:
    """파일 목록으로부터 DataFrame 생성"""
    
    if not df:
        return None
    
    all_dfs = []
    
    for file_path in df:
        try:
            path = Path(file_path)
            
            # Excel 또는 CSV 로드
            if path.suffix.lower() == '.xlsx':
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            # 메타데이터 추가
            df['platform'] = platform
            df['file_date'] = target_date
            all_dfs.append(df)
            
            print(f"  로드: {path.name} ({len(df)}행)")
        
        except Exception as e:
            print(f"  ⚠️ 로드 실패 {path.name}: {e}")
            continue
    
    if not all_dfs:
        return None
    
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"전체 로드 완료: {len(combined_df)}행")
    
    return combined_df