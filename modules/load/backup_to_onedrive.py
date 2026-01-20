"""
로컬 DB -> OneDrive 백업 모듈
로컬 DB에 저장된 CSV 파일을 OneDrive로 복사
"""

import shutil
from pathlib import Path
from typing import Union
import time


def backup_to_onedrive(
    local_file_path: Union[str, Path],
    onedrive_file_path: Union[str, Path],
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> dict:
    """
    로컬 DB 파일을 OneDrive로 백업 복사
    
    Args:
        local_file_path: 로컬 DB 파일 경로
        onedrive_file_path: OneDrive 대상 파일 경로
        max_retries: 최대 재시도 횟수
        retry_delay: 재시도 간 대기 시간(초)
    
    Returns:
        dict: {"success": bool, "message": str, "local_path": str, "onedrive_path": str}
    """
    
    local_path = Path(local_file_path)
    onedrive_path = Path(onedrive_file_path)
    
    # 로컬 파일 존재 확인
    if not local_path.exists():
        return {
            "success": False,
            "message": f"로컬 파일이 존재하지 않습니다: {local_path}",
            "local_path": str(local_path),
            "onedrive_path": str(onedrive_path)
        }
    
    # OneDrive 디렉터리 생성 (권한 오류 시 무시)
    try:
        onedrive_path.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        print(f"⚠️ [경고] OneDrive 디렉터리 생성 실패 (권한 오류): {e}")
        print(f"   이미 존재하는 경로인지 확인 후 백업을 시도합니다.")
        # 디렉토리가 없으면 경고만 출력하고 계속 진행
        if not onedrive_path.parent.exists():
            print(f"⚠️ [경고] OneDrive 경로가 존재하지 않음: {onedrive_path.parent}")
            print(f"   백업을 건너뛰고 로컬 DB 저장만 완료합니다.")
            return {
                "success": False,
                "message": f"OneDrive 디렉터리 생성 실패 (권한 없음): {onedrive_path.parent}",
                "local_path": str(local_path),
                "onedrive_path": str(onedrive_path),
                "warning": True  # 경고 표시
            }
    except Exception as e:
        print(f"⚠️ [경고] OneDrive 디렉터리 생성 중 오류: {e}")
        if not onedrive_path.parent.exists():
            print(f"   백업을 건너뛰고 로컬 DB 저장만 완료합니다.")
            return {
                "success": False,
                "message": f"OneDrive 디렉터리 생성 실패: {e}",
                "local_path": str(local_path),
                "onedrive_path": str(onedrive_path),
                "warning": True
            }
    
    # 재시도 로직으로 복사
    for attempt in range(1, max_retries + 1):
        try:
            # 파일 복사
            shutil.copy2(local_path, onedrive_path)
            
            # 복사 성공 확인
            if onedrive_path.exists():
                file_size = onedrive_path.stat().st_size
                print(f"✅ [백업 완료] {local_path.name} → OneDrive ({file_size:,} bytes)")
                return {
                    "success": True,
                    "message": f"백업 완료 ({file_size:,} bytes)",
                    "local_path": str(local_path),
                    "onedrive_path": str(onedrive_path)
                }
            
        except PermissionError as e:
            if attempt < max_retries:
                print(f"⚠️ [백업 재시도 {attempt}/{max_retries}] 권한 오류: {e}")
                time.sleep(retry_delay)
            else:
                return {
                    "success": False,
                    "message": f"권한 오류 (재시도 {max_retries}회 실패): {e}",
                    "local_path": str(local_path),
                    "onedrive_path": str(onedrive_path)
                }
        
        except Exception as e:
            return {
                "success": False,
                "message": f"백업 실패: {e}",
                "local_path": str(local_path),
                "onedrive_path": str(onedrive_path)
            }
    
    return {
        "success": False,
        "message": "알 수 없는 오류",
        "local_path": str(local_path),
        "onedrive_path": str(onedrive_path)
    }


def backup_directory_to_onedrive(
    local_dir: Union[str, Path],
    onedrive_dir: Union[str, Path],
    file_pattern: str = "*.csv"
) -> dict:
    """
    로컬 DB 디렉터리의 모든 파일을 OneDrive로 백업
    
    Args:
        local_dir: 로컬 DB 디렉터리 경로
        onedrive_dir: OneDrive 대상 디렉터리 경로
        file_pattern: 백업할 파일 패턴 (기본: "*.csv")
    
    Returns:
        dict: {"success": int, "failed": int, "total": int, "results": list}
    """
    
    local_path = Path(local_dir)
    onedrive_path = Path(onedrive_dir)
    
    if not local_path.exists():
        print(f"❌ 로컬 디렉터리가 존재하지 않습니다: {local_path}")
        return {"success": 0, "failed": 0, "total": 0, "results": []}
    
    # 패턴에 맞는 파일 찾기
    files = list(local_path.glob(file_pattern))
    
    if not files:
        print(f"ℹ️ 백업할 파일이 없습니다: {local_path} ({file_pattern})")
        return {"success": 0, "failed": 0, "total": 0, "results": []}
    
    results = []
    success_count = 0
    failed_count = 0
    
    print(f"📦 백업 시작: {len(files)}개 파일")
    
    for file in files:
        target_file = onedrive_path / file.name
        result = backup_to_onedrive(file, target_file)
        results.append(result)
        
        if result["success"]:
            success_count += 1
        else:
            failed_count += 1
            print(f"❌ 백업 실패: {file.name} - {result['message']}")
    
    print(f"📊 백업 완료: 성공 {success_count}개 | 실패 {failed_count}개 | 전체 {len(files)}개")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "total": len(files),
        "results": results
    }
