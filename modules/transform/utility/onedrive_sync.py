"""
OneDrive 동기화 충돌 방지 유틸리티

문제:
- Airflow DAG 실행 중 파일 쓰기
- OneDrive가 쓰기 중인 파일을 감지하고 이전 버전으로 업로드
- Airflow ↔ OneDrive 간 충돌 발생

해결책:
1. 임시 파일에 먼저 쓰기
2. OneDrive 동기화 대기
3. 원자적 교체 (rename/move)
4. 추가 동기화 대기
5. 선택: OneDrive 임시 폴더를 피한 위치에 저장
"""

import os
import time
import tempfile
from pathlib import Path
from typing import Callable, Optional


class OneDriveSafeWriter:
    """OneDrive 동기화 안전 파일 저장소"""
    
    # OneDrive 임시 파일 확장자들 (감지용)
    ONEDRIVE_TEMP_PATTERNS = ['.~', '~$', '.lock', '.tmp', '.odrive']
    
    def __init__(self, csv_path: Path, max_retries: int = 10, retry_delay: int = 2):
        """
        Parameters
        ----------
        csv_path : Path
            저장할 최종 CSV 파일 경로
        max_retries : int, default 10
            OneDrive 동기화 확인 재시도 횟수
        retry_delay : int, default 2
            초기 재시도 대기 시간(초)
        """
        self.csv_path = Path(csv_path)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backup_path = None
        self.tmp_path = None
    
    def wait_for_file_unlock(self, file_path: Path, timeout: int = 60) -> bool:
        """
        파일이 OneDrive에 의해 잠겨있지 않을 때까지 대기
        
        Parameters
        ----------
        file_path : Path
            확인할 파일 경로
        timeout : int, default 60
            최대 대기 시간(초)
        
        Returns
        -------
        bool
            성공하면 True, 타임아웃하면 False
        """
        if not file_path.exists():
            return True
        
        start_time = time.time()
        retry_count = 0
        
        while time.time() - start_time < timeout:
            try:
                # 파일을 읽기 모드로 열어서 접근 가능 확인
                with open(file_path, 'r') as f:
                    # 성공하면 잠금 해제됨
                    return True
                    
            except (IOError, OSError) as e:
                if retry_count < self.max_retries - 1:
                    wait_time = self.retry_delay * (retry_count + 1)
                    print(f"[OneDrive] 파일 접근 대기 {retry_count + 1}/{self.max_retries} "
                          f"({wait_time}초)... ({file_path.name})")
                    time.sleep(wait_time)
                    retry_count += 1
                else:
                    elapsed = time.time() - start_time
                    print(f"[경고] {file_path.name} 접근 제한 미해제 "
                          f"(경과: {elapsed:.1f}초). 계속 진행합니다.")
                    return False
        
        return False
    
    def check_onedrive_temp_files(self) -> bool:
        """
        OneDrive 임시 파일 존재 확인
        
        Returns
        -------
        bool
            임시 파일이 있으면 True
        """
        parent_dir = self.csv_path.parent
        
        for pattern in self.ONEDRIVE_TEMP_PATTERNS:
            if list(parent_dir.glob(f"*{pattern}*")):
                print(f"[OneDrive] 임시 파일 감지: {pattern}")
                return True
        
        return False
    
    def safe_write(self, write_func: Callable[..., None], *args, **kwargs) -> None:
        """
        OneDrive 안전 쓰기
        
        Parameters
        ----------
        write_func : Callable
            데이터를 쓸 콜러블 함수
            - 첫 번째 인자: 임시 파일 경로 (str)
        
        Examples
        --------
        writer = OneDriveSafeWriter(csv_path)
        writer.safe_write(df.to_csv, index=False, encoding='utf-8-sig')
        """
        
        try:
            # 1. 기존 파일 체크
            if self.csv_path.exists():
                print(f"[파일] 기존 파일 존재: {self.csv_path}")
                self.wait_for_file_unlock(self.csv_path)
            
            # 2. OneDrive 임시 파일 확인
            if self.check_onedrive_temp_files():
                print("[OneDrive] 동기화 완료 대기 중... (3초)")
                time.sleep(3)
            
            # 3. 폴더 생성 (필수!)
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 4. 임시 파일 생성
            with tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                dir=str(self.csv_path.parent),
                prefix='tmp_',
                suffix='.csv',
                encoding='utf-8-sig'
            ) as tmp_file:
                self.tmp_path = tmp_file.name
            
            print(f"[임시파일] 생성: {self.tmp_path}")
            
            # 5. 콜러블 함수로 쓰기 (첫 인자로 경로 전달)
            write_func(self.tmp_path, *args, **kwargs)
            print(f"[임시파일] 저장 완료")
            
            # 6. 동기화 대기
            time.sleep(2)
            
            # 7. 백업 생성 (롤백용)
            if self.csv_path.exists():
                self.backup_path = self.csv_path.parent / f"{self.csv_path.name}.bak"
                import shutil
                shutil.copy2(self.csv_path, self.backup_path)
                print(f"[백업] 생성: {self.backup_path}")
            
            # 8. 원자적 교체
            self._atomic_replace()
            print(f"[저장] 완료: {self.csv_path}")
            
            # 9. OneDrive 동기화 대기
            print("[OneDrive] 동기화 대기 중... (5초)")
            time.sleep(5)
            
            # 10. 백업 정리
            self._cleanup_backup()
            
        except Exception as e:
            print(f"[에러] 저장 실패: {e}")
            # 롤백
            if self.backup_path and self.backup_path.exists():
                import shutil
                print(f"[복구] 백업에서 복구: {self.backup_path}")
                shutil.copy2(self.backup_path, self.csv_path)
            # 임시 파일 정리
            self._cleanup_temp()
            raise
    
    def _atomic_replace(self) -> None:
        """원자적 파일 교체 (OS별)"""
        import shutil
        
        if self.tmp_path is None or not os.path.exists(self.tmp_path):
            raise FileNotFoundError(f"임시 파일이 없습니다: {self.tmp_path}")
        
        # Windows: os.replace() 사용 (기존 파일 덮어쓰기 가능)
        if os.name == 'nt':
            os.replace(self.tmp_path, str(self.csv_path))
        else:
            # Unix/Linux: shutil.move() 사용
            shutil.move(self.tmp_path, str(self.csv_path))
        
        self.tmp_path = None
    
    def _cleanup_temp(self) -> None:
        """임시 파일 정리"""
        if self.tmp_path and os.path.exists(self.tmp_path):
            try:
                os.remove(self.tmp_path)
                print(f"[정리] 임시 파일 제거")
            except Exception as e:
                print(f"[경고] 임시 파일 제거 실패: {e}")
    
    def _cleanup_backup(self) -> None:
        """백업 파일 정리"""
        if self.backup_path and self.backup_path.exists():
            try:
                os.remove(self.backup_path)
                print(f"[정리] 백업 파일 제거")
            except Exception as e:
                print(f"[경고] 백업 파일 제거 실패: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 지원"""
        if exc_type is not None:
            self._cleanup_temp()
            if self.backup_path and self.backup_path.exists():
                print(f"[복구] 오류 발생으로 백업 복구")
                import shutil
                shutil.copy2(self.backup_path, self.csv_path)
        
        self._cleanup_backup()


# 권장 사용법 (Context Manager)
def save_with_onedrive_sync(df, csv_path: Path, **to_csv_kwargs) -> None:
    """
    OneDrive 동기화 안전 CSV 저장
    
    Parameters
    ----------
    df : pd.DataFrame
        저장할 데이터프레임
    csv_path : Path
        CSV 저장 경로
    **to_csv_kwargs
        df.to_csv()의 추가 인자들
    
    Examples
    --------
    from modules.transform.utility.onedrive_sync import save_with_onedrive_sync
    
    save_with_onedrive_sync(
        df,
        Path('/opt/airflow/Doridang_DB/영업팀_DB/sales.csv'),
        index=False,
        encoding='utf-8-sig'
    )
    """
    with OneDriveSafeWriter(csv_path) as writer:
        writer.safe_write(df.to_csv, **to_csv_kwargs)
