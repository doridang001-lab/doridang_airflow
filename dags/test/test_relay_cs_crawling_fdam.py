# dags/relay_cs_crawling_fdam.py

"""
Relay FMS 매장CS 크롤링 DAG
- 로컬 DB 저장 → OneDrive 백업
"""

import os
import sys
from pathlib import Path
import shutil
from datetime import datetime

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.smtp.operators.smtp import EmailOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract.crawling_relay_cs_fdam import run_relay_cs_crawling

# 현재 파일명
filename = os.path.basename(__file__)

# ========== 설정 ==========
RELAY_ID = "조민준"
RELAY_PW = "1234"

# 로컬 DB 경로 (컨테이너 안)
LOCAL_CSV = Path("/opt/airflow/Doridang/문의_DB/relay_cs.csv")

# OneDrive 백업 경로
ONEDRIVE_BACKUP = Path("/opt/airflow/onedrive_backup/문의_DB")

to_members = ['a17019@kakao.com']


# ============================================================
# 함수: CSV 저장 (중복 제거)
# ============================================================
def save_cs_to_csv(
    new_df: pd.DataFrame,
    csv_path: Path,
    duplicate_subset: list = None
) -> dict:
    """
    CS 데이터를 CSV에 저장 (중복 제거 후 신규건만 추가)
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    if duplicate_subset is None:
        duplicate_subset = ['접수_접수번호']
    
    try:
        # 기존 CSV 로드
        if csv_path.exists():
            existing_df = pd.read_csv(csv_path, encoding='utf-8-sig')
            print(f"📁 기존 데이터: {len(existing_df)}건")
            
            # 중복 제거
            before_count = len(new_df)
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            merged_df = merged_df.drop_duplicates(
                subset=duplicate_subset, 
                keep='last'
            )
            
            new_count = len(merged_df) - len(existing_df)
            print(f"✅ 신규 데이터: {new_count}건 (총 {before_count}건 중)")
            
        else:
            merged_df = new_df
            new_count = len(new_df)
            print(f"✅ 신규 파일 생성: {new_count}건")
        
        # CSV 저장
        merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        return {
            'success': True,
            'total': len(merged_df),
            'new': new_count,
            'message': f'저장 완료: 총 {len(merged_df)}건 (신규 {new_count}건)'
        }
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': f'저장 실패: {e}'
        }


# ============================================================
# 함수: OneDrive 백업
# ============================================================
def backup_file_to_onedrive(
    local_file_path: Path,
    onedrive_path: Path,
    max_backups: int = 20
) -> dict:
    """
    로컬 파일을 OneDrive로 백업
    """
    try:
        onedrive_path.mkdir(parents=True, exist_ok=True)
        
        # 타임스탬프 백업
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = onedrive_path / f"backup_{timestamp}_{local_file_path.name}"
        
        # 복사
        shutil.copy2(local_file_path, backup_file)
        print(f"✅ 파일 백업: {backup_file}")
        
        # 오래된 백업 정리
        backups = sorted(onedrive_path.glob("backup_*.csv"))
        if len(backups) > max_backups:
            for old_backup in backups[:-max_backups]:
                old_backup.unlink()
                print(f"🗑️ 오래된 백업 삭제: {old_backup.name}")
        
        return {
            'success': True,
            'backup_path': str(backup_file),
            'message': f'백업 완료: {backup_file.name}'
        }
        
    except Exception as e:
        print(f"❌ 백업 실패: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': f'백업 실패: {e}'
        }


# ============================================================
# Task 1: 크롤링
# ============================================================
def crawl_relay_cs(**context):
    """Relay FMS 매장CS 크롤링"""
    df = run_relay_cs_crawling(
        user_id=RELAY_ID,
        password=RELAY_PW,
        headless=True,  # 서버 환경
        window_size='normal'
    )
    
    # XCom에 저장
    context['task_instance'].xcom_push(key='df_json', value=df.to_json())
    context['task_instance'].xcom_push(key='row_count', value=len(df))
    
    print(f"✅ 크롤링 완료: {len(df)}건")
    return f"크롤링 완료: {len(df)}건"


# ============================================================
# Task 2: 로컬 CSV 저장
# ============================================================
def save_to_local_csv(**context):
    """로컬 DB에 CSV 저장"""
    ti = context['task_instance']
    df_json = ti.xcom_pull(task_ids='crawl_task', key='df_json')
    
    df = pd.read_json(df_json)
    
    result = save_cs_to_csv(
        new_df=df,
        csv_path=LOCAL_CSV,
        duplicate_subset=['접수_접수번호']
    )
    
    # XCom에 결과 저장
    ti.xcom_push(key='save_result', value=result['message'])
    
    print(result['message'])
    return result['message']


# ============================================================
# Task 3: OneDrive 백업
# ============================================================
def backup_to_onedrive_task(**context):
    """OneDrive로 백업"""
    result = backup_file_to_onedrive(
        local_file_path=LOCAL_CSV,
        onedrive_path=ONEDRIVE_BACKUP,
        max_backups=20
    )
    
    # XCom에 저장
    context['task_instance'].xcom_push(key='backup_result', value=result['message'])
    
    print(result['message'])
    return result['message']


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule="0 9 * * *",  # 매일 오전 9시
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=['crawling', 'relay', 'cs', 'fdam']
) as dag:
    
    # Task 1: 크롤링
    crawl_task = PythonOperator(
        task_id='crawl_task',
        python_callable=crawl_relay_cs,
    )
    
    # Task 2: 로컬 CSV 저장
    save_task = PythonOperator(
        task_id='save_local_csv',
        python_callable=save_to_local_csv,
    )
    
    # Task 3: OneDrive 백업
    backup_task = PythonOperator(
        task_id='backup_to_onedrive',
        python_callable=backup_to_onedrive_task,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Task 4: 이메일 알림
    email_task = EmailOperator(
        task_id='send_email',
        conn_id='conn_smtp_gmail',
        to=to_members,
        subject='[Relay CS] 크롤링 완료',
        html_content="""
        <html>
            <body>
                <h3>✅ Relay FMS 매장CS 크롤링 완료</h3>
                
                <p><strong>📊 수집 결과:</strong></p>
                <pre>{{ task_instance.xcom_pull(task_ids='crawl_task', key='return_value') }}</pre>
                
                <p><strong>💾 저장 결과:</strong></p>
                <pre>{{ task_instance.xcom_pull(task_ids='save_local_csv', key='save_result') }}</pre>
                
                <p><strong>☁️ 백업 결과:</strong></p>
                <pre>{{ task_instance.xcom_pull(task_ids='backup_to_onedrive', key='backup_result') }}</pre>
                
                <hr>
                <p style="color: gray; font-size: 12px;">
                    실행 시간: {{ ts }}<br>
                    DAG ID: {{ dag.dag_id }}
                </p>
            </body>
        </html>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    # Task 의존성
    crawl_task >> save_task >> backup_task >> email_task