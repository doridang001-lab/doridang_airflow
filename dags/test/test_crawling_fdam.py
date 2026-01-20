# dags/relay_cs_crawling_fdam.py

"""
Relay FMS 매장CS 크롤링 DAG
- 로그인 → 서버 추가 → 크롤링 → 로컬 DB 저장 → OneDrive 백업
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
from airflow.providers.smtp.operators.smtp import EmailOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract.crawling_relay_cs_fdam import run_relay_cs_crawling

# 현재 파일명 (DAG ID로 사용)
filename = os.path.basename(__file__)

# ========== 설정 ==========
RELAY_ID = "조민준"
RELAY_PW = "1234"
SERVER_NAME = "도리당"
SERVER_NUMBER = "10625"

# 로컬 DB 경로 (컨테이너 안)
LOCAL_CSV = Path("/opt/airflow/Doridang/문의_DB/relay_cs.csv")

# OneDrive 백업 경로
ONEDRIVE_BACKUP = Path("/opt/airflow/onedrive_backup/문의_DB")

to_members = ['a17019@kakao.com']


# ============================================================
# 저장 함수
# ============================================================
def save_relay_cs_to_csv(
    df: pd.DataFrame,
    local_csv_path: Path,
    onedrive_backup_path: Path,
    duplicate_subset: list = None
) -> dict:
    """
    Relay CS 데이터를 로컬 CSV에 저장하고 OneDrive로 백업
    """
    if duplicate_subset is None:
        duplicate_subset = ['접수_접수번호']
    
    try:
        local_csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 기존 CSV 로드 및 병합
        if local_csv_path.exists():
            existing_df = pd.read_csv(local_csv_path, encoding='utf-8-sig')
            print(f"📁 기존 데이터: {len(existing_df)}건")
            
            merged_df = pd.concat([existing_df, df], ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=duplicate_subset, keep='last')
            
            new_count = len(merged_df) - len(existing_df)
            print(f"✅ 신규 데이터: {new_count}건")
            
        else:
            merged_df = df
            new_count = len(df)
            print(f"✅ 신규 파일 생성: {new_count}건")
        
        # 로컬 CSV 저장
        merged_df.to_csv(local_csv_path, index=False, encoding='utf-8-sig')
        print(f"💾 로컬 저장 완료: {local_csv_path}")
        
        # OneDrive 백업
        onedrive_backup_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = onedrive_backup_path / f"backup_{timestamp}_relay_cs.csv"
        
        shutil.copy2(local_csv_path, backup_file)
        print(f"☁️ OneDrive 백업 완료: {backup_file.name}")
        
        # 오래된 백업 정리 (최근 20개만)
        backups = sorted(onedrive_backup_path.glob("backup_*.csv"))
        if len(backups) > 20:
            for old_backup in backups[:-20]:
                old_backup.unlink()
                print(f"🗑️ 오래된 백업 삭제: {old_backup.name}")
        
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
# Task 1: 크롤링
# ============================================================
def crawl_relay_cs(**context):
    """Relay FMS 매장CS 크롤링 실행"""
    
    # 크롤링 실행
    result_df = run_relay_cs_crawling(
        user_id=RELAY_ID,
        password=RELAY_PW,
        headless=True,
        server_name=SERVER_NAME,
        server_number=SERVER_NUMBER
    )
    
    # XCom에 DataFrame 저장
    context['task_instance'].xcom_push(key='df_json', value=result_df.to_json())
    
    print(f"크롤링 완료: {len(result_df)}건")
    return f"크롤링 완료: {len(result_df)}건"


# ============================================================
# Task 2: CSV 저장 및 백업
# ============================================================
def save_relay_cs(**context):
    """크롤링한 데이터를 로컬 CSV에 저장하고 OneDrive 백업"""
    ti = context['task_instance']
    df_json = ti.xcom_pull(task_ids='crawl_task', key='df_json')
    
    if not df_json:
        print("❌ 크롤링 데이터 없음")
        return "저장 실패: 데이터 없음"
    
    # JSON → DataFrame
    df = pd.read_json(df_json)
    
    if df.empty:
        print("❌ DataFrame이 비어있습니다")
        return "저장 실패: 빈 DataFrame"
    
    # CSV 저장 및 백업
    result = save_relay_cs_to_csv(
        df=df,
        local_csv_path=LOCAL_CSV,
        onedrive_backup_path=ONEDRIVE_BACKUP,
        duplicate_subset=['접수_접수번호']
    )
    
    return result.get("message", "저장 완료")


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
    
    # Task 2: CSV 저장
    save_task = PythonOperator(
        task_id='save_task',
        python_callable=save_relay_cs,
    )
    
    # Task 3: 이메일 알림
    email_task = EmailOperator(
        task_id='send_email_task',
        conn_id='conn_smtp_gmail',
        to=to_members,
        subject='[Relay CS] 크롤링 완료',
        html_content="""
        <html>
            <body>
                <h3>✅ Relay FMS 매장CS 크롤링 완료</h3>
                <p><strong>수집 결과:</strong></p>
                <pre>{{ task_instance.xcom_pull(task_ids='crawl_task', key='return_value') }}</pre>
                
                <p><strong>저장 결과:</strong></p>
                <pre>{{ task_instance.xcom_pull(task_ids='save_task', key='return_value') }}</pre>
                
                <hr>
                <p style="color: gray; font-size: 12px;">
                    실행 시간: {{ ts }}<br>
                    DAG ID: {{ dag.dag_id }}
                </p>
            </body>
        </html>
        """,
    )
    
    # Task 의존성
    crawl_task >> save_task >> email_task