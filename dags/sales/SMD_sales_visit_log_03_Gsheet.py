"""
매출 이상 알림 데이터를 구글 시트에 업로드하는 DAG

visit_sales_log_master.csv → Google Sheet (덮어쓰기)

📋 처리 흐름:
1. CSV 파일 읽기 (sales_daily_orders_alerts.csv)
2. 데이터 정리 (NaN 처리, 날짜 정규화)
3. **시트 크기 리셋** (누적 방지) ← 추가됨
4. Google Sheets 업로드 (overwrite 모드)
"""

import pendulum
import pandas as pd
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.transform.utility.io import SMD_ORDERS_TIME
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime, timedelta
from modules.transform.utility.io import SMD_ORDERS_TIME, SMD_VISIT_LOG

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)


def _latest_successful_execution_date(dt, **context):
    """외부 DAG의 최신 성공 실행일을 사용 (없으면 현재 dt 사용)."""
    runs = DagRun.find(dag_id='SMD_sales_visit_log_02_transform_Dags', state=State.SUCCESS)
    if not runs:
        return dt
    return max(r.execution_date for r in runs)

from modules.transform.utility.paths import LOCAL_DB
from modules.load.load_gsheet import save_to_gsheet

# ============================================================
# 설정
# ============================================================
# Linux 경로 사용 (Docker 컨테이너 내부)
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

# Google Sheets 설정
ALERTS_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1irF9Sbqc7-x9o_tT6lipKwQPLVV-euTp-_Ph0F4ZB8U/edit?usp=sharing"
ALERTS_SHEET_NAME = "시트1"

# CSV 경로
ALERTS_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'visit_sales_log_master.csv'


# ============================================================
# Google Sheets 업로드 함수
# ============================================================
def upload_alerts_to_gsheet(**context):
    """
    sales_daily_orders_upload_fin.csv를 구글 시트에 업로드
    
    처리 순서:
    1. CSV 파일 읽기
    2. NaN → 빈 문자열 변환
    3. order_daily 날짜 정규화 (엑셀 직렬값/텍스트 모두 처리)
    4. **시트 크기 리셋** (셀 누적 방지)
    5. Google Sheets에 덮어쓰기 (overwrite 모드)
    """
    print(f"\n{'='*60}")
    print(f"[구글시트] 매출 이상 알림 데이터 업로드 시작 (덮어쓰기)")
    print(f"[경로] {ALERTS_CSV_PATH}")
    
    # ============================================================
    # 1️⃣ CSV 파일 확인
    # ============================================================
    if not ALERTS_CSV_PATH.exists():
        print(f"[에러] CSV 파일 없음: {ALERTS_CSV_PATH}")
        return f"업로드 실패: 파일 없음"
    
    # ============================================================
    # 2️⃣ CSV 읽기 (인코딩 자동 감지)
    # ============================================================
    try:
        df = None
        for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
            try:
                df = pd.read_csv(ALERTS_CSV_PATH, encoding=encoding)
                print(f"[CSV] 읽기 성공: {len(df):,}건 ({encoding})")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise ValueError("모든 인코딩 시도 실패")
            
    except Exception as e:
        print(f"[에러] CSV 읽기 실패: {e}")
        return f"업로드 실패: CSV 읽기 오류"
    
    if len(df) == 0:
        print(f"[경고] CSV가 비어있음")
        return "업로드: 데이터 없음"
    
    # ============================================================
    # 3️⃣ 데이터 정리
    # ============================================================
    
    # NaN을 빈 문자열로 변환
    print(f"\n[데이터 정리] NaN → 빈 문자열로 변환")
    nan_count = df.isna().sum().sum()
    print(f"[데이터 정리] 변환할 NaN 개수: {nan_count}개")
    
    df = df.fillna('')
    print(f"[데이터 정리] ✅ NaN 변환 완료")

    # Infinity 값을 None으로 변환 (JSON serialization 오류 방지)
    print(f"\n[데이터 정리] Infinity → None으로 변환 (Google Sheets JSON 호환성)")
    import numpy as np
    inf_count = 0
    for col in df.select_dtypes(include=[np.floating]).columns:
        mask = np.isinf(df[col])
        inf_count += mask.sum()
        df.loc[mask, col] = None
    print(f"[데이터 정리] 변환한 Infinity 개수: {inf_count}개")
    
    # Infinity 변환 후 다시 NaN을 빈 문자열로
    df = df.fillna('')


    # order_daily를 날짜 타입으로 정규화
    if 'order_daily' in df.columns:
        sample = df['order_daily'].head(3).tolist()
        print(f"\n[데이터 정리] order_daily 변환 전 샘플: {sample}")
        
        try:
            # 엑셀 직렬값 → 날짜
            serial_parsed = pd.to_datetime(
                pd.to_numeric(df['order_daily'], errors='coerce'), 
                unit='D', 
                origin='1899-12-30'
            )
            
            # 텍스트 날짜 → 날짜
            text_parsed = pd.to_datetime(df['order_daily'], errors='coerce')
            
            # 둘 중 유효한 값 선택
            order_daily_dt = serial_parsed.fillna(text_parsed)
            
            # NaT는 빈값 유지
            df['order_daily'] = order_daily_dt.dt.date
            
            print(f"[데이터 정리] order_daily 변환 후 샘플: {df['order_daily'].head(3).tolist()}")
            
        except Exception as e:
            print(f"[경고] order_daily 변환 실패: {e}")
    else:
        print(f"[데이터 정리] order_daily 컬럼 없음")
    
    print(f"\n[데이터] 행: {len(df):,}건, 열: {len(df.columns)}개")
    print(f"[컬럼] {', '.join(df.columns.tolist()[:10])}...")  # 처음 10개만 표시
    
    # ============================================================
    # 3.5️⃣ 시트 크기 리셋 (셀 누적 방지) ← 🔥 핵심 추가 부분
    # ============================================================
    print(f"\n[시트 크기 리셋] 시작...")
    
    try:
        from google.oauth2.service_account import Credentials
        import gspread
        
        # 인증
        creds = Credentials.from_service_account_file(
            DEFAULT_CREDENTIALS_PATH,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(creds)
        
        # 스프레드시트 열기
        spreadsheet_id = ALERTS_GSHEET_URL.split('/d/')[1].split('/')[0]
        sh = gc.open_by_key(spreadsheet_id)
        
        # 시트 찾기 or 생성
        try:
            ws = sh.worksheet(ALERTS_SHEET_NAME)
            old_rows = ws.row_count
            old_cols = ws.col_count
            old_cells = old_rows * old_cols
            print(f"[시트] 기존 크기: {old_rows:,}행 × {old_cols:,}열 = {old_cells:,}셀")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=ALERTS_SHEET_NAME, rows=100, cols=50)
            print(f"[시트] 새로 생성: {ALERTS_SHEET_NAME}")
        
        # 시트 비우기
        ws.clear()
        print(f"[시트] 데이터 삭제 완료")
        
        # 새 크기 계산 (데이터 + 여유 10행)
        new_rows = len(df) + 10
        new_cols = len(df.columns)
        new_cells = new_rows * new_cols
        
        # 시트 크기 축소 (누적 방지)
        print(f"[시트] 크기 축소: {new_rows:,}행 × {new_cols:,}열 = {new_cells:,}셀")
        ws.resize(rows=new_rows, cols=new_cols)
        print(f"[시트] ✅ 크기 리셋 완료")
        
    except Exception as e:
        print(f"[경고] 시트 크기 리셋 실패: {e}")
        print(f"[경고] save_to_gsheet로 계속 진행...")
    
    # ============================================================
    # 4️⃣ 구글 시트에 업로드 (overwrite 모드)
    # ============================================================
    print(f"\n[구글시트] 업로드 시작 (mode: overwrite)...")
    
    try:
        result = save_to_gsheet(
            df=df,
            sheet_name=ALERTS_SHEET_NAME,
            mode="overwrite",  # 기존 데이터 삭제 후 덮어쓰기
            credentials_path=DEFAULT_CREDENTIALS_PATH,
            url=ALERTS_GSHEET_URL,
        )
        
        if result.get('success'):
            uploaded_count = len(df)
            print(f"\n[구글시트] ✅ 업로드 완료")
            print(f"  - 업로드: {uploaded_count}건")
            print(f"  - 최종 크기: {new_rows:,}행 × {new_cols:,}열 = {new_cells:,}셀")
            print(f"  - 시트: {ALERTS_SHEET_NAME}")
            print(f"  - URL: {ALERTS_GSHEET_URL}")
            print(f"{'='*60}\n")
            return f"✅ 업로드 완료: {uploaded_count}건 ({new_cells:,}셀)"
        else:
            error = result.get('error', '알 수 없는 오류')
            print(f"[구글시트] ⚠️ 업로드 실패: {error}")
            return f"업로드 실패: {error}"
            
    except Exception as e:
        print(f"[구글시트] ⚠️ 업로드 중 예외 발생: {e}")
        import traceback
        print(f"[상세 오류]\n{traceback.format_exc()}")
        return f"업로드 실패: {str(e)}"


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='매출 이상 알림 데이터를 구글 시트에 업로드 (덮어쓰기)',
    schedule=SMD_VISIT_LOG,  # 매주 화 오전 10시
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 🔒 동시 실행 방지
    default_args={
        'retries': 1,
        'retry_delay': pd.Timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['03_gsheet', 'upload', 'alerts'],
) as dag:
    # ============================================================
    # task 정의
    # ============================================================
    wait_for_smd_02 = ExternalTaskSensor(
        task_id='wait_for_smd_02',
        external_dag_id='SMD_sales_visit_log_02_transform_Dags',
        external_task_id='append_to_master',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=1800,  # 30분 대기
        poke_interval=60,  # 1분마다 확인
        check_existence=True,  # 태스크 존재 여부 확인
        soft_fail=False,  # 실패 시 즉시 실패
        execution_date_fn=_latest_successful_execution_date,
    )
    
    upload_task = PythonOperator(
        task_id='upload_alerts_to_gsheet',
        python_callable=upload_alerts_to_gsheet,
    )
    
    wait_for_smd_02 >> upload_task


