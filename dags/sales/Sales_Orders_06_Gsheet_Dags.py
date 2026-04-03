"""
매출 이상 알림 데이터를 구글 시트에 업로드하는 DAG

sales_daily_orders_alerts.csv → Google Sheet (덮어쓰기)

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
from airflow.models.baseoperator import chain
from airflow.utils.state import State
from airflow import settings
from sqlalchemy import text

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)


def _latest_successful_execution_date(dt, **context):
    """
    SMD_05의 fin_save_to_csv 태스크가 성공한 최신 execution_date를 찾는다.
    DagRun 상태가 아닌 TaskInstance 상태 기준.
    """
    session = settings.Session()
    try:
        row = session.execute(
            text(
                """
                SELECT dr.logical_date
                FROM task_instance ti
                JOIN dag_run dr ON ti.run_id = dr.run_id AND ti.dag_id = dr.dag_id
                WHERE ti.dag_id = :dag_id
                  AND ti.task_id = :task_id
                  AND ti.state = :state
                ORDER BY dr.logical_date DESC
                LIMIT 1
                """
            ),
            {
                'dag_id': 'Sales_Orders_05_Marketing_Dags',
                'task_id': 'fin_save_to_csv',
                'state': 'success',
            },
        ).first()

        if row:
            execution_date = row[0]
            print(f"[sensor] SMD_05 fin_save_to_csv 성공 찾음: {execution_date}")
            return execution_date
    except Exception as e:
        print(f"[sensor] TaskInstance 조회 실패: {e}")
    finally:
        session.close()
    
    print(f"[sensor] 성공한 fin_save_to_csv 없음, 기본값 사용: {dt}")
    return dt

from modules.transform.utility.paths import LOCAL_DB
from modules.load.load_gsheet import save_to_gsheet

# ============================================================
# 설정
# ============================================================
# Linux 경로 사용 (Docker 컨테이너 내부)
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

# Google Sheets 설정
ALERTS_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1JJSPLuqAgSSVaXQjZUwdBug-IyBouwUlsXHZiE20VZU/edit?usp=sharing"
ALERTS_SHEET_NAME = "시트1"

# CSV 경로
ALERTS_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_upload.csv'


# ============================================================
# Google Sheets 업로드 함수
# ============================================================
def upload_alerts_to_gsheet(**context):
    """
    sales_daily_orders_upload.csv를 구글 시트에 업로드
    
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
    # 3.5️⃣ GSheet 업로드 컬럼 — CSV와 동일한 컬럼명 그대로 전체 업로드
    # ============================================================
    # uploaded_at: CSV에 없으므로 업로드 시점 타임스탬프 생성
    df['uploaded_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # 전주상태: 5개 지표 pre 컬럼 → JSON {"매출":"위험","수수료":"정상",...}
    import json as _json
    _PRE_MAP = {
        '매출':     'pre_status',
        '수수료':   'pre_fee_status',
        '배민광고': 'pre_배민_광고효과_status',
        '쿠팡광고': 'pre_쿠팡_광고효과_status',
        '성실지표': 'pre_service_status',
    }
    def _build_prev_status(row):
        return _json.dumps(
            {label: str(row.get(col, '정상') or '정상') for label, col in _PRE_MAP.items()},
            ensure_ascii=False
        )
    df['전주상태'] = df.apply(_build_prev_status, axis=1)
    print(f"[컬럼] ✅ 전주상태 JSON 컬럼 추가 (5개 지표 pre 컬럼 기반)")

    print(f"[컬럼] ✅ CSV 원본 컬럼명 그대로 {len(df.columns)}개 업로드 준비 완료")

    # ============================================================
    # 3.5️⃣ 시트 크기 리셋 (셀 누적 방지) ← 🔥 핵심 추가 부분
    # ============================================================
    print(f"\n[시트 크기 리셋] 시작...")

    # 업로드 후 크기 출력에서 NameError 방지 — try 블록 실패 시에도 참조 가능
    new_rows = len(df) + 10
    new_cols = len(df.columns)
    new_cells = new_rows * new_cols

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

        # 시트 크기 축소 (누적 방지) — new_rows/new_cols는 try 블록 전에 이미 계산됨
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
        
        if isinstance(result, dict) and bool(result.get('success', False)):
            uploaded_count = len(df)
            print(f"\n[구글시트] ✅ 업로드 완료")
            print(f"  - 업로드: {uploaded_count}건")
            print(f"  - 최종 크기: {new_rows:,}행 × {new_cols:,}열 = {new_cells:,}셀")
            print(f"  - 시트: {ALERTS_SHEET_NAME}")
            print(f"  - URL: {ALERTS_GSHEET_URL}")
            print(f"{'='*60}\n")
            return f"✅ 업로드 완료: {uploaded_count}건 ({new_cells:,}셀)"
        else:
            error = result.get('error', '알 수 없는 오류') if isinstance(result, dict) else '알 수 없는 반환 형식'
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
    schedule=SMD_ORDERS_TIME,  # 매주 월, 수 10시 30분 실행
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
    wait_for_smd_05 = ExternalTaskSensor(
        task_id='wait_for_smd_05',
        external_dag_id='Sales_Orders_05_Marketing_Dags',
        external_task_id='fin_save_to_csv',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='reschedule',
        timeout=1800,  # 30분 대기
        poke_interval=60,  # 1분마다 확인
        check_existence=False,
        soft_fail=True,  # 실패해도 다음 태스크 계속 실행
        execution_date_fn=_latest_successful_execution_date,
    )
    
    upload_task = PythonOperator(
        task_id='upload_alerts_to_gsheet',
        python_callable=upload_alerts_to_gsheet,
        trigger_rule='all_done',  # sensor가 skip되더라도 실행
    )
    
    # 의존성: 센서가 성공하거나 soft fail해도 업로드 실행
    chain(wait_for_smd_05, upload_task)


# ============================================================
# DAG 플로우 요약
# ============================================================
"""
┌─────────────────────────────────────────────────────────────┐
│  CSV 읽기                                                    │
│                                                              │
│  sales_daily_orders_alerts.csv 로드                         │
│  - 자동 인코딩 감지 (utf-8-sig, utf-8, cp949)               │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  데이터 정리                                                 │
│                                                              │
│  - NaN → 빈 문자열 변환                                      │
│  - order_daily 날짜 정규화                                  │
│    (엑셀 직렬값 + 텍스트 날짜 모두 처리)                    │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  🔥 시트 크기 리셋 (NEW!)                                   │
│                                                              │
│  - 시트 데이터 삭제                                          │
│  - 시트 크기 축소 (데이터 크기 + 여유 10행)                 │
│  - 셀 누적 방지 → 1천만 셀 제한 해결                        │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│  Google Sheets 업로드                                        │
│                                                              │
│  - 모드: overwrite (기존 데이터 삭제 후 덮어쓰기)           │
│  - 시트: 시트1                                               │
│  - 인증: Service Account JSON                                │
└─────────────────────────────────────────────────────────────┘

📌 주요 기능:
- CSV 자동 인코딩 감지
- NaN 안전 처리
- 날짜 정규화 (엑셀/텍스트)
- **시트 크기 리셋** (셀 누적 방지) ← NEW!
- 덮어쓰기 모드로 항상 최신 데이터 유지

⚙️ 실행 시각: 매주 월/수 11:00 (KST)
📊 대상 파일: sales_daily_orders_alerts.csv

⚠️ 주의사항:
- sales_orders_01_load_baemin_data DAG가 먼저 실행되어야 함
- Google Sheets API 인증 필요 (Service Account JSON)
- gspread 라이브러리 필요 (pip install gspread)

🔥 핵심 수정사항:
- save_to_gsheet 호출 전에 시트 크기를 데이터 크기로 축소
- 매 업로드마다 시트 크기가 누적되는 문제 해결
- 기존 코드 구조 유지하면서 최소한의 수정만 추가
"""