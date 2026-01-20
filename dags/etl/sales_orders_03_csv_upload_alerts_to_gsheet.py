"""
매출 이상 알림 데이터를 구글 시트에 업로드하는 DAG

sales_daily_orders_alerts.csv → Google Sheet (덮어쓰기)

📋 처리 흐름:
1. CSV 파일 읽기 (sales_daily_orders_alerts.csv)
2. 데이터 정리 (NaN 처리, 날짜 정규화)
3. Google Sheets 업로드 (overwrite 모드)
"""

import pendulum
import pandas as pd
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 파일명
filename = os.path.basename(__file__)

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
    sales_daily_orders_alerts.csv를 구글 시트에 업로드
    
    처리 순서:
    1. CSV 파일 읽기
    2. NaN → 빈 문자열 변환
    3. order_daily 날짜 정규화 (엑셀 직렬값/텍스트 모두 처리)
    4. Google Sheets에 덮어쓰기 (overwrite 모드)
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
            print(f"  - 시트: {ALERTS_SHEET_NAME}")
            print(f"  - URL: {ALERTS_GSHEET_URL}")
            print(f"{'='*60}\n")
            return f"✅ 업로드 완료: {uploaded_count}건 (덮어쓰기)"
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
    schedule="0 11 * * 1,3",  # 매주 월/수 11:00 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['03_gsheet', 'upload', 'alerts'],
) as dag:
    
    upload_task = PythonOperator(
        task_id='upload_alerts_to_gsheet',
        python_callable=upload_alerts_to_gsheet,
    )
    
    upload_task


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
- 덮어쓰기 모드로 항상 최신 데이터 유지

⚙️ 실행 시각: 매주 월/수 11:00 (KST)
📊 대상 파일: sales_daily_orders_alerts.csv

⚠️ 주의사항:
- sales_orders_01_load_baemin_data DAG가 먼저 실행되어야 함
- Google Sheets API 인증 필요 (Service Account JSON)
"""