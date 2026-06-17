"""
플로우(Flow) 방문일지 크롤링 DAG
업로드_temp에 직접 저장
"""

import pendulum
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)

from modules.transform.utility.paths import LOCAL_DB

# 크롤링 모듈 import
from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import run_flow_visit_crawling
from modules.transform.utility.io import SMD_VISIT_LOG
from modules.transform.utility.notifier import on_failure_callback


# ============================================================================
# 설정
# ============================================================================

# 수집 모드 설정 (환경변수로 제어 가능)
COLLECTION_MODE = os.getenv("FLOW_COLLECTION_MODE", "recent")  # "recent" 또는 "all"
RECENT_DAYS = int(os.getenv("FLOW_RECENT_DAYS", "8"))  # 최근 N일 (기본 8일)
ALL_DATA_START_DATE = os.getenv("FLOW_ALL_START_DATE", None)  # 전체 수집 시작일

# 출력 디렉토리 설정
OUTPUT_DIR = Path('/opt/airflow/download/업로드_temp')  # ✅ 절대 경로


# ============================================================================
# Task 함수
# ============================================================================

def crawl_flow_visit(**context):
    """플로우 방문일지 크롤링"""
    print(f"\n{'='*60}")
    print(f"[플로우 방문일지] 크롤링 시작")
    print(f"{'='*60}")
    
    try:
        # 출력 디렉토리 생성
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # 날짜 설정 - 실제 실행 시점 기준 (과거 스케줄이 아닌 현재 시간 기준)
        today = pendulum.now('Asia/Seoul')
        end_date = today.strftime("%Y-%m-%d")
        
        if COLLECTION_MODE == "all":
            if ALL_DATA_START_DATE:
                start_date = ALL_DATA_START_DATE
                print(f"[모드] 전체 수집: {start_date} ~ {end_date}")
            else:
                start_date = "2020-01-01"
                print(f"[모드] 전체 수집 (시작일 미지정): {start_date} ~ {end_date}")
        else:
            start_date = (today - timedelta(days=RECENT_DAYS)).strftime("%Y-%m-%d")
            print(f"[모드] 최근 {RECENT_DAYS}일 수집: {start_date} ~ {end_date}")
        
        # 크롤링 실행
        result_df = run_flow_visit_crawling(
            start_date=start_date,
            end_date=end_date
        )
        
        if result_df.empty:
            print("\n[결과] 수집된 데이터가 없습니다")
            return "수집 데이터 없음"
        
        # 결과 확인
        print(f"\n[수집 완료]")
        print(f"  - 총 건수: {len(result_df):,}건")
        print(f"  - 저장 경로: {OUTPUT_DIR}")
        
        if 'visit_date' in result_df.columns:
            visit_dates = result_df['visit_date'].dropna()
            if not visit_dates.empty:
                print(f"  - 방문일 범위: {visit_dates.min().date()} ~ {visit_dates.max().date()}")
        
        if 'author' in result_df.columns:
            print(f"  - 작성자별 건수:")
            for author, count in result_df['author'].value_counts().head(10).items():
                print(f"    • {author}: {count}건")
        
        # 저장된 파일 확인
        output_files = list(OUTPUT_DIR.glob("flow_visit_*.csv"))
        if output_files:
            latest_file = max(output_files, key=lambda p: p.stat().st_mtime)
            print(f"\n[저장 파일] {latest_file}")
        
        return f"✅ 수집 완료: {len(result_df)}건"
        
    except Exception as e:
        print(f"\n[에러] 크롤링 실패: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ 크롤링 실패: {str(e)}"


# ============================================================================
# DAG 정의
# ============================================================================

with DAG(
    dag_id=filename.replace('.py', ''),
    description='플로우 방문일지 크롤링 (업로드_temp 직접 저장)',
    schedule=SMD_VISIT_LOG,  # 매주 화 오전 10시
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['flow', 'crawling', 'visit_log'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    },
) as dag:
    
    # Task: 크롤링 (업로드_temp에 직접 저장)
    crawl_task = PythonOperator(
        task_id='crawl_flow_visit',
        python_callable=crawl_flow_visit,
    )