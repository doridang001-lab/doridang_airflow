"""
투오더 리뷰 크롤링 DAG - 매일 전일 데이터 다운로드

📋 처리 흐름:
1. ID/PW로 투오더 로그인
2. 시스템 날짜 기준 어제 날짜 자동 설정 (KST)
3. 투오더 리뷰 데이터 크롤링
4. 결과 로깅 및 저장

⚙️ 실행 시각: 매일 09:00 (KST)
"""
import pendulum
import pandas as pd
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract.croling_toorder_review import run_toorder_review_crawling

# 파일명
filename = os.path.basename(__file__)

# 설정 - 투오더 계정 정보
TOORDER_ID = "doridang1"
TOORDER_PW = "ehfl0109!!"


# ============================================================
# 크롤링 태스크
# ============================================================
def crawl_toorder_review(**context):
    """투오더 리뷰 크롤링 실행 (어제 날짜)"""
    
    print(f"\n{'='*60}")
    print("[투오더 리뷰 크롤링] 시작")
    print(f"{'='*60}")
    
    # 1. 계정 정보 설정
    print("\n[1단계] 계정 정보 설정...")
    account_df = pd.DataFrame([
        {
            "channel": "toorder",
            "id": TOORDER_ID,
            "pw": TOORDER_PW,
        },
    ])
    print(f"✅ 사용 계정: {len(account_df)}건")
    print(f"   채널: {account_df['channel'].iloc[0]}")
    print(f"   ID: {account_df['id'].iloc[0]}")
    
    # 2. 날짜 설정 (KST 기준 어제)
    print("\n[2단계] 크롤링 날짜 설정 (KST 기준)...")
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst)
    yesterday = today.subtract(days=1)
    yesterday_str = yesterday.format("YYYY-MM-DD")
    
    print(f"  시스템 현재일 (KST): {today.format('YYYY-MM-DD HH:mm:ss')}")
    print(f"  크롤링 대상일: {yesterday_str} (전일)")
    
    # 3. 크롤링 실행
    print(f"\n[3단계] 투오더 리뷰 크롤링 실행 중...")
    
    try:
        result_df = run_toorder_review_crawling(
            account_df=account_df,
            start_date=yesterday_str,
            end_date=yesterday_str,
        )
        
        # 빈 DataFrame 체크
        if len(result_df) == 0:
            print(f"\n{'='*60}")
            print("📊 크롤링 결과: 데이터 없음")
            print(f"{'='*60}\n")
            
            return {
                "total": 0,
                "success": 0,
                "failed": 0,
                "target_date": yesterday_str,
                "status": "no data",
            }
        
        print(f"✅ 크롤링 실행 완료: {len(result_df)}건")
        
    except Exception as e:
        print(f"❌ 크롤링 실패: {e}")
        raise
    
    # 4. 성공/실패 집계
    total_success = int(result_df["success"].sum()) if "success" in result_df.columns else 0
    total_failed = len(result_df) - total_success
    
    # 5. 결과 출력
    print(f"\n{'='*60}")
    print("📊 크롤링 결과 상세:")
    print(f"{'='*60}")
    
    try:
        summary_cols = ["account_id", "target_date", "success", "file_size_mb", "error"]
        available_cols = [col for col in summary_cols if col in result_df.columns]
        
        if available_cols:
            print(result_df[available_cols].to_string(index=False))
        else:
            print(result_df.to_string(index=False))
    except Exception as e:
        print(f"결과 테이블 출력 오류: {e}")
        print(result_df.to_string(index=False))
    
    print(f"\n{'='*60}")
    print(f"[요약] 총 {len(result_df)}개 계정 처리")
    print(f"  ✅ 성공: {total_success}건")
    print(f"  ❌ 실패: {total_failed}건")
    print(f"{'='*60}")
    
    # 6. 실패 내역 출력
    if total_failed > 0 and "success" in result_df.columns:
        print(f"\n[실패 내역]")
        try:
            failed_df = result_df[result_df["success"] == False]
            cols_to_show = ["account_id", "target_date", "error"]
            available_cols = [col for col in cols_to_show if col in failed_df.columns]
            
            if available_cols:
                print(failed_df[available_cols].to_string(index=False))
        except Exception as e:
            print(f"실패 내역 출력 오류: {e}")
    
    print(f"\n{'='*60}")
    print("[투오더 리뷰 크롤링] 종료")
    print(f"{'='*60}\n")
    
    return {
        "total": len(result_df),
        "success": total_success,
        "failed": total_failed,
        "target_date": yesterday_str,
        "status": "completed",
    }


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='투오더 리뷰 데이터 일일 수집 (전일 데이터)',
    schedule="0 9 * * *",  # 매일 09:00 (KST)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['01_crawling', 'toorder', 'review', 'daily'],
    default_args={
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=5),
    },
) as dag:
    
    crawl_task = PythonOperator(
        task_id='crawl_toorder_review',
        python_callable=crawl_toorder_review,
    )
    
    crawl_task
