"""
투오더 VOC 분석 크롤링 DAG - 매일 전일 데이터 다운로드

📋 처리 흐름:
1. ID/PW로 투오더 로그인
2. VOC 분석 페이지 이동
3. KST 기준 어제 날짜 달력 선택 → 조회 버튼 클릭
4. '전체 토픽 내보내기' 버튼 클릭 → 다운로드 완료 대기
5. 다운로드된 파일을 업로드 임시 폴더로 이동

⚙️ 실행 시각: 매일 09:30 (KST)
"""
import pendulum
import pandas as pd
import os
import importlib
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.transform.utility.paths import LOCAL_DB

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)
dag_file_stem = Path(__file__).stem
pipeline_module_name = dag_file_stem.removesuffix("_Dags")
pipeline_module_path = f"modules.transform.pipelines.strategy.{pipeline_module_name}"
pipeline_module = importlib.import_module(pipeline_module_path)

run_toorder_voc_crawling = pipeline_module.run_toorder_voc_crawling
DOWNLOAD_DIR = pipeline_module.DOWNLOAD_DIR

TOORDER_ID = "doridang1"
TOORDER_PW = "ehfl0109!!"


# ============================================================
# Task 1: 크롤링
# ============================================================

def crawl_toorder_voc(**context):
    """투오더 VOC 분석 크롤링 (KST 기준 전일)"""

    print(f"\n{'='*60}")
    print("[투오더 VOC 분석] 크롤링 시작")
    print(f"{'='*60}")

    # 계정 설정
    print("\n[1단계] 계정 정보 설정...")
    account_df = pd.DataFrame([{
        "channel": "toorder",
        "id": TOORDER_ID,
        "pw": TOORDER_PW,
    }])
    print(f"✅ 계정: {account_df['id'].iloc[0]}")

    # KST 기준 어제 날짜
    print("\n[2단계] 날짜 설정 (KST 기준 전일)...")
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst)
    yesterday = today.subtract(days=1)
    yesterday_str = yesterday.format("YYYY-MM-DD")

    print(f"  현재일 (KST): {today.format('YYYY-MM-DD HH:mm:ss')}")
    print(f"  크롤링 대상일: {yesterday_str}")

    # 크롤링 실행
    print(f"\n[3단계] VOC 크롤링 실행...")
    try:
        result_df = run_toorder_voc_crawling(
            account_df=account_df,
            start_date=yesterday_str,
            end_date=yesterday_str,
        )
    except Exception as e:
        print(f"❌ 크롤링 실패: {e}")
        raise

    if len(result_df) == 0:
        print("\n📊 크롤링 결과: 데이터 없음")
        return {"total": 0, "success": 0, "failed": 0, "target_date": yesterday_str, "status": "no_data"}

    total_success = int(result_df["success"].sum()) if "success" in result_df.columns else 0
    total_failed  = len(result_df) - total_success

    print(f"\n{'='*60}")
    print("📊 크롤링 결과:")
    show_cols = [c for c in ["account_id", "target_date", "success", "downloaded_file", "error"] if c in result_df.columns]
    print(result_df[show_cols].to_string(index=False))
    print(f"\n[요약] 성공 {total_success} / 실패 {total_failed}")

    if total_failed > 0 and "success" in result_df.columns:
        failed_df = result_df[result_df["success"] == False]
        fail_cols = [c for c in ["account_id", "target_date", "error"] if c in failed_df.columns]
        print("\n[실패 내역]")
        print(failed_df[fail_cols].to_string(index=False))

    print(f"\n{'='*60}")
    print("[투오더 VOC 분석] 크롤링 종료")
    print(f"{'='*60}\n")

    return {
        "total":       len(result_df),
        "success":     total_success,
        "failed":      total_failed,
        "target_date": yesterday_str,
        "status":      "completed",
    }


# ============================================================
# Task 2: 파일 이동
# ============================================================

def move_voc_files(patterns, dest_dir, **context):
    """
    다운로드 폴더의 VOC 파일을 업로드 임시 폴더로 이동

    Parameters
    ----------
    patterns : list[str]  예) ['*.csv', '*.xlsx']
    dest_dir : str        목적지 경로
    """
    import glob as glob_module
    import shutil

    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    moved = 0
    for pattern in patterns:
        matched = glob_module.glob(str(DOWNLOAD_DIR / pattern))
        print(f"\n[이동] 패턴: {pattern}  →  찾은 파일: {len(matched)}개")

        for filepath in matched:
            try:
                src = Path(filepath)
                dst = dest_path / src.name
                shutil.move(str(src), str(dst))
                print(f"  ✅ {src.name} → {dst}")
                moved += 1
            except Exception as e:
                print(f"  ⚠️  이동 실패: {Path(filepath).name} — {e}")

    print(f"\n✅ 총 {moved}개 파일 이동 완료")
    return f"이동 완료: {moved}개"


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=dag_file_stem,
    description="투오더 VOC 분석 일일 수집 (전일 데이터)",
    schedule="30 9 * * *",   # 매일 09:30 KST
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["01_crawling", "toorder", "voc", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:

    crawl_task = PythonOperator(
        task_id="crawl_toorder_voc",
        python_callable=crawl_toorder_voc,
    )

    move_task = PythonOperator(
        task_id="move_voc_files",
        python_callable=move_voc_files,
        op_kwargs={
            "patterns": ["*.csv", "*.xlsx"],
            "dest_dir": str(DOWNLOAD_DIR / "업로드_temp"),
        },
    )

    crawl_task >> move_task