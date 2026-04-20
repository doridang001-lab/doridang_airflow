"""
DAG 모니터링 HTML 생성 테스트 - 히트맵 렌더링 + NaN 케이스 검증
"""
import sys
import io
import json
import pandas as pd

sys.path.insert(0, "/mnt/c/airflow")

from modules.transform.pipelines.strategy.dag_monitoring_pipeline import _build_report_html

TARGET_DATE = "2026-04-16"

def run_case(name: str, records: list):
    print(f"\n{'='*50}")
    print(f"케이스: {name}")
    json_str = json.dumps(records)
    results_df = pd.read_json(io.StringIO(json_str), orient="records", convert_dates=False)
    print(f"dtypes: start_hour_kst={results_df['start_hour_kst'].dtype}, start_min_kst={results_df['start_min_kst'].dtype}")
    try:
        html = _build_report_html(results_df, TARGET_DATE)
        hourly_ok = "시간별 실행 히트맵" in html
        minute_ok = "5분 단위 실행 히트맵" in html
        print(f"HTML 생성: 성공 | 시간별 히트맵: {hourly_ok} | 5분 히트맵: {minute_ok}")
        return html
    except Exception as e:
        print(f"HTML 생성: 실패 → {type(e).__name__}: {e}")
        import traceback; traceback.print_exc()
        return None


# 케이스 1: 정상 데이터
html = run_case("정상 데이터", [
    {"dag_id": "Sales_OkPos_01", "execution_date": 1744905600000, "dag_run_state": "success",
     "status": "OK", "fail_type": None, "detail": "정상", "duration_sec": 45.2,
     "total_tasks": 3, "success_tasks": 3, "failed_tasks": 0, "skipped_tasks": 0,
     "start_hour_kst": 9, "start_min_kst": 0},
    {"dag_id": "Strategy_Baemin", "execution_date": 1744905600000, "dag_run_state": "failed",
     "status": "FAIL", "fail_type": "dag_failed", "detail": "실패", "duration_sec": 8.0,
     "total_tasks": 2, "success_tasks": 1, "failed_tasks": 1, "skipped_tasks": 0,
     "start_hour_kst": 16, "start_min_kst": 30},
])
if html:
    with open("/mnt/c/airflow/scripts/test_output_normal.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("저장: test_output_normal.html")

# 케이스 2: NaN 포함 (start_date 파싱 실패 케이스)
html2 = run_case("NaN 포함 (파싱 실패 시뮬레이션)", [
    {"dag_id": "TestDag_NaN", "execution_date": None, "dag_run_state": "success",
     "status": "OK", "fail_type": None, "detail": "정상", "duration_sec": 45.0,
     "total_tasks": 3, "success_tasks": 3, "failed_tasks": 0, "skipped_tasks": 0,
     "start_hour_kst": None, "start_min_kst": None},
    {"dag_id": "TestDag_Valid", "execution_date": 1744905600000, "dag_run_state": "success",
     "status": "OK", "fail_type": None, "detail": "정상", "duration_sec": 120.0,
     "total_tasks": 4, "success_tasks": 4, "failed_tasks": 0, "skipped_tasks": 0,
     "start_hour_kst": 10, "start_min_kst": 5},
])
if html2:
    with open("/mnt/c/airflow/scripts/test_output_nan.html", "w", encoding="utf-8") as f:
        f.write(html2)
    print("저장: test_output_nan.html")

# 케이스 3: -1 포함 (정상적인 파싱 실패 fallback)
html3 = run_case("-1 포함 (파싱 실패 fallback)", [
    {"dag_id": "Dag_NoTime", "execution_date": 1744905600000, "dag_run_state": "success",
     "status": "WARN", "fail_type": "short_duration", "detail": "5초", "duration_sec": 5.0,
     "total_tasks": 1, "success_tasks": 0, "failed_tasks": 0, "skipped_tasks": 1,
     "start_hour_kst": -1, "start_min_kst": -1},
    {"dag_id": "Dag_Morning", "execution_date": 1744905600000, "dag_run_state": "success",
     "status": "OK", "fail_type": None, "detail": "정상", "duration_sec": 60.0,
     "total_tasks": 2, "success_tasks": 2, "failed_tasks": 0, "skipped_tasks": 0,
     "start_hour_kst": 9, "start_min_kst": 25},
])

print("\n모든 케이스 완료")
