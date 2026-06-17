"""
Strategy_FdamCS_03_Mart_Dags.py

매장CS 구글시트 원본 → cs_issue_mart.csv 빌드 DAG

📋 처리 흐름:
1. ExternalTaskSensor — Strategy_FdamCS_01_Process_Dags 의 check_data_freshness 완료 대기
2. 구글시트 원본 전체 로드
3. build_cs_issue_mart() — 접수번호별 1행 마트로 변환
4. MART_DB/fdam_cs_report/cs_issue_mart.csv 저장 (덮어쓰기)

⚙️ 저장 경로:
  Docker  : /opt/airflow/onedrive_mart/fdam_cs_report/cs_issue_mart.csv
  로컬    : C:/Users/민준/OneDrive - 주식회사 도리당/data/mart/fdam_cs_report/cs_issue_mart.csv
"""

import os
import pendulum
import pandas as pd
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.io import SMP_FDAM_CS_TIME
from modules.transform.utility.paths import MART_DB
from modules.transform.pipelines.strategy.SMP_fdam_cs_mart import (
    build_cs_issue_mart,
    validate_mart,
    save_mart,
    add_ai_columns,
)
from modules.transform.utility.notifier import on_failure_callback

filename = os.path.basename(__file__)

# ============================================================
# 설정 (Strategy_FdamCS_01 과 공유)
# ============================================================
CREDENTIALS_PATH          = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
FALLBACK_CREDENTIALS_PATH = "/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json"
CS_GSHEET_URL             = "https://docs.google.com/spreadsheets/d/1DHbZX5jDkiZfe0SPr3eRinPXuSFomYm2AN-gMADoBkI/edit?gid=1488056813#gid=1488056813"
CS_SHEET_NAME             = "시트1"

MART_OUTPUT_DIR = MART_DB / "fdam_cs_report"


# ============================================================
# 헬퍼
# ============================================================
def _credential_candidates() -> list:
    candidates = []
    env_cred = os.getenv("SMP_FDAM_CS_GSHEET_CREDENTIALS")
    if env_cred:
        candidates.append(env_cred)
    candidates.extend([CREDENTIALS_PATH, FALLBACK_CREDENTIALS_PATH])
    deduped = []
    for path in candidates:
        if path and path not in deduped and os.path.exists(path):
            deduped.append(path)
    return deduped


def _latest_successful_execution_date(dt, **context):
    """Strategy_FdamCS_01_Process_Dags 의 최신 성공 실행일 반환"""
    runs = DagRun.find(dag_id='Strategy_FdamCS_01_Process_Dags', state=DagRunState.SUCCESS)
    if not runs:
        return dt
    execution_dates = [r.execution_date for r in runs if r.execution_date is not None]
    if not execution_dates:
        return dt
    return max(execution_dates)


# ============================================================
# Task: 구글시트 로드 → 마트 빌드 → parquet 저장
# ============================================================
def build_and_save_mart(**context):
    import gspread
    from google.oauth2.service_account import Credentials

    print(f"\n{'='*60}")
    print(f"[FdamCS Mart] cs_issue_mart 빌드 시작")
    print(f"[FdamCS Mart] 출력 경로: {MART_OUTPUT_DIR / 'cs_issue_mart.csv'}")

    # ── 1. 구글시트 전체 로드 ─────────────────────────────────
    print("[FdamCS Mart] 구글시트 로드 중...")
    try:
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        candidates = _credential_candidates()
        if not candidates:
            raise RuntimeError("사용 가능한 서비스 계정 JSON 없음")
        creds = Credentials.from_service_account_file(candidates[0], scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(CS_GSHEET_URL)
        ws = sh.worksheet(CS_SHEET_NAME)
        df_raw = pd.DataFrame(ws.get_all_records())
        print(f"[FdamCS Mart] 구글시트 로드 완료: {len(df_raw)}건")
    except Exception as e:
        print(f"[FdamCS Mart] ❌ 구글시트 로드 실패: {e}")
        raise

    if df_raw.empty:
        print("[FdamCS Mart] 원본 데이터 없음 → 빈 마트 저장")
        import pyarrow as pa
        import pyarrow.parquet as pq
        MART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = MART_OUTPUT_DIR / "cs_issue_mart.csv"
        df_empty = pd.DataFrame(columns=[
            '접수번호', '등록월', '등록월표시', '등록월정렬', '등록일',
            '처리완료일', '최종진행상태', '처리완료여부', '처리소요기간',
            '매장명', '유형', '매입처', '이슈유형', 'CS구분', 'issue_type', 'issue_summary', '담당자',
            'ai_현상요약', 'ai_예상문제', 'ai_제안액션',
        ])
        df_empty.to_csv(out_path, index=False, encoding='utf-8-sig')
        return "빈 마트 저장"

    # ── 2. 마트 빌드 ──────────────────────────────────────────
    print("[FdamCS Mart] 마트 변환 중...")
    df_mart = build_cs_issue_mart(df_raw)

    # ── 2-1. AI 컬럼 추가 (최신 등록월만) ────────────────────
    print("[FdamCS Mart] AI 컬럼 생성 중 (gpt-oss, 최신 등록월만)...")
    log_path = MART_OUTPUT_DIR / "llm_log.md"
    df_mart = add_ai_columns(df_mart, log_path=log_path)

    # ── 3. 검증 ───────────────────────────────────────────────
    print("[FdamCS Mart] 품질 검증 중...")
    ok = validate_mart(df_mart)
    if not ok:
        print("[FdamCS Mart] ⚠️ 검증 경고 있음 — 저장은 계속 진행")

    # ── 4. parquet 저장 ───────────────────────────────────────
    out_path = save_mart(df_mart, MART_OUTPUT_DIR)

    # 간단한 통계 출력
    done_cnt = int((df_mart['처리완료여부'] == 1).sum())
    print(f"[FdamCS Mart] ✅ 완료")
    print(f"[FdamCS Mart]   총 접수번호 : {len(df_mart)}건")
    print(f"[FdamCS Mart]   완료        : {done_cnt}건")
    print(f"[FdamCS Mart]   미완료      : {len(df_mart) - done_cnt}건")
    if '등록월' in df_mart.columns:
        months = df_mart['등록월'].dropna().unique()
        print(f"[FdamCS Mart]   등록월 범위 : {sorted(months)[0] if len(months) else 'N/A'} ~ {sorted(months)[-1] if len(months) else 'N/A'}")

    return f"✅ {len(df_mart)}건 저장 ({out_path})"


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='매장CS 구글시트 원본 → cs_issue_mart.csv (접수번호별 1행 마트)',
    schedule=SMP_FDAM_CS_TIME,   # 매일 09:05 KST (01번과 동일, ExternalTaskSensor가 순서 보장)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    "on_failure_callback": on_failure_callback,
    },
    tags=['03_mart', 'relay_cs', '매장cs', 'powerbi'],
) as dag:

    # Strategy_FdamCS_01 의 마지막 태스크(check_data_freshness) 완료 대기
    wait_for_cs_process = ExternalTaskSensor(
        task_id='wait_for_cs_process',
        external_dag_id='Strategy_FdamCS_01_Process_Dags',
        external_task_id='check_data_freshness',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=3600,       # 최대 60분 대기
        poke_interval=60,   # 1분마다 확인
        check_existence=False,
        soft_fail=True,     # 타임아웃/실패 시 skipped → downstream 실행 가능
        execution_date_fn=_latest_successful_execution_date,
    )

    t_build = PythonOperator(
        task_id='build_and_save_mart',
        python_callable=build_and_save_mart,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.NONE_FAILED,   # 센서 skipped 여도 실행
    )

    chain(wait_for_cs_process, t_build)
