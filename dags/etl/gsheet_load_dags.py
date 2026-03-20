"""
Google Sheet → DataFrame 로드 샘플 DAG
재활용 목적의 최소 구조 템플릿

사용법:
    1. GSHEET_URL, SHEET_NAME 을 대상 시트에 맞게 수정
    2. load_from_gsheet() 안에서 df 가공 로직 작성
"""

import os
import pendulum
import pandas as pd
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.extract.extract_gsheet import extract_gsheet

filename = os.path.basename(__file__)

# ── 설정 ─────────────────────────────────────────────────
CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
GSHEET_URL   = "https://docs.google.com/spreadsheets/d/1OFTQ0WyKgcwmBxwESWWpCD6E6O2i9kYYN6-YxNlx9xQ/edit?usp=sharing"
SHEET_NAME   = None  # None 이면 첫 번째 시트, 탭 이름 지정 시 해당 탭 로드
# ─────────────────────────────────────────────────────────


def load_from_gsheet(**context):
    df: pd.DataFrame = extract_gsheet(
        url=GSHEET_URL,
        sheet_name=SHEET_NAME,
        credentials_path=CREDENTIALS_PATH,
    )

    print(f"[로드완료] {len(df):,}행 × {len(df.columns)}열")
    print(f"[컬럼] {df.columns.tolist()}")

    # ── 이 아래에 가공 로직 작성 ──────────────────────────
    # df = df[df['컬럼명'].notna()]
    # df['new_col'] = df['col'].str.strip()
    # ─────────────────────────────────────────────────────

    return f"완료: {len(df)}행"


with DAG(
    dag_id=filename.replace(".py", ""),
    description="GSheet → DataFrame 로드 샘플",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["sample", "gsheet", "load"],
) as dag:

    PythonOperator(
        task_id="load_from_gsheet",
        python_callable=load_from_gsheet,
    )
