"""
ToOrder 채널별(일) 매출보고서 수집 DAG.

- 기본 동작: KST 기준 전일 1일 수집
- conf `sale_date`: 단일 날짜 수집
- conf `sale_date_from`, `sale_date_to`: 기간 수집
- 1차 수집 뒤 동일 날짜를 한 번 더 순회해 누락을 보정
"""

from datetime import datetime, timedelta
from pathlib import Path

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.DB_Toorder_store_platform_daily import (
    run_toorder_store_platform_daily,
)
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.schedule import DB_TOORDER_STORE_PLATFORM_TIME

# DAG 기본 날짜 범위 설정
# None이면 conf가 없을 때 KST 기준 전일 1일만 수집
# 예:
# DATE_FROM = "2026-04-23"
# DATE_TO = "2026-05-11"
DATE_FROM = None
DATE_TO = None
STORE_NAME = "해운대중동점"
STORE_SAVE_NAME = "도리당 해운대중동점"
CSV_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.csv"


def resolve_dates(**context) -> str:
    conf = context["dag_run"].conf or {}
    sale_date = conf.get("sale_date")
    sale_date_from = conf.get("sale_date_from")
    sale_date_to = conf.get("sale_date_to")

    if sale_date and (sale_date_from or sale_date_to):
        raise ValueError("sale_date와 sale_date_from/sale_date_to는 함께 사용할 수 없습니다.")

    if sale_date_from or sale_date_to:
        if not (sale_date_from and sale_date_to):
            raise ValueError("sale_date_from과 sale_date_to는 둘 다 필요합니다.")
        start = datetime.strptime(sale_date_from, "%Y-%m-%d")
        end = datetime.strptime(sale_date_to, "%Y-%m-%d")
        if start > end:
            raise ValueError(
                f"sale_date_from({sale_date_from})가 sale_date_to({sale_date_to})보다 늦습니다."
            )
        delta = (end - start).days + 1
        sale_dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta)]
    elif sale_date:
        datetime.strptime(sale_date, "%Y-%m-%d")
        sale_dates = [sale_date]
    elif DATE_FROM or DATE_TO:
        if not (DATE_FROM and DATE_TO):
            raise ValueError("DATE_FROM과 DATE_TO는 둘 다 필요합니다.")
        start = datetime.strptime(DATE_FROM, "%Y-%m-%d")
        end = datetime.strptime(DATE_TO, "%Y-%m-%d")
        if start > end:
            raise ValueError(f"DATE_FROM({DATE_FROM})가 DATE_TO({DATE_TO})보다 늦습니다.")
        delta = (end - start).days + 1
        sale_dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta)]
    else:
        sale_dates = [pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")]

    context["ti"].xcom_push(key="sale_dates", value=sale_dates)
    return f"sale_dates={sale_dates}"


def collect_and_save(**context) -> str:
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")
    return run_toorder_store_platform_daily(
        sale_dates=sale_dates,
        store_name=STORE_NAME,
        log_prefix="[1st] ",
    )


def collect_and_save_guard(**context) -> str:
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")
    return run_toorder_store_platform_daily(
        sale_dates=sale_dates,
        store_name=STORE_NAME,
        log_prefix="[guard] ",
    )


def normalize_csv_date_column(**context) -> str:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV 파일이 없습니다: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig")
    if "date" not in df.columns:
        raise ValueError(f"'date' 컬럼이 없습니다: {CSV_PATH}")
    if "store" not in df.columns:
        raise ValueError(f"'store' 컬럼이 없습니다: {CSV_PATH}")

    changed_date_rows = 0

    def _normalize_date(value: object) -> str:
        nonlocal changed_date_rows
        text = str(value or "").strip()
        if "~" not in text:
            return text
        left, right = [part.strip() for part in text.split("~", 1)]
        if left and left == right:
            changed_date_rows += 1
            return left
        return text

    df["date"] = df["date"].apply(_normalize_date)
    changed_store_rows = int(df["store"].astype(str).str.strip().eq(STORE_NAME).sum())
    df.loc[df["store"].astype(str).str.strip().eq(STORE_NAME), "store"] = STORE_SAVE_NAME
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    return (
        f"normalized_date_rows={changed_date_rows}, "
        f"renamed_store_rows={changed_store_rows}, csv={CSV_PATH}"
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder 채널별 일매출 수집 (1차 + 누락 방지 보정 수집)",
    schedule=DB_TOORDER_STORE_PLATFORM_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "toorder", "platform", "daily"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:
    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_dates,
    )

    t2 = PythonOperator(
        task_id="collect_and_save",
        python_callable=collect_and_save,
    )

    t3 = PythonOperator(
        task_id="collect_and_save_guard",
        python_callable=collect_and_save_guard,
    )

    t4 = PythonOperator(
        task_id="normalize_csv_date_column",
        python_callable=normalize_csv_date_column,
    )

    t1 >> t2 >> t3 >> t4
