"""아침 통합매출에서 분리한 야간 전체기간 계산."""
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.transform.pipelines.db.DB_UnifiedSales_nightly import run_nightly
from modules.transform.utility.schedule import DB_UNIFIED_SALES_NIGHTLY_TIME
from modules.transform.utility.workload import BACKGROUND_QUEUE
from modules.transform.utility.notifier import on_failure_callback

with DAG(dag_id=Path(__file__).stem, schedule=DB_UNIFIED_SALES_NIGHTLY_TIME,
         start_date=pendulum.datetime(2026, 9, 9, tz="Asia/Seoul"), catchup=False,
         max_active_runs=1, max_active_tasks=1, tags=["db", "history"],
         default_args={"retries": 0, "on_failure_callback": on_failure_callback}) as dag:
    PythonOperator(task_id="recalculate", python_callable=run_nightly,
                   queue=BACKGROUND_QUEUE, priority_weight=1, weight_rule="absolute")
