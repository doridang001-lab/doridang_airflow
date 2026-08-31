"""
Baemin macro source lookback trigger.

최근 7일 중 ToOrder에는 배민 매출이 있는데 baemin_macro/orders 원천이 비어 있거나
유의미하게 부족한 날짜를 하루씩 기존 DB_Beamin_Macro_Dags에 orders_only로 넘긴다.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

from modules.transform.pipelines.db.DB_Beamin_Macro_validate import (
    _baemin_orders_by_store,
    _toorder_baemin_by_store,
)
from modules.transform.pipelines.db.DB_BaeminManual_load import (
    cleanup_manual_baemin_files,
    load_manual_baemin_files,
)


dag_id = Path(__file__).stem
KST = pendulum.timezone("Asia/Seoul")
TARGET_DAG_ID = "DB_Beamin_Macro_Dags"
LOOKBACK_DAYS = 7
GAP_RATE_THRESHOLD = 0.02
MAX_DAILY_TRIGGERS = 2


def _date_gap(target_date: str) -> tuple[int, int, int, float]:
    toorder_by_store = _toorder_baemin_by_store(target_date)
    baemin_by_store = _baemin_orders_by_store(target_date)
    toorder_total = int(sum(toorder_by_store.values()))
    baemin_total = int(sum(baemin_by_store.values()))
    diff = toorder_total - baemin_total
    gap_rate = abs(diff) / toorder_total if toorder_total else 0.0
    return toorder_total, baemin_total, diff, gap_rate


@provide_session
def _has_active_target_run(session=None) -> bool:
    active_states = (DagRunState.QUEUED, DagRunState.RUNNING)
    return (
        session.query(DagRun)
        .filter(DagRun.dag_id == TARGET_DAG_ID, DagRun.state.in_(active_states))
        .count()
        > 0
    )


@provide_session
def _run_exists(run_id: str, session=None) -> bool:
    return (
        session.query(DagRun)
        .filter(DagRun.dag_id == TARGET_DAG_ID, DagRun.run_id == run_id)
        .count()
        > 0
    )


def trigger_missing_baemin_orders(**context) -> str:
    if _has_active_target_run():
        return f"{TARGET_DAG_ID} active run 존재: lookback trigger 보류"

    today = pendulum.now(KST).date()
    candidates: list[dict] = []
    for offset in range(LOOKBACK_DAYS, 0, -1):
        target_date = today.subtract(days=offset).to_date_string()
        toorder_total, baemin_total, diff, gap_rate = _date_gap(target_date)
        if toorder_total <= 0:
            continue
        if baemin_total == 0 or gap_rate >= GAP_RATE_THRESHOLD:
            candidates.append(
                {
                    "target_date": target_date,
                    "toorder_total": toorder_total,
                    "baemin_total": baemin_total,
                    "diff": diff,
                    "gap_rate": gap_rate,
                }
            )

    if not candidates:
        return f"최근 {LOOKBACK_DAYS}일 배민 원천 lookback 대상 없음"

    candidates.sort(key=lambda item: abs(int(item["diff"])), reverse=True)
    triggered: list[str] = []
    skipped: list[str] = []
    for target in candidates[:MAX_DAILY_TRIGGERS]:
        target_date = target["target_date"]
        run_id = f"lookback_recovery__{target_date.replace('-', '')}__{context['ds_nodash']}"
        if _run_exists(run_id):
            skipped.append(f"{target_date}:이미존재")
            continue

        conf = {
            "target_date": target_date,
            "orders_only": True,
            "force_restart": True,
            "stability_profile": "safe_daily",
            "source": dag_id,
            "reason": "baemin_orders_recent_lookback_gap",
            "gap": target,
        }
        try:
            trigger_dag(dag_id=TARGET_DAG_ID, run_id=run_id, conf=conf)
        except Exception as exc:
            raise AirflowException(f"배민 lookback trigger 실패: {run_id}") from exc

        triggered.append(
            f"{run_id} target_date={target_date} "
            f"toorder={target['toorder_total']:,} "
            f"baemin={target['baemin_total']:,} "
            f"diff={target['diff']:,} "
            f"gap_rate={target['gap_rate']:.2%}"
        )

    if not triggered:
        return f"lookback 대상은 있으나 신규 트리거 없음: {', '.join(skipped) or '-'}"
    return "배민 원천 lookback 트리거:\n" + "\n".join(triggered)


with DAG(
    dag_id=dag_id,
    schedule="5 0 * * *",
    start_date=pendulum.datetime(2026, 8, 9, 0, 0, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "depends_on_past": False,
    },
    tags=["db", "baemin", "lookback"],
) as dag:
    t_ingest_manual = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=load_manual_baemin_files,
        execution_timeout=timedelta(minutes=15),
    )

    t_trigger_missing = PythonOperator(
        task_id="trigger_missing_baemin_orders",
        python_callable=trigger_missing_baemin_orders,
    )

    t_cleanup_manual = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_files,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_ingest_manual >> t_trigger_missing >> t_cleanup_manual
