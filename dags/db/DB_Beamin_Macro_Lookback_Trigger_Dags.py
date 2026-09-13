"""
Baemin macro source lookback trigger.

최근 60일 중 ToOrder에는 배민 매출이 있는데 baemin_macro/orders 원천이 비어 있는
매장-날짜를 찾아 기존 DB_Beamin_Macro_Dags에 orders_only로 넘긴다.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowException, DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.settings import TIMEZONE as AIRFLOW_TIMEZONE
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
from modules.transform.utility.workload import BACKGROUND_COLLECT_DAG, route_trigger
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT
TARGET_DAG_ID = BACKGROUND_COLLECT_DAG
LOOKBACK_DAYS = 60
LOOKBACK_COOLDOWN_HOURS = 12
MAX_REQUESTS_PER_RUN = 2


def _date_gap(target_date: str) -> dict:
    toorder_by_store = _toorder_baemin_by_store(target_date)
    baemin_by_store = _baemin_orders_by_store(target_date)
    toorder_total = int(sum(toorder_by_store.values()))
    baemin_total = int(sum(baemin_by_store.values()))
    diff = toorder_total - baemin_total
    gap_rate = abs(diff) / toorder_total if toorder_total else 0.0
    missing_stores = sorted(
        store
        for store, amount in toorder_by_store.items()
        if int(amount or 0) > 0 and int(baemin_by_store.get(store) or 0) <= 0
    )
    return {
        "target_date": target_date,
        "toorder_total": toorder_total,
        "baemin_total": baemin_total,
        "diff": diff,
        "gap_rate": gap_rate,
        "missing_store_count": len(missing_stores),
        "missing_stores": missing_stores,
    }


def _lookback_candidates(today=None) -> list[dict]:
    today = today or pendulum.now(KST).date()
    candidates: list[dict] = []
    for offset in range(LOOKBACK_DAYS, 0, -1):
        target_date = today.subtract(days=offset).to_date_string()
        gap = _date_gap(target_date)
        if gap["toorder_total"] <= 0 or gap["missing_store_count"] <= 0:
            continue
        candidates.append(gap)
    candidates.sort(
        key=lambda item: (
            str(item["target_date"]),
            int(item["missing_store_count"]),
            abs(int(item["diff"])),
        ),
        reverse=True,
    )
    return candidates


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
def _active_or_recent_lookback_run_exists(target_date: str, session=None) -> bool:
    prefix = f"lookback_recovery__{target_date.replace('-', '')}__"
    active_states = (DagRunState.QUEUED, DagRunState.RUNNING)
    if (
        session.query(DagRun)
        .filter(DagRun.dag_id == TARGET_DAG_ID)
        .filter(DagRun.run_id.like(f"{prefix}%"))
        .filter(DagRun.state.in_(active_states))
        .count()
        > 0
    ):
        return True

    cutoff = pendulum.now(AIRFLOW_TIMEZONE).subtract(hours=LOOKBACK_COOLDOWN_HOURS)
    return (
        session.query(DagRun)
        .filter(DagRun.dag_id == TARGET_DAG_ID)
        .filter(DagRun.run_id.like(f"{prefix}%"))
        .filter(DagRun.execution_date >= cutoff)
        .count()
        > 0
    )


def trigger_missing_baemin_orders(**context) -> str:
    candidates = _lookback_candidates()

    if not candidates:
        return f"최근 {LOOKBACK_DAYS}일 배민 원천 매장별 lookback 대상 없음"

    triggered: list[str] = []
    skipped: list[str] = []
    admitted = 0
    base_execution_date = pendulum.instance(context["logical_date"]) if context.get("logical_date") else pendulum.now("UTC")
    for target in candidates:
        if admitted >= MAX_REQUESTS_PER_RUN:
            break
        target_date = target["target_date"]
        if _active_or_recent_lookback_run_exists(target_date):
            skipped.append(f"{target_date}:최근실행")
            continue

        run_stamp = context.get("ts_nodash") or context.get("ds_nodash")
        run_id = f"lookback_recovery__{target_date.replace('-', '')}__{run_stamp}"
        conf = {
            "target_date": target_date,
            "orders_only": True,
            "force_restart": True,
            "run_all_batches": False,
            "collect_range": None,
            "stores": target["missing_stores"],
            "stability_profile": "safe_daily",
            "source": dag_id,
            "reason": "baemin_orders_store_lookback_gap",
            "gap": target,
        }
        try:
            result = route_trigger(trigger_dag,
                dag_id=TARGET_DAG_ID,
                run_id=run_id,
                conf=conf,
                execution_date=base_execution_date.add(seconds=len(triggered)),
                replace_microseconds=False,
            )
        except DagRunAlreadyExists:
            skipped.append(f"{target_date}:이미존재")
            continue
        except Exception as exc:
            raise AirflowException(f"배민 lookback trigger 실패: {run_id}") from exc

        if result == "cancelled":
            skipped.append(f"{target_date}:사용자취소")
            continue
        if result in ("deferred", "existing"):
            if result == "deferred":
                admitted += 1
            skipped.append(f"{target_date}:보류목록저장" if result == "deferred" else f"{target_date}:이미존재")
            continue

        admitted += 1
        triggered.append(
            f"{run_id} target_date={target_date} "
            f"toorder={target['toorder_total']:,} "
            f"baemin={target['baemin_total']:,} "
            f"diff={target['diff']:,} "
            f"gap_rate={target['gap_rate']:.2%} "
            f"missing_stores={target['missing_store_count']}"
        )

    if not triggered:
        return f"lookback 대상은 있으나 신규 트리거 없음: {', '.join(skipped) or '-'}"
    return "배민 원천 lookback 트리거:\n" + "\n".join(triggered)


with DAG(
    dag_id=dag_id,
    schedule="5 */2 * * *",
    start_date=pendulum.datetime(2026, 8, 9, 0, 0, tz=KST),
    catchup=False,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
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
