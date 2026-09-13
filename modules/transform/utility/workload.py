"""운영 작업과 과거 복구의 자원 정책. Airflow 의존성은 실행 시 로드한다."""
from __future__ import annotations

import copy
import importlib
import logging
import time
from datetime import timedelta

BACKGROUND_QUEUE = "history"
BACKGROUND_COLLECT_DAG = "DB_Beamin_Macro_Backfill_Dags"
BACKGROUND_RETRY_DAG = "DB_Beamin_Macro_Backfill_Retry_Dags"
NIGHTLY_DAG = "DB_UnifiedSales_Nightly_Dags"
BACKGROUND_UPLOAD_DAG = "DB_Beamin_Macro_Backfill_Upload_Dags"
BACKGROUND_VALIDATE_DAG = "DB_Beamin_Macro_Backfill_Validate_Dags"
BACKGROUND_DAGS = {BACKGROUND_COLLECT_DAG, BACKGROUND_RETRY_DAG, BACKGROUND_UPLOAD_DAG, BACKGROUND_VALIDATE_DAG, NIGHTLY_DAG}
RESOURCE_VARIABLE = "airflow_resource_policy_v1"
MAX_PENDING = 1
MAX_NEW = 1
logger = logging.getLogger(__name__)


def memory_transition(previous: dict, available_gib: float | None) -> dict:
    result = dict(previous)
    old = previous.get("mode", "normal")
    good = int(previous.get("healthy_samples", 0))
    if available_gib is None:
        mode, good = ("all_paused" if old == "all_paused" else "history_paused"), 0
    elif available_gib < 2:
        mode, good = "all_paused", 0
    elif available_gib < 4:
        mode, good = ("all_paused" if old == "all_paused" else "history_paused"), 0
    elif old != "normal":
        good = good + 1 if available_gib >= 6 else 0
        mode = "normal" if good >= 5 else old
    else:
        mode, good = "normal", min(5, good + 1) if available_gib >= 6 else 0
    result.update(mode=mode, healthy_samples=good, available_gib=available_gib)
    return result


def resource_snapshot() -> dict:
    from airflow.models import Variable
    state = Variable.get(RESOURCE_VARIABLE, default_var={}, deserialize_json=True)
    if time.time() - state.get("checked_at", 0) > 180:
        return {**state, "mode": "history_paused", "stale": True}
    return state


def is_background(conf: dict | None = None, source_dag_id: str = "") -> bool:
    conf = conf or {}
    return (conf.get("workload") == "history" or source_dag_id in BACKGROUND_DAGS or str(
        conf.get("source_dag_id", "")
    ) in BACKGROUND_DAGS or conf.get("source") == "DB_Beamin_Macro_Lookback_Trigger_Dags"
        or conf.get("source_dag_id") == "DB_DeliveryCommission_Dags"
        or "lookback_recovery__" in str(conf.get("source_run_id", ""))
        or "delivery_commission_settlement_recollect__" in str(conf.get("source_run_id", "")))


def retry_dag_id(conf: dict | None = None, source_dag_id: str = "") -> str:
    return BACKGROUND_RETRY_DAG if is_background(conf, source_dag_id) else "DB_Beamin_Macro_Dags_Retry"


def background_wait_reason() -> str | None:
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session
    snapshot = resource_snapshot()
    if snapshot.get("mode") != "normal":
        return "resource_stale" if snapshot.get("stale") else "memory_pressure"
    with create_session() as session:
        # 이미 실행 중인 구버전 수집 프로세스에는 새 계정 잠금이 없다.
        # 일반 수집이 끝난 뒤 과거 수집을 배정해 프로필 충돌도 방지한다.
        if session.query(TaskInstance).filter(
            TaskInstance.dag_id.in_(["DB_Beamin_Macro_Dags", "DB_Beamin_Macro_Dags_Retry"]),
            TaskInstance.state == "running",
        ).first():
            return "daily_collection_running"
        # 실행 중 일반 작업은 자기 워커에서 계속 진행한다. 대기할 때만 양보한다.
        if session.query(TaskInstance).filter(
            TaskInstance.state.in_(["queued", "scheduled"]),
            TaskInstance.queue != BACKGROUND_QUEUE,
        ).first():
            return "daily_tasks_waiting"
    return None


def background_ready() -> bool:
    reason = background_wait_reason()
    if reason:
        logger.info("과거 작업 대기: reason=%s, 다음 확인=60초", reason)
    return reason is None


# 나머지는 보수적으로 자원 확인 후 시작한다. 실행 중인 작업은 중단하지 않는다.
_HISTORY_LIGHT_TASKS = frozenset({
    "load_accounts", "load_failed_and_accounts", "init_staging", "has_pending",
    "merge_retry_payloads", "trigger_upload_after_export", "trigger_validate",
    "trigger_retry_if_needed", "notify_collection_result", "notify_upload_result",
    "notify_and_trigger_next", "cleanup_manual_baemin_orders",
})


def history_task_ready(target_task_id: str, **context) -> bool:
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session
    # 운영 중 DAG에 센서가 추가되어도 이미 끝난 작업 앞에서 기다리지 않는다.
    with create_session() as session:
        state = session.query(TaskInstance.state).filter_by(
            dag_id=context["dag"].dag_id, run_id=context["run_id"],
            task_id=target_task_id, map_index=-1,
        ).scalar()
    if state in {"success", "failed", "skipped", "upstream_failed", "removed"}:
        return True
    return background_ready()


# wait_resource 센서 timeout(30일)에 맞춘 상한. 그 이상 남은 런은 어차피 좀비다.
BACKGROUND_DAGRUN_TIMEOUT = timedelta(days=31)


def build_background_dag(template_module: str, dag_id: str):
    """수집 구현·의존 관계를 복제하지 않고 기존 DAG 정의를 재사용한다."""
    from airflow.timetables.simple import NullTimetable
    from airflow.models.dag import DagContext
    from airflow.task.priority_strategy import validate_and_load_priority_weight_strategy
    from airflow.sensors.python import PythonSensor
    from airflow.utils.trigger_rule import TriggerRule
    registered = set(DagContext.autoregistered_dags)
    try:
        template = importlib.import_module(template_module).dag
    finally:
        # 가져온 원본 DAG가 이 파일의 DAG로 자동 등록되는 것을 막는다.
        DagContext.autoregistered_dags.intersection_update(registered)
    dag = copy.deepcopy(template)
    dag.dag_id = dag_id
    dag._dag_display_name = dag_id
    dag.schedule_interval = None
    dag.timetable = NullTimetable()
    dag._max_active_tasks = 1
    dag.max_active_runs = 1
    # 원본 DAG의 dagrun_timeout을 그대로 물려받으면 안 된다.
    # history 큐 DAG는 wait_resource 센서가 최대 30일까지 자원을 기다리는 설계라,
    # 일반 수집용 타임아웃(6~12시간)을 상속하면 정상 대기 중인 백필이 죽는다.
    dag.dagrun_timeout = BACKGROUND_DAGRUN_TIMEOUT
    dag.tags = list(set(dag.tags + ["history"]))
    for task in list(dag.tasks):
        task.queue = BACKGROUND_QUEUE
        task.priority_weight = 1
        task.weight_rule = validate_and_load_priority_weight_strategy("absolute")
        # 일반 PythonOperator에는 ReadyToRescheduleDep가 없으므로 예외만으로
        # 대기하면 스케줄러가 다음 확인 시각 전에도 작업을 다시 실행한다.
        if task.task_id in _HISTORY_LIGHT_TASKS:
            continue
        upstream_ids = set(task.upstream_task_ids)
        gate = PythonSensor(
            task_id=f"wait_resource__{task.task_id}", dag=dag,
            python_callable=history_task_ready, op_kwargs={"target_task_id": task.task_id},
            mode="reschedule", poke_interval=60,
            timeout=30 * 24 * 60 * 60, retries=0,
            queue=BACKGROUND_QUEUE, pool="default_pool", priority_weight=1,
            weight_rule="absolute", trigger_rule=task.trigger_rule,
        )
        for upstream_id in upstream_ids:
            upstream = dag.get_task(upstream_id)
            upstream.downstream_task_ids.discard(task.task_id)
            task.upstream_task_ids.discard(upstream_id)
            upstream >> gate
        task.trigger_rule = TriggerRule.ALL_SUCCESS
        gate >> task
    return dag


def pending_history(session) -> int:
    from modules.transform.utility.history_admission import active_runs
    return len(active_runs(session))


def admit_history(dag_id: str, run_id: str, conf: dict, **kwargs):
    from modules.transform.utility.history_admission import enqueue
    return enqueue(dag_id, run_id, {**conf, "workload": "history"}, **kwargs)


def route_trigger(trigger, *, dag_id, run_id, conf, history_context=None, **kwargs):
    from modules.transform.utility.history_admission import backfill_dags, with_parent
    if dag_id in backfill_dags():
        return admit_history(dag_id, run_id, with_parent(conf, history_context), **kwargs)
    return trigger(dag_id=dag_id, run_id=run_id, conf=conf, **kwargs)


def refresh_lookback_conf(conf: dict) -> dict | None:
    """자동 누락 복구만 재검증한다. 수동/정산 요청의 범위는 변경하지 않는다."""
    if conf.get("reason") != "baemin_orders_store_lookback_gap" or not conf.get("stores"):
        return conf
    from modules.transform.pipelines.db.DB_Beamin_Macro_validate import (
        _baemin_orders_by_store, _toorder_baemin_by_store,
    )
    target_date = conf["target_date"]
    baemin = _baemin_orders_by_store(target_date)
    toorder = _toorder_baemin_by_store(target_date)
    # 비교 원천 자체가 없어졌으면 '복구 완료'로 판단하지 않는다.
    remaining = [store for store in conf["stores"]
                 if store not in toorder or int(baemin.get(store) or 0) <= 0]
    return {**conf, "stores": remaining} if remaining else None


def dispatch_history_requests():
    from modules.transform.utility.history_admission import dispatch
    return dispatch()
