"""메모리 장애에 한정한 검증된 데이터 작업의 1회 복구."""
import hashlib
from datetime import datetime, timezone
from pathlib import Path

ALLOW = {
    "DB_OKPOS_Sales_Today_Dags": {"download_receipt_batch", "download_receipt", "save_to_raw"},
    "DB_MenuHierarchy_Test_Dags": {"build_orders"},
    "DB_DeliveryCommission_Dags": {"monitor_baemin_settlement_missing", "build_delivery_commission",
                                   "build_delivery_revenue", "build_store_cost_allocation", "trigger_baemin_orders_only_recollect"},
    "DB_Beamin_Macro_Upload_Validate_Dags": {"validate_toorder"},
}
ALLOW["DB_Beamin_Macro_Backfill_Validate_Dags"] = {"validate_toorder"}


def claim_key(dag_id, run_id, task_id, map_index=-1):
    return "ops_task_claim_" + hashlib.sha256(
        f"{dag_id}|{run_id}|{task_id}|{map_index}".encode()).hexdigest()


def memory_failure(log):
    return "Cannot allocate memory" in log or "MemoryError" in log or "OOMKilled" in log


def task_log_tail(ti):
    root = Path('/opt/airflow/logs') / ('dag_id=' + ti.dag_id) / ('run_id=' + ti.run_id) / ('task_id=' + ti.task_id)
    if ti.map_index >= 0:
        root /= 'map_index=' + str(ti.map_index)
    logs = sorted(root.glob('attempt=*.log'), key=lambda p: p.stat().st_mtime) if root.exists() else []
    if not logs:
        return ''
    with logs[-1].open('rb') as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - 200000))
        return stream.read().decode('utf-8', errors='replace')


def recover_memory_failures(policy):
    from airflow.models import TaskInstance, DagRun, Variable, XCom
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.taskinstance import clear_task_instances
    from airflow.utils.session import create_session
    from sqlalchemy import text
    from modules.transform.utility.notifier import send_telegram
    if policy.get("mode") != "normal" or policy.get("healthy_samples", 0) < 5:
        return []
    since = datetime.fromtimestamp(policy["started_at"], timezone.utc)
    cleared = []
    with create_session() as session:
        if not session.execute(text("SELECT pg_try_advisory_xact_lock(7090903)")).scalar():
            return []
        failed = session.query(TaskInstance).filter(
            TaskInstance.state == "failed", TaskInstance.end_date >= since,
            TaskInstance.dag_id.in_(list(ALLOW)),
        ).order_by(TaskInstance.end_date).all()
        for ti in failed:
            if ti.task_id not in ALLOW[ti.dag_id]:
                continue
            from modules.transform.utility.history_admission import (
                backfill_dags, lock, active_runs, request_cancelled, admission_paused,
            )
            if ti.dag_id in backfill_dags():
                # 배정기와 같은 잠금에서 확인하고 clear까지 커밋한다.
                lock(session)
                run = session.query(DagRun).filter_by(dag_id=ti.dag_id, run_id=ti.run_id).one()
                if (admission_paused(session)
                        or active_runs(session)
                        or (run.conf or {}).get('history_deferred')
                        or request_cancelled(session, {
                            'dag_id': ti.dag_id, 'run_id': ti.run_id, 'conf': run.conf or {},
                        })):
                    continue
            key = claim_key(ti.dag_id, ti.run_id, ti.task_id, ti.map_index)
            claim = session.query(Variable).filter_by(key=key).first()
            if claim:
                # 1회 복구 이후 실패는 다시 clear하지 않고 한 번만 보고한다.
                if claim.val == "memory_retry":
                    claim.val = "memory_retry_failed_reported"
                    send_telegram(f"[Auto-Heal] 메모리 복구 재처리 실패\n{ti.dag_id} / {ti.task_id}\n추가 자동 재처리 중지")
                continue
            log = task_log_tail(ti)
            interrupted = float(policy.get("worker_interrupt_at", 0))
            correlated = (ti.start_date is not None and interrupted >= ti.start_date.timestamp()
                          and ti.end_date is not None and 0 <= ti.end_date.timestamp() - interrupted < 900
                          and policy.get("interrupt_was_memory_pressure", False))
            if not memory_failure(log) and not correlated:
                continue
            others = session.query(DagRun).filter(
                DagRun.dag_id == ti.dag_id, DagRun.run_id != ti.run_id,
                DagRun.state.in_(["queued", "running"]),
            ).first()
            if others:
                continue
            tasks = session.query(TaskInstance).filter_by(dag_id=ti.dag_id, run_id=ti.run_id).all()
            if any(t.state in ("running", "queued", "scheduled") for t in tasks):
                continue
            # 날짜/배치 계획을 포함한 원래 입력이 사라진 실행은 재개하지 않는다.
            inputs = session.query(XCom).filter_by(dag_id=ti.dag_id, run_id=ti.run_id)
            if not inputs.first():
                continue
            if ti.dag_id == 'DB_MenuHierarchy_Test_Dags' and not inputs.filter_by(task_id='resolve_ym', key='ym_list').first():
                continue
            if ti.dag_id == 'DB_OKPOS_Sales_Today_Dags' and not inputs.filter_by(task_id='resolve_today', key='sale_date').first():
                continue
            run = session.query(DagRun).filter_by(dag_id=ti.dag_id, run_id=ti.run_id).one()
            if 'Validate' in ti.dag_id:
                handoff = (run.conf or {}).get('handoff_path')
                if not handoff or not Path(handoff).is_file():
                    continue
            dag = session.query(SerializedDagModel).filter_by(dag_id=ti.dag_id).one().dag
            downstream = set(dag.get_task(ti.task_id).get_flat_relative_ids(upstream=False))
            selected = [ti] + [t for t in tasks if t.task_id in downstream
                and t.task_id in ALLOW[ti.dag_id] and t.state == "upstream_failed"]
            # 먼저 영구 claim을 확보한다. 기존 watcher도 같은 key를 사용한다.
            keys = [claim_key(t.dag_id, t.run_id, t.task_id, t.map_index) for t in selected]
            if session.query(Variable).filter(Variable.key.in_(keys)).first():
                continue
            for selected_key in keys:
                session.add(Variable(key=selected_key, val="memory_retry"))
            session.flush()
            clear_task_instances(selected, session=session, dag=dag)
            cleared.append({"dag_id": ti.dag_id, "task_id": ti.task_id, "map_index": ti.map_index})
            # 부하를 한꺼번에 되살리지 않는다.
            break
    return cleared
