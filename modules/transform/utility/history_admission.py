"""배민 백필 요청 보존과 전체 DAG 실행 1건 배정. 운영 데이터는 변경하지 않는다."""
from __future__ import annotations

import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)
PREFIX = "history_request_"
PAUSED = "history_admission_paused_v1"
LOCK_ID = 7090901


def backfill_dags():
    from modules.transform.utility.workload import (
        BACKGROUND_COLLECT_DAG, BACKGROUND_RETRY_DAG,
        BACKGROUND_UPLOAD_DAG, BACKGROUND_VALIDATE_DAG,
    )
    return {BACKGROUND_COLLECT_DAG, BACKGROUND_RETRY_DAG,
            BACKGROUND_UPLOAD_DAG, BACKGROUND_VALIDATE_DAG}


def lock(session):
    from sqlalchemy import text
    session.execute(text(f"SELECT pg_advisory_xact_lock({LOCK_ID})"))


def admission_paused(session):
    from airflow.models import Variable
    row = session.query(Variable).filter_by(key=PAUSED).first()
    return bool(json.loads(row.val)) if row else False


def active_runs(session):
    from airflow.models import DagRun
    return session.query(DagRun).filter(
        DagRun.dag_id.in_(backfill_dags()), DagRun.state.in_(["queued", "running"])
    ).all()


def cancelled(session, dag_id, run_id):
    """태스크 오류와 구분되는 UI/API 실패처리 감사 기록만 취소로 취급한다."""
    from airflow.models.log import Log
    return session.query(Log.id).filter(
        Log.dag_id == dag_id, Log.run_id == run_id, Log.event == "dagrun_failed"
    ).first() is not None


def request_key(dag_id, run_id):
    return PREFIX + hashlib.sha256((dag_id + run_id).encode()).hexdigest()


def scope(conf):
    return (conf.get("target_date"), bool(conf.get("orders_only")),
            tuple(sorted(str(x) for x in (conf.get("stores") or []))),
            json.dumps(conf.get("collect_range"), sort_keys=True),
            json.dumps(conf.get("brands"), sort_keys=True), bool(conf.get("run_all_batches")))


def with_parent(conf, context=None):
    conf = {**conf, "workload": "history"}
    if context is None:
        try:
            from airflow.operators.python import get_current_context
            context = get_current_context()
        except Exception:
            context = {}
    run = context.get("dag_run")
    ti = context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(run, "dag_id", None)
    run_id = getattr(run, "run_id", None) or context.get("run_id")
    if dag_id in backfill_dags() and run_id:
        parent_conf = getattr(run, "conf", None) or {}
        ancestors = list(parent_conf.get("history_ancestors") or [])
        parent = {"dag_id": dag_id, "run_id": run_id}
        if parent not in ancestors:
            ancestors.append(parent)
        conf["history_ancestors"] = ancestors
    return conf


def ancestors(conf):
    result = list(conf.get("history_ancestors") or [])
    # 배포 전 요청에도 기존 source 필드가 정확하게 있으면 연결을 보존한다.
    source_dag_id = conf.get("source_dag_id")
    if not source_dag_id and conf.get("source") == "upload_ingest":
        from modules.transform.utility.workload import BACKGROUND_UPLOAD_DAG
        source_dag_id = BACKGROUND_UPLOAD_DAG
    if source_dag_id in backfill_dags() and conf.get("source_run_id"):
        item = {"dag_id": source_dag_id, "run_id": conf["source_run_id"]}
        if item not in result:
            result.append(item)
    return result


def request_cancelled(session, request):
    return any(cancelled(session, item["dag_id"], item["run_id"])
               for item in [request, *ancestors(request["conf"])])


def save_request(session, dag_id, run_id, conf, **options):
    from airflow.models import Variable
    key = request_key(dag_id, run_id)
    row = session.query(Variable).filter_by(key=key).first()
    previous = json.loads(row.val) if row else {}
    request = {"dag_id": dag_id, "run_id": run_id, "conf": conf,
               "created_at": previous.get("created_at", time.time()), **options}
    Variable.set(key, request, serialize_json=True, session=session)
    return request


def enqueue(dag_id, run_id, conf, **kwargs):
    """생산자는 DAG를 생성하지 않는다. 등록은 단일 배정기만 담당한다."""
    from airflow.models import DagRun, Variable
    from airflow.utils.session import create_session
    from modules.transform.utility.workload import BACKGROUND_COLLECT_DAG
    unknown = set(kwargs) - {"execution_date", "replace_microseconds"}
    if unknown:
        raise TypeError(f"지원하지 않는 백필 트리거 옵션: {sorted(unknown)}")
    options = dict(kwargs)
    if options.get("execution_date") is not None:
        options["execution_date"] = options["execution_date"].isoformat()
    request = {"dag_id": dag_id, "run_id": run_id, "conf": conf}
    with create_session() as session:
        lock(session)
        if request_cancelled(session, request):
            return "cancelled"
        if session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).first():
            return "existing"
        if dag_id == BACKGROUND_COLLECT_DAG:
            if any(r.dag_id == dag_id and scope(r.conf or {}) == scope(conf)
                   for r in active_runs(session)):
                return "existing"
            for row in session.query(Variable).filter(Variable.key.like(PREFIX + "%")).all():
                other = json.loads(row.val)
                if (other["dag_id"] == dag_id and other["run_id"] != run_id
                        and scope(other["conf"]) == scope(conf)
                        and not request_cancelled(session, other)):
                    return "existing"
        save_request(session, dag_id, run_id, conf, trigger_options=options)
        return "deferred"


def priority(request):
    # 후속 작업 먼저, 신규 수집은 기존 정책대로 최근 날짜 및 먼저 접수한 순서.
    from modules.transform.utility.workload import BACKGROUND_COLLECT_DAG
    return (request["dag_id"] != BACKGROUND_COLLECT_DAG or bool(ancestors(request["conf"])),
            request["conf"].get("target_date", ""),
            -request.get("created_at", 0))


def create_run(session, request):
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils import timezone
    from airflow.utils.types import DagRunType
    model = session.query(SerializedDagModel).filter_by(dag_id=request["dag_id"]).one()
    dag = model.dag
    options = request.get("trigger_options") or {}
    logical_date = timezone.parse(options["execution_date"]) if options.get("execution_date") else timezone.utcnow()
    if options.get("replace_microseconds"):
        logical_date = logical_date.replace(microsecond=0)
    dag.create_dagrun(
        run_id=request["run_id"], conf=request["conf"], state="queued",
        execution_date=logical_date, run_type=DagRunType.MANUAL, external_trigger=True,
        data_interval=dag.timetable.infer_manual_data_interval(run_after=logical_date),
        dag_hash=model.dag_hash, session=session,
    )


def dispatch():
    from airflow.models import DagRun, Variable, TaskInstance
    from airflow.utils.session import create_session
    from modules.transform.utility.workload import BACKGROUND_COLLECT_DAG, refresh_lookback_conf
    result = {"created": 0, "resolved": 0, "cancelled": 0, "deferred_requests": 0}
    with create_session() as session:
        lock(session)
        rows = session.query(Variable).filter(Variable.key.like(PREFIX + "%")).all()
        requests = sorted([(row, json.loads(row.val)) for row in rows],
                          key=lambda pair: priority(pair[1]), reverse=True)
        result["deferred_requests"] = len(requests)
        paused = admission_paused(session)
        occupied = bool(active_runs(session))
        for row, request in requests:
            if request_cancelled(session, request):
                session.delete(row)
                result["cancelled"] += 1
                result["deferred_requests"] -= 1
                continue
            if occupied or paused:
                continue
            parents = ancestors(request["conf"])
            if any(not session.query(DagRun).filter(
                    DagRun.dag_id == parent["dag_id"], DagRun.run_id == parent["run_id"],
                    DagRun.state.in_(["success", "failed"])).first() for parent in parents):
                continue
            conf = request["conf"]
            if request["dag_id"] == BACKGROUND_COLLECT_DAG:
                try:
                    conf = refresh_lookback_conf(conf)
                except Exception:
                    logger.exception("백필 누락 재검증 실패, 요청 보존: %s", request["run_id"])
                    continue
            if conf is None:
                session.delete(row)
                result["resolved"] += 1
                result["deferred_requests"] -= 1
                continue
            request = {**request, "conf": conf}
            existing = session.query(DagRun).filter_by(
                dag_id=request["dag_id"], run_id=request["run_id"]).first()
            if existing is None:
                create_run(session, request)
            elif request.get("resume_unstarted") and (existing.conf or {}).get("history_deferred"):
                tasks = session.query(TaskInstance).filter_by(
                    dag_id=existing.dag_id, run_id=existing.run_id).all()
                if any(t.start_date is not None or t.state in ("running", "queued") for t in tasks):
                    raise RuntimeError("미실행 백필 전환 후 태스크 시작 발견: " + existing.run_id)
                existing.conf = {**conf, "history_deferred": False}
                for task in tasks:
                    task.state = None
                existing.set_state("queued")
            else:
                session.delete(row)
                result["resolved"] += 1
                result["deferred_requests"] -= 1
                continue
            # 생성과 보류 삭제가 같은 트랜잭션: 장애 시 둘 다 롤백한다.
            session.delete(row)
            session.flush()
            result["created"] = 1
            result["deferred_requests"] -= 1
            occupied = True
        result["active"] = len(active_runs(session))
    return result


def defer_unstarted(apply=False):
    """일시 정지한 백필의 미시작 실행만 보류로 전환한다. 실행 기록은 보존한다."""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    result = {"deferred": [], "started": []}
    with create_session() as session:
        lock(session)
        for run in active_runs(session):
            tasks = session.query(TaskInstance).filter_by(dag_id=run.dag_id, run_id=run.run_id).all()
            if any(t.start_date is not None or t.state in ("running", "queued") for t in tasks):
                result["started"].append({"dag_id": run.dag_id, "run_id": run.run_id})
                continue
            result["deferred"].append({"dag_id": run.dag_id, "run_id": run.run_id})
            if apply:
                conf = {**(run.conf or {}), "workload": "history", "history_deferred": True}
                save_request(session, run.dag_id, run.run_id, conf, resume_unstarted=True)
                run.conf = conf
                run.set_state("failed")
                for task in tasks:
                    task.state = "skipped"
    return result
