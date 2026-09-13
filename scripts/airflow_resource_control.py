"""컨테이너 내부 자원 정책 적용. 호스트 watchdog에서 호출한다."""
import argparse
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def control(apply=False, worker_restarted=False):
    from airflow.models import Variable, Pool, TaskInstance
    from airflow.utils.session import create_session
    from sqlalchemy import text, func
    from modules.transform.utility.workload import RESOURCE_VARIABLE, memory_transition, dispatch_history_requests
    from modules.transform.utility.safe_recovery import recover_memory_failures, memory_failure, task_log_tail
    from modules.transform.utility.notifier import send_telegram
    with create_session() as session:
        if not session.execute(text("SELECT pg_try_advisory_xact_lock(7090902)")).scalar():
            return {"busy": True}
        old = Variable.get(RESOURCE_VARIABLE, default_var={}, deserialize_json=True)
        available = None
        swap_free = None
        try:
            info = {line.split(':')[0]: int(line.split()[1]) for line in
                    Path('/proc/meminfo').read_text().splitlines() if ':' in line}
            available = info['MemAvailable'] / 1024**2
            swap_free = info['SwapFree'] / 1024**2
        except (OSError, KeyError, ValueError):
            pass
        state = memory_transition(old, available)
        state.update(checked_at=time.time(), started_at=old.get('started_at', time.time()), swap_free_gib=swap_free)
        state['tasks'] = dict(session.query(TaskInstance.state, func.count()).filter(
            TaskInstance.state.in_(['running','queued','scheduled'])).group_by(TaskInstance.state).all())
        if worker_restarted:
            state.update(worker_interrupt_at=time.time(), interrupt_was_memory_pressure=old.get('mode') == 'all_paused')
        samples = old.get('observations', [])
        # 판단 모드에서는 DB·pool·알림을 변경하지 않는다.
        if not apply:
            return {k:v for k,v in state.items() if k != 'observations'}
        if time.time() - old.get('checked_at', 0) < 45 and not worker_restarted:
            return {'throttled': True, 'mode': old.get('mode')}
        saved = dict(old.get('saved_pool_slots', {}))
        pools = session.query(Pool).with_for_update().all()
        if state['mode'] == 'all_paused':
            for pool in pools:
                if pool.pool not in saved:
                    saved[pool.pool] = pool.slots
                pool.slots = 0
        elif state['mode'] == 'normal' and saved:
            for pool in pools:
                if pool.pool in saved and pool.slots == 0:
                    pool.slots = saved[pool.pool]
            saved = {}
        state['saved_pool_slots'] = saved
        state['observations'] = (samples + [{'at': state['checked_at'], 'available_gib': available,
                                             'mode': state['mode'], 'tasks': state['tasks']}])[-1500:]
        state['observation_24h_elapsed'] = state['checked_at'] - state['started_at'] >= 86400
        state['critical_entries'] = old.get('critical_entries', 0) + int(
            state['mode'] == 'all_paused' and old.get('mode') != 'all_paused')
        state['monitoring_gaps'] = old.get('monitoring_gaps', 0) + int(
            bool(old.get('checked_at')) and state['checked_at'] - old['checked_at'] > 180)
        measured = [v for v in (old.get('minimum_available_gib'), available) if v is not None]
        state['minimum_available_gib'] = min(measured) if measured else None
        from datetime import datetime, timezone
        cutoff = datetime.fromtimestamp(old.get('checked_at', state['started_at']), timezone.utc)
        new_failures = session.query(TaskInstance).filter(
            TaskInstance.state == 'failed', TaskInstance.end_date > cutoff).all()
        state['memory_failures'] = old.get('memory_failures', 0) + sum(
            memory_failure(task_log_tail(ti)) for ti in new_failures)
        report_due = state['observation_24h_elapsed'] and not old.get('observation_reported')
        if report_due:
            state['observation_reported'] = True
        # 24시간 경과는 기능 검증 합격과 구분한다. 누락 구간과 위험 진입도 함께 보존한다.
        observation_report = {k: state.get(k) for k in (
            'started_at', 'checked_at', 'observation_24h_elapsed', 'critical_entries',
            'monitoring_gaps', 'minimum_available_gib', 'memory_failures', 'mode', 'tasks')}
        Variable.set(RESOURCE_VARIABLE, state, serialize_json=True, session=session)
    from modules.transform.utility.paths import LOCAL_DB
    report = LOCAL_DB / 'airflow_ops' / 'resource_observation.json'
    report.parent.mkdir(parents=True, exist_ok=True)
    temp = report.with_suffix('.tmp')
    temp.write_text(json.dumps(observation_report, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(report)
    if report_due:
        send_telegram(f"[Auto-Heal] 메모리 보호 24시간 관찰 경과\n"
                      f"메모리 실패 {state['memory_failures']}건 / 위험 진입 {state['critical_entries']}회 / "
                      f"감시 간격 누락 {state['monitoring_gaps']}회\n"
                      f"최저 가용 메모리 {state['minimum_available_gib']} GiB\n"
                      "수집 데이터 정합성 검증과는 별도 운영 관찰 결과입니다.")
    if old and old.get('mode') != state['mode']:
        logger.info("메모리 실행 제한 변경: %s → %s, 가용 메모리=%s GiB",
                    old.get('mode'), state['mode'], available)
    if state['mode'] == 'normal':
        state['dispatch'] = dispatch_history_requests()
        state['recovered'] = recover_memory_failures(state)
    return {k:v for k,v in state.items() if k != 'observations'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--worker-restarted', action='store_true')
    args = parser.parse_args()
    print(json.dumps(control(args.apply, args.worker_restarted), ensure_ascii=False))
