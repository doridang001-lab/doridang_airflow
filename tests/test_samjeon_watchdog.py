"""서비스 감시가 실제 중단과 정상적인 시작·네트워크 재시도를 구별한다."""
from datetime import datetime, timedelta, timezone

from scripts.watch_samjeon_material_agent import recovery_reason


def test_watchdog_restarts_stopped_or_stale_service_but_not_startup():
    now = datetime(2026, 10, 15, 7, 0, tzinfo=timezone.utc)
    iso = lambda seconds: (now - timedelta(seconds=seconds)).isoformat()
    healthy = {'checked_at': iso(30), 'telegram_poll_at': iso(20),
               'telegram_poll_error': None}
    running = {'state': 'Running', 'last_run': iso(600)}
    assert recovery_reason(running, healthy, now) is None
    assert recovery_reason({'state': 'Ready', 'last_run': iso(600)}, healthy, now) == 'task_stopped'
    assert recovery_reason({'state': 'Running', 'last_run': iso(30)}, {}, now) is None
    assert recovery_reason(running, {**healthy, 'checked_at': iso(181)}, now) == 'health_stale'
    assert recovery_reason(running, {**healthy, 'telegram_poll_at': iso(121)}, now) == \
        'telegram_poll_stale'
    assert recovery_reason(running, {**healthy, 'telegram_poll_error': 'telegram_temporary'}, now) is None
