"""notifier 실패분류 회귀 테스트.

시계 역행/스택 재시작으로 태스크가 아예 실행되지 않은 경우를 코드 오류로 오인해
Codex 자동수정을 돌리면 멀쩡한 코드를 고치게 된다. 이 경계를 고정한다.
"""

import json

import pytest

from modules.transform.utility import notifier

# 실제 Telegram 알림에 찍힌 스케줄러 메시지
EXECUTOR_MISMATCH = (
    "Executor CeleryExecutor(parallelism=32) reported that the task instance "
    "<TaskInstance: DB_DeliveryCommission_Dags.monitor_baemin_settlement_missing "
    "manual__2026-08-24T04:03:55.448896+00:00 [queued]> finished with state success, "
    "but the task instance's state attribute is queued. Learn more: "
    "https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html"
)

# 실제 워커 로그(attempt=1.log)
WORKER_FUTURE_DATE = (
    "Dependencies not met for <TaskInstance: DB_DeliveryCommission_Dags."
    "monitor_baemin_settlement_missing manual__2026-08-24T04:03:55.448896+00:00 [queued]>, "
    "dependency 'Execution Date' FAILED: Execution date 2026-08-24T04:03:55.448896+00:00 "
    "is in the future (the current date is 2026-08-24T04:03:20.566556+00:00).\n"
    "Task is not able to be run"
)

# 큐 적체 타임아웃(자정 스택 재시작 창)
STUCK_IN_QUEUED = (
    "Task instance <TaskInstance: Strategy_ScheduleGuard_01_Overdue_Dags."
    "check_overdue_schedules scheduled__2026-08-24T14:50:00+00:00 [queued]> "
    "is in queued state for longer than 600 seconds; marking it as failed. timed out"
)


@pytest.mark.parametrize(
    "text",
    [EXECUTOR_MISMATCH, WORKER_FUTURE_DATE, STUCK_IN_QUEUED],
    ids=["executor_mismatch", "future_logical_date", "stuck_in_queued"],
)
def test_not_run_failures_are_classified_separately(text):
    assert notifier.classify_failure(text) == notifier.NOT_RUN_FAILURE_CLASS


def test_stuck_in_queued_wins_over_transient_patterns():
    """미실행 문구에 timeout 계열 단어가 섞여 있어도 transient로 새면 안 된다."""
    assert "timed out" in STUCK_IN_QUEUED
    assert notifier.classify_failure(STUCK_IN_QUEUED) != "transient"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("ModuleNotFoundError: No module named 'pandas'", "code_error"),
        ("BadZipFile: File is not a zip file", "data_file_error"),
        ("chromedriver crashed while starting session", "transient"),
        ("login failed for account 12", "data_or_account"),
        ("something went sideways", "unknown"),
    ],
)
def test_existing_classes_are_unchanged(text, expected):
    assert notifier.classify_failure(text) == expected


class _FakeTaskInstance:
    dag_id = "DB_DeliveryCommission_Dags"
    task_id = "monitor_baemin_settlement_missing"
    run_id = "manual__2026-08-24T04:03:55.448896+00:00"
    try_number = 1
    max_tries = 0
    state = "failed"
    log_url = "http://localhost:8080/log"

    class _ExecutionDate:
        @staticmethod
        def strftime(fmt):
            return "2026-08-24 04:03"

    execution_date = _ExecutionDate()


def _run_callback(monkeypatch, tmp_path, exception_text):
    sent = []
    monkeypatch.setattr(notifier, "HEAL_QUEUE_PATH", tmp_path / "heal_queue.jsonl")
    monkeypatch.setattr(notifier, "_send_email_alert", lambda subject, body: None)
    monkeypatch.setattr(notifier, "send_telegram", lambda text: sent.append(text) or True)

    notifier.on_failure_callback({"task_instance": _FakeTaskInstance(), "exception": exception_text})

    lines = (tmp_path / "heal_queue.jsonl").read_text(encoding="utf-8").splitlines()
    return sent, [json.loads(line) for line in lines if line.strip()]


def test_not_run_alert_does_not_trigger_codex(tmp_path, monkeypatch):
    sent, entries = _run_callback(monkeypatch, tmp_path, EXECUTOR_MISMATCH)

    assert len(sent) == 1
    assert sent[0].startswith("[DAG 미실행]")
    assert notifier._TELEGRAM_TRIGGER not in sent[0]
    assert notifier.NOT_RUN_REASON in sent[0]

    assert len(entries) == 1
    assert entries[0]["failure_class"] == notifier.NOT_RUN_FAILURE_CLASS
    assert entries[0]["auto_edit_allowed"] is False
    assert entries[0]["rerun_requested"] is True


def test_real_failure_still_triggers_codex(tmp_path, monkeypatch):
    sent, entries = _run_callback(monkeypatch, tmp_path, "ModuleNotFoundError: No module named 'pandas'")

    assert len(sent) == 1
    assert sent[0].startswith("[DAG 실패]")
    assert notifier._TELEGRAM_TRIGGER in sent[0]

    assert entries[0]["failure_class"] == "code_error"
    assert entries[0]["auto_edit_allowed"] is True
    assert "rerun_requested" not in entries[0]
