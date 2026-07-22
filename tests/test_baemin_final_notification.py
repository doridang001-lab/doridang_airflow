import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))

from modules.transform.pipelines.db import DB_Beamin_Macro_upload as upload
from modules.transform.utility import notifier
from dags.db import DB_Beamin_Macro_Dags_Retry as retry_dag


def _notification_context():
    return {
        "source_dag_id": "DB_Beamin_Macro_Upload_Dags",
        "source_run_id": "scheduled__test",
        "target_date": "2026-07-21",
        "total_accounts": 12,
        "ingest_stats": {"folders": 2, "cleaned": 1, "skipped": 1, "failed": 0},
        "orders": {"total": 17, "matched": 17, "mismatched": 0, "unknown": 0},
        "ad_funnel": {"total": 6, "still_empty": 0},
        "toorder": {
            "compared": 15,
            "store_results": {
                "매장A": {"baemin": 10000, "toorder": 12000, "matched": False},
                "매장B": {"baemin": 8000, "toorder": 7000, "matched": False},
            },
            "mismatched_stores": ["매장A", "매장B"],
            "gap_stores": [],
            "missing_brand_stores": [],
        },
        "residual_failed": {"accounts": [], "stores": [], "orders": [], "ads": []},
        "hard_failures": [],
    }


def test_final_message_is_compact_and_uses_real_toorder_amounts():
    message = upload.build_final_notification_message(_notification_context())

    assert message.startswith("[배민 최종 결과] 부분완료")
    assert "대상 계정 12 / 수집 완료 12 / 잔여 실패 0" in message
    assert "ToOrder 비교 15 / 일치 13 / 불일치 2" in message
    assert "매장A: 배민=10,000 / ToOrder=12,000 / diff=2,000" in message
    assert "매장B: 배민=8,000 / ToOrder=7,000 / diff=-1,000" in message


def test_retry_result_overlays_root_mismatch_and_sends_terminal_counts():
    final_toorder = {
        "compared_count": 2,
        "store_results": {
            "매장A": {"baemin": 12000, "toorder": 12000, "matched": True},
            "매장B": {"baemin": 8000, "toorder": 7000, "matched": False},
        },
        "mismatched_stores": ["매장B"],
        "toorder_gap_stores": [],
        "missing_brand_stores": [],
    }

    message = upload.build_final_notification_message(
        _notification_context(),
        final_toorder_result=final_toorder,
        final_ad_funnel_result={"still_empty": []},
        residual_failed={"accounts": [], "stores": [], "orders": [], "ads": []},
        attempt=2,
        max_attempts=3,
    )

    assert "ToOrder 비교 15 / 일치 14 / 불일치 1" in message
    assert "Retry 2/3회" in message
    assert "매장A:" not in message
    assert "매장B:" in message


class _FakeTI:
    dag_id = "DB_Beamin_Macro_Upload_Dags"
    run_id = "scheduled__test"

    def __init__(self, values):
        self.values = values

    def xcom_pull(self, task_ids=None, key=None):
        return self.values.get((task_ids, key))


def test_upload_notification_is_suppressed_while_retry_is_running(monkeypatch):
    sent = []
    ti = _FakeTI(
        {
            ("trigger_retry_if_needed", "retry_triggered"): True,
            ("trigger_retry_if_needed", "notification_context"): _notification_context(),
        }
    )
    monkeypatch.setattr(upload, "send_telegram", sent.append)

    result = upload.notify_upload_result(ti=ti)

    assert result == "최종 Telegram 보류: Retry 진행 중"
    assert sent == []


def test_no_telegram_failure_callback_keeps_email_and_heal_queue(monkeypatch):
    calls = {"email": 0, "telegram": 0, "heal": 0}
    monkeypatch.setattr(notifier, "_send_email_alert", lambda *args: calls.__setitem__("email", calls["email"] + 1))
    monkeypatch.setattr(notifier, "send_telegram", lambda *args: calls.__setitem__("telegram", calls["telegram"] + 1))
    monkeypatch.setattr(notifier, "enqueue_heal_task", lambda *args: calls.__setitem__("heal", calls["heal"] + 1) or True)
    ti = SimpleNamespace(
        dag_id="DB_Beamin_Macro_Upload_Dags",
        task_id="ingest",
        run_id="scheduled__test",
        execution_date=SimpleNamespace(strftime=lambda fmt: "2026-07-22 07:40"),
        try_number=1,
        log_url="http://example/log",
        state="failed",
    )

    notifier.on_failure_callback_no_telegram({"task_instance": ti, "exception": RuntimeError("실패")})

    assert calls == {"email": 1, "telegram": 0, "heal": 1}


class _RetryTI:
    def __init__(self, values):
        self.values = values

    def xcom_pull(self, task_ids=None, key=None):
        return self.values.get((task_ids, key))


class _RetryDagRun:
    def __init__(self, conf):
        self.conf = conf

    def get_task_instances(self):
        return [SimpleNamespace(task_id="retry_collect", state="success")]


def _retry_context(*, residual_failed, toorder_result):
    notification_context = _notification_context()
    notification_context["toorder"] = upload._toorder_snapshot(toorder_result)
    conf = {
        "attempt": 1,
        "max_attempts": 3,
        "target_date": "2026-07-21",
        "source_run_id": "scheduled__test",
        "notification_context": notification_context,
    }
    ti = _RetryTI(
        {
            ("load_failed_and_accounts", "attempt"): 1,
            ("load_failed_and_accounts", "target_date"): "2026-07-21",
            ("retry_collect", "retry_result"): "재시도 1회 완료",
            ("retry_collect", "retry_payload"): {"residual_failed": residual_failed},
            ("validate_toorder", "toorder_result"): toorder_result,
            ("validate_ad_funnel", "ad_funnel_result"): {"still_empty": []},
        }
    )
    return {"ti": ti, "dag_run": _RetryDagRun(conf), "run_id": "retry__attempt_1"}


def test_intermediate_retry_triggers_next_without_telegram(monkeypatch):
    sent = []
    triggered = []
    toorder_result = {
        "compared_count": 1,
        "store_results": {"매장A": {"baemin": 10000, "toorder": 12000, "matched": False}},
        "mismatched_stores": ["매장A"],
        "missing_brand_stores": ["매장A"],
        "toorder_gap_stores": [],
    }
    context = _retry_context(
        residual_failed={"accounts": [], "stores": [], "orders": ["매장A"], "ads": []},
        toorder_result=toorder_result,
    )
    next_conf = {
        "failed_account_ids": ["acct-1"],
        "failed_accounts_ids_only": [],
        "failed_stores": [],
        "failed_orders": [{"account_id": "acct-1", "stores": []}],
        "failed_ads": [],
        "notification_context": _notification_context(),
    }
    monkeypatch.setattr(retry_dag, "retry_needed", lambda *args, **kwargs: True)
    monkeypatch.setattr(retry_dag, "build_next_retry_conf", lambda **kwargs: next_conf)
    monkeypatch.setattr(retry_dag, "send_telegram", sent.append)
    monkeypatch.setattr("airflow.api.common.trigger_dag.trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = retry_dag.notify_and_trigger_next(**context)

    assert "attempt 2 트리거" in result
    assert len(triggered) == 1
    assert sent == []


def test_terminal_retry_sends_exactly_one_final_telegram(monkeypatch):
    sent = []
    toorder_result = {
        "compared_count": 1,
        "store_results": {"매장A": {"baemin": 12000, "toorder": 12000, "matched": True}},
        "mismatched_stores": [],
        "missing_brand_stores": [],
        "toorder_gap_stores": [],
    }
    context = _retry_context(
        residual_failed={"accounts": [], "stores": [], "orders": [], "ads": []},
        toorder_result=toorder_result,
    )
    monkeypatch.setattr(retry_dag, "retry_needed", lambda *args, **kwargs: False)
    monkeypatch.setattr(retry_dag, "send_telegram", sent.append)

    result = retry_dag.notify_and_trigger_next(**context)

    assert result.startswith("[배민 최종 결과]")
    assert sent == [result]
