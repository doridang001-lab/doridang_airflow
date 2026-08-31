import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))

from airflow.exceptions import AirflowException
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


class _FakeTelegramResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_baemin_final_complete_telegram_is_suppressed(monkeypatch):
    calls = []
    monkeypatch.setattr(notifier, "_get_telegram_creds", lambda: ("token", "chat"))
    monkeypatch.setattr(
        notifier.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)) or _FakeTelegramResponse(),
    )

    assert notifier.send_telegram("[배민 최종 결과] 완료\ntarget_date: 2026-07-21") is True
    assert calls == []


def test_baemin_final_partial_telegram_is_sent(monkeypatch):
    calls = []
    monkeypatch.setattr(notifier, "_get_telegram_creds", lambda: ("token", "chat"))
    monkeypatch.setattr(
        notifier.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)) or _FakeTelegramResponse(),
    )

    assert notifier.send_telegram("[배민 최종 결과] 부분완료\ntarget_date: 2026-07-21") is True
    assert len(calls) == 1


def test_baemin_housekeeping_telegrams_are_suppressed():
    assert notifier._should_send_telegram("[배민 upload inbox 적체] pattern=manual__ 3개 폴더가 12시간 넘게 미적재") is False
    assert notifier._should_send_telegram("[배민 inbox 잔해 회수] upload_inbox에서 중단된 export 2건을 quarantine으로 옮김") is False


def test_schedule_correction_telegram_is_suppressed():
    assert notifier._should_send_telegram("[Airflow 스케줄 미생성 보정]\ndag_id=DB_UnifiedSales") is False


class _FakeTI:
    dag_id = "DB_Beamin_Macro_Upload_Dags"
    run_id = "scheduled__test"

    def __init__(self, values):
        self.values = values

    def xcom_pull(self, task_ids=None, key=None):
        return self.values.get((task_ids, key))


def _ingest_ti(stats):
    return _FakeTI({("ingest", "ingest_stats"): stats})


def test_pc2_empty_ingest_skips_downstream():
    result = upload.has_ingested_folders(
        ti=_ingest_ti({"folders": 0, "cleaned": 0}),
        dag_run=SimpleNamespace(conf={"skip_if_empty": True}),
    )

    assert result is False


def test_pc2_empty_ingest_removes_handoff(tmp_path):
    handoff = tmp_path / "handoff.json"
    handoff.write_text("{}", encoding="utf-8")
    ti = _FakeTI(
        {
            ("ingest", "ingest_stats"): {"folders": 0, "cleaned": 0},
            ("ingest", "handoff_path"): str(handoff),
        }
    )

    result = upload.has_ingested_folders(
        ti=ti,
        dag_run=SimpleNamespace(conf={"skip_if_empty": True}),
    )

    assert result is False
    assert not handoff.exists()


def test_regular_empty_ingest_keeps_existing_downstream_behavior():
    result = upload.has_ingested_folders(
        ti=_ingest_ti({"folders": 0, "cleaned": 0}),
        dag_run=SimpleNamespace(conf={}),
    )

    assert result is True


def test_existing_folders_without_cleaned_result_fail_loudly():
    with pytest.raises(AirflowException, match="정상 적재 폴더 없음"):
        upload.has_ingested_folders(
            ti=_ingest_ti({"folders": 2, "cleaned": 0, "failed": 1, "skipped": 1}),
            dag_run=SimpleNamespace(conf={"skip_if_empty": True}),
        )


def test_partial_ingest_continues_validation():
    result = upload.has_ingested_folders(
        ti=_ingest_ti({"folders": 2, "cleaned": 1, "failed": 1}),
        dag_run=SimpleNamespace(conf={"skip_if_empty": True}),
    )

    assert result is True


def test_skipped_ingest_folder_marks_final_result_partial():
    context = _notification_context()
    context["ingest_stats"] = {"folders": 2, "cleaned": 1, "skipped": 1, "failed": 0}
    context["orders"] = {"total": 1, "matched": 1, "mismatched": 0, "unknown": 0}
    context["ad_funnel"] = {"total": 1, "still_empty": 0}
    context["toorder"] = {
        "compared": 1,
        "store_results": {"매장A": {"baemin": 10000, "toorder": 10000, "matched": True}},
        "mismatched_stores": [],
        "gap_stores": [],
        "missing_brand_stores": [],
    }

    message = upload.build_final_notification_message(context)

    assert message.startswith("[배민 최종 결과] 부분완료")


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


def test_notify_upload_result_removes_handoff(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    handoff.write_text("{}", encoding="utf-8")
    sent = []
    ti = _FakeTI(
        {
            ("trigger_retry_if_needed", "retry_triggered"): False,
            ("trigger_retry_if_needed", "notification_context"): _notification_context(),
        }
    )
    monkeypatch.setattr(upload, "send_telegram", sent.append)

    result = upload.notify_upload_result(
        ti=ti,
        dag_run=SimpleNamespace(conf={"handoff_path": str(handoff)}),
    )

    assert result.startswith("[배민 최종 결과]")
    assert sent
    assert not handoff.exists()


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

    def xcom_push(self, key=None, value=None):
        self.values[(None, key)] = value


class _RetryDagRun:
    def __init__(self, conf):
        self.conf = conf

    def get_task_instances(self):
        return [SimpleNamespace(task_id="merge_retry_payloads", state="success")]


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
            ("merge_retry_payloads", "retry_result"): "재시도 1회 완료",
            ("merge_retry_payloads", "retry_payload"): {"residual_failed": residual_failed},
            ("validate_toorder", "toorder_result"): toorder_result,
            ("validate_ad_funnel", "ad_funnel_result"): {"still_empty": []},
        }
    )
    return {"ti": ti, "dag_run": _RetryDagRun(conf), "run_id": "retry__attempt_1"}


def _set_retry_attempt(context, attempt, max_attempts=3):
    context["dag_run"].conf["attempt"] = attempt
    context["dag_run"].conf["max_attempts"] = max_attempts
    context["ti"].values[("load_failed_and_accounts", "attempt")] = attempt
    return context


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


def test_retry_collect_accepts_failed_stages_only(monkeypatch):
    pushed = {}
    ti = SimpleNamespace(xcom_push=lambda key=None, value=None: pushed.setdefault(key, value))
    conf = {
        "target_date": "2026-07-21",
        "failed_account_ids": ["acct-1"],
        "failed_stages": [
            {"account_id": "acct-1", "store": {"store_id": "s1"}, "stage": "NOW 수집"}
        ],
        "retry_wait_sec": 0,
    }
    monkeypatch.setattr(
        retry_dag,
        "retry_collect_from_conf",
        lambda value: {
            "target_date": value["target_date"],
            "retry_result": "stage retry",
            "store_info_per_account": [],
            "ad_store_infos": [],
            "residual_failed": {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []},
        },
    )

    result = retry_dag.retry_collect(ti=ti, dag_run=_RetryDagRun(conf))

    assert "stage retry" in result
    assert pushed["retry_payload"]["retry_result"].startswith("stage retry")


def test_retry_collect_skips_wait_for_delivery_commission_recollect(monkeypatch):
    pushed = {}
    sleeps = []
    ti = SimpleNamespace(xcom_push=lambda key=None, value=None: pushed.setdefault(key, value))
    conf = {
        "target_date": "2026-08-23",
        "failed_account_ids": ["acct-1"],
        "source_dag_id": "DB_DeliveryCommission_Dags",
    }
    monkeypatch.setattr(retry_dag.time, "sleep", sleeps.append)
    monkeypatch.setattr(retry_dag.random, "uniform", lambda *_: 600)
    monkeypatch.setattr(
        retry_dag,
        "retry_collect_from_conf",
        lambda value, **_: {
            "target_date": value["target_date"],
            "retry_result": "delivery commission retry",
            "store_info_per_account": [],
            "ad_store_infos": [],
            "residual_failed": {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []},
        },
    )

    result = retry_dag.retry_collect(
        lane_offset=True,
        ti=ti,
        dag_run=_RetryDagRun(conf),
    )

    assert "delivery commission retry" in result
    assert sleeps == []


def test_intermediate_retry_carries_residual_stages_to_next_conf(monkeypatch):
    triggered = []
    context = _retry_context(
        residual_failed={
            "accounts": [],
            "stores": [],
            "orders": [],
            "ads": [],
            "stages": [
                {
                    "account": {"account_id": "acct-1"},
                    "store": {"store_id": "s1"},
                    "stage": "운영시간 수집",
                }
            ],
        },
        toorder_result={
            "compared_count": 1,
            "store_results": {},
            "mismatched_stores": [],
            "missing_brand_stores": [],
            "toorder_gap_stores": [],
        },
    )
    next_conf = {
        "failed_account_ids": [],
        "failed_accounts_ids_only": [],
        "failed_stores": [],
        "failed_orders": [],
        "failed_ads": [],
        "failed_stages": [],
        "notification_context": _notification_context(),
    }
    monkeypatch.setattr(retry_dag, "retry_needed", lambda *args, **kwargs: True)
    monkeypatch.setattr(retry_dag, "build_next_retry_conf", lambda **kwargs: next_conf)
    monkeypatch.setattr(retry_dag, "send_telegram", lambda message: None)
    monkeypatch.setattr("airflow.api.common.trigger_dag.trigger_dag", lambda **kwargs: triggered.append(kwargs))

    result = retry_dag.notify_and_trigger_next(**context)

    assert "stages=1" in result
    assert triggered[0]["conf"]["failed_account_ids"] == ["acct-1"]
    assert triggered[0]["conf"]["failed_stages"] == [
        {"account_id": "acct-1", "store": {"store_id": "s1"}, "stage": "운영시간 수집"}
    ]


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


def test_retry_notify_ignores_residual_ads_after_ad_funnel_validation_passes(monkeypatch):
    sent = []
    toorder_result = {
        "compared_count": 0,
        "store_results": {},
        "mismatched_stores": [],
        "missing_brand_stores": [],
        "toorder_gap_stores": [],
    }
    context = _retry_context(
        residual_failed={
            "accounts": [],
            "stores": [],
            "orders": [],
            "ads": [{"account": {"account_id": "acct-1"}, "stores": [{"store_name": "매장A"}]}],
            "stages": [],
        },
        toorder_result=toorder_result,
    )
    context["ti"].values[("validate_ad_funnel", "ad_funnel_result")] = {
        "empty_stores": [{"store": "매장A"}],
        "retried": [],
        "still_empty": [],
    }
    monkeypatch.setattr(retry_dag, "send_telegram", sent.append)

    result = retry_dag.notify_and_trigger_next(**context)

    assert result.startswith("[배민 최종 결과]")
    assert "ad_funnel 6 / 정상 6 / 잔존 0" in result
    assert "잔여 실패 0" in result
    assert sent == [result]


def test_terminal_retry_with_unresolved_result_fails_dag_after_telegram(monkeypatch):
    sent = []
    toorder_result = {
        "compared_count": 1,
        "store_results": {"매장A": {"baemin": 10000, "toorder": 12000, "matched": False}},
        "mismatched_stores": ["매장A"],
        "missing_brand_stores": ["매장A"],
        "toorder_gap_stores": [],
    }
    context = _set_retry_attempt(
        _retry_context(
            residual_failed={"accounts": [], "stores": [], "orders": ["매장A"], "ads": []},
            toorder_result=toorder_result,
        ),
        attempt=3,
    )
    monkeypatch.setattr(retry_dag, "retry_needed", lambda *args, **kwargs: True)
    monkeypatch.setattr(retry_dag, "send_telegram", sent.append)

    with pytest.raises(AirflowException, match="배민 최종 결과"):
        retry_dag.notify_and_trigger_next(**context)

    assert len(sent) == 1
    assert sent[0].startswith("[배민 최종 결과]")


def test_terminal_retry_with_no_next_target_fails_dag_after_telegram(monkeypatch):
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
        residual_failed={"accounts": [], "stores": [], "orders": [], "ads": []},
        toorder_result=toorder_result,
    )
    monkeypatch.setattr(retry_dag, "retry_needed", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        retry_dag,
        "build_next_retry_conf",
        lambda **kwargs: {
            "failed_account_ids": [],
            "failed_accounts_ids_only": [],
            "failed_stores": [],
            "failed_orders": [],
            "failed_ads": [],
            "failed_stages": [],
            "notification_context": _notification_context(),
        },
    )
    monkeypatch.setattr(retry_dag, "send_telegram", sent.append)
    monkeypatch.setattr("airflow.api.common.trigger_dag.trigger_dag", lambda **kwargs: triggered.append(kwargs))

    with pytest.raises(AirflowException, match="배민 최종 결과"):
        retry_dag.notify_and_trigger_next(**context)

    assert sent and sent[0].startswith("[배민 최종 결과]")
    assert triggered == []
