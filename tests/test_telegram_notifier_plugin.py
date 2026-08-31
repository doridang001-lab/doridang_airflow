from types import SimpleNamespace

from plugins import telegram_notifier_plugin as plugin


def _ti(dag_id="sample_dag", task_id="sample_task", *, try_number=1, retries=0):
    return SimpleNamespace(
        dag_id=dag_id,
        task_id=task_id,
        try_number=try_number,
        execution_date=None,
        log_url="http://example/log",
        task=SimpleNamespace(retries=retries, on_failure_callback=None),
    )


def test_baemin_retry_notify_task_suppresses_generic_failure_alert(monkeypatch):
    sent = []
    monkeypatch.setattr(plugin, "_send", sent.append)

    plugin._Listener().on_task_instance_failed(
        None,
        _ti("DB_Beamin_Macro_Dags_Retry", "notify_and_trigger_next"),
        error=RuntimeError("terminal baemin retry"),
    )

    assert sent == []


def test_other_final_failure_still_sends_generic_alert(monkeypatch):
    sent = []
    monkeypatch.setattr(plugin, "_send", sent.append)

    plugin._Listener().on_task_instance_failed(None, _ti(), error=RuntimeError("failed"))

    assert len(sent) == 1
    assert "DAG: sample_dag" in sent[0]
    assert "Task: sample_task" in sent[0]


def test_non_final_retry_does_not_send_generic_alert(monkeypatch):
    sent = []
    monkeypatch.setattr(plugin, "_send", sent.append)

    plugin._Listener().on_task_instance_failed(None, _ti(try_number=1, retries=1), error=RuntimeError("retrying"))

    assert sent == []
