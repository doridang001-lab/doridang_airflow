import json
from datetime import datetime, timedelta, timezone

import watch_heal_queue as watcher


def _configure_paths(monkeypatch, tmp_path):
    queue_path = tmp_path / "heal_queue.jsonl"
    heartbeat_path = tmp_path / "autoheal_heartbeat.json"
    monkeypatch.setattr(watcher, "QUEUE_PATH", queue_path)
    monkeypatch.setattr(watcher, "QUEUE_LOCK_PATH", queue_path.with_suffix(".jsonl.lock"))
    monkeypatch.setattr(watcher, "HEARTBEAT_PATH", heartbeat_path)
    monkeypatch.setattr(watcher, "WATCHER_LOCK_PATH", heartbeat_path.with_suffix(".json.watcher.lock"))
    monkeypatch.setattr(watcher, "TASK_STATE_PATH", tmp_path / "state.json")
    return queue_path, heartbeat_path


def test_claim_heal_task_is_atomic_and_records_lease(tmp_path, monkeypatch):
    queue_path, _ = _configure_paths(monkeypatch, tmp_path)
    row = {
        "kind": "task_failure",
        "dag_id": "sample",
        "run_id": "run-1",
        "task_id": "task-1",
        "try_number": 1,
        "claimed_by": None,
    }
    queue_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    assert watcher.claim_heal_task("sample", "run-1", "task-1", "codex", try_number=1) is True
    assert watcher.claim_heal_task("sample", "run-1", "task-1", "second", try_number=1) is False
    saved = json.loads(queue_path.read_text(encoding="utf-8"))
    assert saved["claimed_by"] == "codex"
    assert saved["claim_expires_at"]


def test_expired_claim_can_be_recovered(tmp_path, monkeypatch):
    queue_path, _ = _configure_paths(monkeypatch, tmp_path)
    row = {
        "kind": "task_failure",
        "dag_id": "sample",
        "run_id": "run-1",
        "task_id": "task-1",
        "try_number": 1,
        "claimed_by": "dead-watcher",
        "claim_expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    }
    queue_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    assert watcher.claim_heal_task("sample", "run-1", "task-1", "recovery", try_number=1) is True
    saved = json.loads(queue_path.read_text(encoding="utf-8"))
    assert saved["claimed_by"] == "recovery"


def test_heartbeat_exposes_backend_and_token(tmp_path, monkeypatch):
    _, heartbeat_path = _configure_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(watcher, "AUTOHEAL_BACKEND", "windows")
    monkeypatch.setattr(watcher, "_WATCHER_TOKEN", "token-1")

    watcher._write_heartbeat("idle")

    payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert payload["backend"] == "windows"
    assert payload["watcher_token"] == "token-1"
    assert payload["last_result"] == "idle"


def test_heartbeat_with_dead_pid_is_not_fresh(tmp_path, monkeypatch):
    _, heartbeat_path = _configure_paths(monkeypatch, tmp_path)
    heartbeat_path.write_text(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "pid": 999999,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(watcher, "_pid_is_alive", lambda _pid: False)

    assert watcher._heartbeat_is_fresh(180) is False


def test_prepare_codex_workdir_fast_forwards_clean_workspace(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    isolated = tmp_path / "isolated"
    runtime.mkdir()
    isolated.mkdir()
    monkeypatch.setattr(watcher, "AIRFLOW_RUNTIME_WORKDIR", str(runtime))
    monkeypatch.setattr(watcher, "CODEX_WORKDIR", str(isolated))
    calls = []

    def fake_git(workdir, *args):
        calls.append((workdir, args))
        if args == ("status", "--porcelain"):
            return ""
        if args == ("rev-parse", "HEAD"):
            return "runtime-head" if workdir == str(runtime) else "old-head"
        if args == ("merge", "--ff-only", "runtime-head"):
            return ""
        raise AssertionError((workdir, args))

    monkeypatch.setattr(watcher, "_git_output", fake_git)

    assert watcher._prepare_codex_workdir() == "runtime-head"
    assert (str(isolated), ("merge", "--ff-only", "runtime-head")) in calls


def test_prepare_codex_workdir_continues_with_dirty_isolated_workspace(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    isolated = tmp_path / "isolated"
    runtime.mkdir()
    isolated.mkdir()
    monkeypatch.setattr(watcher, "AIRFLOW_RUNTIME_WORKDIR", str(runtime))
    monkeypatch.setattr(watcher, "CODEX_WORKDIR", str(isolated))
    calls = []

    def fake_git(workdir, *args):
        calls.append((workdir, args))
        if args == ("status", "--porcelain"):
            return " M modules/example.py"
        if args == ("rev-parse", "HEAD"):
            return "runtime-head" if workdir == str(runtime) else "old-head"
        if args == ("merge", "--ff-only", "runtime-head"):
            raise AssertionError("dirty isolated workspace must not be fast-forwarded")
        raise AssertionError((workdir, args))

    monkeypatch.setattr(watcher, "_git_output", fake_git)

    assert watcher._prepare_codex_workdir() == "runtime-head"
    assert (str(isolated), ("merge", "--ff-only", "runtime-head")) not in calls


def test_codex_prompt_is_sent_through_stdin(monkeypatch):
    monkeypatch.setattr(watcher, "CODEX_COMMAND", "codex.cmd")
    args = watcher._codex_args("x" * 20_000)

    assert args[-1] == "-"
    assert all("x" * 100 not in value for value in args)


def test_codex_uses_cli_default_model_when_not_configured(monkeypatch):
    monkeypatch.setattr(watcher, "CODEX_COMMAND", "codex.cmd")
    monkeypatch.setattr(watcher, "CODEX_MODEL", "")

    args = watcher._codex_args("prompt")

    assert "--model" not in args


def test_autoheal_telegram_is_suppressed_by_default(monkeypatch):
    monkeypatch.delenv("AUTOHEAL_TELEGRAM_ENABLED", raising=False)
    monkeypatch.delenv("AUTOHEAL_TELEGRAM_MODE", raising=False)
    monkeypatch.setattr(watcher, "_ensure_telegram_creds", lambda: (_ for _ in ()).throw(AssertionError("credentials not needed")))

    assert watcher.send_telegram("[자동복구] 코드 자동수정 생략") is True


def test_autoheal_telegram_sends_final_failures_by_default(monkeypatch):
    calls = []

    class DummyResponse:
        def __enter__(self):
            calls.append("opened")
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.delenv("AUTOHEAL_TELEGRAM_ENABLED", raising=False)
    monkeypatch.delenv("AUTOHEAL_TELEGRAM_MODE", raising=False)
    monkeypatch.setattr(watcher, "_ensure_telegram_creds", lambda: ("token", "chat"))
    monkeypatch.setattr(watcher.urllib.request, "urlopen", lambda req, timeout: DummyResponse())

    assert watcher.send_telegram("[자동복구] Codex 시간초과 - 수동확인 필요", final_failure=True) is True
    assert calls == ["opened"]


def test_autoheal_telegram_can_send_all_when_enabled(monkeypatch):
    calls = []

    class DummyResponse:
        def __enter__(self):
            calls.append("opened")
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setenv("AUTOHEAL_TELEGRAM_ENABLED", "true")
    monkeypatch.setattr(watcher, "_ensure_telegram_creds", lambda: ("token", "chat"))
    monkeypatch.setattr(watcher.urllib.request, "urlopen", lambda req, timeout: DummyResponse())

    assert watcher.send_telegram("[자동복구] 코드 자동수정 생략") is True
    assert calls == ["opened"]


def test_manual_only_skip_does_not_send_telegram(tmp_path, monkeypatch):
    queue_path, _ = _configure_paths(monkeypatch, tmp_path)
    row = {
        "kind": "task_failure",
        "dag_id": "DB_Beamin_Macro_Dags",
        "run_id": "run-1",
        "task_id": "retry_failed",
        "try_number": 1,
        "failure_class": "data_or_account",
        "error": "account login failed",
    }
    queue_path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
    monkeypatch.setattr(watcher, "get_task_log", lambda *args: "account login failed")
    monkeypatch.setattr(watcher, "_send_skip_notification", lambda *args: (_ for _ in ()).throw(AssertionError("skip telegram must not send")))

    assert watcher.process_once() is True

    saved = json.loads(queue_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "manual_only"
    assert saved["notification_suppressed"] is True
    assert saved["notification_suppressed_reason"] == "manual_only"


def test_task_prompt_allows_runtime_fix_without_repository_approval():
    prompt = watcher._build_prompt(
        {
            "kind": "task_failure",
            "dag_id": "sample",
            "task_id": "task",
            "run_id": "run",
            "try_number": 1,
            "failure_class": "transient",
            "error": "invalid session id",
        },
        "log",
    )

    assert "운영 소스까지 반영" in prompt
    assert "Approval is required only for OneDrive writes or git commit/push" in prompt


def test_task_signature_ignores_run_specific_log_text():
    entry = {
        "dag_id": "DB_UnifiedSales",
        "task_id": "reconcile_baemin",
        "failure_class": "allocator_error",
        "error": "배민수동 item_id 배정 실패: store/item_name 필수",
    }

    first = watcher._task_signature(entry, "run_id=one\n2026-07-22 01:00:00")
    second = watcher._task_signature(entry, "run_id=two\n2026-07-23 02:00:00")

    assert first == second


def test_same_incident_today_recognizes_legacy_signature(tmp_path, monkeypatch):
    _configure_paths(monkeypatch, tmp_path)
    entry = {
        "dag_id": "DB_UnifiedSales",
        "task_id": "reconcile_baemin",
        "failure_class": "allocator_error",
    }
    watcher.TASK_STATE_PATH.write_text(
        json.dumps(
            {
                "legacy-signature": {
                    "date": watcher._today_key(),
                    "attempts": 1,
                    **entry,
                }
            }
        ),
        encoding="utf-8",
    )

    assert watcher._already_attempted_today("new-signature", entry) is True


NOT_RUN_ERROR = (
    "Executor CeleryExecutor(parallelism=32) reported that the task instance "
    "<TaskInstance: DB_DeliveryCommission_Dags.monitor_baemin_settlement_missing "
    "manual__2026-08-24T04:03:55.448896+00:00 [queued]> finished with state success, "
    "but the task instance's state attribute is queued."
)

NOT_RUN_LOG = (
    "Dependencies not met, dependency 'Execution Date' FAILED: Execution date "
    "2026-08-24T04:03:55.448896+00:00 is in the future (the current date is "
    "2026-08-24T04:03:20.566556+00:00).\nTask is not able to be run"
)


def _write_not_run_row(queue_path):
    row = {
        "kind": "task_failure",
        "dag_id": "DB_DeliveryCommission_Dags",
        "run_id": "manual__2026-08-24T04:03:55.448896+00:00",
        "task_id": "monitor_baemin_settlement_missing",
        "try_number": 1,
        "failure_class": watcher.NOT_RUN_FAILURE_CLASS,
        "auto_edit_allowed": False,
        "rerun_requested": True,
        "error": NOT_RUN_ERROR,
        "claimed_by": None,
    }
    queue_path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
    return row


def _stub_airflow_api(monkeypatch, task_instance):
    calls = []

    def fake_request(url, accept="application/json", *, method="GET", data=None):
        calls.append((method, url, data))
        if method == "GET":
            return json.dumps(task_instance).encode("utf-8")
        return b"{}"

    monkeypatch.setattr(watcher, "_request", fake_request)
    monkeypatch.setattr(watcher, "get_task_log", lambda *args: NOT_RUN_LOG)
    monkeypatch.setattr(
        watcher,
        "_run_codex",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("codex must not run for not-run tasks")),
    )
    return calls


def test_not_run_task_is_cleared_instead_of_codex(tmp_path, monkeypatch):
    queue_path, _ = _configure_paths(monkeypatch, tmp_path)
    _write_not_run_row(queue_path)
    calls = _stub_airflow_api(monkeypatch, {"state": "failed", "start_date": None})
    sent = []
    monkeypatch.setattr(watcher, "send_telegram", lambda text, **kwargs: sent.append(text) or True)

    assert watcher.process_once() is True

    posts = [call for call in calls if call[0] == "POST"]
    assert len(posts) == 1
    assert posts[0][1].endswith("/clearTaskInstances")
    assert posts[0][2]["dag_run_id"] == "manual__2026-08-24T04:03:55.448896+00:00"
    assert posts[0][2]["task_ids"] == ["monitor_baemin_settlement_missing"]
    assert posts[0][2]["only_failed"] is True
    assert posts[0][2]["dry_run"] is False

    saved = json.loads(queue_path.read_text(encoding="utf-8"))
    assert saved["claimed_by"] == "autoheal-rerun"
    assert saved["skip_reason"] == "executor_state_mismatch_rerun"
    assert saved["result_status"] == "success"
    assert any(text.startswith("[자동복구] 미실행 태스크 재실행") for text in sent)


def test_started_task_is_never_cleared(tmp_path, monkeypatch):
    queue_path, _ = _configure_paths(monkeypatch, tmp_path)
    _write_not_run_row(queue_path)
    calls = _stub_airflow_api(
        monkeypatch,
        {"state": "failed", "start_date": "2026-08-24T04:03:21+00:00"},
    )
    sent = []
    monkeypatch.setattr(watcher, "send_telegram", lambda text, **kwargs: sent.append(text) or True)

    assert watcher.process_once() is True

    assert [call for call in calls if call[0] == "POST"] == []
    saved = json.loads(queue_path.read_text(encoding="utf-8"))
    assert saved["skip_reason"] == "not_run_guard"
    assert saved["result_status"] == "skipped"
    assert any("생략이유=state=failed" in text for text in sent)
