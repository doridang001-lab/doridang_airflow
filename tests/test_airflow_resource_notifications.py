"""메모리 보호 상태 전환은 적용하되 상태 변경 메시지는 보내지 않는다."""
import importlib
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.mark.parametrize("previous,current", [
    ("normal", "history_paused"),
    ("history_paused", "all_paused"),
    ("all_paused", "normal"),
])
def test_memory_transition_keeps_protection_without_telegram(monkeypatch, tmp_path, previous, current):
    control = importlib.import_module("scripts.airflow_resource_control")
    now = time.time()
    old = {"mode": previous, "checked_at": now - 120, "started_at": now - 600,
           "saved_pool_slots": {"default_pool": 8} if previous == "all_paused" else {}}
    variable = MagicMock()
    variable.get.return_value = old
    task = MagicMock()
    task.end_date.__gt__.return_value = True
    pool = SimpleNamespace(pool="default_pool", slots=0 if previous == "all_paused" else 8)
    session = MagicMock()
    session.execute.return_value.scalar.return_value = True
    session.query.return_value.filter.return_value.group_by.return_value.all.return_value = []
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.with_for_update.return_value.all.return_value = [pool]
    sender = MagicMock()
    dispatch = MagicMock(return_value=[])
    recover = MagicMock(return_value=[])

    def module(name, **attrs):
        stub = ModuleType(name)
        stub.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, stub)

    module("airflow.models", Variable=variable, Pool=MagicMock(), TaskInstance=task)
    module("airflow.utils.session", create_session=lambda: nullcontext(session))
    module("sqlalchemy", text=lambda value: value, func=MagicMock())
    module("modules.transform.utility.workload", RESOURCE_VARIABLE="policy",
           memory_transition=lambda state, available: {**state, "mode": current},
           dispatch_history_requests=dispatch)
    module("modules.transform.utility.safe_recovery", recover_memory_failures=recover,
           memory_failure=MagicMock(), task_log_tail=MagicMock())
    module("modules.transform.utility.notifier", send_telegram=sender)
    module("modules.transform.utility.paths", LOCAL_DB=tmp_path)
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemAvailable: 7000000 kB\nSwapFree: 4000000 kB\n", encoding="utf-8")
    monkeypatch.setattr(control, "Path", lambda value: meminfo if value == "/proc/meminfo" else Path(value))
    result = control.control(apply=True)

    assert result["mode"] == current
    assert pool.slots == (0 if current == "all_paused" else 8)
    variable.set.assert_called_once()
    sender.assert_not_called()
    assert (tmp_path / "airflow_ops" / "resource_observation.json").is_file()
    assert dispatch.called == recover.called == (current == "normal")
