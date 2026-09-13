import ast
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def load_guard(rows, gate):
    path = Path(__file__).resolve().parents[1] / "dags/db/DB_UnifiedSales_Dags.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_assert_no_blocking_run_active")
    session = MagicMock()
    dag_query, task_query = MagicMock(), MagicMock()
    dag_query.filter.return_value.filter.return_value.order_by.return_value.all.return_value = rows
    task_query.filter_by.return_value.first.return_value = gate
    session.query.side_effect = lambda *args: dag_query if len(args) == 3 else task_query

    @contextmanager
    def create_session():
        yield session

    ns = dict(create_session=create_session, DagRun=MagicMock(), TaskInstance=MagicMock(),
              BLOCKING_DAG_IDS=(), BLOCKING_STATES=("running",), TODAY_DAG_ID="today", AirflowException=RuntimeError)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), ns)
    return ns["_assert_no_blocking_run_active"]


class Run(tuple):
    dag_id = property(lambda self: self[0])
    run_id = property(lambda self: self[1])


@pytest.mark.parametrize("state", [None, "queued", "scheduled", "up_for_retry", "failed", "skipped"])
def test_waiting_today_does_not_deadlock_regular(state):
    load_guard([Run(("today", "run", "running"))], SimpleNamespace(state=state))()


@pytest.mark.parametrize("gate", [None, SimpleNamespace(state="success"), SimpleNamespace(state="running")])
def test_started_or_unknown_today_still_blocks_writes(gate):
    with pytest.raises(RuntimeError):
        load_guard([Run(("today", "run", "running"))], gate)()


def test_total_run_always_blocks():
    with pytest.raises(RuntimeError):
        load_guard([Run(("total", "run", "running"))], SimpleNamespace(state="up_for_retry"))()
