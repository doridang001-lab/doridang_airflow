import importlib
import sys
import types


class _FakeDAG:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakePythonOperator:
    def __init__(self, *args, **kwargs):
        pass

    def __rshift__(self, other):
        return other


airflow_module = types.ModuleType("airflow")
airflow_module.DAG = _FakeDAG
operators_module = types.ModuleType("airflow.operators")
python_operator_module = types.ModuleType("airflow.operators.python")
python_operator_module.PythonOperator = _FakePythonOperator
sys.modules.setdefault("airflow", airflow_module)
sys.modules.setdefault("airflow.operators", operators_module)
sys.modules.setdefault("airflow.operators.python", python_operator_module)

dag_module = importlib.import_module("dags.db.DB_MenuHierarchy_Test_Dags")


class _DummyTi:
    def __init__(self):
        self.values = {}

    def xcom_push(self, key, value):
        self.values[key] = value

    def xcom_pull(self, task_ids, key):
        return self.values.get(key)


def test_scheduled_menu_hierarchy_uses_all_months_by_default(monkeypatch):
    pushed = _DummyTi()
    monkeypatch.setattr(dag_module, "pipeline_resolve_yms", lambda ym: ["2026-04", "2026-05"] if ym is None else [ym])

    assert dag_module.resolve_ym(ti=pushed) == ["2026-04", "2026-05"]
    assert pushed.values["ym_list"] == ["2026-04", "2026-05"]


def test_menu_hierarchy_ignores_conf_and_uses_all_months(monkeypatch):
    pushed = _DummyTi()

    class DagRun:
        conf = {"ym": "2026-08"}

    monkeypatch.setattr(dag_module, "pipeline_resolve_yms", lambda ym: ["2026-04", "2026-05"] if ym is None else [ym])

    assert dag_module.resolve_ym(ti=pushed, dag_run=DagRun()) == ["2026-04", "2026-05"]


def test_build_orders_fallback_uses_all_months(monkeypatch):
    pulled = _DummyTi()
    called = {}

    monkeypatch.setattr(dag_module, "pipeline_resolve_yms", lambda ym: ["2026-04", "2026-05"] if ym is None else [ym])

    def fake_build_orders(yms):
        called["yms"] = yms
        return "ok"

    monkeypatch.setattr(dag_module, "pipeline_build_orders", fake_build_orders)

    assert dag_module.build_orders(ti=pulled) == "ok"
    assert called["yms"] == ["2026-04", "2026-05"]
