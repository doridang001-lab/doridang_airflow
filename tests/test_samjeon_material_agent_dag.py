"""Airflow 없이도 DAG의 시간·수동 실행 경계를 검증한다."""
import runpy
import sys
import types
from pathlib import Path
from types import SimpleNamespace
import pendulum


def load_dag(monkeypatch):
    captured = {}
    class DAG:
        def __init__(self, **kwargs): captured.update(kwargs)
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Variable:
        enabled = 'true'
        @classmethod
        def get(cls, *args, **kwargs): return cls.enabled
    airflow = types.ModuleType('airflow')
    airflow.DAG = DAG
    models = types.ModuleType('airflow.models')
    models.Variable = Variable
    operators = types.ModuleType('airflow.operators.python')
    operators.PythonOperator = lambda **kwargs: kwargs
    for name, module in [('airflow', airflow), ('airflow.models', models), ('airflow.operators.python', operators)]:
        monkeypatch.setitem(sys.modules, name, module)
    namespace = runpy.run_path(str(Path(__file__).parents[1] / 'dags/db/DB_Samjeon_Material_Agent_Dags.py'))
    return namespace, captured, Variable


def test_dag_disabled_manual_stale_and_slot(monkeypatch, tmp_path):
    namespace, config, Variable = load_dag(monkeypatch)
    assert config['is_paused_upon_creation'] is True
    assert config['catchup'] is False and config['max_active_runs'] == 1
    assert config['dagrun_timeout'].total_seconds() == 300
    assert config['dag_id'] == 'DB_Samjeon_Material_Agent_Dags'
    from modules.transform.pipelines.db.samjeon_material_agent import locations
    monkeypatch.setattr(locations, 'LOCAL_DB', tmp_path)
    now = pendulum.datetime(2026, 10, 12, 9, 30, tz='Asia/Seoul')
    monkeypatch.setattr(pendulum, 'now', lambda *args: now)
    ctx = {'dag_run': SimpleNamespace(run_type='scheduled', conf={}), 'data_interval_end': now}
    Variable.enabled = 'false'
    assert namespace['queue_check'](**ctx)['queued'] is False
    Variable.enabled = 'true'
    assert namespace['queue_check'](**ctx)['queued'] is True
    ctx['dag_run'].run_type = 'manual'
    assert namespace['queue_check'](**ctx)['queued'] is False
    ctx['dag_run'].run_type = 'scheduled'
    ctx['data_interval_end'] = now.subtract(days=1)
    assert namespace['queue_check'](**ctx)['queued'] is False


def test_dag_parse_has_no_service_or_model_side_effects(monkeypatch):
    _, config, _ = load_dag(monkeypatch)
    assert config['schedule'] == '0,30 9-12 * * *'
