"""Airflow DB나 운영 저장 없이 실제 DAG callable의 실행 계약을 검증한다."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


class Skip(Exception):
    pass


class TI:
    def __init__(self):
        self.values = {}

    def xcom_push(self, key, value):
        self.values[key] = value

    def xcom_pull(self, task_ids, key):
        return self.values.get(key)


def functions():
    path = Path(__file__).resolve().parents[1] / "dags/db/DB_OrderCrossAnalysis_Dags.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tree.body = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    namespace = {"LOOKBACK_DAYS": 3, "json": json, "AirflowSkipException": Skip,
                 "Variable": SimpleNamespace(get=lambda *a, **k: "false")}
    exec(compile(tree, str(path), "exec"), namespace)
    return namespace


def test_unapproved_dag_skips_before_writing():
    ns = functions()
    with pytest.raises(Skip, match="승인 대기"):
        ns["build_cross"](dag_run=SimpleNamespace(conf={}), ti=TI())


@pytest.mark.parametrize("conf,dates", [({"publish": True, "sale_date": "2026-09-08"}, ["2026-09-08"]),
    ({"publish": True, "backfill": True}, ["2026-09-07", "2026-09-08"]),
    ({"publish": True}, ["2026-09-08"])])
def test_every_built_date_is_validated(conf, dates):
    ns, ti = functions(), TI()
    ns["backfill_order_cross_analysis"] = lambda **kw: json.dumps({"dates": dates})
    ns["run_lookback_order_cross_analysis"] = lambda **kw: json.dumps({"dates": dates})
    ns["run_order_cross_analysis"] = lambda *a, **kw: "built"
    seen = []
    def validate(date):
        seen.append(date)
        return date
    ns["validate_order_cross"] = validate
    context = {"dag_run": SimpleNamespace(conf=conf), "ti": ti}
    ns["resolve_date"](**context)
    ns["build_cross"](**context)
    ns["validate_cross"](**context)
    assert seen == dates


def test_missing_processed_date_record_is_error():
    with pytest.raises(ValueError, match="기록 누락"):
        functions()["validate_cross"](ti=TI())


def test_persistent_approval_enables_scheduled_run():
    ns, ti = functions(), TI()
    ns["Variable"] = SimpleNamespace(get=lambda *a, **k: "true")
    ns["run_lookback_order_cross_analysis"] = lambda **kw: json.dumps({"dates": []})
    ns["build_cross"](dag_run=SimpleNamespace(conf={}), ti=ti)
    assert ti.values["processed_dates"] == []
