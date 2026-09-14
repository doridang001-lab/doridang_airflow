import importlib.util
import sys
import types
from pathlib import Path

import pendulum


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


def _fake_module(name: str, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def _load_dag_module(monkeypatch):
    airflow_module = _fake_module("airflow", DAG=_FakeDAG)
    airflow_exceptions_module = _fake_module("airflow.exceptions", AirflowSkipException=Exception)
    python_operator_module = _fake_module("airflow.operators.python", PythonOperator=_FakePythonOperator)
    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.exceptions", airflow_exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.operators", _fake_module("airflow.operators"))
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python_operator_module)

    monkeypatch.setitem(
        sys.modules,
        "modules.extract.crawling_toorder_sales_report_daily_date",
        _fake_module(
            "modules.extract.crawling_toorder_sales_report_daily_date",
            run_crawling_single_date=lambda **kwargs: {"success": True, "file": "dummy.xlsx"},
        ),
    )

    integrator = _fake_module(
        "modules.transform.ai_daily_collection_integrator",
        AI_SALES_ALERT_TREND_CHART_CID="trend",
        ingest_pending_ai_daily_collection_xlsx_files=lambda *args, **kwargs: [],
        rebuild_ai_daily_collection_compat_outputs=lambda *args, **kwargs: {
            "integrated_xlsx": "integrated.xlsx",
            "daily_summary_csv": "daily.csv",
        },
        build_ai_score_sheet=lambda *args, **kwargs: {},
        build_llm_ai_diagnosis=lambda *args, **kwargs: {},
        save_llm_diagnosis_to_db=lambda *args, **kwargs: None,
        build_ai_sales_alert_html=lambda *args, **kwargs: "",
        build_ai_sales_alert_trend_chart_png=lambda *args, **kwargs: None,
        export_daily_summary_csv=lambda *args, **kwargs: 0,
        load_ai_daily_collection_daily_totals=lambda *args, **kwargs: [],
    )
    monkeypatch.setitem(sys.modules, "modules.transform.ai_daily_collection_integrator", integrator)

    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.paths",
        _fake_module(
            "modules.transform.utility.paths",
            ANALYTICS_DB=Path(".tmp/analytics"),
            DOWN_DIR=Path(".tmp/down"),
            TEMP_DIR=Path(".tmp/temp"),
            LOCAL_DB=Path(".tmp/local"),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.schedule",
        _fake_module("modules.transform.utility.schedule", AI_DAILY_COLLECTION_TIME="20 7 * * *"),
    )
    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.mailer",
        _fake_module("modules.transform.utility.mailer", send_email=lambda *args, **kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.notifier",
        _fake_module("modules.transform.utility.notifier", on_failure_callback=lambda *args, **kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.account",
        _fake_module("modules.transform.utility.account", get_default_account=lambda channel: ("id", "pw")),
    )
    monkeypatch.setitem(
        sys.modules,
        "modules.transform.utility.mail_recipients",
        _fake_module(
            "modules.transform.utility.mail_recipients",
            MAIL_CMJ_PM="cmj@example.com",
            MAIL_OH_NAYOUNG="oh@example.com",
        ),
    )

    module_name = "_test_sales_alert_dag_module"
    path = Path("dags/sales/DB_Sales_Alert_01_Score_AI_Daily_Collection_Dags.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_target_date_uses_logical_date_previous_day(monkeypatch):
    dag_module = _load_dag_module(monkeypatch)
    logical_date = pendulum.datetime(2026, 7, 29, 7, 20, tz="Asia/Seoul")

    assert dag_module._target_date_from_context({"logical_date": logical_date}) == "2026-07-28"


def test_target_date_uses_dag_run_logical_date(monkeypatch):
    dag_module = _load_dag_module(monkeypatch)

    class DagRun:
        logical_date = pendulum.datetime(2026, 8, 1, 7, 20, tz="Asia/Seoul")

    assert dag_module._target_date_from_context({"dag_run": DagRun()}) == "2026-07-31"
