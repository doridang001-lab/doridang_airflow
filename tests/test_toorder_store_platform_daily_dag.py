import os
from pathlib import Path

import pandas as pd
import pendulum

os.environ.setdefault("AIRFLOW_HOME", str((Path(".tmp") / "airflow-test-home").resolve()))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from dags.sales import DB_Toorder_store_platform_daily_Dags as dag_module


def test_default_date_bounds_catches_up_from_parquet_max_date(tmp_path, monkeypatch):
    parquet_path = tmp_path / "toorder_store_platform_daily.parquet"
    pd.DataFrame({"date": ["2026-08-08", "2026-08-09"]}).to_parquet(parquet_path, index=False)

    monkeypatch.setattr(dag_module, "PARQUET_PATH", parquet_path)
    monkeypatch.setattr(
        dag_module.pendulum,
        "now",
        lambda tz: pendulum.datetime(2026, 8, 24, tz=tz),
    )

    assert dag_module._default_date_bounds() == ("2026-08-10", "2026-08-23")


def test_default_date_bounds_uses_yesterday_when_parquet_is_current(tmp_path, monkeypatch):
    parquet_path = tmp_path / "toorder_store_platform_daily.parquet"
    pd.DataFrame({"date": ["2026-08-23"]}).to_parquet(parquet_path, index=False)

    monkeypatch.setattr(dag_module, "PARQUET_PATH", parquet_path)
    monkeypatch.setattr(
        dag_module.pendulum,
        "now",
        lambda tz: pendulum.datetime(2026, 8, 24, tz=tz),
    )

    assert dag_module._default_date_bounds() == ("2026-08-23", "2026-08-23")


def test_default_date_bounds_falls_back_to_recent_missing_dates(tmp_path, monkeypatch):
    parquet_path = tmp_path / "missing.parquet"

    monkeypatch.setattr(dag_module, "PARQUET_PATH", parquet_path)
    monkeypatch.setattr(
        dag_module.pendulum,
        "now",
        lambda tz: pendulum.datetime(2026, 8, 24, tz=tz),
    )

    assert dag_module._default_date_bounds() == ("2026-08-21", "2026-08-23")
