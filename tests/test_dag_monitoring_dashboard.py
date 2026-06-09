from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.dashboard.dag_monitoring_dashboard import (  # noqa: E402
    DAG_MONITORING_SNAPSHOT_FILENAME,
    DagMonitoringDashboardService,
    render_dashboard_html,
)
from modules.transform.pipelines.strategy.dag_monitoring_pipeline import (  # noqa: E402
    _build_dashboard_snapshot,
    _build_report_html,
    _extract_error_details,
    _write_dashboard_html,
)


def test_extract_error_details_reads_traceback(tmp_path: Path):
    log_path = tmp_path / "attempt=1.log"
    log_path.write_text(
        "\n".join(
            [
                "[2026-06-06T08:00:00] INFO starting",
                "Traceback (most recent call last):",
                '  File "/opt/airflow/dags/example.py", line 1, in <module>',
                '    raise RuntimeError("boom")',
                "RuntimeError: boom",
            ]
        ),
        encoding="utf-8",
    )

    summary, excerpt = _extract_error_details(log_path)

    assert summary == "RuntimeError: boom"
    assert "Traceback" in excerpt
    assert "RuntimeError: boom" in excerpt


def test_dashboard_service_returns_task_log_tail(tmp_path: Path):
    log_path = tmp_path / "task.log"
    log_path.write_text("line 1\nline 2\nline 3", encoding="utf-8")
    snapshot = {
        "generated_at": "2026-06-06T00:00:00+00:00",
        "target_date": "2026-06-06",
        "summary": {"total_count": 1, "fail_count": 1, "warn_count": 0, "ok_count": 0},
        "dags": [
            {
                "dag_id": "DB_Test_Dag",
                "run_id": "scheduled__2026-06-06",
                "status": "FAIL",
                "detail": "DAG run failed (1 failed tasks)",
                "grid_url": "http://localhost:8080/dags/DB_Test_Dag/grid",
                "tasks": [
                    {
                        "task_id": "prepare_data",
                        "state": "success",
                        "try_number": 1,
                        "log_path": str(log_path),
                        "log_url": "http://localhost:8080/log/prepare",
                        "error_summary": None,
                        "error_excerpt": None,
                        "copy_text": "copy prepare",
                    },
                    {
                        "task_id": "load_data",
                        "state": "failed",
                        "try_number": 1,
                        "log_path": str(log_path),
                        "log_url": "http://localhost:8080/log",
                        "error_summary": "RuntimeError: boom",
                        "error_excerpt": "RuntimeError: boom",
                        "copy_text": "copy me",
                    }
                ],
            }
        ],
    }
    (tmp_path / DAG_MONITORING_SNAPSHOT_FILENAME).write_text(json.dumps(snapshot), encoding="utf-8")

    service = DagMonitoringDashboardService(root=tmp_path)
    details = service.get_dag_details("DB_Test_Dag", "scheduled__2026-06-06")

    assert details is not None
    assert [task["task_id"] for task in details["tasks"]] == ["load_data", "prepare_data"]
    assert details["tasks"][0]["log_tail"] == "line 1\nline 2\nline 3"


def test_render_dashboard_html_contains_copy_and_log_labels():
    html = render_dashboard_html(
        {
            "generated_at": "2026-06-06T00:00:00+00:00",
            "target_date": "2026-06-06",
            "summary": {"total_count": 1, "fail_count": 1, "warn_count": 0, "ok_count": 0},
            "dags": [
                {
                    "dag_id": "DB_Test_Dag",
                    "run_id": "scheduled__2026-06-06",
                    "status": "FAIL",
                    "detail": "DAG run failed (1 failed tasks)",
                    "duration_sec": 12.4,
                    "last_run_at_kst": "2026-06-06 09:00:00",
                    "failed_task_ids": ["load_data"],
                    "upstream_failed_task_ids": [],
                    "group": "db",
                    "copy_text": "copy me",
                    "grid_url": "http://localhost:8080/dags/DB_Test_Dag/grid",
                    "tasks": [],
                    "start_hour_kst": 9,
                }
            ],
        }
    )

    assert "Airflow DAG 모니터링" in html
    assert "오류 복사" in html
    assert "task 로그를 불러오는 중입니다" in html or "Task 로그 미리보기" in html or "로그 보기" in html


def test_build_dashboard_snapshot_counts_statuses():
    snapshot = _build_dashboard_snapshot(
        target_date="2026-06-06",
        dag_payloads=[
            {"dag_id": "a", "status": "FAIL", "last_run_at_kst": "2026-06-06 09:00:00"},
            {"dag_id": "b", "status": "WARN", "last_run_at_kst": "2026-06-06 10:00:00"},
            {"dag_id": "c", "status": "OK", "last_run_at_kst": "2026-06-06 11:00:00"},
        ],
    )

    assert snapshot["summary"] == {"total_count": 3, "fail_count": 1, "warn_count": 1, "ok_count": 1}


def test_build_report_html_contains_hourly_heatmap():
    results_df = pd.DataFrame(
        [
            {
                "dag_id": "DB_Test_Dag",
                "status": "FAIL",
                "detail": "DAG run failed (1 failed tasks)",
                "duration_sec": 9.5,
                "start_hour_kst": 9,
            }
        ]
    )

    html = _build_report_html(results_df, "2026-06-06")

    assert "Hourly execution heatmap" in html
    assert "Failures and warnings" in html


def test_write_dashboard_html_persists_file(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "modules.transform.pipelines.strategy.dag_monitoring_pipeline.DASHBOARD_DB",
        tmp_path,
    )
    snapshot = {
        "generated_at": "2026-06-06T00:00:00+00:00",
        "target_date": "2026-06-06",
        "summary": {"total_count": 1, "fail_count": 1, "warn_count": 0, "ok_count": 0},
        "dags": [],
    }

    html_path = _write_dashboard_html(snapshot)

    assert html_path.exists()
    assert "Airflow DAG 모니터링" in html_path.read_text(encoding="utf-8")
