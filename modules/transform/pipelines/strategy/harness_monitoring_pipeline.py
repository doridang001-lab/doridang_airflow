"""Codex harness monitoring snapshot and dashboard pipeline."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from modules.transform.dashboard.harness_monitoring_dashboard import (
    HARNESS_MONITORING_HTML_FILENAME,
    HARNESS_MONITORING_SNAPSHOT_FILENAME,
    build_alert_text,
    build_harness_snapshot,
    _file_diagnostic,
    render_dashboard_html,
    should_alert,
)
from modules.transform.utility.paths import DASHBOARD_DB

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _write_dashboard_snapshot(snapshot: dict[str, Any], *, root: Path | str = DASHBOARD_DB) -> Path:
    target_root = Path(root)
    target_root.mkdir(parents=True, exist_ok=True)
    path = target_root / HARNESS_MONITORING_SNAPSHOT_FILENAME
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_dashboard_html(snapshot: dict[str, Any], *, root: Path | str = DASHBOARD_DB) -> Path:
    target_root = Path(root)
    target_root.mkdir(parents=True, exist_ok=True)
    path = target_root / HARNESS_MONITORING_HTML_FILENAME
    path.write_text(render_dashboard_html(snapshot), encoding="utf-8")
    return path


def collect_harness_status(**context: Any) -> str:
    snapshot = build_harness_snapshot(root=_repo_root())
    dag_run = context.get("dag_run")
    airflow_diag = (snapshot.get("diagnostics") or {}).get("airflow_dag") or {}
    if dag_run is not None:
        airflow_diag.update(
            {
                "status": "OK",
                "message": f"DAG 실행 중: {getattr(dag_run, 'run_id', '-')}",
                "run_id": getattr(dag_run, "run_id", None),
                "run_type": str(getattr(dag_run, "run_type", "") or ""),
                "execution_date": str(getattr(dag_run, "execution_date", "") or ""),
            }
        )
        snapshot.setdefault("diagnostics", {})["airflow_dag"] = airflow_diag
    context["ti"].xcom_push(key="harness_snapshot", value=json.dumps(snapshot, ensure_ascii=False))
    logger.info("Collected harness snapshot: %s", snapshot.get("summary"))
    return snapshot["generated_at"]


def render_harness_dashboard(**context: Any) -> str:
    payload = context["ti"].xcom_pull(task_ids="collect_harness_status", key="harness_snapshot")
    snapshot = json.loads(payload)
    snapshot_path = _write_dashboard_snapshot(snapshot)
    html_path = _write_dashboard_html(snapshot)
    dashboard_outputs = {
        "status": "OK",
        "label": "대시보드 산출물",
        "message": "snapshot/html 파일 상태",
        "snapshot": _file_diagnostic(snapshot_path, "snapshot"),
        "html": _file_diagnostic(html_path, "html"),
        "root": str(DASHBOARD_DB),
    }
    if any(dashboard_outputs[key]["status"] == "ERROR" for key in ("snapshot", "html")):
        dashboard_outputs["status"] = "ERROR"
    snapshot.setdefault("diagnostics", {})["dashboard_outputs"] = dashboard_outputs
    snapshot_path = _write_dashboard_snapshot(snapshot)
    html_path = _write_dashboard_html(snapshot)
    context["ti"].xcom_push(key="harness_snapshot_path", value=str(snapshot_path))
    context["ti"].xcom_push(key="harness_html_path", value=str(html_path))
    logger.info("Saved harness dashboard snapshot=%s html=%s", snapshot_path, html_path)
    return str(html_path)


def send_harness_alert_if_needed(**context: Any) -> str:
    from modules.transform.utility.notifier import send_telegram

    payload = context["ti"].xcom_pull(task_ids="collect_harness_status", key="harness_snapshot")
    snapshot = json.loads(payload)
    if not should_alert(snapshot):
        logger.info("Harness monitoring alert skipped: no problem states")
        return "alert skipped"
    send_telegram(build_alert_text(snapshot))
    logger.info("Harness monitoring alert sent")
    return "alert sent"
