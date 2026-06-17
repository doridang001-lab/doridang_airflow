"""Daily Airflow DAG monitoring pipeline."""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import UTC
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd

from modules.common.config import ADMIN_EMAIL
from modules.transform.dashboard.dag_monitoring_dashboard import render_dashboard_html as render_monitoring_dashboard_html
from modules.transform.utility.onedrive import save_to_onedrive_csv
from modules.transform.utility.paths import DASHBOARD_DB, MART_DB

logger = logging.getLogger(__name__)

AIRFLOW_UI_BASE_URL = os.getenv("AIRFLOW_UI_BASE_URL", "http://localhost:8080").rstrip("/")
HEATMAP_TIME_SOURCE = "start_date"
DAG_MONITORING_SNAPSHOT_FILENAME = "dag_monitoring_snapshot.json"
DAY_MONITORING_HTML_FILENAME = "dag_monitoring_dashboard.html"
RUNNING_STATES = {"queued", "running", "scheduled", "deferred", "up_for_retry"}
SHORT_DURATION_WHITELIST = {
    "DB_UnifiedReview_Dags",
    "Sales_StoreSales_01_Report_Dags",
    "Strategy_FdamCS_02_FlowMacro_Dags",
}


def _parse_payload_timestamp(value: Any) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"nat", "none", "null"}:
            return pd.NaT
        if text.isdigit():
            return pd.to_datetime(int(text), unit="ms", utc=True, errors="coerce")
        return pd.to_datetime(text, utc=True, errors="coerce")
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return pd.NaT
        return pd.to_datetime(value, unit="ms", utc=True, errors="coerce")
    return pd.to_datetime(value, utc=True, errors="coerce")


def _to_iso_text(value: Any) -> str | None:
    ts = _parse_payload_timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _to_kst_display(value: Any) -> str:
    ts = _parse_payload_timestamp(value)
    if pd.isna(ts):
        return "-"
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.tz_convert("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")


def _get_logs_root() -> Path:
    candidates = [
        Path(os.getenv("AIRFLOW_HOME", "")) / "logs" if os.getenv("AIRFLOW_HOME") else None,
        Path("/opt/airflow/logs"),
        Path.cwd() / "logs",
        Path("C:/airflow/logs"),
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    return Path.cwd() / "logs"


def _find_latest_task_log(dag_id: str, run_id: str, task_id: str) -> Path | None:
    task_root = _get_logs_root() / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    if not task_root.exists():
        return None
    attempts = sorted(task_root.glob("attempt=*.log"))
    return attempts[-1] if attempts else None


def _extract_error_details(log_path: Path | None) -> tuple[str | None, str | None]:
    if not log_path or not log_path.exists():
        return None, None

    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not lines:
        return None, None

    summary = None
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if "traceback" in lowered:
            continue
        if "error" in lowered or "exception" in lowered or stripped.startswith(tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")):
            summary = stripped[-220:]
            break

    excerpt_lines: list[str] = []
    traceback_start = None
    for index in range(len(lines) - 1, -1, -1):
        if "Traceback" in lines[index]:
            traceback_start = index
            break

    if traceback_start is not None:
        excerpt_lines = lines[traceback_start : traceback_start + 40]
    else:
        excerpt_lines = lines[-40:]

    excerpt = "\n".join(line.rstrip() for line in excerpt_lines if line.strip()).strip()
    if not summary and excerpt:
        summary = excerpt.splitlines()[-1][-220:]
    if excerpt and len(excerpt) > 4000:
        excerpt = excerpt[-4000:]
    return summary, excerpt or None


def _infer_group(dag_id: str) -> str:
    if dag_id.startswith("Sales_"):
        return "sales"
    if dag_id.startswith("DB_"):
        return "db"
    if dag_id.startswith("Strategy_"):
        return "strategy"
    if dag_id.startswith("Private_"):
        return "private"
    if dag_id.startswith("ETL_") or "_ETL_" in dag_id or dag_id.startswith("DB_ETL_"):
        return "etl"
    return "other"


def _build_dag_grid_url(dag_id: str) -> str:
    return f"{AIRFLOW_UI_BASE_URL}/dags/{quote(dag_id, safe='')}/grid"


def _build_copy_text(dag: dict[str, Any], task: dict[str, Any] | None = None) -> str:
    lines = [
        f"DAG: {dag['dag_id']}",
        f"Status: {dag['status']}",
        f"Run: {dag.get('last_run_at_kst') or '-'}",
    ]
    if task:
        lines.append(f"Task: {task['task_id']}")
        lines.append(f"Task State: {task['state']}")
        if task.get("error_summary"):
            lines.append(f"Error: {task['error_summary']}")
    else:
        lines.append(f"Reason: {dag.get('detail') or '-'}")
    return "\n".join(lines)


def collect_dag_runs(**context: Any) -> str:
    """Collect DagRun and TaskInstance rows for the target logical day."""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    logical_date = context["logical_date"]
    logical_kst = logical_date.in_timezone("Asia/Seoul")
    target_date = logical_kst.strftime("%Y-%m-%d")
    date_start = logical_kst.start_of("day")
    date_end = date_start.add(days=1)

    dag_run_rows: list[dict[str, Any]] = []
    task_rows: list[dict[str, Any]] = []
    excluded_prefixes = ("Strategy_DagMonitoring",)

    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= date_start)
            .filter(DagRun.execution_date < date_end)
            .all()
        )

        for dag_run in dag_runs:
            if dag_run.dag_id.startswith(excluded_prefixes):
                continue

            dag_run_rows.append(
                {
                    "dag_id": dag_run.dag_id,
                    "run_id": dag_run.run_id,
                    "state": dag_run.state,
                    "execution_date": _to_iso_text(dag_run.execution_date),
                    "start_date": _to_iso_text(dag_run.start_date),
                    "end_date": _to_iso_text(dag_run.end_date),
                }
            )

            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_run.dag_id)
                .filter(TaskInstance.run_id == dag_run.run_id)
                .all()
            )
            for task in task_instances:
                task_rows.append(
                    {
                        "dag_id": task.dag_id,
                        "run_id": task.run_id,
                        "task_id": task.task_id,
                        "state": task.state,
                        "start_date": _to_iso_text(task.start_date),
                        "end_date": _to_iso_text(task.end_date),
                        "duration": task.duration,
                        "try_number": getattr(task, "try_number", None),
                        "log_url": getattr(task, "log_url", None),
                    }
                )

    payload = {
        "target_date": target_date,
        "dag_runs": json.dumps(dag_run_rows, ensure_ascii=False),
        "task_instances": json.dumps(task_rows, ensure_ascii=False),
    }
    context["ti"].xcom_push(key="dag_runs_and_tasks", value=payload)
    logger.info("Collected dag monitoring source rows: dags=%s tasks=%s", len(dag_run_rows), len(task_rows))
    return target_date


def apply_failure_rules(**context: Any) -> str:
    """Score DAG runs and build dashboard snapshot payload."""
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="collect_dag_runs", key="dag_runs_and_tasks")
    target_date = payload["target_date"]

    dag_runs_df = pd.read_json(io.StringIO(payload["dag_runs"]), orient="records", convert_dates=False)
    task_df = pd.read_json(io.StringIO(payload["task_instances"]), orient="records", convert_dates=False)

    if dag_runs_df.empty:
        results_df = pd.DataFrame(
            columns=[
                "dag_id",
                "run_id",
                "execution_date",
                "dag_run_state",
                "status",
                "fail_type",
                "detail",
                "duration_sec",
                "total_tasks",
                "success_tasks",
                "failed_tasks",
                "skipped_tasks",
                "start_hour_kst",
                "start_min_kst",
                "last_run_at_kst",
                "group",
            ]
        )
        snapshot = _build_dashboard_snapshot(target_date=target_date, dag_payloads=[])
    else:
        dag_runs_df["execution_date"] = pd.to_datetime(dag_runs_df["execution_date"], utc=True, errors="coerce")
        dag_runs_df["start_date"] = pd.to_datetime(dag_runs_df["start_date"], utc=True, errors="coerce")
        dag_runs_df = dag_runs_df[~dag_runs_df["run_id"].astype(str).str.startswith("__airflow_temporary_run_")]
        dag_runs_df = (
            dag_runs_df.sort_values(["dag_id", "execution_date", "start_date"], ascending=[True, False, False])
            .drop_duplicates(subset=["dag_id"], keep="first")
            .reset_index(drop=True)
        )

        if not task_df.empty:
            valid_keys = set(zip(dag_runs_df["dag_id"], dag_runs_df["run_id"]))
            task_df = task_df[
                task_df.apply(lambda row: (row["dag_id"], row["run_id"]) in valid_keys, axis=1)
            ].reset_index(drop=True)

        dag_payloads: list[dict[str, Any]] = []
        result_rows: list[dict[str, Any]] = []

        for _, dag_run in dag_runs_df.iterrows():
            dag_id = str(dag_run["dag_id"])
            run_id = str(dag_run["run_id"])
            run_tasks = (
                task_df[(task_df["dag_id"] == dag_id) & (task_df["run_id"] == run_id)].copy()
                if not task_df.empty
                else pd.DataFrame()
            )

            total_tasks = len(run_tasks)
            success_tasks = int((run_tasks["state"] == "success").sum()) if total_tasks else 0
            failed_tasks = int((run_tasks["state"] == "failed").sum()) if total_tasks else 0
            skipped_tasks = int((run_tasks["state"] == "skipped").sum()) if total_tasks else 0
            upstream_failed_tasks = int((run_tasks["state"] == "upstream_failed").sum()) if total_tasks else 0

            start = _parse_payload_timestamp(dag_run.get("start_date"))
            end = _parse_payload_timestamp(dag_run.get("end_date"))
            duration_sec = (end - start).total_seconds() if pd.notna(start) and pd.notna(end) else 0.0

            preferred_col = "start_date" if HEATMAP_TIME_SOURCE == "start_date" else "execution_date"
            ts = _parse_payload_timestamp(dag_run.get(preferred_col))
            if pd.isna(ts):
                ts = _parse_payload_timestamp(dag_run.get("execution_date"))
            if pd.notna(ts):
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                ts_kst = ts.tz_convert("Asia/Seoul")
                minute_rounded = int(round(ts_kst.minute / 5.0) * 5)
                hour = int(ts_kst.hour)
                if minute_rounded == 60:
                    minute_rounded = 0
                    hour = (hour + 1) % 24
                start_hour_kst = hour
                start_min_kst = minute_rounded
                last_run_at_kst = ts_kst.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_hour_kst = -1
                start_min_kst = -1
                last_run_at_kst = "-"

            status = "OK"
            fail_type = None
            detail = "Healthy"

            intentional_skip_run = (
                total_tasks > 0
                and skipped_tasks > 0
                and failed_tasks == 0
                and upstream_failed_tasks == 0
                and dag_run["state"] == "success"
                and (success_tasks + skipped_tasks) == total_tasks
            )

            if dag_run["state"] == "failed":
                status = "FAIL"
                fail_type = "dag_failed"
                detail = f"DAG 실행 실패 (실패 Task {failed_tasks}건)"

            if status != "FAIL":
                if total_tasks > 0 and (skipped_tasks / total_tasks) >= 0.5 and not intentional_skip_run:
                    status = "FAIL"
                    fail_type = "high_skip_ratio"
                    detail = f"스킵 비율 과다 ({skipped_tasks}/{total_tasks})"
                elif upstream_failed_tasks > 0:
                    status = "FAIL"
                    fail_type = "upstream_failed"
                    detail = f"선행 Task 실패 감지 ({upstream_failed_tasks}건)"

            if status == "OK":
                if total_tasks > 0 and skipped_tasks == total_tasks:
                    status = "WARN"
                    fail_type = "all_skipped"
                    detail = f"전체 Task 스킵 ({total_tasks}건)"
                elif intentional_skip_run:
                    detail = f"의도된 스킵 포함 완료 ({skipped_tasks}/{total_tasks})"
                elif 0 < duration_sec < 10 and dag_id not in SHORT_DURATION_WHITELIST:
                    status = "WARN"
                    fail_type = "short_duration"
                    detail = f"실행시간이 너무 짧음 ({duration_sec:.1f}초)"

            task_payloads: list[dict[str, Any]] = []
            for _, task in run_tasks.sort_values(["task_id"]).iterrows():
                task_id = str(task["task_id"])
                log_path = _find_latest_task_log(dag_id, run_id, task_id)
                error_summary = None
                error_excerpt = None
                if task.get("state") in {"failed", "upstream_failed"}:
                    error_summary, error_excerpt = _extract_error_details(log_path)

                task_payload = {
                    "task_id": task_id,
                    "state": str(task.get("state") or "unknown"),
                    "try_number": int(task["try_number"]) if pd.notna(task.get("try_number")) else None,
                    "duration_sec": float(task["duration"]) if pd.notna(task.get("duration")) else None,
                    "log_url": task.get("log_url"),
                    "log_path": str(log_path) if log_path else None,
                    "error_summary": error_summary,
                    "error_excerpt": error_excerpt,
                    "copy_text": "",
                }
                task_payloads.append(task_payload)

            dag_payload = {
                "dag_id": dag_id,
                "run_id": run_id,
                "status": status,
                "fail_type": fail_type,
                "detail": detail,
                "dag_run_state": str(dag_run["state"]),
                "duration_sec": round(float(duration_sec), 1),
                "total_tasks": total_tasks,
                "success_tasks": success_tasks,
                "failed_tasks": failed_tasks,
                "skipped_tasks": skipped_tasks,
                "upstream_failed_tasks": upstream_failed_tasks,
                "failed_task_ids": [task["task_id"] for task in task_payloads if task["state"] == "failed"],
                "upstream_failed_task_ids": [task["task_id"] for task in task_payloads if task["state"] == "upstream_failed"],
                "execution_date": _to_iso_text(dag_run.get("execution_date")),
                "start_date": _to_iso_text(dag_run.get("start_date")),
                "end_date": _to_iso_text(dag_run.get("end_date")),
                "last_run_at_kst": last_run_at_kst,
                "start_hour_kst": start_hour_kst,
                "start_min_kst": start_min_kst,
                "group": _infer_group(dag_id),
                "grid_url": _build_dag_grid_url(dag_id),
                "tasks": task_payloads,
                "copy_text": "",
            }
            dag_payload["copy_text"] = _build_copy_text(dag_payload)
            for task_payload in dag_payload["tasks"]:
                task_payload["copy_text"] = _build_copy_text(dag_payload, task_payload)
            dag_payloads.append(dag_payload)

            result_rows.append(
                {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "execution_date": dag_payload["execution_date"],
                    "dag_run_state": dag_payload["dag_run_state"],
                    "status": status,
                    "fail_type": fail_type,
                    "detail": detail,
                    "duration_sec": dag_payload["duration_sec"],
                    "total_tasks": total_tasks,
                    "success_tasks": success_tasks,
                    "failed_tasks": failed_tasks,
                    "skipped_tasks": skipped_tasks,
                    "start_hour_kst": start_hour_kst,
                    "start_min_kst": start_min_kst,
                    "last_run_at_kst": last_run_at_kst,
                    "group": dag_payload["group"],
                }
            )

        results_df = pd.DataFrame(result_rows)
        snapshot = _build_dashboard_snapshot(target_date=target_date, dag_payloads=dag_payloads)

    payload_out = {
        "target_date": target_date,
        "results": results_df.to_json(orient="records", force_ascii=False),
        "snapshot": json.dumps(snapshot, ensure_ascii=False),
    }
    ti.xcom_push(key="monitoring_results", value=payload_out)
    logger.info(
        "Applied dag monitoring rules: fail=%s warn=%s ok=%s",
        snapshot["summary"]["fail_count"],
        snapshot["summary"]["warn_count"],
        snapshot["summary"]["ok_count"],
    )
    return target_date


def _build_dashboard_snapshot(*, target_date: str, dag_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    fail_count = sum(1 for dag in dag_payloads if dag["status"] == "FAIL")
    warn_count = sum(1 for dag in dag_payloads if dag["status"] == "WARN")
    ok_count = sum(1 for dag in dag_payloads if dag["status"] == "OK")
    return {
        "generated_at": pd.Timestamp.now(tz=UTC).isoformat(),
        "target_date": target_date,
        "summary": {
            "total_count": len(dag_payloads),
            "fail_count": fail_count,
            "warn_count": warn_count,
            "ok_count": ok_count,
        },
        "dags": sorted(
            dag_payloads,
            key=lambda dag: (
                {"FAIL": 0, "WARN": 1, "OK": 2}.get(dag["status"], 3),
                dag.get("last_run_at_kst") or "",
                dag["dag_id"],
            ),
        ),
    }


def _build_report_html(results_df: pd.DataFrame, target_date: str) -> str:
    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0
    total_count = len(results_df)

    colors = {
        "FAIL": "#c0392b",
        "WARN": "#d97706",
        "OK": "#15803d",
        "EMPTY": "#e5e7eb",
        "HEADER": "#111827",
    }

    def card(color: str, label: str, count: int) -> str:
        return (
            f'<div style="display:inline-block;background:{color};color:#fff;border-radius:8px;'
            f'padding:12px 18px;margin:4px;min-width:96px;">'
            f'<div style="font-size:12px;opacity:.85;">{label}</div>'
            f'<div style="font-size:24px;font-weight:700;">{count}</div>'
            f"</div>"
        )

    summary_html = "".join(
        [
            card(colors["FAIL"], "FAIL", fail_count),
            card(colors["WARN"], "WARN", warn_count),
            card(colors["OK"], "OK", ok_count),
            card("#475569", "TOTAL", total_count),
        ]
    )

    alert_html = ""
    if not results_df.empty and (fail_count or warn_count):
        rows_html = ""
        alert_rows = results_df[results_df["status"].isin(["FAIL", "WARN"])].sort_values(["status", "dag_id"])
        for _, row in alert_rows.iterrows():
            color = colors["FAIL"] if row["status"] == "FAIL" else colors["WARN"]
            rows_html += (
                "<tr>"
                f'<td style="padding:8px 10px;border-bottom:1px solid #e5e7eb;"><a href="{_build_dag_grid_url(str(row["dag_id"]))}" '
                f'style="color:{color};font-weight:700;text-decoration:none;">{row["dag_id"]}</a></td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #e5e7eb;">'
                f'<span style="background:{color};color:#fff;border-radius:999px;padding:2px 8px;font-size:12px;">{row["status"]}</span></td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #e5e7eb;">{row["detail"]}</td>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #e5e7eb;text-align:right;">{row["duration_sec"]:.1f}s</td>'
                "</tr>"
            )
        alert_html = (
            '<h3 style="margin:24px 0 8px;">Failures and warnings</h3>'
            '<table style="width:100%;border-collapse:collapse;font-size:13px;">'
            '<thead><tr>'
            '<th style="padding:8px 10px;background:#1f2937;color:#fff;text-align:left;">DAG</th>'
            '<th style="padding:8px 10px;background:#1f2937;color:#fff;text-align:left;">Status</th>'
            '<th style="padding:8px 10px;background:#1f2937;color:#fff;text-align:left;">Detail</th>'
            '<th style="padding:8px 10px;background:#1f2937;color:#fff;text-align:right;">Duration</th>'
            f"</tr></thead><tbody>{rows_html}</tbody></table>"
        )

    def legend(label: str, color: str) -> str:
        return (
            f'<span style="display:inline-block;background:{color};color:#fff;border-radius:999px;'
            f'padding:2px 8px;font-size:11px;margin-right:6px;">{label}</span>'
        )

    heatmap_html = ""
    if not results_df.empty:
        status_marker = {"OK": "O", "FAIL": "F", "WARN": "W"}
        status_color = {"OK": colors["OK"], "FAIL": colors["FAIL"], "WARN": colors["WARN"]}
        legend_html = "".join(
            [
                legend("OK", colors["OK"]),
                legend("FAIL", colors["FAIL"]),
                legend("WARN", colors["WARN"]),
                legend("EMPTY", colors["EMPTY"]),
            ]
        )
        header_cells = "".join(
            f'<th style="padding:4px 6px;background:{colors["HEADER"]};color:#fff;font-size:11px;">{hour:02d}</th>'
            for hour in range(24)
        )
        body_rows = ""
        for _, row in results_df.sort_values("dag_id").iterrows():
            hour = int(row["start_hour_kst"]) if pd.notna(row["start_hour_kst"]) else -1
            cells = ""
            for slot in range(24):
                if hour == slot and hour >= 0:
                    color = status_color.get(row["status"], colors["OK"])
                    marker = status_marker.get(row["status"], "?")
                    cells += (
                        f'<td style="background:{color};color:#fff;text-align:center;font-size:11px;height:18px;">{marker}</td>'
                    )
                else:
                    cells += f'<td style="background:{colors["EMPTY"]};height:18px;"></td>'
            body_rows += (
                f"<tr><td style=\"padding:4px 8px;border-bottom:1px solid #e5e7eb;\">{row['dag_id']}</td>{cells}</tr>"
            )
        heatmap_html = (
            '<h3 style="margin:28px 0 8px;">Hourly execution heatmap (KST)</h3>'
            f'<p style="font-size:12px;color:#64748b;">{legend_html}</p>'
            '<div style="overflow-x:auto;"><table style="border-collapse:collapse;font-size:12px;min-width:640px;">'
            f'<thead><tr><th style="padding:6px 10px;background:{colors["HEADER"]};color:#fff;text-align:left;">DAG</th>{header_cells}</tr></thead>'
            f"<tbody>{body_rows}</tbody></table></div>"
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Segoe UI,Arial,sans-serif;max-width:1100px;margin:0 auto;padding:24px;color:#0f172a;">
  <div style="background:#111827;color:#fff;padding:18px 22px;border-radius:12px;">
    <h2 style="margin:0 0 4px;">Airflow DAG monitoring</h2>
    <p style="margin:0;opacity:.8;">Daily summary for {target_date}</p>
  </div>
  <div style="margin-top:16px;">{summary_html}</div>
  {alert_html}
  {heatmap_html}
  <p style="margin-top:28px;color:#94a3b8;font-size:11px;">Generated by the Airflow DAG monitoring pipeline.</p>
</body>
</html>"""


def _write_dashboard_snapshot(snapshot: dict[str, Any]) -> Path:
    DASHBOARD_DB.mkdir(parents=True, exist_ok=True)
    snapshot_path = DASHBOARD_DB / DAG_MONITORING_SNAPSHOT_FILENAME
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_path


def _write_dashboard_html(snapshot: dict[str, Any]) -> Path:
    DASHBOARD_DB.mkdir(parents=True, exist_ok=True)
    html_path = DASHBOARD_DB / DAY_MONITORING_HTML_FILENAME
    html_path.write_text(render_monitoring_dashboard_html(snapshot), encoding="utf-8")
    return html_path


def _write_mart_dashboard_html(snapshot: dict[str, Any]) -> Path:
    mart_dir = MART_DB / "dags_monitoring"
    mart_dir.mkdir(parents=True, exist_ok=True)
    html_path = mart_dir / DAY_MONITORING_HTML_FILENAME
    html_path.write_text(render_monitoring_dashboard_html(snapshot), encoding="utf-8")
    return html_path


def _write_mart_dashboard_snapshot(snapshot: dict[str, Any]) -> Path:
    mart_dir = MART_DB / "dags_monitoring"
    mart_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = mart_dir / DAG_MONITORING_SNAPSHOT_FILENAME
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_path


def save_monitoring_results(**context: Any) -> str:
    """Persist daily CSV and latest dashboard snapshot."""
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="apply_failure_rules", key="monitoring_results")
    target_date = payload["target_date"]
    results_df = pd.read_json(io.StringIO(payload["results"]), orient="records", convert_dates=False)
    snapshot = json.loads(payload["snapshot"])

    date_str = target_date.replace("-", "")
    file_path = str(MART_DB / "dags_monitoring" / f"dags_monitoring_{date_str}.csv")
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    save_to_onedrive_csv(df=results_df, file_path=file_path, pk_col="dag_id", if_exists="replace")
    snapshot_path = _write_dashboard_snapshot(snapshot)
    html_path = _write_dashboard_html(snapshot)
    mart_snapshot_path = _write_mart_dashboard_snapshot(snapshot)
    mart_html_path = _write_mart_dashboard_html(snapshot)

    ti.xcom_push(key="results_file_path", value=file_path)
    ti.xcom_push(key="dashboard_snapshot_path", value=str(snapshot_path))
    ti.xcom_push(key="dashboard_html_path", value=str(html_path))
    ti.xcom_push(key="dashboard_mart_snapshot_path", value=str(mart_snapshot_path))
    ti.xcom_push(key="dashboard_mart_html_path", value=str(mart_html_path))
    logger.info(
        "Saved dag monitoring csv to %s, snapshot to %s, dashboard html to %s, mart snapshot to %s, and mart html to %s",
        file_path,
        snapshot_path,
        html_path,
        mart_snapshot_path,
        mart_html_path,
    )
    return file_path


def send_monitoring_alert_email(**context: Any) -> str:
    """Send daily monitoring email with the current summary HTML."""
    from modules.transform.utility.mailer import send_email

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="apply_failure_rules", key="monitoring_results")
    target_date = payload["target_date"]
    results_df = pd.read_json(io.StringIO(payload["results"]), orient="records", convert_dates=False)

    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0

    html_content = _build_report_html(results_df, target_date)
    if fail_count > 0:
        prefix = "FAIL"
    elif warn_count > 0:
        prefix = "WARN"
    else:
        prefix = "OK"

    subject = f"[DAG Monitoring {prefix}] {target_date} - FAIL {fail_count} / WARN {warn_count} / OK {ok_count}"
    result = send_email(subject=subject, html_content=html_content, to_emails=ADMIN_EMAIL, **context)
    logger.info("Sent dag monitoring alert email: %s", subject)
    return result
