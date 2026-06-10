"""Dashboard helpers for Airflow DAG monitoring."""

from __future__ import annotations

import json
import logging
import os
import re
from csv import DictReader
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from modules.transform.utility.paths import DASHBOARD_DB, MART_DB

logger = logging.getLogger(__name__)

DAG_MONITORING_SNAPSHOT_FILENAME = "dag_monitoring_snapshot.json"
AIRFLOW_UI_BASE_URL = os.getenv("AIRFLOW_UI_BASE_URL", "http://localhost:8080").rstrip("/")


def _safe_json_embed(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/")


def _default_snapshot() -> dict[str, Any]:
    return {
        "generated_at": None,
        "target_date": None,
        "summary": {"total_count": 0, "fail_count": 0, "warn_count": 0, "ok_count": 0},
        "dags": [],
    }


def _to_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any, default: int = -1) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _trim_log_text(text: str, *, limit: int = 12000) -> str:
    return text if len(text) <= limit else text[-limit:]


def _read_log_tail(log_path: str | None, *, max_lines: int = 160) -> str | None:
    if not log_path:
        return None
    path = Path(log_path)
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not lines:
        return None
    return _trim_log_text("\n".join(lines[-max_lines:]))


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


def _build_grid_url(dag_id: str) -> str:
    return f"{AIRFLOW_UI_BASE_URL}/dags/{quote(dag_id, safe='')}/grid"


def _normalize_detail_text(value: Any) -> str:
    text = str(value or "-").strip() or "-"
    patterns = [
        (r"^DAG run failed \((\d+) failed tasks\)$", lambda m: f"DAG 실행 실패 (실패 Task {m.group(1)}건)"),
        (r"^High skip ratio \((\d+)/(\d+)\)$", lambda m: f"스킵 비율 과다 ({m.group(1)}/{m.group(2)})"),
        (r"^Upstream failures detected \((\d+)\)$", lambda m: f"선행 Task 실패 감지 ({m.group(1)}건)"),
        (r"^All tasks skipped \((\d+)\)$", lambda m: f"전체 Task 스킵 ({m.group(1)}건)"),
        (r"^Completed with intentional skips \((\d+)/(\d+)\)$", lambda m: f"의도된 스킵 포함 완료 ({m.group(1)}/{m.group(2)})"),
        (r"^Run duration looks too short \(([\d.]+)s\)$", lambda m: f"실행시간이 너무 짧음 ({m.group(1)}초)"),
        (r"^Healthy$", lambda m: "정상"),
    ]
    for pattern, formatter in patterns:
        match = re.match(pattern, text)
        if match:
            return formatter(match)
    return text


def _format_generated_at_kst(value: Any) -> str | None:
    if value in ("", None):
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=UTC)
        else:
            text = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
        kst = dt.astimezone(datetime.now().astimezone().tzinfo) if False else dt.astimezone(datetime.strptime("+0900", "%z").tzinfo)
        return kst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


class DagMonitoringDashboardService:
    def __init__(self, *, root: Path | str = DASHBOARD_DB):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.snapshot_path = self.root / DAG_MONITORING_SNAPSHOT_FILENAME

    def get_snapshot(self) -> dict[str, Any]:
        if not self.snapshot_path.exists():
            return self._load_snapshot_from_latest_csv()
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.exception("Failed to parse dag monitoring snapshot: %s", self.snapshot_path)
            return self._load_snapshot_from_latest_csv()
        snapshot = _default_snapshot()
        snapshot.update(payload if isinstance(payload, dict) else {})
        snapshot["summary"] = {**_default_snapshot()["summary"], **(snapshot.get("summary") or {})}
        snapshot["dags"] = list(snapshot.get("dags") or [])
        for dag in snapshot["dags"]:
            if isinstance(dag, dict):
                dag["detail"] = _normalize_detail_text(dag.get("detail"))
        return snapshot

    def _load_snapshot_from_latest_csv(self) -> dict[str, Any]:
        csv_dir = Path(MART_DB) / "dags_monitoring"
        if not csv_dir.exists():
            return _default_snapshot()
        csv_files = sorted(csv_dir.glob("dags_monitoring_*.csv"))
        if not csv_files:
            return _default_snapshot()

        latest = csv_files[-1]
        dags: list[dict[str, Any]] = []
        with latest.open("r", encoding="utf-8-sig", errors="ignore") as fp:
            reader = DictReader(fp)
            for row in reader:
                dag_id = (row.get("dag_id") or "").strip()
                if not dag_id:
                    continue
                status = (row.get("status") or "OK").strip().upper()
                run_id = (row.get("run_id") or "").strip()
                failed_ids = [x.strip() for x in str(row.get("failed_task_ids") or "").split(",") if x.strip()]
                upstream_ids = [x.strip() for x in str(row.get("upstream_failed_task_ids") or "").split(",") if x.strip()]
                last_run = row.get("last_run_at_kst") or row.get("execution_date") or "-"
                detail = _normalize_detail_text(row.get("detail") or "-")
                dags.append(
                    {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "status": status,
                        "detail": detail,
                        "duration_sec": _to_float(row.get("duration_sec")),
                        "last_run_at_kst": last_run,
                        "failed_task_ids": failed_ids,
                        "upstream_failed_task_ids": upstream_ids,
                        "group": row.get("group") or _infer_group(dag_id),
                        "copy_text": "\n".join(
                            [
                                f"DAG: {dag_id}",
                                f"상태: {status}",
                                f"최근 실행(KST): {last_run}",
                                f"상세: {detail}",
                            ]
                        ),
                        "grid_url": _build_grid_url(dag_id),
                        "tasks": [],
                        "task_detail_available": False,
                        "task_detail_message": "최신 스냅샷이 아직 없어 task별 상세 상태는 준비되지 않았습니다. 모니터링 DAG를 한 번 실행하면 성공/실패 task를 모두 볼 수 있습니다.",
                        "start_hour_kst": _to_int(row.get("start_hour_kst"), -1),
                        "start_min_kst": _to_int(row.get("start_min_kst"), -1),
                    }
                )

        target = latest.stem.replace("dags_monitoring_", "")
        target_date = f"{target[:4]}-{target[4:6]}-{target[6:8]}" if len(target) == 8 else target
        return {
            "generated_at": _format_generated_at_kst(latest.stat().st_mtime),
            "target_date": target_date,
            "summary": {
                "total_count": len(dags),
                "fail_count": sum(1 for dag in dags if dag["status"] == "FAIL"),
                "warn_count": sum(1 for dag in dags if dag["status"] == "WARN"),
                "ok_count": sum(1 for dag in dags if dag["status"] == "OK"),
            },
            "dags": dags,
        }

    def get_dag_details(self, dag_id: str, run_id: str | None = None) -> dict[str, Any] | None:
        snapshot = self.get_snapshot()
        state_rank = {
            "failed": 0,
            "upstream_failed": 1,
            "running": 2,
            "queued": 3,
            "up_for_retry": 4,
            "success": 5,
            "skipped": 6,
        }
        for dag in snapshot["dags"]:
            if dag.get("dag_id") != dag_id:
                continue
            if run_id and dag.get("run_id") != run_id:
                continue
            payload = dict(dag)
            tasks = []
            for task in dag.get("tasks", []):
                task_payload = dict(task)
                task_payload["log_tail"] = _read_log_tail(task_payload.get("log_path"))
                tasks.append(task_payload)
            payload["tasks"] = sorted(
                tasks,
                key=lambda item: (
                    state_rank.get(str(item.get("state") or "").lower(), 9),
                    item.get("task_id") or "",
                ),
            )
            if not payload["tasks"]:
                payload["task_detail_available"] = False
                payload.setdefault(
                    "task_detail_message",
                    "이 DAG에 대한 task별 상세 상태 데이터가 없습니다.",
                )
            else:
                payload["task_detail_available"] = True
            return payload
        return None

    def render_html(self) -> str:
        return render_dashboard_html(self.get_snapshot())


def render_dashboard_html(snapshot: dict[str, Any]) -> str:
    embedded = _safe_json_embed(snapshot)
    refresh_seconds = int(os.getenv("DAG_MONITORING_REFRESH_SECONDS", "20"))
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Airflow DAG 모니터링</title>
  <style>
    :root {{
      --bg: #071018;
      --panel: #0f1b26;
      --panel-2: #132331;
      --ink: #e8f0f7;
      --muted: #92a8bb;
      --line: #223547;
      --fail: #ef4444;
      --warn: #f59e0b;
      --ok: #22c55e;
      --chip: #183041;
      --code: #081018;
      --link: #7ce7d7;
      --hover: #102230;
      --active: #143427;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Malgun Gothic", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(45, 212, 191, 0.16), transparent 24%),
        linear-gradient(180deg, #061019 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    .page {{ max-width: 1440px; margin: 0 auto; padding: 28px 20px 48px; }}
    .hero {{ display: grid; gap: 16px; grid-template-columns: 1.3fr .7fr; margin-bottom: 18px; }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; box-shadow: 0 18px 42px rgba(0, 0, 0, .28); }}
    .hero-main {{ padding: 24px; background: linear-gradient(135deg, rgba(45, 212, 191, .12), rgba(19, 35, 49, .96)), var(--panel); }}
    .hero-title {{ margin: 0 0 8px; font-size: 32px; letter-spacing: -.04em; }}
    .hero-copy {{ margin: 0; color: var(--muted); line-height: 1.6; }}
    .hero-meta {{ margin-top: 16px; display: flex; gap: 10px; flex-wrap: wrap; }}
    .meta-pill {{ background: rgba(45, 212, 191, .12); color: #8ef1e2; padding: 8px 12px; border-radius: 999px; font-size: 13px; font-weight: 700; }}
    .hero-side {{ padding: 20px; display: grid; gap: 12px; align-content: start; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 18px; }}
    .kpi {{ padding: 16px; border-radius: 16px; color: #fff; }}
    .kpi label {{ display: block; font-size: 12px; opacity: .84; margin-bottom: 8px; }}
    .kpi strong {{ font-size: 28px; letter-spacing: -.03em; }}
    .kpi.fail {{ background: linear-gradient(135deg, #ef4444, #991b1b); }}
    .kpi.warn {{ background: linear-gradient(135deg, #f59e0b, #92400e); }}
    .kpi.ok {{ background: linear-gradient(135deg, #22c55e, #166534); }}
    .kpi.total {{ background: linear-gradient(135deg, #334155, #0f172a); }}
    .controls {{ display: grid; grid-template-columns: 220px 220px 1fr; gap: 12px; margin-bottom: 18px; }}
    .input, .select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      background: var(--panel-2);
      font-size: 14px;
      color: var(--ink);
    }}
    .table-panel {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; min-width: 1240px; }}
    thead th {{
      background: #13202d;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: .04em;
      padding: 0;
      text-align: left;
      border-bottom: 1px solid var(--line);
    }}
    .sort-button {{
      width: 100%;
      border: 0;
      background: transparent;
      color: inherit;
      font: inherit;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 14px 16px;
      cursor: pointer;
    }}
    .sort-button:hover {{ background: rgba(255, 255, 255, .02); }}
    .sort-indicator {{ font-size: 11px; color: #bfd0de; min-width: 24px; text-align: right; }}
    tbody td {{ padding: 14px 16px; border-bottom: 1px solid #162635; vertical-align: middle; font-size: 14px; }}
    .dag-row {{ cursor: pointer; transition: background .15s ease; }}
    .dag-row:hover {{ background: var(--hover); }}
    .dag-row.active {{ background: var(--active); }}
    .dag-row.status-fail {{ background: rgba(239, 68, 68, .08); }}
    .dag-row.status-warn {{ background: rgba(245, 158, 11, .05); }}
    .dag-row.status-fail:hover {{ background: rgba(239, 68, 68, .14); }}
    .dag-row.status-warn:hover {{ background: rgba(245, 158, 11, .10); }}
    .dag-row.status-fail td:first-child {{ box-shadow: inset 3px 0 0 var(--fail); }}
    .dag-name {{ font-weight: 700; display: flex; align-items: center; gap: 10px; min-width: 0; }}
    .dag-title {{ min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .status {{ display: inline-flex; align-items: center; gap: 6px; border-radius: 999px; padding: 4px 10px; font-size: 12px; font-weight: 700; color: #fff; }}
    .status.FAIL {{ background: var(--fail); }}
    .status.WARN {{ background: var(--warn); }}
    .status.OK {{ background: var(--ok); }}
    .task-status {{ display: inline-flex; align-items: center; justify-content: center; min-width: 68px; }}
    .task-status.failed, .task-status.upstream_failed {{ background: var(--fail); }}
    .task-status.running, .task-status.queued, .task-status.up_for_retry {{ background: #2563eb; }}
    .task-status.success {{ background: var(--ok); }}
    .task-status.skipped {{ background: #64748b; }}
    .group-chip {{ display: inline-flex; border-radius: 999px; padding: 4px 10px; font-size: 12px; background: var(--chip); color: #d7e3ef; text-transform: uppercase; font-weight: 700; }}
    .cell-status, .cell-time, .cell-duration {{ white-space: nowrap; }}
    .cell-status {{ width: 92px; }}
    .cell-duration {{ width: 92px; color: #dbe6f2; font-variant-numeric: tabular-nums; }}
    .cell-time {{ width: 180px; color: #dbe6f2; font-variant-numeric: tabular-nums; }}
    .cell-failed {{ color: #dbe6f2; word-break: break-word; }}
    .cell-detail {{ color: #dbe6f2; }}
    .cell-actions {{ width: 210px; }}
    .action-wrap {{ display:flex; gap:8px; flex-wrap:nowrap; align-items:center; }}
    .copy-button, .link-button, .task-button {{
      border: 0;
      border-radius: 10px;
      padding: 9px 12px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      white-space: nowrap;
    }}
    .copy-button {{ background: #dce8f3; color: #061019; }}
    .link-button {{ background: #10382f; color: #8ef1e2; text-decoration: none; display: inline-flex; align-items: center; }}
    .task-button {{ background: #1c2944; color: #c6d4ff; }}
    .details-row td {{ background: #0c1721; padding: 0; border-bottom: 1px solid var(--line); }}
    .details {{ padding: 18px; display: grid; grid-template-columns: 1fr; gap: 18px; }}
    .task-overview {{ display: grid; gap: 16px; }}
    .task-summary-grid {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; }}
    .task-summary-card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--panel-2);
      padding: 12px 14px;
    }}
    .task-summary-card strong {{ display: block; font-size: 22px; margin-top: 4px; }}
    .task-summary-card label {{ display: block; font-size: 12px; color: var(--muted); }}
    .task-board {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 10px; }}
    .task-list {{ display: grid; gap: 10px; }}
    .task-item {{ border: 1px solid var(--line); border-radius: 14px; padding: 12px; background: var(--panel); }}
    .task-item.active {{ border-color: #2dd4bf; box-shadow: inset 0 0 0 1px rgba(45, 212, 191, .15); }}
    .task-item.clickable {{ cursor: pointer; }}
    .task-head {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 8px; }}
    .task-name {{ font-weight: 700; font-size: 14px; }}
    .task-state {{ font-size: 12px; color: var(--muted); }}
    .task-actions {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    .task-meta {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 8px; }}
    .log-panel {{ border: 1px solid var(--line); border-radius: 14px; background: var(--panel); overflow: hidden; }}
    .log-panel-header {{ padding: 14px 16px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; align-items: center; gap: 12px; }}
    .log-panel pre {{ margin: 0; padding: 16px; background: var(--code); color: #d8e6f5; font-family: "Cascadia Code", Consolas, monospace; font-size: 12px; line-height: 1.5; max-height: 560px; overflow: auto; white-space: pre-wrap; word-break: break-word; }}
    .muted {{ color: var(--muted); }}
    .empty {{ padding: 28px; text-align: center; color: var(--muted); }}
    .heatmap {{ margin-top: 18px; padding: 18px; }}
    .heatmap-toolbar {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .heatmap-toggle {{
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--muted);
      border-radius: 999px;
      padding: 8px 14px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }}
    .heatmap-toggle.active {{
      background: #10382f;
      color: #8ef1e2;
      border-color: #1f5f53;
    }}
    .heatmap-grid {{ display: grid; grid-template-columns: 220px repeat(24, minmax(16px, 1fr)); gap: 6px; align-items: center; overflow-x: auto; }}
    .heat-head {{ font-size: 11px; color: var(--muted); text-align: center; }}
    .heat-name {{ font-size: 12px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .heat-cell {{ height: 16px; border-radius: 4px; background: #1b2d3d; }}
    .heat-cell.OK {{ background: #22c55e; }}
    .heat-cell.WARN {{ background: #f59e0b; }}
    .heat-cell.FAIL {{ background: #ef4444; }}
    .minute-sections {{ display: grid; gap: 18px; }}
    .minute-section-title {{ font-size: 13px; font-weight: 700; color: #dbe6f2; margin-bottom: 8px; }}
    .minute-grid {{ display: grid; grid-template-columns: 220px repeat(12, minmax(36px, 1fr)); gap: 6px; align-items: center; overflow-x: auto; }}
    .note-box {{ border: 1px dashed #31506a; border-radius: 12px; padding: 14px; color: #b5c6d5; background: rgba(19, 35, 49, .48); }}
    @media (max-width: 980px) {{
      .hero, .controls, .details, .kpis, .task-summary-grid {{ grid-template-columns: 1fr; }}
      .page {{ padding: 16px 12px 32px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="panel hero-main">
        <h1 class="hero-title">Airflow DAG 모니터링</h1>
        <p class="hero-copy">표 헤더를 눌러 정렬하고, DAG를 펼치면 성공/실패/스킵을 포함한 전체 task 상태를 한 번에 볼 수 있습니다.</p>
        <div class="hero-meta">
          <span class="meta-pill" id="meta-date">기준일: -</span>
          <span class="meta-pill" id="meta-generated">갱신: -</span>
          <span class="meta-pill">자동 새로고침: {refresh_seconds}초</span>
        </div>
      </div>
      <div class="panel hero-side">
        <div>
          <strong style="display:block;margin-bottom:6px;">핵심 기능</strong>
          <div class="muted">FAIL 우선 조회, 컬럼 클릭 정렬, 전체 task 상태 확인, 오류/로그 복사.</div>
        </div>
        <div>
          <strong style="display:block;margin-bottom:6px;">시간 기준</strong>
          <div class="muted">최근 실행 시각은 KST 기준으로 표시합니다. 실행시간은 DAG 수행 시간입니다.</div>
        </div>
      </div>
    </section>

    <section class="kpis" id="kpis"></section>

    <section class="controls">
      <select id="status-filter" class="select">
        <option value="ALL">전체 상태</option>
        <option value="FAIL">FAIL</option>
        <option value="WARN">WARN</option>
        <option value="OK">OK</option>
      </select>
      <select id="group-filter" class="select">
        <option value="ALL">전체 그룹</option>
      </select>
      <input id="search-input" class="input" type="search" placeholder="DAG명, 상세 사유, 실패 task 검색">
    </section>

    <section class="panel table-panel">
      <table>
        <colgroup>
          <col style="width: 29%;">
          <col style="width: 8%;">
          <col style="width: 16%;">
          <col style="width: 8%;">
          <col style="width: 16%;">
          <col style="width: 15%;">
          <col style="width: 8%;">
        </colgroup>
        <thead>
          <tr>
            <th><button class="sort-button" data-sort-key="dag_id"><span>DAG</span><span class="sort-indicator" data-indicator="dag_id"></span></button></th>
            <th><button class="sort-button" data-sort-key="status"><span>상태</span><span class="sort-indicator" data-indicator="status"></span></button></th>
            <th><button class="sort-button" data-sort-key="last_run_at_kst"><span>최근 실행(KST)</span><span class="sort-indicator" data-indicator="last_run_at_kst"></span></button></th>
            <th><button class="sort-button" data-sort-key="duration_sec"><span>실행시간</span><span class="sort-indicator" data-indicator="duration_sec"></span></button></th>
            <th><button class="sort-button" data-sort-key="failed_count"><span>실패 Task</span><span class="sort-indicator" data-indicator="failed_count"></span></button></th>
            <th><button class="sort-button" data-sort-key="detail"><span>상세</span><span class="sort-indicator" data-indicator="detail"></span></button></th>
            <th>동작</th>
          </tr>
        </thead>
        <tbody id="dag-body"></tbody>
      </table>
      <div id="empty-state" class="empty" hidden>표시할 모니터링 데이터가 없습니다.</div>
    </section>

    <section class="panel heatmap">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px;">
        <div>
          <strong>시간대 히트맵</strong>
          <div class="muted" style="font-size:13px;">현재 필터와 정렬이 적용된 순서로 KST 시간대와 5분 단위 실행 위치를 전환해서 볼 수 있습니다.</div>
        </div>
        <div class="heatmap-toolbar">
          <button class="heatmap-toggle active" data-heatmap-view="hourly">시간대 히트맵</button>
          <button class="heatmap-toggle" data-heatmap-view="minute">5분별 히트맵</button>
        </div>
      </div>
      <div id="heatmap-grid" class="heatmap-grid"></div>
    </section>
  </div>

  <script>
    const initialSnapshot = {embedded};
    let snapshot = initialSnapshot;
    let expandedDag = null;
    let cachedDetails = new Map();
    let selectedTaskId = null;
    let sortState = {{ key: "status", direction: "asc" }};
    let heatmapView = "hourly";

    const statusRank = {{ FAIL: 0, WARN: 1, OK: 2 }};
    const taskStateRank = {{ failed: 0, upstream_failed: 1, running: 2, queued: 3, up_for_retry: 4, success: 5, skipped: 6 }};

    const kpisEl = document.getElementById("kpis");
    const dagBodyEl = document.getElementById("dag-body");
    const emptyEl = document.getElementById("empty-state");
    const heatmapEl = document.getElementById("heatmap-grid");
    const statusFilterEl = document.getElementById("status-filter");
    const groupFilterEl = document.getElementById("group-filter");
    const searchInputEl = document.getElementById("search-input");

    function escapeHtml(value) {{
      return String(value ?? "").replace(/[&<>"]/g, (ch) => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[ch]));
    }}

    function formatDuration(value) {{
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
      const total = Math.round(Number(value));
      const hours = Math.floor(total / 3600);
      const minutes = Math.floor((total % 3600) / 60);
      const seconds = total % 60;
      return `${{String(hours).padStart(2, "0")}}:${{String(minutes).padStart(2, "0")}}:${{String(seconds).padStart(2, "0")}}`;
    }}

    function formatKstDate(date) {{
      const parts = new Intl.DateTimeFormat("sv-SE", {{
        timeZone: "Asia/Seoul",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }}).formatToParts(date);
      const values = Object.fromEntries(parts.filter((part) => part.type !== "literal").map((part) => [part.type, part.value]));
      return `${{values.year}}-${{values.month}}-${{values.day}} ${{values.hour}}:${{values.minute}}:${{values.second}}`;
    }}

    function normalizeTimestampNumber(value) {{
      const numeric = Number(value);
      if (Number.isNaN(numeric)) return null;
      return numeric > 1000000000000 ? numeric : numeric * 1000;
    }}

    function formatKstDisplay(value) {{
      if (value === null || value === undefined || value === "") return "-";
      if (typeof value === "number") {{
        const millis = normalizeTimestampNumber(value);
        return millis === null ? "-" : formatKstDate(new Date(millis));
      }}
      const text = String(value);
      if (/^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}$/.test(text)) return text;
      if (/^\\d+(\\.\\d+)?$/.test(text)) {{
        const millis = normalizeTimestampNumber(text);
        if (millis !== null) return formatKstDate(new Date(millis));
      }}
      const parsed = new Date(text);
      if (!Number.isNaN(parsed.getTime())) {{
        return formatKstDate(parsed);
      }}
      return text;
    }}

    function toSortableTimestamp(value) {{
      if (value === null || value === undefined || value === "") return 0;
      if (typeof value === "number") return value;
      const text = String(value);
      if (/^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}$/.test(text)) {{
        return Date.parse(text.replace(" ", "T") + "+09:00") || 0;
      }}
      return Date.parse(text) || 0;
    }}

    function statusClass(status) {{
      return ["status", status || "OK"].join(" ");
    }}

    function taskStatusClass(state) {{
      return ["status", "task-status", String(state || "unknown").toLowerCase()].join(" ");
    }}

    function getFailedCount(dag) {{
      return (dag.failed_task_ids || []).length + (dag.upstream_failed_task_ids || []).length;
    }}

    function dagMatchesFilters(dag) {{
      const statusFilter = statusFilterEl.value;
      const groupFilter = groupFilterEl.value;
      const q = searchInputEl.value.trim().toLowerCase();
      if (statusFilter !== "ALL" && dag.status !== statusFilter) return false;
      if (groupFilter !== "ALL" && dag.group !== groupFilter) return false;
      if (!q) return true;
      const haystack = [dag.dag_id, dag.detail, ...(dag.failed_task_ids || []), ...(dag.upstream_failed_task_ids || [])].join(" ").toLowerCase();
      return haystack.includes(q);
    }}

    function compareDags(a, b) {{
      const direction = sortState.direction === "asc" ? 1 : -1;
      let result = 0;
      switch (sortState.key) {{
        case "status":
          result = (statusRank[a.status] ?? 9) - (statusRank[b.status] ?? 9);
          break;
        case "last_run_at_kst":
          result = toSortableTimestamp(a.last_run_at_kst) - toSortableTimestamp(b.last_run_at_kst);
          break;
        case "duration_sec":
          result = (a.duration_sec ?? -1) - (b.duration_sec ?? -1);
          break;
        case "failed_count":
          result = getFailedCount(a) - getFailedCount(b);
          break;
        case "detail":
          result = String(a.detail || "").localeCompare(String(b.detail || ""), "ko");
          break;
        case "dag_id":
        default:
          result = String(a.dag_id || "").localeCompare(String(b.dag_id || ""), "ko");
          break;
      }}
      if (result === 0) {{
        result = String(a.dag_id || "").localeCompare(String(b.dag_id || ""), "ko");
      }}
      return result * direction;
    }}

    function getVisibleDags() {{
      return (snapshot.dags || []).filter(dagMatchesFilters).sort(compareDags);
    }}

    function updateSortIndicators() {{
      document.querySelectorAll("[data-indicator]").forEach((el) => {{
        const key = el.dataset.indicator;
        if (key !== sortState.key) {{
          el.textContent = "";
          return;
        }}
        el.textContent = sortState.direction === "asc" ? "▲" : "▼";
      }});
    }}

    function buildKpis() {{
      const summary = snapshot.summary || {{}};
      const cards = [
        ["실패", summary.fail_count || 0, "fail"],
        ["경고", summary.warn_count || 0, "warn"],
        ["정상", summary.ok_count || 0, "ok"],
        ["전체", summary.total_count || 0, "total"],
      ];
      kpisEl.innerHTML = cards.map(([label, value, cls]) => `<div class="kpi ${{cls}}"><label>${{label}}</label><strong>${{value}}</strong></div>`).join("");
      document.getElementById("meta-date").textContent = `기준일: ${{snapshot.target_date || "-"}}`;
      document.getElementById("meta-generated").textContent = `갱신: ${{formatKstDisplay(snapshot.generated_at)}}`;
    }}

    function buildGroupOptions() {{
      const current = groupFilterEl.value;
      const groups = Array.from(new Set((snapshot.dags || []).map((dag) => dag.group).filter(Boolean))).sort();
      groupFilterEl.innerHTML = '<option value="ALL">전체 그룹</option>' + groups.map((group) => `<option value="${{group}}">${{group.toUpperCase()}}</option>`).join("");
      if (groups.includes(current)) {{
        groupFilterEl.value = current;
      }}
    }}

    function renderHeatmap() {{
      const dags = getVisibleDags();
      if (!dags.length) {{
        heatmapEl.innerHTML = '<div class="muted">현재 필터에 맞는 DAG가 없습니다.</div>';
        return;
      }}
      if (heatmapView === "minute") {{
        renderMinuteHeatmap(dags);
        return;
      }}
      const hourHeaders = Array.from({{ length: 25 }}, (_, idx) => idx === 0 ? "<div></div>" : `<div class="heat-head">${{String(idx - 1).padStart(2, "0")}}</div>`);
      const rows = [];
      dags.forEach((dag) => {{
        rows.push(`<div class="heat-name" title="${{escapeHtml(dag.dag_id)}}">${{escapeHtml(dag.dag_id)}}</div>`);
        for (let hour = 0; hour < 24; hour += 1) {{
          const cls = dag.start_hour_kst === hour ? dag.status : "";
          rows.push(`<div class="heat-cell ${{cls}}" title="${{cls || "EMPTY"}}"></div>`);
        }}
      }});
      heatmapEl.innerHTML = hourHeaders.join("") + rows.join("");
    }}

    function renderMinuteHeatmap(dags) {{
      const minuteSlots = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55];
      const activeHours = Array.from(new Set(dags.map((dag) => Number(dag.start_hour_kst)).filter((hour) => hour >= 0))).sort((a, b) => a - b);
      if (!activeHours.length) {{
        heatmapEl.innerHTML = '<div class="muted">5분 단위로 표시할 실행 시각 정보가 없습니다.</div>';
        return;
      }}
      const sections = activeHours.map((hour) => {{
        const rows = [];
        const header = ['<div></div>'].concat(minuteSlots.map((minute) => `<div class="heat-head">${{String(hour).padStart(2, "0")}}:${{String(minute).padStart(2, "0")}}</div>`));
        dags.filter((dag) => Number(dag.start_hour_kst) === hour).forEach((dag) => {{
          rows.push(`<div class="heat-name" title="${{escapeHtml(dag.dag_id)}}">${{escapeHtml(dag.dag_id)}}</div>`);
          minuteSlots.forEach((minute) => {{
            const cls = Number(dag.start_min_kst) === minute ? dag.status : "";
            rows.push(`<div class="heat-cell ${{cls}}" title="${{cls || "EMPTY"}}"></div>`);
          }});
        }});
        return `
          <div>
            <div class="minute-section-title">${{String(hour).padStart(2, "0")}}시 실행</div>
            <div class="minute-grid">${{header.join("") + rows.join("")}}</div>
          </div>
        `;
      }});
      heatmapEl.innerHTML = `<div class="minute-sections">${{sections.join("")}}</div>`;
    }}

    function updateHeatmapToggles() {{
      document.querySelectorAll("[data-heatmap-view]").forEach((button) => {{
        button.classList.toggle("active", button.dataset.heatmapView === heatmapView);
      }});
    }}

    async function copyText(value, label) {{
      try {{
        await navigator.clipboard.writeText(value || "");
        window.alert(`${{label}} 복사 완료`);
      }} catch (error) {{
        window.alert(`복사 실패: ${{error}}`);
      }}
    }}

    function buildEmbeddedDetails(dag) {{
      const tasks = Array.isArray(dag.tasks) ? dag.tasks : [];
      return {{
        dag_id: dag.dag_id,
        run_id: dag.run_id,
        detail: dag.detail,
        grid_url: dag.grid_url,
        task_detail_available: tasks.length > 0,
        task_detail_message: dag.task_detail_message || "최신 스냅샷에 task 상세 정보가 없습니다.",
        tasks,
      }};
    }}

    async function fetchDetails(dag) {{
      const key = `${{dag.dag_id}}::${{dag.run_id}}`;
      if (cachedDetails.has(key)) return cachedDetails.get(key);
      const embedded = buildEmbeddedDetails(dag);
      if (window.location.protocol === "file:" && embedded.task_detail_available) {{
        cachedDetails.set(key, embedded);
        return embedded;
      }}
      const url = `/api/dag-monitoring/logs?dag_id=${{encodeURIComponent(dag.dag_id)}}&run_id=${{encodeURIComponent(dag.run_id || "")}}`;
      try {{
        const response = await fetch(url);
        if (!response.ok) throw new Error(`task 로그 조회 실패: ${{response.status}}`);
        const payload = await response.json();
        cachedDetails.set(key, payload);
        return payload;
      }} catch (error) {{
        if (embedded.task_detail_available || embedded.task_detail_message) {{
          cachedDetails.set(key, embedded);
          return embedded;
        }}
        throw error;
      }}
    }}

    function summarizeTasks(tasks) {{
      const summary = {{ total: tasks.length, success: 0, failed: 0, skipped: 0, running: 0 }};
      tasks.forEach((task) => {{
        const state = String(task.state || "").toLowerCase();
        if (state === "success") summary.success += 1;
        else if (state === "failed" || state === "upstream_failed") summary.failed += 1;
        else if (state === "skipped") summary.skipped += 1;
        else summary.running += 1;
      }});
      return summary;
    }}

    function renderTaskDetails(details) {{
      if (!details.task_detail_available || !(details.tasks || []).length) {{
        return `<div class="details"><div class="note-box">${{escapeHtml(details.task_detail_message || "task 상세 데이터가 없습니다.")}}</div></div>`;
      }}

      const tasks = details.tasks || [];
      const taskSummary = summarizeTasks(tasks);
      if (!selectedTaskId || !tasks.some((task) => task.task_id === selectedTaskId)) {{
        const firstTask = tasks.find((task) => ["failed", "upstream_failed"].includes(task.state)) || tasks[0];
        selectedTaskId = firstTask ? firstTask.task_id : null;
      }}
      const selected = tasks.find((task) => task.task_id === selectedTaskId) || tasks[0];
      const taskList = tasks.map((task) => `
        <div class="task-item clickable ${{task.task_id === selectedTaskId ? "active" : ""}}" data-task-id="${{escapeHtml(task.task_id)}}">
          <div class="task-head">
            <div>
              <div class="task-name">${{escapeHtml(task.task_id)}}</div>
              <div class="task-meta">
                <span class="${{taskStatusClass(task.state)}}">${{escapeHtml(String(task.state || "unknown").toUpperCase())}}</span>
                <span class="muted">시도 ${{task.try_number ?? "-"}}회</span>
                <span class="muted">실행시간 ${{formatDuration(task.duration_sec)}}</span>
              </div>
            </div>
            <button class="task-button select-task" data-task-id="${{escapeHtml(task.task_id)}}">로그 보기</button>
          </div>
          <div class="muted">${{escapeHtml(task.error_summary || "오류 요약 없음")}}</div>
          <div class="task-actions">
            <button class="copy-button copy-summary" data-copy="${{encodeURIComponent(task.copy_text || "")}}">오류 복사</button>
            ${{task.log_tail ? `<button class="task-button copy-log" data-copy="${{encodeURIComponent(task.log_tail)}}">로그 복사</button>` : ""}}
            ${{task.log_url ? `<a class="link-button" target="_blank" rel="noreferrer" href="${{escapeHtml(task.log_url)}}">Airflow 로그 열기</a>` : ""}}
          </div>
        </div>
      `).join("");

      const logText = selected?.log_tail || selected?.error_excerpt || "표시할 로그 내용이 없습니다.";
      return `
        <div class="details">
          <div class="task-overview">
            <div class="task-summary-grid">
              <div class="task-summary-card"><label>전체 Task</label><strong>${{taskSummary.total}}</strong></div>
              <div class="task-summary-card"><label>성공</label><strong>${{taskSummary.success}}</strong></div>
              <div class="task-summary-card"><label>실패</label><strong>${{taskSummary.failed}}</strong></div>
              <div class="task-summary-card"><label>스킵</label><strong>${{taskSummary.skipped}}</strong></div>
              <div class="task-summary-card"><label>진행/대기</label><strong>${{taskSummary.running}}</strong></div>
            </div>
            <div class="task-board">${{taskList}}</div>
          </div>
          <div class="log-panel">
            <div class="log-panel-header">
              <div>
                <strong>${{escapeHtml(selected?.task_id || "-")}}</strong>
                <div class="muted" style="font-size:13px;">${{escapeHtml(selected?.error_summary || details.detail || "Task 로그 미리보기")}}</div>
              </div>
              <div style="display:flex;gap:8px;flex-wrap:wrap;">
                <button class="copy-button copy-summary" data-copy="${{encodeURIComponent(selected?.copy_text || "")}}">오류 복사</button>
                ${{selected?.log_tail ? `<button class="task-button copy-log" data-copy="${{encodeURIComponent(selected.log_tail)}}">로그 복사</button>` : ""}}
                <a class="link-button" target="_blank" rel="noreferrer" href="${{escapeHtml(details.grid_url || "#")}}">DAG 그리드 열기</a>
              </div>
            </div>
            <pre>${{escapeHtml(logText)}}</pre>
          </div>
        </div>
      `;
    }}

    function bindDetailActions(row, details) {{
      row.querySelectorAll(".task-item.clickable").forEach((card) => {{
        card.addEventListener("click", (event) => {{
          if (event.target.closest("button, a")) return;
          selectedTaskId = card.dataset.taskId;
          row.querySelector(".details-cell").innerHTML = renderTaskDetails(details);
          bindDetailActions(row, details);
        }});
      }});
      row.querySelectorAll(".select-task").forEach((button) => {{
        button.addEventListener("click", () => {{
          selectedTaskId = button.dataset.taskId;
          row.querySelector(".details-cell").innerHTML = renderTaskDetails(details);
          bindDetailActions(row, details);
        }});
      }});
      row.querySelectorAll(".copy-summary").forEach((button) => {{
        button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copy || ""), "오류 요약"));
      }});
      row.querySelectorAll(".copy-log").forEach((button) => {{
        button.addEventListener("click", () => copyText(decodeURIComponent(button.dataset.copy || ""), "로그"));
      }});
    }}

    async function toggleDetails(dag) {{
      const key = `${{dag.dag_id}}::${{dag.run_id}}`;
      expandedDag = expandedDag === key ? null : key;
      selectedTaskId = null;
      renderTable();
      if (!expandedDag) return;

      const detailsRow = document.querySelector(`tr.details-row[data-key="${{CSS.escape(key)}}"] .details-cell`);
      if (!detailsRow) return;
      detailsRow.innerHTML = '<div class="details"><div class="muted">task 로그를 불러오는 중입니다...</div></div>';
      try {{
        const details = await fetchDetails(dag);
        const row = document.querySelector(`tr.details-row[data-key="${{CSS.escape(key)}}"]`);
        if (!row) return;
        row.querySelector(".details-cell").innerHTML = renderTaskDetails(details);
        bindDetailActions(row, details);
      }} catch (error) {{
        detailsRow.innerHTML = `<div class="details"><div class="note-box">${{escapeHtml(error.message)}}</div></div>`;
      }}
    }}

    function renderTable() {{
      const dags = getVisibleDags();
      emptyEl.hidden = dags.length > 0;
      dagBodyEl.innerHTML = dags.map((dag) => {{
        const key = `${{dag.dag_id}}::${{dag.run_id}}`;
        const active = expandedDag === key;
        const failedList = [...(dag.failed_task_ids || []), ...(dag.upstream_failed_task_ids || [])].join(", ");
        return `
          <tr class="dag-row status-${{String(dag.status || '').toLowerCase()}} ${{active ? "active" : ""}}" data-key="${{escapeHtml(key)}}" data-dag-id="${{escapeHtml(dag.dag_id)}}" data-run-id="${{escapeHtml(dag.run_id || "")}}">
            <td><div class="dag-name"><span class="dag-title">${{escapeHtml(dag.dag_id)}}</span><span class="group-chip">${{escapeHtml((dag.group || "other").toUpperCase())}}</span></div></td>
            <td class="cell-status"><span class="${{statusClass(dag.status)}}">${{escapeHtml(dag.status)}}</span></td>
            <td class="cell-time">${{escapeHtml(formatKstDisplay(dag.last_run_at_kst))}}</td>
            <td class="cell-duration">${{escapeHtml(formatDuration(dag.duration_sec))}}</td>
            <td class="cell-failed">${{escapeHtml(failedList || "-")}}</td>
            <td class="cell-detail">${{escapeHtml(dag.detail || "-")}}</td>
            <td class="cell-actions"><div class="action-wrap"><button class="copy-button row-copy" data-copy="${{encodeURIComponent(dag.copy_text || "")}}">오류 복사</button><a class="link-button" target="_blank" rel="noreferrer" href="${{escapeHtml(dag.grid_url || "#")}}">Airflow 열기</a></div></td>
          </tr>
          ${{active ? `<tr class="details-row" data-key="${{escapeHtml(key)}}"><td colspan="7" class="details-cell"></td></tr>` : ""}}
        `;
      }}).join("");

      dagBodyEl.querySelectorAll(".row-copy").forEach((button) => {{
        button.addEventListener("click", (event) => {{
          event.stopPropagation();
          copyText(decodeURIComponent(button.dataset.copy || ""), "오류 요약");
        }});
      }});
      dagBodyEl.querySelectorAll(".dag-row").forEach((row) => {{
        row.addEventListener("click", (event) => {{
          if (event.target.closest("button, a")) return;
          const dagId = row.dataset.dagId;
          const runId = row.dataset.runId;
          const dag = (snapshot.dags || []).find((item) => item.dag_id === dagId && String(item.run_id || "") === String(runId || ""));
          if (dag) toggleDetails(dag);
        }});
      }});
    }}

    async function refreshSnapshot() {{
      if (window.location.protocol === "file:") {{
        window.location.reload();
        return;
      }}
      try {{
        const response = await fetch("/api/dag-monitoring/snapshot");
        if (!response.ok) return;
        snapshot = await response.json();
        buildGroupOptions();
        buildKpis();
        updateSortIndicators();
        renderTable();
        renderHeatmap();
      }} catch (error) {{
        console.error(error);
      }}
    }}

    document.querySelectorAll(".sort-button").forEach((button) => {{
      button.addEventListener("click", () => {{
        const key = button.dataset.sortKey;
        if (sortState.key === key) {{
          sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
        }} else {{
          sortState.key = key;
          sortState.direction = "asc";
        }}
        updateSortIndicators();
        renderTable();
        renderHeatmap();
      }});
    }});

    document.querySelectorAll("[data-heatmap-view]").forEach((button) => {{
      button.addEventListener("click", () => {{
        heatmapView = button.dataset.heatmapView || "hourly";
        updateHeatmapToggles();
        renderHeatmap();
      }});
    }});

    statusFilterEl.addEventListener("change", () => {{
      expandedDag = null;
      renderTable();
      renderHeatmap();
    }});
    groupFilterEl.addEventListener("change", () => {{
      expandedDag = null;
      renderTable();
      renderHeatmap();
    }});
    searchInputEl.addEventListener("input", () => {{
      expandedDag = null;
      renderTable();
      renderHeatmap();
    }});

    buildGroupOptions();
    buildKpis();
    updateSortIndicators();
    updateHeatmapToggles();
    renderTable();
    renderHeatmap();
    window.setInterval(refreshSnapshot, {refresh_seconds * 1000});
  </script>
</body>
</html>"""
