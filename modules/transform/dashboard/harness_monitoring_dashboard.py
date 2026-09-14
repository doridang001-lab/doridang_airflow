"""Dashboard helpers for Codex harness phase monitoring."""

from __future__ import annotations

import json
import os
import platform
import subprocess
from datetime import UTC, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any

from modules.transform.utility.paths import DASHBOARD_DB

HARNESS_MONITORING_SNAPSHOT_FILENAME = "harness_monitoring_snapshot.json"
HARNESS_MONITORING_HTML_FILENAME = "harness_monitoring_dashboard.html"
STALE_PENDING_HOURS = 24
PROBLEM_DIAGNOSTIC_STATUSES = {"ERROR"}
WARN_DIAGNOSTIC_STATUSES = {"WARN", "UNKNOWN"}
MARKDOWN_EMBED_LIMIT = 20000
SETUP_CHECKS = ("phases_index", "phase_steps", "harness_docs", "executor")
DASHBOARD_CHECKS = ("airflow_dag", "dashboard_outputs")


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"_read_error": str(exc)}
    return payload if isinstance(payload, dict) else {"_read_error": "JSON root must be an object"}


def _read_markdown_payload(path: Path) -> dict[str, Any]:
    payload: dict[str, Any] = {"path": str(path), "exists": path.exists(), "content": ""}
    if not path.exists():
        return payload
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        payload["read_error"] = str(exc)
        return payload
    payload["content"] = text[:MARKDOWN_EMBED_LIMIT]
    payload["truncated"] = len(text) > MARKDOWN_EMBED_LIMIT
    return payload


def _diag(status: str, label: str, message: str, **extra: Any) -> dict[str, Any]:
    payload = {"status": status, "label": label, "message": message}
    payload.update(extra)
    return payload


def _iso_mtime(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def _file_diagnostic(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        return _diag("ERROR", label, f"{path} 파일이 없습니다.", path=str(path), exists=False)
    try:
        stat = path.stat()
    except OSError as exc:
        return _diag("ERROR", label, f"{path} 상태 확인 실패: {exc}", path=str(path), exists=False)
    status = "OK" if stat.st_size > 0 else "ERROR"
    message = "파일 생성 확인" if stat.st_size > 0 else "파일 크기가 0입니다."
    return _diag(
        status,
        label,
        message,
        path=str(path),
        exists=True,
        size=stat.st_size,
        modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
    )


def _query_windows_scheduler() -> dict[str, Any]:
    if platform.system().lower() != "windows":
        return _diag("UNAVAILABLE", "작업스케줄러", "Windows 작업스케줄러는 이 실행 환경에서 조회할 수 없습니다.")
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "Get-ScheduledTask | "
                    "Where-Object { $_.TaskName -match 'harness|codex|airflow|dashboard' -or $_.TaskPath -match 'harness|codex|airflow|dashboard' } | "
                    "Select-Object TaskName,TaskPath,State,Description | ConvertTo-Json -Depth 3"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return _diag("UNKNOWN", "작업스케줄러", f"작업스케줄러 조회 실패: {exc}")
    if result.returncode != 0:
        return _diag("UNKNOWN", "작업스케줄러", result.stderr.strip() or "작업스케줄러 조회 실패")

    text = result.stdout.strip()
    if not text:
        return _diag("WARN", "작업스케줄러", "하네스 관련 작업스케줄러가 없습니다.", tasks=[])
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return _diag("UNKNOWN", "작업스케줄러", f"작업스케줄러 결과 파싱 실패: {exc}")
    tasks = payload if isinstance(payload, list) else [payload]
    tasks = [task for task in tasks if isinstance(task, dict)]
    if not tasks:
        return _diag("WARN", "작업스케줄러", "하네스 관련 작업스케줄러가 없습니다.", tasks=[])
    states = ", ".join(f"{task.get('TaskName')}={task.get('State')}" for task in tasks)
    return _diag("OK", "작업스케줄러", f"관련 작업 {len(tasks)}개 확인: {states}", tasks=tasks)


def _read_output_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = _read_json(path)
    if payload.get("_read_error"):
        return {"path": str(path), "read_error": payload["_read_error"]}
    stdout = str(payload.get("stdout") or "")
    stderr = str(payload.get("stderr") or "")
    return {
        "path": str(path),
        "exitCode": payload.get("exitCode"),
        "stdout_tail": stdout[-1200:],
        "stderr_tail": stderr[-1200:],
    }


def _read_step_harness_docs(root: Path, docs: Any) -> list[dict[str, Any]]:
    if isinstance(docs, str):
        docs = [docs]
    if not isinstance(docs, list):
        return []
    payloads: list[dict[str, Any]] = []
    harness_root = root / "harness"
    for raw_name in docs:
        name = str(raw_name).strip()
        if not name or "/" in name or "\\" in name or name.startswith("."):
            continue
        filename = name if name.endswith(".md") else f"{name}.md"
        payloads.append(_read_markdown_payload(harness_root / filename))
    return payloads


def _collect_harness_doc_issues(root: Path, phases: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    missing_required: list[str] = []
    harness_root = root / "harness"
    for name in ("common.md", "review_checklist.md"):
        if not (harness_root / name).exists():
            missing_required.append(str(harness_root / name))

    missing_step_docs: list[str] = []
    for phase in phases:
        for step in phase.get("steps") or []:
            docs = step.get("harness_docs") or []
            if isinstance(docs, str):
                docs = [docs]
            if not isinstance(docs, list):
                missing_step_docs.append(f"{phase.get('dir')} step{step.get('step')}: harness_docs 형식 오류")
                continue
            for raw_name in docs:
                name = str(raw_name).strip()
                if not name:
                    continue
                if "/" in name or "\\" in name or name.startswith("."):
                    missing_step_docs.append(f"{phase.get('dir')} step{step.get('step')}: invalid {name}")
                    continue
                filename = name if name.endswith(".md") else f"{name}.md"
                doc = harness_root / filename
                if not doc.exists():
                    missing_step_docs.append(str(doc))
    return missing_required, missing_step_docs


def _collect_phase_step_issues(root: Path, phases: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    for phase in phases:
        if phase.get("read_error"):
            issues.append(f"{phase.get('dir')}: {phase.get('read_error')}")
            continue
        phase_dir = root / "phases" / str(phase.get("dir"))
        if not phase.get("steps"):
            issues.append(f"{phase.get('dir')}: step 정보 없음")
        for step in phase.get("steps") or []:
            step_num = step.get("step")
            if step_num is None:
                issues.append(f"{phase.get('dir')}: step 번호 없음")
                continue
            step_file = phase_dir / f"step{step_num}.md"
            if not step_file.exists():
                issues.append(f"{phase.get('dir')}: {step_file.name} 없음")
    return issues


def _build_diagnostics(
    *,
    root: Path,
    phases_root: Path,
    top_index: dict[str, Any],
    phase_dirs: list[str],
    phases: list[dict[str, Any]],
    dashboard_root: Path,
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    top_error = top_index.get("_read_error")
    if not phases_root.exists():
        diagnostics["phases_index"] = _diag(
            "ERROR",
            "phases/index.json",
            f"{phases_root} 디렉터리가 없습니다. docker-compose 마운트를 확인하세요.",
            path=str(phases_root / "index.json"),
        )
    elif top_error:
        diagnostics["phases_index"] = _diag(
            "ERROR",
            "phases/index.json",
            f"phases/index.json 읽기 실패: {top_error}",
            path=str(phases_root / "index.json"),
        )
    elif not phase_dirs:
        diagnostics["phases_index"] = _diag(
            "OK",
            "phases/index.json",
            "진행 중인 phase가 없습니다. phases/index.json은 정상입니다.",
            path=str(phases_root / "index.json"),
            phase_count=0,
        )
    else:
        diagnostics["phases_index"] = _diag(
            "OK",
            "phases/index.json",
            f"phase {len(phase_dirs)}개 확인",
            path=str(phases_root / "index.json"),
            phase_count=len(phase_dirs),
        )

    phase_issues = _collect_phase_step_issues(root, phases)
    diagnostics["phase_steps"] = _diag(
        "ERROR" if phase_issues else "OK",
        "phase/step 파일",
        "phase/step 설정 문제 확인" if phase_issues else "phase/step 파일 확인",
        issues=phase_issues,
    )

    missing_required, missing_step_docs = _collect_harness_doc_issues(root, phases)
    harness_status = "ERROR" if missing_required or missing_step_docs else "OK"
    diagnostics["harness_docs"] = _diag(
        harness_status,
        "harness 문서",
        "하네스 문서 누락 확인" if harness_status == "ERROR" else "하네스 문서 확인",
        missing_required=missing_required,
        missing_step_docs=missing_step_docs,
    )

    executor = root / "scripts" / "execute.py"
    diagnostics["executor"] = _diag(
        "OK" if executor.exists() else "ERROR",
        "scripts/execute.py",
        "하네스 실행기 확인" if executor.exists() else f"{executor} 파일이 없습니다.",
        path=str(executor),
    )

    dag_file = root / "dags" / "strategy" / "Strategy_HarnessMonitoring_01_Dashboard_Dags.py"
    diagnostics["airflow_dag"] = _diag(
        "OK" if dag_file.exists() else "ERROR",
        "Airflow DAG",
        "DAG 파일 확인, schedule=0 13 * * *" if dag_file.exists() else f"{dag_file} 파일이 없습니다.",
        dag_id="Strategy_HarnessMonitoring_01_Dashboard_Dags",
        schedule="0 13 * * *",
        path=str(dag_file),
    )

    diagnostics["dashboard_outputs"] = {
        "status": "OK",
        "label": "대시보드 산출물",
        "message": "snapshot/html 파일 상태",
        "snapshot": _file_diagnostic(dashboard_root / HARNESS_MONITORING_SNAPSHOT_FILENAME, "snapshot"),
        "html": _file_diagnostic(dashboard_root / HARNESS_MONITORING_HTML_FILENAME, "html"),
        "root": str(dashboard_root),
    }
    if any(
        diagnostics["dashboard_outputs"][key]["status"] == "ERROR"
        for key in ("snapshot", "html")
    ):
        diagnostics["dashboard_outputs"]["status"] = "ERROR"

    diagnostics["scheduler"] = _query_windows_scheduler()
    return diagnostics


def _score_status(status: str, *, unavailable_points: int = 10) -> int:
    normalized = status.upper()
    if normalized == "OK":
        return 10
    if normalized == "UNAVAILABLE":
        return unavailable_points
    if normalized in {"WARN", "UNKNOWN"}:
        return 5
    return 0


def _grade_for_score(score: int) -> str:
    if score >= 90:
        return "정상"
    if score >= 75:
        return "보완 필요"
    if score >= 50:
        return "주의"
    return "조치 필요"


def _add_action(actions: list[dict[str, Any]], priority: int, title: str, guide: str, command: str | None = None):
    action = {"priority": priority, "title": title, "guide": guide}
    if command:
        action["command"] = command
    actions.append(action)


def _build_score_guide(
    *,
    summary: dict[str, Any],
    diagnostics: dict[str, Any],
    phases: list[dict[str, Any]],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    why: list[str] = []
    actions: list[dict[str, Any]] = []

    setup_score = 0
    for key in SETUP_CHECKS:
        item = diagnostics.get(key) or {}
        status = str(item.get("status") or "UNKNOWN")
        points = _score_status(status)
        setup_score += points
        reason = str(item.get("message") or "-")
        remediation = "정상입니다."
        if status == "ERROR":
            remediation = "해당 파일/마운트/설정을 복구한 뒤 하네스 모니터링 DAG를 다시 실행하세요."
            why.append(f"{item.get('label') or key}: {reason}")
        elif status in WARN_DIAGNOSTIC_STATUSES:
            remediation = "경고 원인을 확인하고 필요하면 문서나 설정을 보완하세요."
            why.append(f"{item.get('label') or key}: {reason}")
        checks.append(
            {
                "key": key,
                "label": item.get("label") or key,
                "status": status,
                "score": points,
                "max_score": 10,
                "reason": reason,
                "remediation": remediation,
            }
        )

    dashboard_score = 0
    for key in DASHBOARD_CHECKS:
        item = diagnostics.get(key) or {}
        status = str(item.get("status") or "UNKNOWN")
        points = _score_status(status)
        dashboard_score += points
        reason = str(item.get("message") or "-")
        remediation = "정상입니다."
        if status == "ERROR":
            remediation = "Airflow DAG 등록 상태 또는 dashboard snapshot/html 산출물을 확인하세요."
            why.append(f"{item.get('label') or key}: {reason}")
        elif status in WARN_DIAGNOSTIC_STATUSES:
            remediation = "최근 실행 상태와 산출물 갱신 시각을 확인하세요."
            why.append(f"{item.get('label') or key}: {reason}")
        checks.append(
            {
                "key": key,
                "label": item.get("label") or key,
                "status": status,
                "score": points,
                "max_score": 10,
                "reason": reason,
                "remediation": remediation,
            }
        )

    scheduler = diagnostics.get("scheduler") or {}
    scheduler_status = str(scheduler.get("status") or "UNKNOWN")
    scheduler_points = _score_status(scheduler_status, unavailable_points=10)
    visibility_score = scheduler_points
    checks.append(
        {
            "key": "scheduler",
            "label": scheduler.get("label") or "작업스케줄러",
            "status": scheduler_status,
            "score": scheduler_points,
            "max_score": 10,
            "reason": scheduler.get("message") or "-",
            "remediation": (
                "컨테이너에서는 조회 불가가 정상일 수 있습니다. Windows 호스트 작업스케줄러는 필요할 때 별도로 확인하세요."
                if scheduler_status == "UNAVAILABLE"
                else "정상입니다." if scheduler_status == "OK" else "Windows 작업스케줄러 등록/상태를 확인하세요."
            ),
        }
    )

    total_steps = sum(len(phase.get("steps") or []) for phase in phases)
    completed_steps = sum(
        1
        for phase in phases
        for step in (phase.get("steps") or [])
        if str(step.get("status") or "").lower() == "completed"
    )
    execution_score = 35 if total_steps == 0 else round((completed_steps / total_steps) * 35)
    error_count = int(summary.get("error") or 0)
    blocked_count = int(summary.get("blocked") or 0)
    stale_count = int(summary.get("stale_pending") or 0)
    pending_count = int(summary.get("pending") or 0)
    if total_steps and pending_count and not (error_count or blocked_count or stale_count):
        execution_score = max(execution_score, 15)
    execution_score = max(0, execution_score - (error_count * 15) - (blocked_count * 12) - (stale_count * 8))
    checks.append(
        {
            "key": "phase_execution",
            "label": "phase 실행",
            "status": "OK" if total_steps and completed_steps == total_steps else "PENDING" if pending_count else "ERROR" if error_count else "WARN",
            "score": execution_score,
            "max_score": 35,
            "reason": f"{completed_steps}/{total_steps} step completed",
            "remediation": "pending step은 MD 보기로 지시서를 확인하고 Acceptance Criteria를 실행하세요.",
        }
    )

    for key in ("phases_index", "phase_steps", "harness_docs", "executor", "airflow_dag", "dashboard_outputs"):
        item = diagnostics.get(key) or {}
        if item.get("status") == "ERROR":
            _add_action(
                actions,
                10,
                f"{item.get('label') or key} 오류 해결",
                str(item.get("message") or "진단 오류를 확인하세요."),
            )

    for phase in phases:
        for step in phase.get("steps") or []:
            status = str(step.get("status") or "pending").lower()
            phase_name = str(phase.get("phase") or phase.get("dir") or "")
            step_label = f"{phase_name} step{step.get('step')} {step.get('name') or ''}".strip()
            if status == "error":
                _add_action(actions, 20, f"{step_label} 오류 해결", step.get("error_message") or "step output과 error_message를 확인하세요.")
            elif status == "blocked":
                _add_action(actions, 30, f"{step_label} 차단 해소", step.get("blocked_reason") or "사용자 승인 또는 외부 조치가 필요합니다.")
            elif step.get("stale_pending"):
                _add_action(actions, 40, f"{step_label} 장기 pending 정리", "24시간 이상 pending입니다. 실행할지, blocked/error로 정리할지 결정하세요.")
            elif status == "pending":
                _add_action(
                    actions,
                    50,
                    f"{step_label} 실행",
                    "MD 보기로 step 지시서를 확인하고 Acceptance Criteria를 실행한 뒤 completed와 summary를 기록하세요.",
                    command=f"python scripts/execute.py {phase.get('dir')}",
                )

    if pending_count:
        why.append(f"완료되지 않은 phase가 {pending_count}개 있습니다.")
    if not why:
        why.append("감점 원인이 없습니다.")
    if not actions:
        _add_action(actions, 90, "추가 조치 없음", "모든 하네스 설정과 phase 실행 상태가 정상입니다. 다음 정기 점검을 기다리면 됩니다.")

    overall_score = max(0, min(100, setup_score + dashboard_score + visibility_score + execution_score))
    critical_error_keys = {"phases_index", "phase_steps", "harness_docs", "executor", "airflow_dag", "dashboard_outputs"}
    if any((diagnostics.get(key) or {}).get("status") == "ERROR" for key in critical_error_keys):
        overall_score = min(overall_score, 49)
    grade = _grade_for_score(overall_score)
    if error_count or blocked_count or stale_count:
        summary_text = "즉시 확인이 필요한 error/blocked/stale 상태가 있습니다."
    elif pending_count:
        summary_text = "하네스 설정은 대체로 정상입니다. 아직 완료되지 않은 pending step을 실행하면 점수가 올라갑니다."
    elif overall_score >= 90:
        summary_text = "하네스 설정과 실행 상태가 정상입니다."
    else:
        summary_text = "하네스 설정 일부에 보완할 항목이 있습니다."

    return {
        "overall_score": overall_score,
        "grade": grade,
        "summary_text": summary_text,
        "setup_score": setup_score,
        "setup_max": 40,
        "execution_score": execution_score,
        "execution_max": 35,
        "dashboard_score": dashboard_score,
        "dashboard_max": 20,
        "visibility_score": visibility_score,
        "visibility_max": 10,
        "why_this_score": why,
        "next_actions": sorted(actions, key=lambda item: item["priority"])[:6],
        "checks": checks,
    }


def _status_from_steps(steps: list[dict[str, Any]], read_error: str | None = None) -> str:
    if read_error:
        return "ERROR"
    statuses = [str(step.get("status") or "pending").lower() for step in steps]
    if any(status == "error" for status in statuses):
        return "ERROR"
    if any(status == "blocked" for status in statuses):
        return "BLOCKED"
    if statuses and all(status == "completed" for status in statuses):
        return "COMPLETED"
    return "PENDING"


def _build_phase_payload(root: Path, phase_dir: str, *, now: datetime) -> dict[str, Any]:
    phase_path = root / "phases" / phase_dir
    index = _read_json(phase_path / "index.json")
    read_error = index.get("_read_error")
    steps: list[dict[str, Any]] = []

    if not read_error:
        for raw_step in index.get("steps") or []:
            if not isinstance(raw_step, dict):
                continue
            step = dict(raw_step)
            started_at = _parse_ts(step.get("started_at"))
            stale_pending = (
                str(step.get("status") or "").lower() == "pending"
                and started_at is not None
                and now - started_at >= timedelta(hours=STALE_PENDING_HOURS)
            )
            step["stale_pending"] = stale_pending
            if step.get("step") is not None:
                step_file = phase_path / f"step{step['step']}.md"
                step["step_md"] = _read_markdown_payload(step_file)
                step["harness_doc_markdown"] = _read_step_harness_docs(root, step.get("harness_docs"))
                step["output"] = _read_output_summary(phase_path / f"step{step['step']}-output.json")
            steps.append(step)

    status = _status_from_steps(steps, read_error)
    if status == "PENDING" and any(step.get("stale_pending") for step in steps):
        status = "STALE_PENDING"

    return {
        "dir": phase_dir,
        "project": index.get("project", "airflow") if not read_error else "airflow",
        "phase": index.get("phase", phase_dir) if not read_error else phase_dir,
        "status": status,
        "read_error": read_error,
        "created_at": index.get("created_at") if not read_error else None,
        "completed_at": index.get("completed_at") if not read_error else None,
        "steps": steps,
    }


def build_harness_snapshot(
    *,
    root: Path | str = Path.cwd(),
    now: datetime | None = None,
    dashboard_root: Path | str = DASHBOARD_DB,
) -> dict[str, Any]:
    root_path = Path(root)
    dashboard_path = Path(dashboard_root)
    now = now or _utc_now()
    phases_root = root_path / "phases"
    top_index = _read_json(phases_root / "index.json") if phases_root.exists() else {"_read_error": "phases directory not found", "phases": []}
    phase_dirs = [
        str(item.get("dir")).strip()
        for item in top_index.get("phases", [])
        if isinstance(item, dict) and str(item.get("dir") or "").strip()
    ]
    if not phase_dirs and phases_root.exists():
        phase_dirs = sorted(path.name for path in phases_root.iterdir() if path.is_dir())

    phases = [_build_phase_payload(root_path, phase_dir, now=now) for phase_dir in phase_dirs]
    diagnostics = _build_diagnostics(
        root=root_path,
        phases_root=phases_root,
        top_index=top_index,
        phase_dirs=phase_dirs,
        phases=phases,
        dashboard_root=dashboard_path,
    )
    problem_steps: list[dict[str, Any]] = []
    for phase in phases:
        for step in phase["steps"]:
            status = str(step.get("status") or "").lower()
            if status in {"error", "blocked"} or step.get("stale_pending"):
                problem_steps.append(
                    {
                        "phase": phase["phase"],
                        "phase_dir": phase["dir"],
                        "step": step.get("step"),
                        "name": step.get("name"),
                        "status": "stale_pending" if step.get("stale_pending") else status,
                        "message": step.get("error_message") or step.get("blocked_reason") or step.get("summary") or "",
                    }
                )

    summary = {
        "total_phases": len(phases),
        "completed": sum(1 for phase in phases if phase["status"] == "COMPLETED"),
        "pending": sum(1 for phase in phases if phase["status"] == "PENDING"),
        "error": sum(1 for phase in phases if phase["status"] == "ERROR"),
        "blocked": sum(1 for phase in phases if phase["status"] == "BLOCKED"),
        "stale_pending": sum(1 for phase in phases if phase["status"] == "STALE_PENDING"),
        "problem_steps": len(problem_steps),
    }
    score_guide = _build_score_guide(summary=summary, diagnostics=diagnostics, phases=phases)
    return {
        "generated_at": now.isoformat(),
        "root": str(root_path),
        "summary": summary,
        "diagnostics": diagnostics,
        "score_guide": score_guide,
        "phases": phases,
        "problem_steps": problem_steps,
    }


def should_alert(snapshot: dict[str, Any]) -> bool:
    summary = snapshot.get("summary") or {}
    if any(int(summary.get(key) or 0) > 0 for key in ("error", "blocked", "stale_pending")):
        return True
    diagnostics = snapshot.get("diagnostics") or {}
    for item in diagnostics.values():
        if isinstance(item, dict) and item.get("status") in PROBLEM_DIAGNOSTIC_STATUSES:
            return True
    return False


def build_alert_text(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get("summary") or {}
    lines = [
        "[Harness Monitoring]",
        f"generated_at={snapshot.get('generated_at')}",
        f"ERROR={summary.get('error', 0)} BLOCKED={summary.get('blocked', 0)} STALE={summary.get('stale_pending', 0)}",
    ]
    for key, item in (snapshot.get("diagnostics") or {}).items():
        if isinstance(item, dict) and item.get("status") in PROBLEM_DIAGNOSTIC_STATUSES:
            lines.append(f"- {item.get('label') or key}: {item.get('status')} {item.get('message')}")
    for item in (snapshot.get("problem_steps") or [])[:10]:
        lines.append(
            f"- {item.get('phase')} step{item.get('step')} {item.get('name')}: {item.get('status')} {item.get('message')}"
        )
    return "\n".join(lines)


def _safe_json_embed(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/")


def render_dashboard_html(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get("summary") or {}
    diagnostics = snapshot.get("diagnostics") or {}
    score_guide = snapshot.get("score_guide") or {}
    kpis = [
        ("전체", summary.get("total_phases", 0), "total"),
        ("완료", summary.get("completed", 0), "completed"),
        ("진행", summary.get("pending", 0), "pending"),
        ("차단", summary.get("blocked", 0), "blocked"),
        ("오류", summary.get("error", 0), "error"),
        ("지연", summary.get("stale_pending", 0), "stale"),
    ]
    kpi_html = "".join(
        f'<div class="kpi {cls}"><span>{escape(label)}</span><strong>{value}</strong></div>'
        for label, value, cls in kpis
    )
    overall_score = escape(str(score_guide.get("overall_score", "-")))
    grade = escape(str(score_guide.get("grade") or "판정 없음"))
    summary_text = escape(str(score_guide.get("summary_text") or "최신 DAG 실행으로 점수 정보를 갱신하세요."))
    next_actions = score_guide.get("next_actions") or []
    if next_actions:
        actions_html = "".join(
            "<li><strong>{title}</strong><p>{guide}</p>{command}</li>".format(
                title=escape(str(item.get("title") or "-")),
                guide=escape(str(item.get("guide") or "-")),
                command=(
                    f'<code>{escape(str(item.get("command")))}</code>'
                    if item.get("command")
                    else ""
                ),
            )
            for item in next_actions
            if isinstance(item, dict)
        )
    else:
        actions_html = "<li><strong>점수 정보 없음</strong><p>최신 DAG 실행으로 snapshot을 갱신하세요.</p></li>"
    why_html = "".join(f"<li>{escape(str(item))}</li>" for item in (score_guide.get("why_this_score") or [])[:5])
    score_cards = [
        ("설정", score_guide.get("setup_score", "-"), score_guide.get("setup_max", 40)),
        ("실행", score_guide.get("execution_score", "-"), score_guide.get("execution_max", 35)),
        ("대시보드", score_guide.get("dashboard_score", "-"), score_guide.get("dashboard_max", 20)),
        ("가시성", score_guide.get("visibility_score", "-"), score_guide.get("visibility_max", 10)),
    ]
    score_breakdown_html = "".join(
        f'<div class="score-mini"><span>{escape(label)}</span><strong>{escape(str(score))}/{escape(str(max_score))}</strong></div>'
        for label, score, max_score in score_cards
    )

    def _diagnostic_card(key: str, item: dict[str, Any]) -> str:
        status = str(item.get("status") or "UNKNOWN")
        detail_lines: list[str] = []
        for field in ("issues", "missing_required", "missing_step_docs"):
            values = item.get(field) or []
            if isinstance(values, list) and values:
                detail_lines.extend(str(value) for value in values[:4])
        if key == "dashboard_outputs":
            for subkey in ("snapshot", "html"):
                sub = item.get(subkey) or {}
                detail_lines.append(
                    f"{subkey}: {sub.get('status', '-')}, size={sub.get('size', '-')}, mtime={sub.get('modified_at', '-')}"
                )
        details = "".join(f"<li>{escape(line)}</li>" for line in detail_lines)
        detail_html = f"<ul>{details}</ul>" if details else ""
        check = next((c for c in (score_guide.get("checks") or []) if isinstance(c, dict) and c.get("key") == key), {})
        score_line = ""
        guide_line = ""
        if check:
            score_line = f'<div class="diag-score">{escape(str(check.get("score")))} / {escape(str(check.get("max_score")))}</div>'
            guide_line = f'<div class="diag-guide">{escape(str(check.get("remediation") or ""))}</div>'
        return (
            f'<div class="diag {escape(status.lower())}">'
            f'<div class="diag-head"><strong>{escape(str(item.get("label") or key))}</strong><span>{escape(status)}</span></div>'
            f'{score_line}<p>{escape(str(item.get("message") or "-"))}</p>{guide_line}{detail_html}</div>'
        )

    diagnostic_order = [
        "phases_index",
        "phase_steps",
        "harness_docs",
        "executor",
        "airflow_dag",
        "dashboard_outputs",
        "scheduler",
    ]
    diagnostics_html = "".join(
        _diagnostic_card(key, diagnostics[key])
        for key in diagnostic_order
        if isinstance(diagnostics.get(key), dict)
    )
    if not diagnostics_html:
        diagnostics_html = '<div class="empty">진단 정보가 없습니다. 최신 DAG 실행으로 snapshot을 갱신하세요.</div>'

    phase_empty_message = "진행 중인 phase가 없습니다. 새 작업은 scaffold phase로 생성하세요."
    phase_html: list[str] = []
    for phase in snapshot.get("phases") or []:
        steps: list[str] = []
        phase_steps = phase.get("steps") or []
        phase_total = len(phase_steps)
        phase_completed = sum(1 for step in phase_steps if str(step.get("status") or "").lower() == "completed")
        phase_progress = f"{phase_completed}/{phase_total} completed" if phase_total else "0/0 completed"
        for step in phase.get("steps") or []:
            status = "stale_pending" if step.get("stale_pending") else str(step.get("status") or "pending").lower()
            message = step.get("error_message") or step.get("blocked_reason") or step.get("summary") or ""
            docs = step.get("harness_docs") or []
            harness_docs = ", ".join(str(x) for x in docs) if isinstance(docs, list) else str(docs)
            output = step.get("output") or {}
            output_line = ""
            if output:
                output_text = str(output.get("stderr_tail") or output.get("stdout_tail") or "")[:220]
                output_line = f'<div class="output">exit={escape(str(output.get("exitCode")))} {escape(output_text)}</div>'
            steps.append(
                "<div class=\"step {status}\">"
                "<div class=\"step-head\"><strong>Step {num}: {name}</strong><span>{label}</span></div>"
                "<div class=\"step-meta\">harness: {harness}</div>"
                "<div class=\"step-msg\">{message}</div>"
                "<button class=\"md-btn\" type=\"button\" data-phase=\"{phase_dir}\" data-step=\"{num}\">MD 보기</button>"
                "{output}</div>".format(
                    status=escape(status),
                    phase_dir=escape(str(phase.get("dir") or "")),
                    num=escape(str(step.get("step"))),
                    name=escape(str(step.get("name") or "")),
                    label=escape(status.upper()),
                    harness=escape(harness_docs or "-"),
                    message=escape(str(message or "-")),
                    output=output_line,
                )
            )
        if not steps:
            steps.append('<div class="step pending"><div class="step-msg">step 정보가 없습니다.</div></div>')
        phase_html.append(
            "<section class=\"phase {status}\">"
            "<div class=\"phase-head\"><div><h2>{phase}</h2><p>{dir}</p><p>{progress}</p></div><span>{status_label}</span></div>"
            "<div class=\"flow\">{steps}</div></section>".format(
                status=escape(str(phase.get("status") or "PENDING").lower()),
                phase=escape(str(phase.get("phase") or phase.get("dir") or "")),
                dir=escape(str(phase.get("dir") or "")),
                progress=escape(phase_progress),
                status_label=escape(str(phase.get("status") or "PENDING")),
                steps='<div class="arrow">→</div>'.join(steps),
            )
        )
    phases_markup = "\n".join(phase_html) or f'<div class="empty error-empty">{escape(phase_empty_message)}</div>'
    embedded = _safe_json_embed(snapshot)
    generated = escape(str(snapshot.get("generated_at") or "-"))
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Harness Monitoring</title>
  <style>
    body {{ margin:0; font-family:"Malgun Gothic","Segoe UI",sans-serif; background:#f4f7fb; color:#172033; }}
    .page {{ max-width:1400px; margin:0 auto; padding:28px 20px 48px; }}
    header {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-end; margin-bottom:18px; }}
    h1 {{ margin:0; font-size:30px; }}
    .muted {{ color:#65758b; }}
    .kpis {{ display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:10px; margin:18px 0; }}
    .kpi {{ background:#fff; border:1px solid #dbe3ef; border-radius:8px; padding:14px; }}
    .kpi span {{ display:block; color:#65758b; font-size:12px; }}
    .kpi strong {{ display:block; font-size:26px; margin-top:4px; }}
    .kpi.error strong,.kpi.blocked strong,.kpi.stale strong {{ color:#b42318; }}
    .score-panel {{ display:grid; grid-template-columns:320px 1fr; gap:14px; margin:18px 0; }}
    .score-main,.actions-panel {{ background:#fff; border:1px solid #dbe3ef; border-radius:8px; padding:16px; }}
    .score-value {{ display:flex; align-items:baseline; gap:8px; margin-top:8px; }}
    .score-value strong {{ font-size:48px; line-height:1; }}
    .score-value span {{ color:#64748b; font-weight:700; }}
    .grade {{ display:inline-block; margin-top:10px; border-radius:999px; padding:5px 10px; background:#e2e8f0; font-size:12px; font-weight:700; }}
    .score-summary {{ margin:12px 0 0; color:#475569; line-height:1.5; }}
    .score-breakdown {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; margin-top:14px; }}
    .score-mini {{ border:1px solid #e2e8f0; border-radius:8px; padding:9px; background:#f8fafc; }}
    .score-mini span {{ display:block; color:#64748b; font-size:12px; }}
    .score-mini strong {{ display:block; margin-top:3px; }}
    .actions-panel h2 {{ margin:0 0 10px; font-size:18px; }}
    .actions-panel ol,.why-list {{ margin:0; padding-left:20px; }}
    .actions-panel li {{ margin-bottom:10px; }}
    .actions-panel p {{ margin:4px 0; color:#475569; line-height:1.45; }}
    .actions-panel code {{ display:inline-block; margin-top:4px; background:#eef2ff; color:#1e3a8a; border-radius:6px; padding:5px 7px; }}
    .why-box {{ margin-top:12px; border-top:1px solid #e2e8f0; padding-top:10px; }}
    .why-box h3 {{ margin:0 0 6px; font-size:13px; color:#334155; }}
    .section-title {{ margin:26px 0 10px; font-size:18px; }}
    .diagnostics {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin:12px 0 22px; }}
    .diag {{ background:#fff; border:1px solid #dbe3ef; border-radius:8px; padding:13px; min-height:92px; }}
    .diag-head {{ display:flex; justify-content:space-between; gap:8px; align-items:center; }}
    .diag-head span {{ border-radius:999px; padding:4px 8px; font-size:11px; font-weight:700; background:#e2e8f0; color:#334155; }}
    .diag.ok .diag-head span {{ background:#dcfce7; color:#166534; }}
    .diag.warn .diag-head span,.diag.unknown .diag-head span,.diag.unavailable .diag-head span {{ background:#fef3c7; color:#92400e; }}
    .diag.error .diag-head span {{ background:#fee2e2; color:#b42318; }}
    .diag p {{ margin:9px 0 0; color:#64748b; font-size:12px; line-height:1.45; }}
    .diag ul {{ margin:8px 0 0 18px; padding:0; color:#475569; font-size:12px; line-height:1.45; }}
    .diag-score {{ margin-top:8px; font-size:18px; font-weight:800; }}
    .diag-guide {{ margin-top:8px; color:#475569; font-size:12px; line-height:1.45; background:#f8fafc; border-radius:6px; padding:7px; }}
    .toolbar {{ display:flex; gap:10px; margin-bottom:14px; }}
    input,select {{ border:1px solid #cbd5e1; border-radius:8px; padding:10px 12px; background:#fff; min-width:180px; }}
    .phase {{ background:#fff; border:1px solid #dbe3ef; border-radius:8px; margin-bottom:14px; overflow:hidden; }}
    .phase-head {{ display:flex; justify-content:space-between; gap:12px; padding:16px 18px; border-bottom:1px solid #e6edf5; }}
    .phase-head h2 {{ margin:0 0 4px; font-size:18px; }}
    .phase-head p {{ margin:0; color:#65758b; font-size:13px; }}
    .phase-head span {{ align-self:start; border-radius:999px; padding:5px 10px; background:#e2e8f0; font-size:12px; font-weight:700; }}
    .phase.error .phase-head span,.phase.blocked .phase-head span,.phase.stale_pending .phase-head span {{ background:#fee4e2; color:#b42318; }}
    .flow {{ display:flex; gap:10px; align-items:stretch; overflow-x:auto; padding:16px 18px; }}
    .step {{ min-width:250px; max-width:320px; border:1px solid #dbe3ef; border-radius:8px; padding:12px; background:#f8fafc; }}
    .step.completed {{ border-color:#86efac; background:#f0fdf4; }}
    .step.error,.step.blocked,.step.stale_pending {{ border-color:#fca5a5; background:#fff1f2; }}
    .step-head {{ display:flex; justify-content:space-between; gap:8px; font-size:13px; }}
    .step-head span {{ font-size:11px; font-weight:700; color:#475569; }}
    .step-meta,.step-msg,.output {{ margin-top:8px; color:#64748b; font-size:12px; line-height:1.45; word-break:break-word; }}
    .output {{ color:#334155; background:#e8eef6; padding:8px; border-radius:6px; }}
    .md-btn {{ margin-top:10px; border:1px solid #2563eb; background:#fff; color:#1d4ed8; border-radius:6px; padding:7px 10px; font-weight:700; cursor:pointer; }}
    .md-btn:hover {{ background:#eff6ff; }}
    .arrow {{ display:grid; place-items:center; color:#94a3b8; font-weight:700; }}
    .empty {{ padding:28px; text-align:center; color:#64748b; background:#fff; border-radius:8px; }}
    .error-empty {{ color:#b42318; border:1px solid #fecaca; }}
    .modal-backdrop {{ position:fixed; inset:0; background:rgba(15,23,42,.48); display:none; align-items:center; justify-content:center; padding:24px; z-index:20; }}
    .modal-backdrop.open {{ display:flex; }}
    .modal {{ width:min(980px,100%); max-height:88vh; background:#fff; border-radius:8px; border:1px solid #cbd5e1; box-shadow:0 24px 60px rgba(15,23,42,.22); display:flex; flex-direction:column; }}
    .modal-head {{ display:flex; justify-content:space-between; gap:12px; align-items:center; padding:14px 16px; border-bottom:1px solid #e2e8f0; }}
    .modal-head h2 {{ margin:0; font-size:18px; }}
    .modal-close {{ border:1px solid #cbd5e1; background:#fff; border-radius:6px; padding:6px 10px; cursor:pointer; }}
    .modal-body {{ padding:16px; overflow:auto; }}
    .md-block {{ margin:0 0 16px; }}
    .md-block h3 {{ margin:0 0 6px; font-size:14px; color:#334155; }}
    .md-block pre {{ margin:0; white-space:pre-wrap; word-break:break-word; background:#0f172a; color:#e2e8f0; border-radius:8px; padding:14px; font-size:12px; line-height:1.55; }}
    @media (max-width:900px) {{ .score-panel {{ grid-template-columns:1fr; }} .kpis,.diagnostics {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} header {{ display:block; }} }}
    @media (max-width:640px) {{ .diagnostics {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="page">
    <header>
      <div>
        <h1>Harness Monitoring</h1>
        <div class="muted">phase/step 흐름과 차단 상태를 매일 13시에 점검합니다.</div>
      </div>
      <div class="muted">갱신: {generated}</div>
    </header>
    <section class="score-panel">
      <div class="score-main">
        <div class="muted">하네스 건강점수</div>
        <div class="score-value"><strong>{overall_score}</strong><span>/ 100</span></div>
        <div class="grade">{grade}</div>
        <p class="score-summary">{summary_text}</p>
        <div class="score-breakdown">{score_breakdown_html}</div>
      </div>
      <div class="actions-panel">
        <h2>다음에 할 일</h2>
        <ol>{actions_html}</ol>
        <div class="why-box">
          <h3>점수 판단 근거</h3>
          <ul class="why-list">{why_html}</ul>
        </div>
      </div>
    </section>
    <div class="kpis">{kpi_html}</div>
    <h2 class="section-title">하네스 설정 점검</h2>
    <section class="diagnostics">{diagnostics_html}</section>
    <div class="toolbar">
      <input id="search" placeholder="phase 또는 step 검색">
      <select id="status"><option value="ALL">전체 상태</option><option>COMPLETED</option><option>PENDING</option><option>STALE_PENDING</option><option>BLOCKED</option><option>ERROR</option></select>
    </div>
    <main id="phases">{phases_markup}</main>
  </div>
  <div id="mdModal" class="modal-backdrop" role="dialog" aria-modal="true" aria-hidden="true">
    <div class="modal">
      <div class="modal-head">
        <h2 id="mdTitle">Markdown</h2>
        <button id="mdClose" class="modal-close" type="button">닫기</button>
      </div>
      <div id="mdBody" class="modal-body"></div>
    </div>
  </div>
  <script id="snapshot" type="application/json">{embedded}</script>
  <script>
    const snapshot = JSON.parse(document.getElementById("snapshot").textContent);
    const search = document.getElementById("search");
    const status = document.getElementById("status");
    const cards = Array.from(document.querySelectorAll(".phase"));
    const modal = document.getElementById("mdModal");
    const mdTitle = document.getElementById("mdTitle");
    const mdBody = document.getElementById("mdBody");
    const mdClose = document.getElementById("mdClose");
    function applyFilters() {{
      const q = search.value.trim().toLowerCase();
      const s = status.value.toLowerCase();
      cards.forEach(card => {{
        const text = card.textContent.toLowerCase();
        const statusOk = s === "all" || card.classList.contains(s);
        card.style.display = (!q || text.includes(q)) && statusOk ? "" : "none";
      }});
    }}
    search.addEventListener("input", applyFilters);
    status.addEventListener("change", applyFilters);
    function findStep(phaseDir, stepNum) {{
      const phases = snapshot.phases || [];
      const phase = phases.find(item => String(item.dir || "") === phaseDir);
      if (!phase) return null;
      return (phase.steps || []).find(item => String(item.step) === String(stepNum));
    }}
    function addMarkdownBlock(title, md) {{
      const section = document.createElement("section");
      section.className = "md-block";
      const h = document.createElement("h3");
      h.textContent = title;
      const pre = document.createElement("pre");
      const content = md && md.content ? md.content : "파일이 없거나 읽을 수 없습니다.";
      pre.textContent = content + (md && md.truncated ? "\\n\\n...[20,000자 이후 생략]" : "");
      section.appendChild(h);
      section.appendChild(pre);
      mdBody.appendChild(section);
    }}
    function openMarkdown(phaseDir, stepNum) {{
      const step = findStep(phaseDir, stepNum);
      mdBody.textContent = "";
      if (!step) {{
        mdTitle.textContent = "Markdown";
        addMarkdownBlock("오류", {{content: "step 정보를 찾을 수 없습니다."}});
      }} else {{
        mdTitle.textContent = `${{phaseDir}} / Step ${{step.step}}: ${{step.name || ""}}`;
        addMarkdownBlock(step.step_md && step.step_md.path ? step.step_md.path : "step markdown", step.step_md);
        (step.harness_doc_markdown || []).forEach(doc => addMarkdownBlock(doc.path || "harness markdown", doc));
      }}
      modal.classList.add("open");
      modal.setAttribute("aria-hidden", "false");
    }}
    function closeMarkdown() {{
      modal.classList.remove("open");
      modal.setAttribute("aria-hidden", "true");
    }}
    document.querySelectorAll(".md-btn").forEach(btn => {{
      btn.addEventListener("click", () => openMarkdown(btn.dataset.phase || "", btn.dataset.step || ""));
    }});
    mdClose.addEventListener("click", closeMarkdown);
    modal.addEventListener("click", event => {{
      if (event.target === modal) closeMarkdown();
    }});
    document.addEventListener("keydown", event => {{
      if (event.key === "Escape") closeMarkdown();
    }});
  </script>
</body>
</html>"""


class HarnessMonitoringDashboardService:
    def __init__(self, *, root: Path | str = DASHBOARD_DB):
        self.root = Path(root)
        self.snapshot_path = self.root / HARNESS_MONITORING_SNAPSHOT_FILENAME

    def get_snapshot(self) -> dict[str, Any]:
        if not self.snapshot_path.exists():
            return {"generated_at": None, "summary": {}, "phases": [], "problem_steps": []}
        payload = _read_json(self.snapshot_path)
        if payload.get("_read_error"):
            return {"generated_at": None, "summary": {}, "phases": [], "problem_steps": [], "read_error": payload["_read_error"]}
        return payload

    def render_html(self) -> str:
        return render_dashboard_html(self.get_snapshot())
