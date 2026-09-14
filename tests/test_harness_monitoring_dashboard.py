from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.dashboard.harness_monitoring_dashboard import (  # noqa: E402
    HARNESS_MONITORING_SNAPSHOT_FILENAME,
    HarnessMonitoringDashboardService,
    build_alert_text,
    build_harness_snapshot,
    render_dashboard_html,
    should_alert,
)
from modules.transform.pipelines.strategy import harness_monitoring_pipeline as pipeline  # noqa: E402


def _write_minimal_harness_files(root: Path):
    harness = root / "harness"
    harness.mkdir()
    (harness / "common.md").write_text("# Common", encoding="utf-8")
    (harness / "review_checklist.md").write_text("# Review", encoding="utf-8")
    scripts = root / "scripts"
    scripts.mkdir()
    (scripts / "execute.py").write_text("print('ok')", encoding="utf-8")
    dag_dir = root / "dags" / "strategy"
    dag_dir.mkdir(parents=True)
    (dag_dir / "Strategy_HarnessMonitoring_01_Dashboard_Dags.py").write_text("# dag", encoding="utf-8")


def _write_phase(root: Path, phase: str, steps: list[dict], *, output: dict | None = None):
    phase_dir = root / "phases" / phase
    phase_dir.mkdir(parents=True)
    (phase_dir / "index.json").write_text(
        json.dumps({"project": "airflow", "phase": phase, "steps": steps}, ensure_ascii=False),
        encoding="utf-8",
    )
    if output is not None:
        (phase_dir / "step0-output.json").write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")


def test_build_harness_snapshot_counts_problem_states(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(
        json.dumps({"phases": [{"dir": "ok"}, {"dir": "blocked"}, {"dir": "old"}]}),
        encoding="utf-8",
    )
    _write_phase(tmp_path, "ok", [{"step": 0, "name": "done", "status": "completed", "summary": "완료"}])
    _write_phase(tmp_path, "blocked", [{"step": 0, "name": "wait", "status": "blocked", "blocked_reason": "승인 필요"}])
    _write_phase(
        tmp_path,
        "old",
        [{"step": 0, "name": "pending", "status": "pending", "started_at": "2026-06-29T00:00:00+09:00"}],
    )
    for name in ("ok", "blocked", "old"):
        (tmp_path / "phases" / name / "step0.md").write_text("# Step", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, now=datetime(2026, 6, 30, 16, 0, tzinfo=UTC))

    assert snapshot["summary"]["completed"] == 1
    assert snapshot["summary"]["blocked"] == 1
    assert snapshot["summary"]["stale_pending"] == 1
    assert snapshot["summary"]["problem_steps"] == 2
    assert should_alert(snapshot) is True
    assert snapshot["score_guide"]["next_actions"][0]["title"].endswith("차단 해소")


def test_build_harness_snapshot_reads_output_tail(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    (tmp_path / "phases").mkdir()
    (tmp_path / "phases" / "index.json").write_text(json.dumps({"phases": [{"dir": "p"}]}), encoding="utf-8")
    _write_phase(
        tmp_path,
        "p",
        [{"step": 0, "name": "run", "status": "error", "error_message": "boom"}],
        output={"exitCode": 1, "stdout": "a" * 1400, "stderr": "traceback"},
    )
    (tmp_path / "phases" / "p" / "step0.md").write_text("# Step 0\n\n검증 절차", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path)

    output = snapshot["phases"][0]["steps"][0]["output"]
    assert output["exitCode"] == 1
    assert output["stderr_tail"] == "traceback"
    assert len(output["stdout_tail"]) == 1200
    assert "검증 절차" in snapshot["phases"][0]["steps"][0]["step_md"]["content"]


def test_render_dashboard_html_contains_flow_labels():
    snapshot = {
        "generated_at": "2026-06-30T04:00:00+00:00",
        "summary": {"total_phases": 1, "completed": 0, "pending": 0, "blocked": 1, "error": 0, "stale_pending": 0},
        "score_guide": {
            "overall_score": 64,
            "grade": "주의",
            "summary_text": "차단 상태가 있습니다.",
            "setup_score": 30,
            "setup_max": 40,
            "execution_score": 12,
            "execution_max": 35,
            "dashboard_score": 20,
            "dashboard_max": 20,
            "visibility_score": 10,
            "visibility_max": 10,
            "why_this_score": ["차단 step이 있습니다."],
            "next_actions": [{"title": "차단 해소", "guide": "blocked_reason을 확인하세요."}],
            "checks": [],
        },
        "phases": [
                {
                    "dir": "phase-a",
                    "phase": "phase-a",
                    "status": "BLOCKED",
                    "steps": [
                        {"step": 0, "name": "prepare", "status": "completed", "summary": "준비"},
                        {"step": 1, "name": "review", "status": "blocked", "blocked_reason": "확인 필요", "harness_docs": ["common"]},
                    ],
                }
            ],
        "problem_steps": [],
    }

    html = render_dashboard_html(snapshot)

    assert "Harness Monitoring" in html
    assert "phase-a" in html
    assert "확인 필요" in html
    assert "→" in html
    assert "MD 보기" in html
    assert "mdModal" in html
    assert "하네스 건강점수" in html
    assert "다음에 할 일" in html
    assert "1/2 completed" in html


def test_dashboard_service_reads_snapshot(tmp_path: Path):
    snapshot = {"generated_at": "x", "summary": {"total_phases": 1}, "phases": [], "problem_steps": []}
    (tmp_path / HARNESS_MONITORING_SNAPSHOT_FILENAME).write_text(json.dumps(snapshot), encoding="utf-8")

    service = HarnessMonitoringDashboardService(root=tmp_path)

    assert service.get_snapshot()["summary"]["total_phases"] == 1
    assert "Harness Monitoring" in service.render_html()


def test_alert_text_contains_problem_step():
    snapshot = {
        "generated_at": "now",
        "summary": {"error": 1, "blocked": 0, "stale_pending": 0},
        "problem_steps": [{"phase": "p", "step": 0, "name": "x", "status": "error", "message": "boom"}],
    }

    text = build_alert_text(snapshot)

    assert "ERROR=1" in text
    assert "boom" in text


def test_pipeline_writes_dashboard_files(tmp_path: Path):
    snapshot = {
        "generated_at": "2026-06-30T04:00:00+00:00",
        "summary": {"total_phases": 0, "completed": 0, "pending": 0, "blocked": 0, "error": 0, "stale_pending": 0},
        "phases": [],
        "problem_steps": [],
    }

    snap_path = pipeline._write_dashboard_snapshot(snapshot, root=tmp_path)
    html_path = pipeline._write_dashboard_html(snapshot, root=tmp_path)

    assert snap_path.exists()
    assert html_path.exists()
    assert "Harness Monitoring" in html_path.read_text(encoding="utf-8")


def test_should_alert_false_for_clean_snapshot():
    snapshot = {"summary": {"error": 0, "blocked": 0, "stale_pending": 0}}

    assert should_alert(snapshot) is False


def test_stale_pending_threshold_is_24_hours(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    (tmp_path / "phases").mkdir()
    (tmp_path / "phases" / "index.json").write_text(json.dumps({"phases": [{"dir": "recent"}]}), encoding="utf-8")
    started = datetime(2026, 6, 30, 3, 0, tzinfo=UTC)
    _write_phase(tmp_path, "recent", [{"step": 0, "name": "pending", "status": "pending", "started_at": started.isoformat()}])
    (tmp_path / "phases" / "recent" / "step0.md").write_text("# Step", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, now=started + timedelta(hours=23, minutes=59))

    assert snapshot["summary"]["stale_pending"] == 0


def test_missing_phases_directory_is_diagnostic_error(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)

    assert snapshot["summary"]["total_phases"] == 0
    assert snapshot["diagnostics"]["phases_index"]["status"] == "ERROR"
    assert snapshot["score_guide"]["overall_score"] < 50
    assert "phases/index.json" in snapshot["score_guide"]["next_actions"][0]["title"]
    assert should_alert(snapshot) is True
    assert "phases/index.json" in build_alert_text(snapshot)


def test_empty_phase_list_is_healthy_idle_state(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(json.dumps({"phases": []}), encoding="utf-8")
    (tmp_path / HARNESS_MONITORING_SNAPSHOT_FILENAME).write_text("{}", encoding="utf-8")
    (tmp_path / "harness_monitoring_dashboard.html").write_text("<html></html>", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)
    html = render_dashboard_html(snapshot)

    assert snapshot["diagnostics"]["phases_index"]["status"] == "OK"
    assert snapshot["score_guide"]["overall_score"] >= 90
    assert "진행 중인 phase가 없습니다" in html
    assert "/opt/airflow/phases 마운트" not in html
    assert "하네스 설정 점검" in html


def test_missing_harness_doc_is_diagnostic_error(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(json.dumps({"phases": [{"dir": "p"}]}), encoding="utf-8")
    _write_phase(tmp_path, "p", [{"step": 0, "name": "run", "status": "pending", "harness_docs": ["missing"]}])
    (tmp_path / "phases" / "p" / "step0.md").write_text("# Step", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)

    assert snapshot["diagnostics"]["harness_docs"]["status"] == "ERROR"
    assert any("missing.md" in item for item in snapshot["diagnostics"]["harness_docs"]["missing_step_docs"])
    assert any(check["key"] == "harness_docs" and check["score"] == 0 for check in snapshot["score_guide"]["checks"])


def test_dashboard_output_diagnostics_reports_files(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(json.dumps({"phases": [{"dir": "p"}]}), encoding="utf-8")
    _write_phase(tmp_path, "p", [{"step": 0, "name": "run", "status": "completed"}])
    (tmp_path / "phases" / "p" / "step0.md").write_text("# Step", encoding="utf-8")
    (tmp_path / HARNESS_MONITORING_SNAPSHOT_FILENAME).write_text("{}", encoding="utf-8")
    (tmp_path / "harness_monitoring_dashboard.html").write_text("<html></html>", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)

    outputs = snapshot["diagnostics"]["dashboard_outputs"]
    assert outputs["status"] == "OK"
    assert outputs["snapshot"]["size"] > 0
    assert outputs["html"]["size"] > 0


def test_score_guide_completed_phase_is_healthy(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(json.dumps({"phases": [{"dir": "p"}]}), encoding="utf-8")
    _write_phase(tmp_path, "p", [{"step": 0, "name": "run", "status": "completed", "summary": "완료"}])
    (tmp_path / "phases" / "p" / "step0.md").write_text("# Step", encoding="utf-8")
    (tmp_path / HARNESS_MONITORING_SNAPSHOT_FILENAME).write_text("{}", encoding="utf-8")
    (tmp_path / "harness_monitoring_dashboard.html").write_text("<html></html>", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)

    assert snapshot["score_guide"]["overall_score"] >= 90
    assert snapshot["score_guide"]["grade"] == "정상"
    assert snapshot["score_guide"]["next_actions"][0]["title"] == "추가 조치 없음"


def test_score_guide_pending_phase_gives_execution_command(tmp_path: Path):
    _write_minimal_harness_files(tmp_path)
    phases = tmp_path / "phases"
    phases.mkdir()
    (phases / "index.json").write_text(json.dumps({"phases": [{"dir": "sample-pending"}]}), encoding="utf-8")
    _write_phase(tmp_path, "sample-pending", [{"step": 0, "name": "run", "status": "pending"}])
    (tmp_path / "phases" / "sample-pending" / "step0.md").write_text("# Step", encoding="utf-8")
    (tmp_path / HARNESS_MONITORING_SNAPSHOT_FILENAME).write_text("{}", encoding="utf-8")
    (tmp_path / "harness_monitoring_dashboard.html").write_text("<html></html>", encoding="utf-8")

    snapshot = build_harness_snapshot(root=tmp_path, dashboard_root=tmp_path)

    assert snapshot["score_guide"]["grade"] == "보완 필요"
    assert snapshot["score_guide"]["next_actions"][0]["command"] == "python scripts/execute.py sample-pending"
    html = render_dashboard_html(snapshot)
    assert "하네스 건강점수" in html
    assert "python scripts/execute.py sample-pending" in html
    assert "점수 판단 근거" in html
