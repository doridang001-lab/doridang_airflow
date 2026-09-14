import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


sys.path.insert(0, str(Path(__file__).parent))
import execute as ex


@pytest.fixture
def tmp_project(tmp_path):
    (tmp_path / "phases").mkdir()
    (tmp_path / "AGENTS.md").write_text("# Rules\n- 항상 한글", encoding="utf-8")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text("# Guide", encoding="utf-8")
    harness = tmp_path / "harness"
    harness.mkdir()
    (harness / "common.md").write_text("# Common", encoding="utf-8")
    (harness / "review_checklist.md").write_text("# Review", encoding="utf-8")
    return tmp_path


@pytest.fixture
def phase_dir(tmp_project):
    phase = tmp_project / "phases" / "airflow-smoke"
    phase.mkdir()
    index = {
        "project": "airflow",
        "phase": "airflow-smoke",
        "steps": [
            {"step": 0, "name": "setup", "status": "completed", "summary": "준비 완료"},
            {"step": 1, "name": "smoke", "status": "pending"},
        ],
    }
    (phase / "index.json").write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    (phase / "step1.md").write_text("# Step 1\n\n검증하세요.", encoding="utf-8")
    return phase


@pytest.fixture
def executor(tmp_project, phase_dir):
    with patch.object(ex, "ROOT", tmp_project):
        inst = ex.StepExecutor("airflow-smoke")
    inst._root = str(tmp_project)
    inst._phases_dir = tmp_project / "phases"
    inst._phase_dir = phase_dir
    inst._phase_dir_name = "airflow-smoke"
    inst._index_file = phase_dir / "index.json"
    inst._top_index_file = tmp_project / "phases" / "index.json"
    return inst


def test_push_requires_git(tmp_project):
    with patch.object(ex, "ROOT", tmp_project):
        with pytest.raises(SystemExit) as err:
            ex.StepExecutor("missing", auto_push=True)
    assert err.value.code == 2


def test_read_write_json_utf8(tmp_path):
    path = tmp_path / "x.json"
    ex.StepExecutor._write_json(path, {"한글": "값"})
    assert "\\u" not in path.read_text(encoding="utf-8")
    assert ex.StepExecutor._read_json(path) == {"한글": "값"}


def test_load_guardrails_common_docs_only(executor, tmp_project):
    (tmp_project / "harness" / "domain.md").write_text("# Domain", encoding="utf-8")
    with patch.object(ex, "ROOT", tmp_project):
        text = executor._load_guardrails()
    assert "# Rules" in text
    assert "# Guide" not in text
    assert "# Common" in text
    assert "# Review" in text
    assert "# Domain" not in text


def test_load_step_harness_docs_accepts_string(executor, tmp_project):
    (tmp_project / "harness" / "domain.md").write_text("# Domain", encoding="utf-8")
    with patch.object(ex, "ROOT", tmp_project):
        text = executor._load_step_harness_docs({"harness_docs": "domain"})
    assert "# Domain" in text


def test_load_step_harness_docs_rejects_traversal(executor):
    with pytest.raises(ValueError):
        executor._load_step_harness_docs({"harness_docs": ["../secret"]})


def test_load_step_harness_docs_missing_blocks_candidate(executor, tmp_project):
    with patch.object(ex, "ROOT", tmp_project):
        with pytest.raises(FileNotFoundError):
            executor._load_step_harness_docs({"harness_docs": ["missing"]})


def test_build_step_context_includes_completed_summary(phase_dir):
    index = json.loads((phase_dir / "index.json").read_text(encoding="utf-8"))
    text = ex.StepExecutor._build_step_context(index)
    assert "Step 0 (setup): 준비 완료" in text
    assert "smoke" not in text


def test_build_preamble_says_no_direct_commit(executor):
    text = executor._build_preamble("GUARD", "CTX")
    assert "GUARD" in text
    assert "CTX" in text
    assert "git commit을 직접 만들지 않습니다" in text


def test_invoke_codex_uses_expected_command(executor):
    result = MagicMock(returncode=0, stdout='{"ok": true}', stderr="")
    with patch("subprocess.run", return_value=result) as run:
        output = executor._invoke_codex({"step": 1, "name": "smoke"}, "PRE\n")

    cmd = run.call_args[0][0]
    assert cmd[:2] == ["codex", "exec"]
    assert "--cd" in cmd
    assert "--sandbox" in cmd
    assert "workspace-write" in cmd
    assert "--json" in cmd
    assert "PRE" in cmd[-1]
    assert output["exitCode"] == 0
    assert (executor._phase_dir / "step1-output.json").exists()


def test_ensure_clean_worktree_allows_clean(executor):
    executor._run_git = lambda *args: MagicMock(returncode=0, stdout="", stderr="")
    executor._ensure_clean_worktree()


def test_ensure_clean_worktree_rejects_dirty(executor):
    executor._run_git = lambda *args: MagicMock(returncode=0, stdout=" M file.py\n", stderr="")
    with pytest.raises(SystemExit) as err:
        executor._ensure_clean_worktree()
    assert err.value.code == 1


def test_commit_step_skips_when_git_disabled(executor):
    calls = []
    executor._run_git = lambda *args: calls.append(args) or MagicMock(returncode=0, stdout="", stderr="")
    executor._commit_step(1, "smoke")
    assert calls == []


def test_commit_step_runs_when_git_enabled(executor):
    executor._use_git = True
    calls = []

    def fake_git(*args):
        calls.append(args)
        if args[:2] == ("diff", "--cached"):
            return MagicMock(returncode=1)
        return MagicMock(returncode=0, stdout="", stderr="")

    executor._run_git = fake_git
    executor._commit_step(1, "smoke")
    assert any(call[0] == "commit" for call in calls)


def test_main_push_without_git_exits(tmp_project):
    with patch.object(ex, "ROOT", tmp_project):
        with patch("sys.argv", ["execute.py", "airflow-smoke", "--push"]):
            with pytest.raises(SystemExit) as err:
                ex.main()
    assert err.value.code == 2


def test_subprocess_import_not_shadowed():
    assert subprocess.PIPE is not None
