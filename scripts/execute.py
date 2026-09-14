#!/usr/bin/env python3
"""
Airflow DAG Harness Step Executor.

Usage:
    python scripts/execute.py <phase-dir> [--git] [--push]
"""

import argparse
import contextlib
import json
import subprocess
import sys
import threading
import time
import types
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from harness_registry import RegistryError, load_dag_context

ROOT = Path(__file__).resolve().parent.parent


@contextlib.contextmanager
def progress_indicator(label: str):
    """터미널 진행 표시기. with 문으로 사용하며 .elapsed 로 경과 시간을 읽는다."""
    frames = "◐◓◑◒"
    stop = threading.Event()
    t0 = time.monotonic()

    def _animate():
        idx = 0
        while not stop.wait(0.12):
            sec = int(time.monotonic() - t0)
            sys.stderr.write(f"\r{frames[idx % len(frames)]} {label} [{sec}s]")
            sys.stderr.flush()
            idx += 1
        sys.stderr.write("\r" + " " * (len(label) + 20) + "\r")
        sys.stderr.flush()

    th = threading.Thread(target=_animate, daemon=True)
    th.start()
    info = types.SimpleNamespace(elapsed=0.0)
    try:
        yield info
    finally:
        stop.set()
        th.join()
        info.elapsed = time.monotonic() - t0


class StepExecutor:
    """Phase 디렉토리 안의 step들을 순차 실행하는 하네스."""

    MAX_RETRIES = 3
    FEAT_MSG = "feat({phase}): step {num} — {name}"
    CHORE_MSG = "chore({phase}): step {num} output"
    TZ = timezone(timedelta(hours=9))

    def __init__(self, phase_dir_name: str, *, use_git: bool = False, auto_push: bool = False):
        if auto_push and not use_git:
            print("ERROR: --push는 --git과 함께 사용해야 합니다.")
            sys.exit(2)
        self._root = str(ROOT)
        self._phases_dir = ROOT / "phases"
        self._phase_dir = self._phases_dir / phase_dir_name
        self._phase_dir_name = phase_dir_name
        self._top_index_file = self._phases_dir / "index.json"
        self._use_git = use_git
        self._auto_push = auto_push

        if not self._phase_dir.is_dir():
            print(f"ERROR: {self._phase_dir} not found")
            sys.exit(1)

        self._index_file = self._phase_dir / "index.json"
        if not self._index_file.exists():
            print(f"ERROR: {self._index_file} not found")
            sys.exit(1)

        idx = self._read_json(self._index_file)
        self._project = idx.get("project", "airflow")
        self._phase_name = idx.get("phase", phase_dir_name)
        self._total = len(idx["steps"])

    def run(self):
        self._print_header()
        self._check_blockers()
        if self._use_git:
            self._ensure_clean_worktree()
            self._checkout_branch()
        guardrails = self._load_guardrails()
        self._ensure_created_at()
        self._execute_all_steps(guardrails)
        self._finalize()

    # --- timestamps ---

    def _stamp(self) -> str:
        return datetime.now(self.TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

    # --- JSON I/O ---

    @staticmethod
    def _read_json(p: Path) -> dict:
        return json.loads(p.read_text(encoding="utf-8"))

    @staticmethod
    def _write_json(p: Path, data: dict):
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # --- git ---

    def _run_git(self, *args) -> subprocess.CompletedProcess:
        cmd = ["git"] + list(args)
        return subprocess.run(cmd, cwd=self._root, capture_output=True, text=True)

    def _ensure_clean_worktree(self):
        result = self._run_git("status", "--porcelain")
        if result.returncode != 0:
            print(f"ERROR: git status 실패: {result.stderr.strip()}")
            sys.exit(1)
        if result.stdout.strip():
            print("ERROR: --git 실행은 깨끗한 작업트리에서만 허용됩니다.")
            sys.exit(1)

    def _checkout_branch(self):
        branch = f"feat-{self._phase_name}"

        r = self._run_git("rev-parse", "--abbrev-ref", "HEAD")
        if r.returncode != 0:
            print(f"  ERROR: git을 사용할 수 없거나 git repo가 아닙니다.")
            print(f"  {r.stderr.strip()}")
            sys.exit(1)

        if r.stdout.strip() == branch:
            return

        r = self._run_git("rev-parse", "--verify", branch)
        r = self._run_git("checkout", branch) if r.returncode == 0 else self._run_git("checkout", "-b", branch)

        if r.returncode != 0:
            print(f"  ERROR: 브랜치 '{branch}' checkout 실패.")
            print(f"  {r.stderr.strip()}")
            print(f"  Hint: 변경사항을 stash하거나 commit한 후 다시 시도하세요.")
            sys.exit(1)

        print(f"  Branch: {branch}")

    def _commit_step(self, step_num: int, step_name: str):
        if not getattr(self, "_use_git", True):
            return
        output_rel = f"phases/{self._phase_dir_name}/step{step_num}-output.json"
        index_rel = f"phases/{self._phase_dir_name}/index.json"

        self._run_git("add", "-A")
        self._run_git("reset", "HEAD", "--", output_rel)
        self._run_git("reset", "HEAD", "--", index_rel)

        if self._run_git("diff", "--cached", "--quiet").returncode != 0:
            msg = self.FEAT_MSG.format(phase=self._phase_name, num=step_num, name=step_name)
            r = self._run_git("commit", "-m", msg)
            if r.returncode == 0:
                print(f"  Commit: {msg}")
            else:
                print(f"  WARN: 코드 커밋 실패: {r.stderr.strip()}")

        self._run_git("add", "-A")
        if self._run_git("diff", "--cached", "--quiet").returncode != 0:
            msg = self.CHORE_MSG.format(phase=self._phase_name, num=step_num)
            r = self._run_git("commit", "-m", msg)
            if r.returncode != 0:
                print(f"  WARN: housekeeping 커밋 실패: {r.stderr.strip()}")

    # --- top-level index ---

    def _update_top_index(self, status: str):
        if not self._top_index_file.exists():
            return
        top = self._read_json(self._top_index_file)
        ts = self._stamp()
        for phase in top.get("phases", []):
            if phase.get("dir") == self._phase_dir_name:
                phase["status"] = status
                ts_key = {"completed": "completed_at", "error": "failed_at", "blocked": "blocked_at"}.get(status)
                if ts_key:
                    phase[ts_key] = ts
                break
        self._write_json(self._top_index_file, top)

    # --- guardrails & context ---

    def _load_guardrails(self) -> str:
        sections = []
        codex_md = ROOT / "AGENTS.md"
        if codex_md.exists():
            sections.append(f"## 프로젝트 규칙 (AGENTS.md)\n\n{codex_md.read_text(encoding='utf-8')}")
        harness_dir = ROOT / "harness"
        if harness_dir.is_dir():
            for name in ("common.md", "review_checklist.md"):
                doc = harness_dir / name
                if doc.exists():
                    sections.append(f"## harness/{doc.name}\n\n{doc.read_text(encoding='utf-8')}")
        return "\n\n---\n\n".join(sections) if sections else ""

    @staticmethod
    def _coerce_optional_list(value, field_name: str) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [value]
        if not isinstance(value, list):
            raise ValueError(f"{field_name} must be a string or list")
        return value

    @staticmethod
    def _validate_relative_doc_path(raw_name) -> str:
        name = str(raw_name).strip().replace("\\", "/")
        if not name:
            return ""
        p = Path(name)
        if p.is_absolute() or ".." in p.parts or name.startswith("."):
            raise ValueError(f"invalid docs path: {raw_name}")
        if not name.startswith("docs/"):
            raise ValueError(f"docs path must start with docs/: {raw_name}")
        if not name.endswith(".md"):
            raise ValueError(f"docs path must be a markdown file: {raw_name}")
        return name

    def _load_step_docs(self, step: dict) -> str:
        names = self._coerce_optional_list(step.get("docs"), "docs")

        sections = []
        for raw_name in names:
            name = self._validate_relative_doc_path(raw_name)
            if not name:
                continue
            doc = ROOT / name
            if not doc.exists():
                raise FileNotFoundError(f"docs 문서를 찾을 수 없습니다: {doc}")
            if not doc.is_file():
                raise ValueError(f"docs 경로가 파일이 아닙니다: {doc}")
            sections.append(f"## {name}\n\n{doc.read_text(encoding='utf-8')}")
        return "\n\n---\n\n".join(sections)

    def _load_step_harness_docs(self, step: dict) -> str:
        names = self._coerce_optional_list(step.get("harness_docs"), "harness_docs")

        sections = []
        harness_dir = ROOT / "harness"
        for raw_name in names:
            name = str(raw_name).strip()
            if not name:
                continue
            if "/" in name or "\\" in name or name.startswith("."):
                raise ValueError(f"invalid harness doc name: {name}")
            filename = name if name.endswith(".md") else f"{name}.md"
            doc = harness_dir / filename
            if not doc.exists():
                raise FileNotFoundError(f"harness 문서를 찾을 수 없습니다: {doc}")
            sections.append(f"## harness/{doc.name}\n\n{doc.read_text(encoding='utf-8')}")
        return "\n\n---\n\n".join(sections)

    def _load_step_dag_docs(self, step: dict) -> str:
        contexts = load_dag_context(ROOT, step.get("dag_targets"))
        guidance: list[str] = []
        harness_docs: list[str] = []
        for context in contexts:
            guidance.extend(context.guidance)
            harness_docs.extend(context.harness_docs)

        sections = []
        for name in dict.fromkeys(guidance):
            doc = ROOT / name
            sections.append(f"## {name}\n\n{doc.read_text(encoding='utf-8')}")
        for name in dict.fromkeys(harness_docs):
            doc = ROOT / "harness" / f"{name}.md"
            sections.append(f"## harness/{doc.name}\n\n{doc.read_text(encoding='utf-8')}")
        return "\n\n---\n\n".join(sections)

    @staticmethod
    def _build_step_context(index: dict) -> str:
        lines = [
            f"- Step {s['step']} ({s['name']}): {s['summary']}"
            for s in index["steps"]
            if s["status"] == "completed" and s.get("summary")
        ]
        if not lines:
            return ""
        return "## 이전 Step 산출물\n\n" + "\n".join(lines) + "\n\n"

    def _build_preamble(self, guardrails: str, step_context: str,
                        prev_error: Optional[str] = None) -> str:
        retry_section = ""
        if prev_error:
            retry_section = (
                f"\n## ⚠ 이전 시도 실패 — 아래 에러를 반드시 참고하여 수정하라\n\n"
                f"{prev_error}\n\n---\n\n"
            )
        return (
            f"당신은 {self._project} 프로젝트의 개발자입니다. 아래 step을 수행하세요.\n\n"
            f"{guardrails}\n\n---\n\n"
            f"{step_context}{retry_section}"
            f"## 작업 규칙\n\n"
            f"1. 이전 step에서 작성된 코드를 확인하고 일관성을 유지하라.\n"
            f"2. 이 step에 명시된 작업만 수행하라. 추가 기능이나 파일을 만들지 마라.\n"
            f"3. step의 '읽어야 할 파일'에 없는 파일은 먼저 검색으로 실제 호출 경로를 확인한 뒤 필요한 최소 범위만 읽어라.\n"
            f"4. 금지된 파일이나 범위 밖 도메인 문서가 필요해 보이면 추정으로 진행하지 말고 blocked로 중단하라.\n"
            f"5. 기존 테스트를 깨뜨리지 마라.\n"
            f"6. AC(Acceptance Criteria) 검증을 직접 실행하라.\n"
            f"7. /phases/{self._phase_dir_name}/index.json의 해당 step status를 업데이트하라:\n"
            f"   - AC 통과 → \"completed\" + \"summary\" 필드에 이 step의 산출물을 한 줄로 요약\n"
            f"   - {self.MAX_RETRIES}회 수정 시도 후에도 실패 → \"error\" + \"error_message\" 기록\n"
            f"   - 사용자 개입이 필요한 경우 (API 키, 인증, 수동 설정 등) → \"blocked\" + \"blocked_reason\" 기록 후 즉시 중단\n"
            f"8. step 수행 세션은 git commit을 직접 만들지 않습니다. 커밋은 실행기의 --git 옵션이 담당합니다.\n\n---\n\n"
        )

    # --- Codex 호출 ---

    def _invoke_codex(self, step: dict, preamble: str) -> dict:
        step_num, step_name = step["step"], step["name"]
        step_file = self._phase_dir / f"step{step_num}.md"

        if not step_file.exists():
            print(f"  ERROR: {step_file} not found")
            sys.exit(1)

        prompt = preamble + step_file.read_text(encoding="utf-8")
        result = subprocess.run(
            ["codex", "exec", "--cd", self._root, "--sandbox", "workspace-write", "--json", prompt],
            cwd=self._root, capture_output=True, text=True, timeout=1800,
        )

        if result.returncode != 0:
            print(f"\n  WARN: Codex가 비정상 종료됨 (code {result.returncode})")
            if result.stderr:
                print(f"  stderr: {result.stderr[:500]}")

        output = {
            "step": step_num, "name": step_name,
            "exitCode": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr,
        }
        out_path = self._phase_dir / f"step{step_num}-output.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        return output

    # --- 헤더 & 검증 ---

    def _print_header(self):
        print(f"\n{'='*60}")
        print(f"  Codex Harness Step Executor")
        print(f"  Phase: {self._phase_name} | Steps: {self._total}")
        if self._auto_push:
            print(f"  Auto-push: enabled")
        print(f"{'='*60}")

    def _check_blockers(self):
        index = self._read_json(self._index_file)
        for s in reversed(index["steps"]):
            if s["status"] == "error":
                print(f"\n  ✗ Step {s['step']} ({s['name']}) failed.")
                print(f"  Error: {s.get('error_message', 'unknown')}")
                print(f"  Fix and reset status to 'pending' to retry.")
                sys.exit(1)
            if s["status"] == "blocked":
                print(f"\n  ⏸ Step {s['step']} ({s['name']}) blocked.")
                print(f"  Reason: {s.get('blocked_reason', 'unknown')}")
                print(f"  Resolve and reset status to 'pending' to retry.")
                sys.exit(2)
            if s["status"] != "pending":
                break

    def _ensure_created_at(self):
        index = self._read_json(self._index_file)
        if "created_at" not in index:
            index["created_at"] = self._stamp()
            self._write_json(self._index_file, index)

    # --- 실행 루프 ---

    def _execute_single_step(self, step: dict, guardrails: str) -> bool:
        """단일 step 실행 (재시도 포함). 완료되면 True, 실패/차단이면 False."""
        step_num, step_name = step["step"], step["name"]
        done = sum(1 for s in self._read_json(self._index_file)["steps"] if s["status"] == "completed")
        prev_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            index = self._read_json(self._index_file)
            step_context = self._build_step_context(index)
            try:
                step_docs = self._load_step_docs(step)
                step_harness = self._load_step_harness_docs(step)
                step_dag_docs = self._load_step_dag_docs(step)
            except (FileNotFoundError, ValueError, RegistryError) as e:
                index = self._read_json(self._index_file)
                ts = self._stamp()
                for s in index["steps"]:
                    if s["step"] == step_num:
                        s["status"] = "blocked"
                        s["blocked_reason"] = str(e)
                        s["blocked_at"] = ts
                self._write_json(self._index_file, index)
                print(f"  ⏸ Step {step_num}: {step_name} blocked")
                print(f"    Reason: {e}")
                self._update_top_index("blocked")
                sys.exit(2)

            combined_guardrails = "\n\n---\n\n".join(
                part for part in (guardrails, step_dag_docs, step_docs, step_harness) if part
            )
            preamble = self._build_preamble(combined_guardrails, step_context, prev_error)

            tag = f"Step {step_num}/{self._total - 1} ({done} done): {step_name}"
            if attempt > 1:
                tag += f" [retry {attempt}/{self.MAX_RETRIES}]"

            with progress_indicator(tag) as pi:
                self._invoke_codex(step, preamble)
                elapsed = int(pi.elapsed)

            index = self._read_json(self._index_file)
            status = next((s.get("status", "pending") for s in index["steps"] if s["step"] == step_num), "pending")
            ts = self._stamp()

            if status == "completed":
                for s in index["steps"]:
                    if s["step"] == step_num:
                        s["completed_at"] = ts
                self._write_json(self._index_file, index)
                self._commit_step(step_num, step_name)
                print(f"  ✓ Step {step_num}: {step_name} [{elapsed}s]")
                return True

            if status == "blocked":
                for s in index["steps"]:
                    if s["step"] == step_num:
                        s["blocked_at"] = ts
                self._write_json(self._index_file, index)
                reason = next((s.get("blocked_reason", "") for s in index["steps"] if s["step"] == step_num), "")
                print(f"  ⏸ Step {step_num}: {step_name} blocked [{elapsed}s]")
                print(f"    Reason: {reason}")
                self._update_top_index("blocked")
                sys.exit(2)

            err_msg = next(
                (s.get("error_message", "Step did not update status") for s in index["steps"] if s["step"] == step_num),
                "Step did not update status",
            )

            if attempt < self.MAX_RETRIES:
                for s in index["steps"]:
                    if s["step"] == step_num:
                        s["status"] = "pending"
                        s.pop("error_message", None)
                self._write_json(self._index_file, index)
                prev_error = err_msg
                print(f"  ↻ Step {step_num}: retry {attempt}/{self.MAX_RETRIES} — {err_msg}")
            else:
                for s in index["steps"]:
                    if s["step"] == step_num:
                        s["status"] = "error"
                        s["error_message"] = f"[{self.MAX_RETRIES}회 시도 후 실패] {err_msg}"
                        s["failed_at"] = ts
                self._write_json(self._index_file, index)
                self._commit_step(step_num, step_name)
                print(f"  ✗ Step {step_num}: {step_name} failed after {self.MAX_RETRIES} attempts [{elapsed}s]")
                print(f"    Error: {err_msg}")
                self._update_top_index("error")
                sys.exit(1)

        return False  # unreachable

    def _execute_all_steps(self, guardrails: str):
        while True:
            index = self._read_json(self._index_file)
            pending = next((s for s in index["steps"] if s["status"] == "pending"), None)
            if pending is None:
                print("\n  All steps completed!")
                return

            step_num = pending["step"]
            for s in index["steps"]:
                if s["step"] == step_num and "started_at" not in s:
                    s["started_at"] = self._stamp()
                    self._write_json(self._index_file, index)
                    break

            self._execute_single_step(pending, guardrails)

    def _finalize(self):
        index = self._read_json(self._index_file)
        index["completed_at"] = self._stamp()
        self._write_json(self._index_file, index)
        self._update_top_index("completed")

        if self._use_git:
            self._run_git("add", "-A")
            if self._run_git("diff", "--cached", "--quiet").returncode != 0:
                msg = f"chore({self._phase_name}): mark phase completed"
                r = self._run_git("commit", "-m", msg)
                if r.returncode == 0:
                    print(f"  ✓ {msg}")

        if self._auto_push:
            branch = f"feat-{self._phase_name}"
            r = self._run_git("push", "-u", "origin", branch)
            if r.returncode != 0:
                print(f"\n  ERROR: git push 실패: {r.stderr.strip()}")
                sys.exit(1)
            print(f"  ✓ Pushed to origin/{branch}")

        print(f"\n{'='*60}")
        print(f"  Phase '{self._phase_name}' completed!")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Airflow DAG Codex Harness Step Executor")
    parser.add_argument("phase_dir", help="phases/{phase_dir} directory name")
    parser.add_argument("--git", action="store_true", help="깨끗한 작업트리에서 브랜치와 커밋 수행")
    parser.add_argument("--push", action="store_true", help="--git 완료 후 origin에 push")
    args = parser.parse_args()

    StepExecutor(args.phase_dir, use_git=args.git, auto_push=args.push).run()


if __name__ == "__main__":
    main()
