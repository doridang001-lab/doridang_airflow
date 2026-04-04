"""PRD-Light runner — watches request files, calls claude -p for each signal.

Signals: questions, prd, spec, patch, section
Tree regeneration is Python-only (no signal needed).

Usage: py -3.12 runner.py <PORT>
"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TEMP_DIR = SCRIPT_DIR / "temp"
SIGNALS = ["questions", "prd", "spec", "patch", "section"]
IDLE_TIMEOUT = 600  # 10분


def _run_claude(prompt: str, sig: str) -> str:
    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    claude_exe = shutil.which("claude.cmd") or shutil.which("claude")
    if not claude_exe:
        return f"Error: Claude CLI not found in PATH"

    result = subprocess.run(
        [claude_exe, "-p", "--output-format", "text", "--permission-mode", "bypassPermissions", prompt],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    if result.returncode == 0:
        return result.stdout
    error = (result.stderr or result.stdout or "unknown error").strip()
    print(f"CLAUDE_ERROR ({sig}): {error[:300]}", flush=True)
    return f"Error: {error[:300]}"


def main():
    if len(sys.argv) < 2:
        print("Usage: runner.py <PORT>", flush=True)
        sys.exit(1)

    port = sys.argv[1]
    print(f"LIGHT_RUNNER_START: port={port}", flush=True)

    for _ in range(3600):  # 60분 타임아웃
        time.sleep(1)

        # 유휴 타임아웃
        activity_file = TEMP_DIR / "last_activity"
        if activity_file.exists():
            idle = time.time() - activity_file.stat().st_mtime
            if idle > IDLE_TIMEOUT:
                print(f"RUNNER_IDLE_EXIT: {idle:.0f}s idle", flush=True)
                sys.exit(0)

        for sig in SIGNALS:
            req_file = TEMP_DIR / f"request_{sig}.json"
            if req_file.exists():
                print(f"SIGNAL: {sig}", flush=True)
                try:
                    req = json.loads(req_file.read_text(encoding="utf-8"))
                    prompt = req.get("prompt", "")
                except Exception as exc:
                    prompt = ""
                    print(f"REQUEST_READ_ERROR ({sig}): {exc}", flush=True)

                response = _run_claude(prompt, sig)
                (TEMP_DIR / f"{sig}.txt").write_text(response, encoding="utf-8")
                print(f"RESP_WRITTEN: {sig}.txt", flush=True)
                break  # 한 번에 하나만 처리

    print("RUNNER_TIMEOUT", flush=True)


if __name__ == "__main__":
    main()
