"""Unified request runner using Claude CLI.

Claude Code runs this ONCE. It loops internally:
    1. Watch for request_{type}.json files
    2. Read prompt from request file
    3. Call `claude -p` directly
    4. Write {type}.txt for server.py to consume
    5. Exit when export_complete is detected

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
SIGNALS = ["questions", "prd", "spec", "trd", "child", "chat"]


def _fallback_response(sig: str, error_text: str) -> str:
    if sig == "questions":
        return json.dumps(
            [
                {
                    "id": "q_limit_notice",
                    "title": "Claude 사용량 한도에 도달했습니다",
                    "hint": error_text,
                    "type": "textarea",
                    "required": False,
                },
                {
                    "id": "q_additional",
                    "title": "필요하면 여기서 직접 요구사항을 적고 일반 대화로 이어갈 수 있습니다",
                    "hint": "6pm 이후 다시 시도하거나, 지금은 채팅으로 바로 PRD 초안을 요청하세요.",
                    "type": "textarea",
                    "required": False,
                },
            ],
            ensure_ascii=False,
        )
    if sig == "spec":
        return json.dumps({"requirements": [], "_raw": error_text}, ensure_ascii=False)
    if sig == "child":
        return json.dumps([], ensure_ascii=False)
    return f"Claude CLI error: {error_text}"


def _run_claude(prompt: str, sig: str) -> str:
    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    claude_exe = shutil.which("claude.cmd") or shutil.which("claude")
    if not claude_exe:
        error_text = "Claude CLI not found in PATH"
        print(f"CLAUDE_ERROR ({sig}): {error_text}", flush=True)
        return _fallback_response(sig, error_text)

    result = subprocess.run(
        [
            claude_exe,
            "-p",
            "--output-format",
            "text",
            "--permission-mode",
            "bypassPermissions",
            prompt,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )
    if result.returncode == 0:
        return result.stdout

    error_text = (result.stderr or result.stdout or "unknown error").strip()
    print(f"CLAUDE_ERROR ({sig}): {error_text[:300]}", flush=True)
    return _fallback_response(sig, error_text)


def main():
    if len(sys.argv) < 2:
        print("Usage: runner.py <PORT>", flush=True)
        sys.exit(1)

    port = sys.argv[1]
    print(f"RUNNER_START: port={port}", flush=True)

    for _ in range(1800):  # 30분 타임아웃
        time.sleep(1)

        # export_complete 체크
        if (TEMP_DIR / "export_complete").exists():
            print("RUNNER_DONE: export_complete", flush=True)
            sys.exit(0)

        # 시그널 감지
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

                response_text = _run_claude(prompt, sig)
                resp_file = TEMP_DIR / f"{sig}.txt"
                resp_file.write_text(response_text, encoding="utf-8")
                print(f"RESP_WRITTEN: {resp_file.name}", flush=True)
                break  # 한 번에 하나만 처리 후 다시 루프

    print("RUNNER_TIMEOUT", flush=True)


if __name__ == "__main__":
    main()
