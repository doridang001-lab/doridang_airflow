import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import requests
from config import MODEL_NAME, OLLAMA_URL
from prompts import SYSTEM_PROMPT
from tools import read_file, write_file, run_command

CHAT_URL = f"{OLLAMA_URL.rstrip('/')}/api/chat"

history = [{"role": "system", "content": SYSTEM_PROMPT}]


def call_ollama() -> str:
    try:
        res = requests.post(
            CHAT_URL,
            json={"model": MODEL_NAME, "messages": history, "stream": False},
            timeout=120,
        )
        res.raise_for_status()
        return res.json()["message"]["content"]
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Ollama 연결 실패 - {OLLAMA_URL} 에서 실행 중인지 확인하세요.")
        sys.exit(1)


def parse_response(text: str) -> dict:
    text = text.strip()

    if text.startswith("FINAL:"):
        return {"type": "final", "content": text[6:].strip()}

    action_match = re.search(r"ACTION:\s*(\w+)", text)
    if not action_match:
        return {"type": "final", "content": text}

    action = action_match.group(1).lower()

    if action == "read_file":
        path_match = re.search(r"PATH:\s*(.+)", text)
        return {
            "type": "action",
            "action": "read_file",
            "path": path_match.group(1).strip() if path_match else "",
        }

    if action == "write_file":
        path_match = re.search(r"PATH:\s*(.+)", text)
        # ```text ... ``` 또는 일반 텍스트 양쪽 처리
        content_match = re.search(r"CONTENT:\s*```(?:\w+)?\n([\s\S]+?)```", text)
        if not content_match:
            content_match = re.search(r"CONTENT:\s*\n([\s\S]+)", text)
        return {
            "type": "action",
            "action": "write_file",
            "path": path_match.group(1).strip() if path_match else "",
            "content": content_match.group(1) if content_match else "",
        }

    if action == "run_command":
        cmd_match = re.search(r"COMMAND:\s*(.+)", text)
        return {
            "type": "action",
            "action": "run_command",
            "command": cmd_match.group(1).strip() if cmd_match else "",
        }

    return {"type": "final", "content": text}


def execute_action(parsed: dict) -> str:
    action = parsed.get("action")
    try:
        if action == "read_file":
            return read_file(parsed["path"])
        if action == "write_file":
            return write_file(parsed["path"], parsed["content"])
        if action == "run_command":
            return run_command(parsed["command"])
    except ValueError as e:
        return f"[ERROR] {e}"
    return "[ERROR] 알 수 없는 ACTION"


def run_agent(user_input: str):
    history.append({"role": "user", "content": user_input})

    for step in range(10):
        print(f"\n[thinking... step {step + 1}]", flush=True)
        response = call_ollama()
        history.append({"role": "assistant", "content": response})

        parsed = parse_response(response)

        if parsed["type"] == "final":
            print(f"\n{parsed['content']}")
            return

        label = parsed.get("path") or parsed.get("command", "")
        print(f"[{parsed['action']}] {label}", flush=True)
        result = execute_action(parsed)
        preview = result[:300] + ("..." if len(result) > 300 else "")
        print(f"[result] {preview}", flush=True)

        history.append({"role": "user", "content": f"[TOOL RESULT]\n{result}"})

    print("[MAX STEPS 도달 - 작업 중단]")


def main():
    print(f"Local Agent  model={MODEL_NAME}  workspace={__import__('config').WORKSPACE_ROOT}")
    print("종료: quit / exit / Ctrl+C\n")

    while True:
        try:
            user_input = input("You> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n종료")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            break

        run_agent(user_input)


if __name__ == "__main__":
    main()
