from pathlib import Path

from config import WORKSPACE_ROOT


INSTRUCTION_FILE_CANDIDATES = [
    "AGENTS.md",
    "AIGENDS.MD",
    "AIGEND.MD",
    "CLAUDE.md",
    "CALUDE.MD",
    "CALUDE.ME",
]


def load_workspace_instructions() -> str:
    root = Path(WORKSPACE_ROOT).resolve()
    loaded = []

    for name in INSTRUCTION_FILE_CANDIDATES:
        path = root / name
        if not path.exists():
            continue

        try:
            content = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            loaded.append(f"[{name} unreadable: {exc}]")
            continue

        if content:
            loaded.append(f"[{name}]\n{content}")

    if not loaded:
        return ""

    return "\n\nWorkspace instruction files:\n" + "\n\n".join(loaded)


SYSTEM_PROMPT = f"""
You are a local coding agent.

Goals:
- Understand and complete the user's request.
- Use tools when needed.
- Reply using exactly one of the formats below.

Response formats:

1) Final answer:
FINAL: message to show the user

2) Read a file:
ACTION: read_file
PATH: relative/path

3) Write a file:
ACTION: write_file
PATH: relative/path
CONTENT:
```text
file contents
```

4) Run a command:
ACTION: run_command
COMMAND: command to run

Rules:
- Output only one ACTION at a time.
- Use relative paths only.
- If unsure, inspect files first with read_file.
- Be conservative before running commands.
- When the task is complete, respond with FINAL.
{load_workspace_instructions()}
"""
