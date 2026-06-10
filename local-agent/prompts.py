import os
from pathlib import Path

from config import WORKSPACE_ROOT


CODEX_FILE_CANDIDATES = [
    "AGENTS.md",
    "AIGENDS.MD",
    "AIGEND.MD",
]

CLAUDE_FILE_CANDIDATES = [
    "CLAUDE.md",
    "CALUDE.MD",
    "CALUDE.ME",
]

INSTRUCTION_MODE = os.getenv("LOCAL_AGENT_INSTRUCTION_MODE", "codex_first").strip().lower()


def _instruction_file_candidates() -> list[str]:
    if INSTRUCTION_MODE == "codex_only":
        return [*CODEX_FILE_CANDIDATES]
    if INSTRUCTION_MODE == "shared":
        return [*CODEX_FILE_CANDIDATES, *CLAUDE_FILE_CANDIDATES]
    if INSTRUCTION_MODE == "claude_first":
        return [*CLAUDE_FILE_CANDIDATES, *CODEX_FILE_CANDIDATES]
    return [*CODEX_FILE_CANDIDATES, *CLAUDE_FILE_CANDIDATES]


def load_workspace_instructions() -> str:
    root = Path(WORKSPACE_ROOT).resolve()
    loaded = []

    for name in _instruction_file_candidates():
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

    header = (
        "\n\nWorkspace instruction mode: "
        f"{INSTRUCTION_MODE}\nWorkspace instruction files:\n"
    )
    return header + "\n\n".join(loaded)


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
