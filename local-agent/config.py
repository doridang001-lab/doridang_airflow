from pathlib import Path


MODEL_NAME = "qwen2.5:14b"
OLLAMA_URL = "http://localhost:11434"

# Use the repository root that contains local-agent/.
WORKSPACE_ROOT = str(Path(__file__).resolve().parent.parent)

BLOCKED_COMMANDS = [
    "rm -rf",
    "del /f /s /q",
    "format",
    "shutdown",
    "reboot",
]
