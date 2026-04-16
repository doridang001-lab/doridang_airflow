MODEL_NAME = "qwen2.5:14b"   # 설치된 모델: gpt-oss:20b / qwen2.5:14b / qwen2.5:7b
OLLAMA_URL = "http://localhost:11434"

# 에이전트가 접근 가능한 루트 경로
WORKSPACE_ROOT = r"C:\Users\민준\workspace"

# 위험 명령 차단 목록
BLOCKED_COMMANDS = [
    "rm -rf",
    "del /f /s /q",
    "format",
    "shutdown",
    "reboot",
]
