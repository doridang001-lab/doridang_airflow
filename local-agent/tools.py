from pathlib import Path
import subprocess
from config import WORKSPACE_ROOT, BLOCKED_COMMANDS

ROOT = Path(WORKSPACE_ROOT).resolve()


def safe_path(relative_path: str) -> Path:
    target = (ROOT / relative_path).resolve()
    if not str(target).startswith(str(ROOT)):
        raise ValueError("허용된 workspace 밖의 경로에는 접근할 수 없습니다.")
    return target


def read_file(relative_path: str) -> str:
    path = safe_path(relative_path)
    if not path.exists():
        return f"[ERROR] 파일이 없습니다: {relative_path}"
    return path.read_text(encoding="utf-8")


def write_file(relative_path: str, content: str) -> str:
    path = safe_path(relative_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return f"[OK] 저장 완료: {relative_path}"


def run_command(command: str) -> str:
    lowered = command.lower()
    for blocked in BLOCKED_COMMANDS:
        if blocked in lowered:
            return f"[BLOCKED] 위험 명령 차단: {command}"

    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )

    return (
        f"[exit_code] {result.returncode}\n"
        f"[stdout]\n{result.stdout}\n"
        f"[stderr]\n{result.stderr}"
    )
