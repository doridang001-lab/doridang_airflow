from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def no_window_kwargs() -> dict:
    if sys.platform != "win32":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {
        "creationflags": subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
        "startupinfo": startupinfo,
    }


def is_healthy(port: int, timeout_seconds: int = 5) -> bool:
    url = f"http://127.0.0.1:{port}/health"
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=r"C:\airflow")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--health-timeout-seconds", type=int, default=40)
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    python = repo_root / ".venv" / "Scripts" / "pythonw.exe"
    if not python.exists():
        python = repo_root / ".venv" / "Scripts" / "python.exe"

    if not args.restart and is_healthy(args.port):
        return 0

    logs = repo_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stdout = logs / f"doridang_bot_{time.strftime('%Y%m%d')}.log"
    stderr = logs / f"doridang_bot_{time.strftime('%Y%m%d')}.err.log"

    with stdout.open("ab") as out, stderr.open("ab") as err:
        subprocess.Popen(
            [str(python), "-m", "modules.transform.doridang_bot.server"],
            cwd=repo_root,
            stdout=out,
            stderr=err,
            stdin=subprocess.DEVNULL,
            close_fds=False,
            **no_window_kwargs(),
        )

    deadline = time.time() + args.health_timeout_seconds
    while time.time() < deadline:
        if is_healthy(args.port):
            return 0
        time.sleep(2)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
