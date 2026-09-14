"""Airflow scheduler heartbeat watchdog.

Checks the scheduler health and restarts only airflow-scheduler when the
scheduler heartbeat is stale. Intended for Windows Task Scheduler.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / ".tmp" / "scheduler_watchdog"
SCHEDULER_SERVICE = "airflow-scheduler"
SCHEDULER_CONTAINER = "airflow-airflow-scheduler-1"
WEBSERVER_CONTAINER = "airflow-airflow-webserver-1"
AIRFLOW_BIN = "/home/airflow/.local/bin/airflow"


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / "airflow_scheduler_watchdog.log"
    handlers: list[logging.Handler] = [logging.FileHandler(log_path, encoding="utf-8")]
    if "--console" in sys.argv:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=handlers,
    )


logger = logging.getLogger(__name__)


def subprocess_window_kwargs() -> dict:
    if sys.platform != "win32":
        return {}

    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {
        "creationflags": subprocess.CREATE_NO_WINDOW,
        "startupinfo": startupinfo,
    }


def run_cmd(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    logger.info("실행: %s", " ".join(args))
    try:
        return subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            encoding="utf-8",
            errors="replace",
            text=True,
            timeout=timeout,
            check=False,
            **subprocess_window_kwargs(),
        )
    except subprocess.TimeoutExpired as exc:
        logger.error("Command timed out after %ss: %s", timeout, args)
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        return subprocess.CompletedProcess(args, 124, output, "timeout")
    except OSError as exc:
        logger.error("Command unavailable: %s", exc)
        return subprocess.CompletedProcess(args, 127, "", str(exc))


def log_result(result: subprocess.CompletedProcess[str]) -> None:
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if stdout.strip():
        logger.info("stdout: %s", stdout.strip())
    if stderr.strip():
        logger.warning("stderr: %s", stderr.strip())
    logger.info("exit_code=%s", result.returncode)


def airflow_jobs_check() -> bool:
    result = run_cmd(
        [
            "docker",
            "exec",
            SCHEDULER_CONTAINER,
            "bash",
            "-lc",
            f"{AIRFLOW_BIN} jobs check --job-type SchedulerJob",
        ],
        timeout=90,
    )
    log_result(result)
    return result.returncode == 0


def web_health() -> tuple[bool, dict]:
    try:
        with urllib.request.urlopen("http://localhost:8080/health", timeout=5) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (OSError, urllib.error.URLError) as exc:
        logger.warning("health 요청 실패: %s", exc)
        return False, {}

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("health 응답 JSON 파싱 실패")
        return False, {}

    if not isinstance(payload, dict):
        return False, {}
    return all((payload.get(key) or {}).get("status") == "healthy"
               for key in ("scheduler", "metadatabase")), payload


def heartbeat_age_seconds(payload: dict) -> float | None:
    raw = ((payload.get("scheduler") or {}).get("latest_scheduler_heartbeat"))
    if not raw:
        return None
    try:
        heartbeat = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if heartbeat.tzinfo is None:
            return None
    except (ValueError, TypeError, AttributeError):
        return None
    now = datetime.now(timezone.utc)
    return max(0.0, (now - heartbeat).total_seconds())


def recovery_lock():
    from filelock import FileLock
    root = PROJECT_ROOT / ".tmp"
    root.mkdir(parents=True, exist_ok=True)
    return FileLock(str(root / "airflow_recovery.lock"), timeout=0)


def restart_scheduler() -> bool:
    with recovery_lock():
        result = run_cmd(["docker", "compose", "restart", SCHEDULER_SERVICE], timeout=180)
        log_result(result)
        return result.returncode == 0


def wait_until_healthy(wait_seconds: int, poll_seconds: int) -> bool:
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        jobs_ok = airflow_jobs_check()
        health_ok, payload = web_health()
        age = heartbeat_age_seconds(payload)
        logger.info("재확인: jobs_ok=%s health_ok=%s heartbeat_age=%s", jobs_ok, health_ok, age)
        if jobs_ok and health_ok:
            return True
        time.sleep(poll_seconds)
    return False


def list_import_errors() -> bool:
    result = run_cmd(
        [
            "docker",
            "exec",
            SCHEDULER_CONTAINER,
            "bash",
            "-lc",
            f"{AIRFLOW_BIN} dags list-import-errors",
        ],
        timeout=120,
    )
    log_result(result)
    return result.returncode == 0 and "No data found" in result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description="Restart stale Airflow scheduler only.")
    parser.add_argument("--stale-minutes", type=int, default=10)
    parser.add_argument("--wait-seconds", type=int, default=180)
    parser.add_argument("--poll-seconds", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--console", action="store_true", help="Also write logs to stdout.")
    args = parser.parse_args()

    setup_logging()
    engine = run_cmd(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15)
    if engine.returncode != 0:
        logger.error("Docker engine unavailable; scheduler restart cannot recover this failure")
        return 3
    health_ok, payload = web_health()
    age = heartbeat_age_seconds(payload)
    stale = age is None or age > args.stale_minutes * 60
    logger.info("1차 판정: health_ok=%s stale=%s heartbeat_age=%s", health_ok, stale, age)

    if health_ok and not stale:
        logger.info("scheduler 정상")
        return 0

    jobs_ok = airflow_jobs_check()
    logger.info("2차 판정: jobs_ok=%s health_ok=%s stale=%s heartbeat_age=%s", jobs_ok, health_ok, stale, age)

    if args.dry_run:
        logger.warning("dry-run: scheduler 재시작 필요")
        return 2

    free_gb = shutil.disk_usage(PROJECT_ROOT).free / (1024 ** 3)
    if free_gb < 10:
        logger.error("Insufficient host disk space: %.2f GiB; automatic restart withheld", free_gb)
        return 4

    if jobs_ok and not payload:
        logger.error("Scheduler job healthy but webserver unavailable; defer to host watchdog")
        return 3

    logger.warning("scheduler 비정상 감지, %s만 재시작", SCHEDULER_SERVICE)
    if not restart_scheduler():
        return 1

    if not wait_until_healthy(args.wait_seconds, args.poll_seconds):
        logger.error("scheduler 재시작 후 정상화 실패")
        return 1

    if not list_import_errors():
        logger.error("DAG import error 확인 실패 또는 import error 존재")
        return 1

    logger.info("scheduler 정상화 완료")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
