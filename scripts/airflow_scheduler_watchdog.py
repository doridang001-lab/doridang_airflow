"""Airflow scheduler heartbeat watchdog.

Checks the scheduler health and restarts only airflow-scheduler when the
scheduler heartbeat is stale. Intended for Windows Task Scheduler.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
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


def run_cmd(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    logger.info("실행: %s", " ".join(args))
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        text=True,
        timeout=timeout,
        check=False,
    )


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
    result = run_cmd(
        [
            "docker",
            "exec",
            WEBSERVER_CONTAINER,
            "bash",
            "-lc",
            "curl -s http://localhost:8080/health",
        ],
        timeout=30,
    )
    log_result(result)
    if result.returncode != 0:
        return False, {}

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("health 응답 JSON 파싱 실패")
        return False, {}

    scheduler = payload.get("scheduler") or {}
    return scheduler.get("status") == "healthy", payload


def heartbeat_age_seconds(payload: dict) -> float | None:
    raw = ((payload.get("scheduler") or {}).get("latest_scheduler_heartbeat"))
    if not raw:
        return None
    heartbeat = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return max(0.0, (now - heartbeat).total_seconds())


def restart_scheduler() -> bool:
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
    jobs_ok = airflow_jobs_check()
    health_ok, payload = web_health()
    age = heartbeat_age_seconds(payload)
    stale = age is None or age > args.stale_minutes * 60
    logger.info("판정: jobs_ok=%s health_ok=%s stale=%s heartbeat_age=%s", jobs_ok, health_ok, stale, age)

    if health_ok and not stale:
        logger.info("scheduler 정상")
        return 0

    if args.dry_run:
        logger.warning("dry-run: scheduler 재시작 필요")
        return 2

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
