"""Host-side Docker/Airflow recovery with bounded commands and restart cooldown."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from airflow_scheduler_watchdog import PROJECT_ROOT, run_cmd, web_health, recovery_lock

STATE_DIR = PROJECT_ROOT / ".tmp" / "docker_watchdog"
SERVICES = ("postgres", "doridang-postgres", "redis", "airflow-webserver",
            "airflow-scheduler", "airflow-worker", "airflow-backfill-worker", "airflow-triggerer", "airflow-dashboard")
logger = logging.getLogger(__name__)


def decision(state: dict, healthy: bool, free_gb: float, now: float) -> str:
    if healthy:
        state["failures"] = 0
        return "healthy" if free_gb >= 30 else "disk_warning"
    state["failures"] = state.get("failures", 0) + 1
    if free_gb < 10:
        return "disk_blocked"
    if state["failures"] < 3:
        return "observe"
    if now - state.get("last_restart", 0) < 1800:
        return "cooldown"
    return "recover"


def container_states() -> dict:
    result = run_cmd(["docker", "compose", "ps", "--all", "--format", "json"], timeout=20)
    if result.returncode:
        return {}
    try:
        raw = result.stdout.strip()
        rows = json.loads(raw) if raw.startswith("[") else [json.loads(line) for line in raw.splitlines()]
        return {row["Service"]: row for row in rows}
    except (ValueError, KeyError, TypeError):
        logger.exception("Invalid container status")
        return {}


def worker_responds(service="airflow-worker") -> bool:
    result = run_cmd([
        "docker", "compose", "exec", "-T", service, "bash", "-lc",
        '/home/airflow/.local/bin/celery --app airflow.providers.celery.executors.celery_executor.app '
        'inspect ping --timeout 5 --destination "celery@$HOSTNAME"',
    ], timeout=45)
    return (result.returncode == 0 and "pong" in (result.stdout or "")
            and "1 node online" in (result.stdout or ""))


def run(dry_run: bool = False) -> int:
    state_path = STATE_DIR / "state.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            state = {}
    except (OSError, ValueError):
        state = {}
    engine_ok = run_cmd(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15).returncode == 0
    rows = container_states() if engine_ok else {}
    web_ok, payload = web_health() if engine_ok else (False, {})
    bad = [name for name in SERVICES if rows.get(name, {}).get("State") != "running"
           or rows.get(name, {}).get("Health") != "healthy"]
    for worker in ("airflow-worker", "airflow-backfill-worker"):
        if worker in bad and rows.get(worker, {}).get("State") == "running" and worker_responds(worker):
            logger.warning("Worker responds despite slow Docker healthcheck: %s", worker)
            bad.remove(worker)
    healthy = engine_ok and web_ok and not bad
    free_gb = shutil.disk_usage(PROJECT_ROOT).free / 1024 ** 3
    now = time.time()
    if engine_ok and free_gb < 30 and now - state.get("last_trim", 0) >= 21600 and not dry_run:
        state["last_trim"] = now
        # TRIM returns only blocks the guest filesystem already considers free.
        result = run_cmd(["wsl.exe", "-d", "docker-desktop", "--", "fstrim", "-av"], timeout=90)
        logger.warning("Unused guest blocks trim exit=%s", result.returncode)
        free_gb = shutil.disk_usage(PROJECT_ROOT).free / 1024 ** 3
    if engine_ok:
        command = ["docker", "compose", "exec", "-T", "airflow-scheduler", "python",
                   "/opt/airflow/scripts/airflow_resource_control.py"]
        if not dry_run:
            command.append("--apply")
        result = run_cmd(command, timeout=45)
        logger.info("resource policy exit=%s result=%s", result.returncode, (result.stdout or "")[-1800:])
    action = decision(state, healthy, free_gb, now)
    state.update(checked_at=now, action=action, free_gb=round(free_gb, 2),
                 engine_ok=engine_ok, unhealthy_services=bad, health=payload)
    logger.info("status=%s engine=%s bad=%s free_gb=%.2f dry_run=%s", action, engine_ok, bad, free_gb, dry_run)
    if dry_run:
        return 0 if healthy else 2
    if action == "recover":
        # Persist before acting so an interrupted recovery cannot form a restart loop.
        state["last_restart"] = now
    temp = state_path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(state_path)
    if action != "recover":
        return 0 if healthy else 2
    with recovery_lock():
        return recover_services(engine_ok, rows, bad, web_ok, payload)


def recover_services(engine_ok, rows, bad, web_ok, payload):
    if not engine_ok:
        result = run_cmd(["docker", "desktop", "restart", "--timeout", "60"], timeout=70)
        if result.returncode:
            # Docker Desktop processes may survive a dead WSL VM and ignore restart.
            command = ("Get-Process -Name 'com.docker.backend','Docker Desktop','com.docker.build' "
                       "-ErrorAction SilentlyContinue | Stop-Process -Force; "
                       "Start-Process -FilePath 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe' "
                       "-WindowStyle Hidden")
            result = run_cmd(["powershell.exe", "-NoProfile", "-Command", command], timeout=20)
        logger.warning("Docker recovery requested, exit=%s; next cycle verifies health", result.returncode)
        return result.returncode
    # Never recreate volumes or run airflow-init during routine recovery.
    if any(name in bad for name in ("airflow-worker", "airflow-backfill-worker")):
        marker = run_cmd(["docker", "compose", "exec", "-T", "airflow-scheduler", "python",
                          "/opt/airflow/scripts/airflow_resource_control.py", "--apply", "--worker-restarted"], timeout=45)
        logger.info("Worker interruption evidence recorded: exit=%s", marker.returncode)
    stopped = [name for name in bad if rows.get(name, {}).get("State") != "running"]
    if stopped:
        return run_cmd(["docker", "compose", "up", "--no-deps", "--no-build", "-d", *stopped], timeout=120).returncode
    if not bad and not web_ok:
        bad = ["airflow-scheduler"] if payload else ["airflow-webserver"]
    return run_cmd(["docker", "compose", "restart", *bad], timeout=120).returncode


def main() -> int:
    import msvcrt

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[RotatingFileHandler(STATE_DIR / "watchdog.log", maxBytes=2_000_000,
                                                      backupCount=3, encoding="utf-8")])
    with (STATE_DIR / "lock").open("a+b") as lock:
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            logger.info("Another watchdog is active")
            return 0
        try:
            return run(args.dry_run)
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


if __name__ == "__main__":
    raise SystemExit(main())
