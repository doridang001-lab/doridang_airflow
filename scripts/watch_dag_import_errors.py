#!/usr/bin/env python3
"""Watch Airflow DAG import errors and enqueue unique import_error items."""

import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv(Path("/mnt/c/airflow/.env"))

QUEUE_PATH = Path(os.getenv("HEAL_QUEUE_PATH", "/mnt/c/airflow/logs/heal_queue.jsonl"))
STATE_PATH = Path(os.getenv("IMPORT_ERROR_STATE_PATH", "/mnt/c/airflow/logs/heal_import_error_state.json"))
POLL_INTERVAL = int(os.getenv("IMPORT_ERROR_POLL_INTERVAL", "60"))
AIRFLOW_CMD = os.getenv("AIRFLOW_CMD", "airflow")
DOCKER_AIRFLOW_CONTAINER = os.getenv("AIRFLOW_SCHEDULER_CONTAINER", "airflow-airflow-scheduler-1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _list_import_errors_json() -> list[dict]:
    """Return list of dict from `airflow dags list-import-errors`."""
    commands: list[list[str]] = []
    if shutil.which(AIRFLOW_CMD):
        commands.append([AIRFLOW_CMD, "dags", "list-import-errors", "--output", "json"])
    commands.append(["docker", "exec", DOCKER_AIRFLOW_CONTAINER, AIRFLOW_CMD, "dags", "list-import-errors", "--output", "json"])

    for cmd in commands:
        try:
            result = _run(cmd)
            if result.returncode != 0:
                LOGGER.warning("list-import-errors command failed: %s", " ".join(cmd))
                continue
            text = (result.stdout or "").strip()
            if not text or "No data found" in text:
                return []
            data = json.loads(text)
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
        except json.JSONDecodeError as e:
            LOGGER.warning("list-import-errors output parse failed (%s): %s", " ".join(cmd), e)
        except Exception as e:
            LOGGER.warning("list-import-errors command error (%s): %s", " ".join(cmd), e)
    return []


def _signature(item: dict) -> str:
    stack_lines = [
        line.strip()
        for line in (item.get("stacktrace") or "").splitlines()
        if line.strip()
    ]
    base = {
        "dag_id": item.get("dag_id", ""),
        "filepath": item.get("filepath", ""),
        "filename": item.get("filename", ""),
        "error": (item.get("error") or "").strip().splitlines()[:1],
        "stacktrace": stack_lines[:1],
    }
    raw = json.dumps(base, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _normalize(item: dict) -> dict:
    return {
        "kind": "import_error",
        "dag_id": item.get("dag_id", ""),
        "filename": item.get("filename", item.get("filepath", "<unknown>")),
        "filepath": item.get("filepath", item.get("filename", "<unknown>")),
        "error": (item.get("error") or "").strip(),
        "stacktrace": (item.get("stacktrace") or item.get("traceback") or item.get("stack_trace") or "").strip(),
        "kind_code": item.get("kind", ""),
        "ts": datetime.now(timezone.utc).isoformat(),
        "claimed_by": None,
    }


def _load_seen() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            if isinstance(payload, dict):
                return payload
            return {}
    except Exception as e:
        LOGGER.warning("failed to load state: %s", e)
        corrupt_path = STATE_PATH.with_suffix(
            STATE_PATH.suffix + f".corrupt.{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        )
        try:
            STATE_PATH.replace(corrupt_path)
            LOGGER.warning("moved corrupt state to: %s", corrupt_path)
        except Exception as move_error:
            LOGGER.warning("failed to move corrupt state: %s", move_error)
        return {}


def _save_seen(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _append_queue(entry: dict) -> None:
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with QUEUE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _collect_new_errors() -> list[dict]:
    raw_items = _list_import_errors_json()
    if not raw_items:
        return []

    seen = _load_seen()
    new_errors = []

    for item in raw_items:
        normalized = _normalize(item)
        sig = _signature(normalized)
        if sig in seen:
            continue
        normalized["signature"] = sig
        new_errors.append(normalized)
        seen[sig] = {
            "dag_id": normalized.get("dag_id", ""),
            "filename": normalized.get("filename", ""),
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "error": normalized.get("error", "")[:200],
        }

    if new_errors:
        _save_seen(seen)
    return new_errors


def _enqueue_new_errors() -> int:
    new_errors = _collect_new_errors()
    if not new_errors:
        return 0

    for entry in new_errors:
        _append_queue(entry)
        LOGGER.info(
            "queued import_error: dag_id=%s file=%s signature=%s",
            entry.get("dag_id"),
            entry.get("filename"),
            entry.get("signature"),
        )
    return len(new_errors)


def main() -> None:
    LOGGER.info("import error watcher start: queue=%s state=%s", QUEUE_PATH, STATE_PATH)
    while True:
        try:
            count = _enqueue_new_errors()
            if count:
                LOGGER.info("queued %d new import error(s)", count)
        except Exception:
            LOGGER.exception("failed to collect import errors")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
