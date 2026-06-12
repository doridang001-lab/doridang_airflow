import json
import logging
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

_BASE_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1").rstrip("/")
_AUTH = (
    os.getenv("AIRFLOW_USERNAME", "airflow"),
    os.getenv("AIRFLOW_PASSWORD", "airflow"),
)
_HEAL_QUEUE_PATH = Path(os.getenv("HEAL_QUEUE_PATH", "C:/airflow/logs/heal_queue.jsonl"))


def _quote(value) -> str:
    return urllib.parse.quote(str(value), safe="")


def _auth_header() -> str:
    import base64

    raw = f"{_AUTH[0]}:{_AUTH[1]}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request(url: str, method: str = "GET", data: dict | None = None, accept: str = "application/json"):
    body = None
    headers = {
        "Authorization": _auth_header(),
        "Accept": accept,
    }
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def _container_log_path(dag_id, run_id, task_id, try_number) -> str:
    return (
        f"/opt/airflow/logs/dag_id={dag_id}/"
        f"run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    )


def get_task_log(dag_id, run_id, task_id, try_number) -> str:
    url = (
        f"{_BASE_URL}/dags/{_quote(dag_id)}/dagRuns/{_quote(run_id)}"
        f"/taskInstances/{_quote(task_id)}/logs/{_quote(try_number)}"
    )
    try:
        return _request(url, accept="text/plain").decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Airflow REST 로그 조회 실패, docker fallback 시도: %s", e)

    log_path = _container_log_path(dag_id, run_id, task_id, try_number)
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "airflow-airflow-worker-1",
                "bash",
                "-c",
                f"cat {log_path!r}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout
        logger.warning("docker fallback 로그 조회 실패: %s", result.stderr.strip())
    except Exception as e:
        logger.warning("docker fallback 로그 조회 예외: %s", e)
    return ""


def trigger_dag(dag_id, conf=None) -> str:
    url = f"{_BASE_URL}/dags/{_quote(dag_id)}/dagRuns"
    try:
        raw = _request(url, method="POST", data={"conf": conf or {}})
        payload = json.loads(raw.decode("utf-8"))
        return payload.get("dag_run_id", "")
    except Exception as e:
        logger.error("DAG 재실행 트리거 실패: %s", e)
        return ""


def _load_queue_lines():
    if not _HEAL_QUEUE_PATH.exists():
        return []
    rows = []
    with _HEAL_QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            try:
                rows.append(("json", json.loads(stripped)))
            except json.JSONDecodeError:
                rows.append(("raw", stripped))
    return rows


def claim_heal_task(dag_id, run_id, task_id, claimed_by: str) -> bool:
    rows = _load_queue_lines()
    if not rows:
        return False

    claimed = False
    for kind, row in rows:
        if kind != "json" or claimed:
            continue
        if (
            row.get("dag_id") == dag_id
            and row.get("run_id") == run_id
            and row.get("task_id") == task_id
            and row.get("claimed_by") is None
        ):
            row["claimed_by"] = claimed_by
            claimed = True

    if not claimed:
        return False

    _HEAL_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _HEAL_QUEUE_PATH.with_suffix(_HEAL_QUEUE_PATH.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for kind, row in rows:
            if kind == "json":
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            else:
                f.write(row + "\n")
    tmp_path.replace(_HEAL_QUEUE_PATH)
    return True
