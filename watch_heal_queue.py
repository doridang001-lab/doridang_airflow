"""
WSL?먯꽌 ?ㅽ뻾:
nohup python /mnt/c/airflow/watch_heal_queue.py >> /tmp/heal_watch.log 2>&1 &
"""

import hashlib
import json
import logging
import os
import shlex
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


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
TASK_STATE_PATH = Path(os.getenv("HEAL_TASK_STATE_PATH", "/mnt/c/airflow/logs/heal_task_state.json"))
POLL_INTERVAL = int(os.getenv("HEAL_QUEUE_POLL_INTERVAL", "60"))
CODEX_COMMAND = os.getenv("CODEX_COMMAND", "codex")
CODEX_MODEL = os.getenv("CODEX_MODEL", "codex-spark")
CODEX_WORKDIR = os.getenv("CODEX_WORKDIR", "/mnt/c/airflow")
CODEX_EXTRA_ARGS = os.getenv("CODEX_EXTRA_ARGS", "")
CODEX_TIMEOUT_SECONDS = int(os.getenv("CODEX_TIMEOUT_SECONDS", "1800"))
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1").rstrip("/")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

os.environ["HEAL_QUEUE_PATH"] = str(QUEUE_PATH)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
_TELEGRAM_CREDS_LOADED = False
_AUTO_EDIT_CLASSES = {"code_error"}

try:
    from modules.transform.utility.notifier import classify_failure
except Exception:
    def classify_failure(text: str) -> str:
        lowered = (text or "").lower()
        if any(token in lowered for token in ("[content_types].xml", "badzipfile", "invalid xlsx", "xlsx required parts missing")):
            return "data_file_error"
        if any(token in lowered for token in ("syntaxerror", "nameerror", "modulenotfounderror", "importerror")):
            return "code_error"
        if any(token in lowered for token in ("timeout", "selenium", "webdriver", "connection", "chrome", "crash")):
            return "transient"
        if any(token in lowered for token in ("login", "account", "password", "credential")):
            return "data_or_account"
        return "unknown"


def _airflow_variable_get(key: str) -> str:
    try:
        result = subprocess.run(
            ["docker", "exec", "airflow-airflow-scheduler-1", "airflow", "variables", "get", key],
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception as e:
        logger.warning("Airflow Variable lookup failed for %s: %s", key, e)
    return ""


def _ensure_telegram_creds() -> tuple[str, str]:
    global _BOT_TOKEN, _CHAT_ID, _TELEGRAM_CREDS_LOADED

    if _BOT_TOKEN and _CHAT_ID:
        return _BOT_TOKEN, _CHAT_ID
    if _TELEGRAM_CREDS_LOADED:
        return _BOT_TOKEN, _CHAT_ID

    _TELEGRAM_CREDS_LOADED = True
    _BOT_TOKEN = _BOT_TOKEN or _airflow_variable_get("TELEGRAM_BOT_TOKEN")
    _CHAT_ID = _CHAT_ID or _airflow_variable_get("TELEGRAM_CHAT_ID")
    return _BOT_TOKEN, _CHAT_ID


def _quote(value) -> str:
    return urllib.parse.quote(str(value), safe="")


def _auth_header() -> str:
    import base64

    raw = f"{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request(url: str, accept: str = "application/json") -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": _auth_header(),
            "Accept": accept,
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def _container_log_path(dag_id, run_id, task_id, try_number) -> str:
    return (
        f"/opt/airflow/logs/dag_id={dag_id}/"
        f"run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    )


def get_task_log(dag_id, run_id, task_id, try_number) -> str:
    url = (
        f"{AIRFLOW_API_URL}/dags/{_quote(dag_id)}/dagRuns/{_quote(run_id)}"
        f"/taskInstances/{_quote(task_id)}/logs/{_quote(try_number)}"
    )
    try:
        return _request(url, accept="text/plain").decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Airflow REST log lookup failed, trying docker fallback: %s", e)

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
        logger.warning("docker log fallback failed: %s", result.stderr.strip())
    except Exception as e:
        logger.warning("docker log fallback error: %s", e)
    return ""


def _load_queue_rows() -> list[tuple[str, object]]:
    if not QUEUE_PATH.exists():
        return []
    rows: list[tuple[str, object]] = []
    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            try:
                rows.append(("json", json.loads(stripped)))
            except json.JSONDecodeError:
                rows.append(("raw", stripped))
    return rows


def _write_queue_rows(rows: list[tuple[str, object]]) -> None:
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = QUEUE_PATH.with_suffix(QUEUE_PATH.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for kind, row in rows:
            if kind == "json":
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            else:
                f.write(f"{row}\n")
    tmp_path.replace(QUEUE_PATH)


def claim_heal_task(dag_id, run_id, task_id, claimed_by: str, try_number=None, updates: dict | None = None) -> bool:
    rows = _load_queue_rows()
    if not rows:
        return False

    claimed = False
    for kind, row in rows:
        if kind != "json" or claimed or not isinstance(row, dict):
            continue
        if (
            row.get("dag_id") == dag_id
            and row.get("run_id") == run_id
            and row.get("task_id") == task_id
            and (try_number is None or row.get("try_number") == try_number)
            and row.get("claimed_by") is None
        ):
            row["claimed_by"] = claimed_by
            row["claimed_at"] = datetime.now(timezone.utc).isoformat()
            if updates:
                row.update(updates)
            claimed = True

    if not claimed:
        return False

    _write_queue_rows(rows)
    return True


def send_telegram(text: str) -> None:
    token, chat_id = _ensure_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing")
        return
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        logger.warning("Telegram send failed: %s", e)


def _read_first_unclaimed() -> dict | None:
    if not QUEUE_PATH.exists():
        return None
    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("heal_queue JSON parse error: %s", line[:200])
                continue
            if entry.get("claimed_by") is None:
                return entry
    return None


def _codex_args(prompt: str) -> list[str]:
    args = shlex.split(CODEX_COMMAND) + ["exec", "--model", CODEX_MODEL]
    if CODEX_EXTRA_ARGS:
        args.extend(shlex.split(CODEX_EXTRA_ARGS))
    args.append(prompt)
    return args


def _run_codex(prompt: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["CODEX_WORKDIR"] = CODEX_WORKDIR
    return subprocess.run(
        _codex_args(prompt),
        check=False,
        timeout=CODEX_TIMEOUT_SECONDS,
        cwd=CODEX_WORKDIR,
        env=env,
    )


def _build_import_signature(entry: dict) -> str:
    stack_lines = [
        line.strip()
        for line in (entry.get("stacktrace") or "").splitlines()
        if line.strip()
    ]
    signature_source = json.dumps(
        {
            "kind": "import_error",
            "dag_id": entry.get("dag_id", ""),
            "filename": entry.get("filename", ""),
            "filepath": entry.get("filepath", ""),
            "error": (entry.get("error") or "").splitlines()[:1],
            "stacktrace": stack_lines[:1],
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(signature_source.encode("utf-8")).hexdigest()


def _task_signature(entry: dict, log_text: str = "") -> str:
    source = json.dumps(
        {
            "kind": "task_failure",
            "dag_id": entry.get("dag_id", ""),
            "task_id": entry.get("task_id", ""),
            "failure_class": entry.get("failure_class", ""),
            "error": (entry.get("error") or "").splitlines()[:2],
            "log": (log_text or "").splitlines()[-20:],
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _load_task_state() -> dict:
    if not TASK_STATE_PATH.exists():
        return {}
    try:
        with TASK_STATE_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except Exception as e:
        logger.warning("failed to load task state: %s", e)
        return {}


def _save_task_state(state: dict) -> None:
    TASK_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TASK_STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _already_attempted_today(signature: str) -> bool:
    state = _load_task_state()
    item = state.get(signature) or {}
    return item.get("date") == _today_key() and int(item.get("attempts", 0)) >= 1


def _mark_attempt(signature: str, entry: dict) -> None:
    state = _load_task_state()
    current = state.get(signature) or {}
    attempts = int(current.get("attempts", 0))
    state[signature] = {
        "date": _today_key(),
        "attempts": attempts + 1,
        "dag_id": entry.get("dag_id"),
        "task_id": entry.get("task_id"),
        "failure_class": entry.get("failure_class"),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }
    _save_task_state(state)


def _classify_task_entry(entry: dict, log_text: str) -> str:
    combined = "\n".join([str(entry.get("error") or ""), log_text or ""])
    detected = classify_failure(combined)
    queued = entry.get("failure_class")
    if detected == "data_file_error":
        return detected
    if queued in {"transient", "data_or_account"}:
        return queued
    return detected


def _send_skip_notification(entry: dict, failure_class: str, reason: str) -> None:
    send_telegram(
        "[자동복구] 코드 자동수정 생략\n"
        f"dag_id={entry.get('dag_id')}\n"
        f"task_id={entry.get('task_id')}\n"
        f"run_id={entry.get('run_id')}\n"
        f"오류분류={failure_class}\n"
        f"생략이유={reason}\n"
        "다음조치=재시도 또는 수동확인"
    )


def _send_data_file_notification(entry: dict) -> None:
    send_telegram(
        "[자동복구] 데이터 파일 오류 감지\n"
        f"dag_id={entry.get('dag_id')}\n"
        f"task_id={entry.get('task_id')}\n"
        f"run_id={entry.get('run_id')}\n"
        "오류분류=data_file_error\n"
        "다음조치=깨진 xlsx 격리 후 동일 DAG 재실행"
    )


def _build_prompt(entry: dict, log_text: str) -> str:
    if entry.get("kind") == "import_error":
        return (
            "Airflow DAG import 오류를 처리하세요.\n"
            "목표: 원인을 진단하고, 가능하면 가장 작은 안전한 수정만 적용한 뒤 결과를 보고하세요.\n"
            "최종 텔레그램 보고는 반드시 쉽고 짧은 한국어로 작성하세요.\n\n"
            "역할 분리 규칙:\n"
            "- Codex auto-heal은 Airflow DAG import/task 실패 복구만 담당합니다.\n"
            "- Telegram/Claude 루프 파일은 해당 오류의 직접 원인이 아니면 수정하지 마세요.\n"
            "- 사용자의 일반 질문이나 보고서 작성은 Claude 루프 담당으로 남겨두세요.\n\n"
            f"dag_id={entry.get('dag_id', '<unknown>')}\n"
            f"filename={entry.get('filename', '<unknown>')}\n"
            f"filepath={entry.get('filepath', '<unknown>')}\n"
            f"signature={entry.get('signature', _build_import_signature(entry))}\n"
            f"error={entry.get('error', '')}\n\n"
            "--- import error log ---\n"
            f"{log_text}\n\n"
            "필수 지시:\n"
            "1. 원인을 먼저 파악하세요.\n"
            "2. DAG 또는 module 파일에 최소 수정만 하세요.\n"
            "3. 가능하면 가장 작은 import 검증을 실행하세요.\n"
            "4. modules.transform.utility.notifier.send_telegram()으로 아래 형식의 한글 보고를 보내세요.\n\n"
            "[자동복구 결과]\n"
            "상태: 성공/실패\n"
            "원인: <쉬운 한글 요약>\n"
            "수정파일: <경로 또는 없음>\n"
            "다음조치: <재실행/수동확인 등>\n"
        )

    return (
        "Airflow DAG task 실패를 처리하세요.\n"
        "최종 텔레그램 보고는 반드시 쉽고 짧은 한국어로 작성하세요.\n\n"
        "역할 분리 규칙:\n"
        "- Codex auto-heal은 Airflow DAG import/task 실패 복구만 담당합니다.\n"
        "- Telegram/Claude 루프 파일은 해당 오류의 직접 원인이 아니면 수정하지 마세요.\n"
        "- 사용자의 일반 질문이나 보고서 작성은 Claude 루프 담당으로 남겨두세요.\n\n"
        f"dag_id={entry['dag_id']}\n"
        f"task_id={entry['task_id']}\n"
        f"run_id={entry['run_id']}\n"
        f"try_number={entry['try_number']}\n"
        f"error={entry.get('error', '')}\n\n"
        f"--- log ---\n{log_text}\n\n"
        "필수 지시:\n"
        "1. 원인을 먼저 파악하세요.\n"
        "2. 필요한 경우에만 최소 수정하세요.\n"
        "3. modules.transform.utility.airflow_api.trigger_dag(dag_id) 재실행이 적절한지 판단하세요.\n"
        "4. modules.transform.utility.notifier.send_telegram()으로 아래 형식의 한글 보고를 보내세요.\n\n"
        "[자동복구 결과]\n"
        "상태: 성공/실패\n"
        "원인: <쉬운 한글 요약>\n"
        "수정파일: <경로 또는 없음>\n"
        "다음조치: <새 run_id 또는 수동조치>\n"
    )


def _claim_import_entry(signature: str) -> bool:
    if not QUEUE_PATH.exists():
        return False

    updated_rows: list[object] = []
    claimed = False

    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                updated_rows.append(line.rstrip("\n"))
                continue

            if (
                isinstance(row, dict)
                and row.get("kind") == "import_error"
                and row.get("signature") == signature
                and row.get("claimed_by") is None
            ):
                row["claimed_by"] = "codex"
                claimed = True
            updated_rows.append(row)

    if not claimed:
        return False

    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = QUEUE_PATH.with_suffix(QUEUE_PATH.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for row in updated_rows:
            if isinstance(row, dict):
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            else:
                f.write(f"{row}\n")
    tmp_path.replace(QUEUE_PATH)
    return True


def _send_import_start_notification(entry: dict) -> None:
    send_telegram(
        "[자동복구] DAG import 오류 감지\n"
        f"dag_id={entry.get('dag_id', '<unknown>')}\n"
        f"파일={entry.get('filename', '<unknown>')}\n"
        f"signature={entry.get('signature', _build_import_signature(entry))}\n"
        f"오류={(entry.get('error') or '')[:300]}"
    )


def process_once() -> bool:
    entry = _read_first_unclaimed()
    if not entry:
        return False

    if entry.get("kind") == "import_error":
        signature = entry.get("signature") or _build_import_signature(entry)
        if not _claim_import_entry(signature):
            return False

        entry["signature"] = signature
        _send_import_start_notification(entry)

        log_text = (entry.get("stacktrace") or entry.get("error") or "").strip()
        prompt = _build_prompt(entry, log_text)
        try:
            result = _run_codex(prompt)
            if result.returncode != 0:
                send_telegram(
                    "[자동복구] Codex 실행 실패(import 오류)\n"
                    f"signature={signature}\n"
                    f"returncode={result.returncode}"
                )
        except subprocess.TimeoutExpired:
            send_telegram(
                "[자동복구] Codex 시간초과(import 오류)\n"
                f"signature={signature}\n"
                f"제한시간초={CODEX_TIMEOUT_SECONDS}\n"
                "수동확인필요=true"
            )
        except Exception as e:
            send_telegram(
                "[자동복구] Codex 실행 오류(import 오류)\n"
                f"signature={signature}\n"
                f"오류={e}"
            )
        return True

    dag_id = entry.get("dag_id")
    run_id = entry.get("run_id")
    task_id = entry.get("task_id")
    try_number = entry.get("try_number")
    if not all([dag_id, run_id, task_id, try_number]):
        logger.warning("heal_queue ?꾩닔 ?꾨뱶 ?꾨씫: %s", entry)
        return False

    log_text = get_task_log(dag_id, run_id, task_id, try_number)
    failure_class = _classify_task_entry(entry, log_text)
    entry["failure_class"] = failure_class
    signature = _task_signature(entry, log_text)

    if entry.get("pending_retry"):
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "pending_retry", "signature": signature},
        ):
            _send_skip_notification(entry, failure_class, "pending_retry")
            return True
        return False

    if failure_class == "data_file_error":
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-data-file",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "data_file_error", "signature": signature},
        ):
            _send_data_file_notification(entry)
            return True
        return False

    if failure_class not in _AUTO_EDIT_CLASSES:
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "not_code_error", "signature": signature},
        ):
            _send_skip_notification(entry, failure_class, "not_code_error")
            return True
        return False

    if _already_attempted_today(signature):
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "duplicate_signature_today", "signature": signature},
        ):
            _send_skip_notification(entry, failure_class, "duplicate_signature_today")
            return True
        return False

    if not claim_heal_task(
        dag_id,
        run_id,
        task_id,
        "codex",
        try_number=try_number,
        updates={"failure_class": failure_class, "signature": signature},
    ):
        return False

    _mark_attempt(signature, entry)
    prompt = _build_prompt(entry, log_text)
    try:
        result = _run_codex(prompt)
        if result.returncode != 0:
            send_telegram(
                "[자동복구] Codex 실행 실패 - 수동확인 필요\n"
                f"dag_id={dag_id}\n"
                f"task_id={task_id}\n"
                f"run_id={run_id}\n"
                f"returncode={result.returncode}"
            )
    except subprocess.TimeoutExpired:
        send_telegram(
            "[자동복구] Codex 시간초과 - 수동확인 필요\n"
            f"dag_id={dag_id}\n"
            f"task_id={task_id}\n"
            f"run_id={run_id}\n"
            f"제한시간초={CODEX_TIMEOUT_SECONDS}"
        )
    except Exception as e:
        send_telegram(
            "[자동복구] Codex 실행 오류\n"
            f"dag_id={dag_id}\n"
            f"task_id={task_id}\n"
            f"run_id={run_id}\n"
            f"오류={e}"
        )
    return True


def main() -> None:
    logger.info("watch_heal_queue start: %s", QUEUE_PATH)
    logger.info("CODEX_WORKDIR=%s", CODEX_WORKDIR)
    logger.info("CODEX_TIMEOUT_SECONDS=%s", CODEX_TIMEOUT_SECONDS)
    logger.info("started_at=%s", datetime.now(timezone.utc).isoformat())
    while True:
        try:
            process_once()
        except Exception as e:
            logger.exception("heal_queue 泥섎━ ?ㅽ뙣: %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
