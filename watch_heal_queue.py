"""
WSL 실행 예:
python /mnt/c/airflow/watch_heal_queue.py >> /tmp/codex_autoheal_queue.log 2>&1
"""

import hashlib
import json
import logging
import os
import signal
import shlex
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _subprocess_window_kwargs() -> dict:
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    return {
        "creationflags": subprocess.CREATE_NO_WINDOW,
        "startupinfo": startupinfo,
    }


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


_DEFAULT_RUNTIME_ROOT = Path("C:/airflow") if os.name == "nt" else Path("/mnt/c/airflow")
_load_dotenv(_DEFAULT_RUNTIME_ROOT / ".env")

QUEUE_PATH = Path(os.getenv("HEAL_QUEUE_PATH", str(_DEFAULT_RUNTIME_ROOT / "logs" / "heal_queue.jsonl")))
TASK_STATE_PATH = Path(os.getenv("HEAL_TASK_STATE_PATH", str(_DEFAULT_RUNTIME_ROOT / "logs" / "heal_task_state.json")))
HEARTBEAT_PATH = Path(
    os.getenv("AUTOHEAL_HEARTBEAT_PATH", str(QUEUE_PATH.parent / "autoheal_heartbeat.json"))
)
POLL_INTERVAL = int(os.getenv("HEAL_QUEUE_POLL_INTERVAL", "60"))
CODEX_COMMAND = os.getenv("CODEX_COMMAND", "codex")
# 비어 있으면 Codex CLI가 현재 계정에 맞는 기본 모델을 선택한다.
CODEX_MODEL = os.getenv("CODEX_MODEL", "").strip()
AIRFLOW_RUNTIME_WORKDIR = os.getenv("AIRFLOW_RUNTIME_WORKDIR", str(_DEFAULT_RUNTIME_ROOT))
AUTOHEAL_WORKTREE = os.getenv(
    "CODEX_AUTOHEAL_WORKTREE",
    "C:/tmp/airflow-autoheal" if os.name == "nt" else "/mnt/c/tmp/airflow-autoheal",
)
CODEX_WORKDIR = os.getenv("CODEX_WORKDIR", AUTOHEAL_WORKTREE)
CODEX_EXTRA_ARGS = os.getenv("CODEX_EXTRA_ARGS", "")
CODEX_TIMEOUT_SECONDS = int(os.getenv("CODEX_TIMEOUT_SECONDS", "1800"))
AUTOHEAL_BACKEND = os.getenv("CODEX_AUTOHEAL_BACKEND", "windows" if os.name == "nt" else "wsl")
CLAIM_LEASE_SECONDS = int(os.getenv("HEAL_CLAIM_LEASE_SECONDS", str(CODEX_TIMEOUT_SECONDS + 600)))
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1").rstrip("/")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

os.environ["HEAL_QUEUE_PATH"] = str(QUEUE_PATH)
os.environ["AUTOHEAL_HEARTBEAT_PATH"] = str(HEARTBEAT_PATH)

QUEUE_LOCK_PATH = QUEUE_PATH.with_suffix(QUEUE_PATH.suffix + ".lock")
WATCHER_LOCK_PATH = HEARTBEAT_PATH.with_suffix(HEARTBEAT_PATH.suffix + ".watcher.lock")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
_TELEGRAM_CREDS_LOADED = False
_AUTOHEAL_TELEGRAM_ENV = "AUTOHEAL_TELEGRAM_ENABLED"
_AUTOHEAL_TELEGRAM_MODE_ENV = "AUTOHEAL_TELEGRAM_MODE"
_AUTO_EDIT_CLASSES = {"code_error", "allocator_error", "transient", "unknown"}
# 코드 수정 대신 태스크 재실행으로 복구하는 분류. _AUTO_EDIT_CLASSES와 겹치면 안 된다.
NOT_RUN_FAILURE_CLASS = "executor_state_mismatch"
NOT_RUN_REASON = "시계 역행/스택 재시작으로 태스크가 실행되지 않음"
_NOT_RUN_TOKENS = (
    "reported that the task instance",
    "state attribute is queued",
    "dependencies not met",
    "is in the future (the current date is",
    "task is not able to be run",
    "stuck in queued",
    "in queued state for longer than",
)
_WATCHER_TOKEN = ""

try:
    from modules.transform.utility.notifier import classify_failure
except Exception:
    def classify_failure(text: str) -> str:
        lowered = (text or "").lower()
        if any(token in lowered for token in _NOT_RUN_TOKENS):
            return NOT_RUN_FAILURE_CLASS
        if any(token in lowered for token in ("[content_types].xml", "badzipfile", "invalid xlsx", "xlsx required parts missing")):
            return "data_file_error"
        if any(token in lowered for token in ("상품 슬롯 초과", "slot overflow", "manual_item_block_size", "item_id 배정 실패")):
            return "allocator_error"
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
            stdin=subprocess.DEVNULL,
            **_subprocess_window_kwargs(),
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


def _autoheal_telegram_enabled() -> bool:
    return os.getenv(_AUTOHEAL_TELEGRAM_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _autoheal_telegram_mode() -> str:
    mode = os.getenv(_AUTOHEAL_TELEGRAM_MODE_ENV, "failures_only").strip().lower()
    if mode in {"0", "false", "none", "off", "silent"}:
        return "off"
    if mode in {"1", "true", "yes", "on", "all"} or _autoheal_telegram_enabled():
        return "all"
    return "failures_only"


def _quote(value) -> str:
    return urllib.parse.quote(str(value), safe="")


def _auth_header() -> str:
    import base64

    raw = f"{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request(
    url: str,
    accept: str = "application/json",
    *,
    method: str = "GET",
    data: dict | None = None,
) -> bytes:
    headers = {
        "Authorization": _auth_header(),
        "Accept": accept,
    }
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def get_task_instance(dag_id, run_id, task_id) -> dict | None:
    url = (
        f"{AIRFLOW_API_URL}/dags/{_quote(dag_id)}/dagRuns/{_quote(run_id)}"
        f"/taskInstances/{_quote(task_id)}"
    )
    try:
        return json.loads(_request(url).decode("utf-8"))
    except Exception as e:
        logger.warning("task instance 조회 실패: dag_id=%s task_id=%s %s", dag_id, task_id, e)
        return None


def clear_task_instance(dag_id, run_id, task_id) -> bool:
    """미실행 태스크를 clear 해서 재실행시킨다. upstream_failed 하위까지 같이 되살린다."""
    from modules.transform.utility.history_admission import backfill_dags
    if dag_id in backfill_dags():
        logger.info("배민 백필 자동 복구는 전체 1건 제한을 사용하는 자원 제어기에 위임: %s", dag_id)
        return False
    url = f"{AIRFLOW_API_URL}/dags/{_quote(dag_id)}/clearTaskInstances"
    payload = {
        "dry_run": False,
        "dag_run_id": run_id,
        "task_ids": [task_id],
        "only_failed": True,
        "include_downstream": True,
        "reset_dag_runs": True,
    }
    try:
        from modules.transform.utility.safe_recovery import claim_key, ALLOW
        if task_id in ALLOW.get(dag_id, set()):
            # Variable key의 DB unique 제약으로 메모리 복구기와 중복 claim을 막는다.
            instances = json.loads(_request(f"{AIRFLOW_API_URL}/dags/{_quote(dag_id)}/dagRuns/{_quote(run_id)}/taskInstances").decode("utf-8"))
            indices = [row.get("map_index", -1) for row in (instances or {}).get("task_instances", [])
                       if row.get("task_id") == task_id and row.get("state") in ("failed", "upstream_failed")]
            for map_index in indices or [-1]:
                _request(f"{AIRFLOW_API_URL}/variables", method="POST", data={
                    "key": claim_key(dag_id, run_id, task_id, map_index), "value": "legacy_autoheal"})
        _request(url, method="POST", data=payload)
        logger.info("미실행 태스크 재실행 요청: dag_id=%s task_id=%s run_id=%s", dag_id, task_id, run_id)
        return True
    except Exception as e:
        logger.warning("태스크 clear 실패: dag_id=%s task_id=%s %s", dag_id, task_id, e)
        return False


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
            stdin=subprocess.DEVNULL,
            **_subprocess_window_kwargs(),
        )
        if result.returncode == 0:
            return result.stdout
        logger.warning("docker log fallback failed: %s", result.stderr.strip())
    except Exception as e:
        logger.warning("docker log fallback error: %s", e)
    return ""


def _parse_utc(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _claim_available(row: dict) -> bool:
    if row.get("result_status"):
        return False
    if row.get("claimed_by") is None:
        return True
    expires_at = _parse_utc(row.get("claim_expires_at"))
    return bool(expires_at and expires_at <= datetime.now(timezone.utc))


@contextmanager
def _exclusive_file_lock(path: Path, *, timeout_seconds: int = 10, stale_seconds: int = 120):
    token = uuid.uuid4().hex
    deadline = time.monotonic() + timeout_seconds
    path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"token": token, "pid": os.getpid(), "created_at": datetime.now(timezone.utc).isoformat()}, f)
            break
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
                if age > stale_seconds:
                    path.unlink(missing_ok=True)
                    continue
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError(f"lock acquisition timed out: {path}")
            time.sleep(0.1)
    try:
        yield token
    finally:
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
            if current.get("token") == token:
                path.unlink(missing_ok=True)
        except (OSError, ValueError, AttributeError):
            pass


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


def _pending_queue_count() -> int:
    return sum(
        1
        for kind, row in _load_queue_rows()
        if kind == "json" and isinstance(row, dict) and _claim_available(row)
    )


def _write_heartbeat(last_result: str = "running", last_error: str = "") -> None:
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "pid": os.getpid(),
            "backend": AUTOHEAL_BACKEND,
            "queue_path": str(QUEUE_PATH),
            "task_state_path": str(TASK_STATE_PATH),
            "code_workdir": CODEX_WORKDIR,
            "runtime_workdir": AIRFLOW_RUNTIME_WORKDIR,
            "pending_count": _pending_queue_count(),
            "last_result": last_result,
            "last_error": last_error[:500],
            "watcher_token": _WATCHER_TOKEN,
        }
        tmp_path = HEARTBEAT_PATH.with_suffix(
            HEARTBEAT_PATH.suffix + f".{os.getpid()}.{_WATCHER_TOKEN or 'startup'}.tmp"
        )
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        tmp_path.replace(HEARTBEAT_PATH)
    except Exception as e:
        logger.warning("heartbeat write failed: %s", e)


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
    with _exclusive_file_lock(QUEUE_LOCK_PATH):
        rows = _load_queue_rows()
        if not rows:
            return False

        claimed = False
        now = datetime.now(timezone.utc)
        for kind, row in rows:
            if kind != "json" or claimed or not isinstance(row, dict):
                continue
            if (
                row.get("dag_id") == dag_id
                and row.get("run_id") == run_id
                and row.get("task_id") == task_id
                and (try_number is None or row.get("try_number") == try_number)
                and _claim_available(row)
            ):
                row["claimed_by"] = claimed_by
                row["claimed_at"] = now.isoformat()
                row["claim_expires_at"] = (now + timedelta(seconds=CLAIM_LEASE_SECONDS)).isoformat()
                row.pop("result_status", None)
                row.pop("result_reported_at", None)
                if updates:
                    row.update(updates)
                claimed = True

        if not claimed:
            return False

        _write_queue_rows(rows)
        return True


def update_heal_task(dag_id, run_id, task_id, try_number=None, updates: dict | None = None) -> bool:
    with _exclusive_file_lock(QUEUE_LOCK_PATH):
        rows = _load_queue_rows()
        changed = False
        for kind, row in rows:
            if kind != "json" or changed or not isinstance(row, dict):
                continue
            if (
                row.get("dag_id") == dag_id
                and row.get("run_id") == run_id
                and row.get("task_id") == task_id
                and (try_number is None or row.get("try_number") == try_number)
            ):
                if updates:
                    row.update(updates)
                row.pop("claim_expires_at", None)
                changed = True
        if changed:
            _write_queue_rows(rows)
        return changed


def update_import_entry(signature: str, updates: dict | None = None) -> bool:
    with _exclusive_file_lock(QUEUE_LOCK_PATH):
        rows = _load_queue_rows()
        changed = False
        for kind, row in rows:
            if kind != "json" or changed or not isinstance(row, dict):
                continue
            if row.get("kind") == "import_error" and row.get("signature") == signature:
                if updates:
                    row.update(updates)
                row.pop("claim_expires_at", None)
                changed = True
        if changed:
            _write_queue_rows(rows)
        return changed


def send_telegram(text: str, *, final_failure: bool = False) -> bool:
    mode = _autoheal_telegram_mode()
    if mode == "off" or (mode == "failures_only" and not final_failure):
        logger.info(
            "autoheal telegram suppressed: mode=%s final_failure=%s",
            mode,
            final_failure,
        )
        return True
    token, chat_id = _ensure_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing")
        return False
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        return True
    except Exception as e:
        logger.warning("Telegram send failed: %s", e)
        return False


def _read_first_unclaimed() -> dict | None:
    if not QUEUE_PATH.exists():
        return None
    candidates: list[dict] = []
    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("heal_queue JSON parse error: %s", line[:200])
                continue
            if _claim_available(entry):
                candidates.append(entry)
    if not candidates:
        return None
    return max(candidates, key=_queue_entry_sort_key)


def _queue_entry_sort_key(entry: dict) -> tuple[int, str]:
    kind_priority = 1 if entry.get("kind") == "task_failure" else 0
    return kind_priority, str(entry.get("ts") or "")


def _codex_args(prompt: str) -> list[str]:
    args = shlex.split(CODEX_COMMAND, posix=os.name != "nt") + ["exec"]
    if CODEX_MODEL:
        args.extend(["--model", CODEX_MODEL])
    if CODEX_EXTRA_ARGS:
        args.extend(shlex.split(CODEX_EXTRA_ARGS))
    args.append("-")
    return args


def _git_output(workdir: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-c", f"safe.directory={Path(workdir).as_posix()}", "-C", workdir, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=30,
        stdin=subprocess.DEVNULL,
        **_subprocess_window_kwargs(),
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"git {' '.join(args)} 실패")
    return result.stdout.strip()


def _prepare_codex_workdir() -> str:
    if not Path(CODEX_WORKDIR).is_dir():
        raise RuntimeError(f"Codex 격리 워크트리 없음: {CODEX_WORKDIR}")
    runtime_head = _git_output(AIRFLOW_RUNTIME_WORKDIR, "rev-parse", "HEAD")
    current_head = _git_output(CODEX_WORKDIR, "rev-parse", "HEAD")
    dirty = _git_output(CODEX_WORKDIR, "status", "--porcelain")
    if dirty:
        logger.warning(
            "Codex 격리 워크트리에 기존 변경이 남아 있어 그대로 이어서 실행합니다: %s",
            CODEX_WORKDIR,
        )
        if current_head != runtime_head:
            logger.warning(
                "Codex 격리 워크트리가 dirty 상태라 fast-forward를 생략합니다: current=%s runtime=%s",
                current_head,
                runtime_head,
            )
        return runtime_head
    if current_head != runtime_head:
        _git_output(CODEX_WORKDIR, "merge", "--ff-only", runtime_head)
    return runtime_head


def _run_codex(prompt: str) -> subprocess.CompletedProcess:
    runtime_head = _prepare_codex_workdir()
    logger.info("Codex 격리 워크트리 준비 완료: runtime_head=%s", runtime_head)
    env = os.environ.copy()
    env["CODEX_WORKDIR"] = CODEX_WORKDIR
    kwargs = _subprocess_window_kwargs()
    if os.name == "nt":
        kwargs["creationflags"] |= subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True

    started = time.monotonic()
    proc = subprocess.Popen(
        _codex_args(prompt),
        cwd=CODEX_WORKDIR,
        env=env,
        stdin=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        **kwargs,
    )
    if proc.stdin is not None:
        try:
            proc.stdin.write(prompt)
            proc.stdin.close()
        except (BrokenPipeError, OSError) as exc:
            logger.warning("Codex prompt stdin write failed: %s", exc)
    while True:
        returncode = proc.poll()
        if returncode is not None:
            return subprocess.CompletedProcess(proc.args, returncode)

        elapsed = time.monotonic() - started
        if elapsed > CODEX_TIMEOUT_SECONDS:
            _terminate_process_tree(proc)
            raise subprocess.TimeoutExpired(proc.args, CODEX_TIMEOUT_SECONDS)

        _write_heartbeat("codex_running")
        time.sleep(min(POLL_INTERVAL, 30))


def _terminate_process_tree(proc: subprocess.Popen) -> None:
    try:
        if os.name == "nt":
            proc.terminate()
        else:
            os.killpg(proc.pid, signal.SIGTERM)
    except Exception as e:
        logger.warning("Codex terminate failed: %s", e)

    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass

    try:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(proc.pid, signal.SIGKILL)
    except Exception as e:
        logger.warning("Codex kill failed: %s", e)


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
    # 실행 시각·run_id·로그 꼬리는 같은 원인에서도 매번 달라진다. 자동복구
    # 중복 판정은 안정적인 장애 식별자만 사용해 Telegram 폭주를 막는다.
    source = json.dumps(
        {
            "kind": "task_failure",
            "dag_id": entry.get("dag_id", ""),
            "task_id": entry.get("task_id", ""),
            "failure_class": entry.get("failure_class", ""),
            "error": (entry.get("error") or "").splitlines()[:2],
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


def _already_attempted_today(signature: str, entry: dict | None = None) -> bool:
    state = _load_task_state()
    item = state.get(signature) or {}
    today = _today_key()
    if item.get("date") == today and int(item.get("attempts", 0)) >= 1:
        return True
    if not entry:
        return False

    # 서명 형식 변경 전 오늘 생성된 기록도 이어받는다. 같은 DAG·태스크·
    # 장애유형은 하루에 한 번만 Codex를 실행한다.
    incident_key = (
        entry.get("dag_id"),
        entry.get("task_id"),
        entry.get("failure_class"),
    )
    return any(
        value.get("date") == today
        and int(value.get("attempts", 0)) >= 1
        and (
            value.get("dag_id"),
            value.get("task_id"),
            value.get("failure_class"),
        )
        == incident_key
        for value in state.values()
        if isinstance(value, dict)
    )


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
    # 스케줄러가 강제 실패시킨 미실행 태스크는 로그 내용과 무관하게 재실행 대상이다.
    # 오판을 막기 위해 판정은 로그가 아니라 스케줄러 메시지(entry.error)로만 한다.
    if NOT_RUN_FAILURE_CLASS in (queued, classify_failure(str(entry.get("error") or ""))):
        return NOT_RUN_FAILURE_CLASS
    if detected in {"data_file_error", "allocator_error", "code_error"}:
        return detected
    if queued == "data_or_account":
        return queued
    if queued == "transient" and detected == "unknown":
        return queued
    return detected


def _send_skip_notification(entry: dict, failure_class: str, reason: str) -> bool:
    return send_telegram(
        "[자동복구] 코드 자동수정 생략\n"
        f"dag_id={entry.get('dag_id')}\n"
        f"task_id={entry.get('task_id')}\n"
        f"run_id={entry.get('run_id')}\n"
        f"오류분류={failure_class}\n"
        f"생략이유={reason}\n"
        "다음조치=재시도 또는 수동확인"
    )


def _handle_not_run_entry(entry: dict, signature: str) -> bool:
    """실행조차 되지 않은 태스크를 clear 해서 재실행한다(코드 자동수정 대상 아님)."""
    dag_id = entry.get("dag_id")
    run_id = entry.get("run_id")
    task_id = entry.get("task_id")
    try_number = entry.get("try_number")

    ti = get_task_instance(dag_id, run_id, task_id)
    state = str((ti or {}).get("state") or "")
    started = (ti or {}).get("start_date")
    # 한 번이라도 실행된 태스크(= 진짜 실패)는 절대 clear 하지 않는다.
    if ti is None or state != "failed" or started:
        if not claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={
                "failure_class": NOT_RUN_FAILURE_CLASS,
                "skip_reason": "not_run_guard",
                "signature": signature,
            },
        ):
            return False
        if ti is None:
            reason = "태스크 조회 실패"
        else:
            reason = f"state={state or 'unknown'} start_date={started or '없음'}"
        _record_terminal(entry, "skipped", _send_skip_notification(entry, NOT_RUN_FAILURE_CLASS, reason))
        return True

    if _already_attempted_today(signature, entry):
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={
                "failure_class": NOT_RUN_FAILURE_CLASS,
                "skip_reason": "duplicate_incident_today",
                "signature": signature,
                "result_status": "skipped",
                "result_finished_at": datetime.now(timezone.utc).isoformat(),
                "notification_suppressed": True,
            },
        ):
            logger.info("오늘 이미 재실행한 미실행 장애라 건너뜀: dag_id=%s task_id=%s", dag_id, task_id)
            return True
        return False

    if not claim_heal_task(
        dag_id,
        run_id,
        task_id,
        "autoheal-rerun",
        try_number=try_number,
        updates={
            "failure_class": NOT_RUN_FAILURE_CLASS,
            "skip_reason": "executor_state_mismatch_rerun",
            "signature": signature,
        },
    ):
        return False

    _mark_attempt(signature, entry)
    ok = clear_task_instance(dag_id, run_id, task_id)
    result_text = "재실행 요청 완료" if ok else "재실행 요청 실패(수동 확인 필요)"
    _finish_task_entry(
        entry,
        "success" if ok else "failed",
        "[자동복구] 미실행 태스크 재실행\n"
        f"dag_id={dag_id}\n"
        f"task_id={task_id}\n"
        f"run_id={run_id}\n"
        f"사유={NOT_RUN_REASON}\n"
        f"결과={result_text}",
        error="" if ok else "clear_task_instance_failed",
    )
    return True


def _send_data_file_notification(entry: dict) -> bool:
    return send_telegram(
        "[자동복구] 데이터 파일 오류 감지\n"
        f"dag_id={entry.get('dag_id')}\n"
        f"task_id={entry.get('task_id')}\n"
        f"run_id={entry.get('run_id')}\n"
        "오류분류=data_file_error\n"
        "다음조치=깨진 xlsx 격리 후 동일 DAG 재실행",
        final_failure=True,
    )


def _record_terminal(entry: dict, status: str, reported: bool, *, error: str = "") -> None:
    updates = {
        "result_status": status,
        "result_finished_at": datetime.now(timezone.utc).isoformat(),
        "result_reported_at": datetime.now(timezone.utc).isoformat() if reported else None,
    }
    if error:
        updates["result_error"] = error[:500]
    if not reported:
        updates["result_report_error"] = "telegram_send_failed"
    update_heal_task(
        entry.get("dag_id"),
        entry.get("run_id"),
        entry.get("task_id"),
        try_number=entry.get("try_number"),
        updates=updates,
    )


def _record_suppressed_terminal(entry: dict, status: str, *, reason: str) -> None:
    updates = {
        "result_status": status,
        "result_finished_at": datetime.now(timezone.utc).isoformat(),
        "notification_suppressed": True,
        "notification_suppressed_reason": reason,
    }
    update_heal_task(
        entry.get("dag_id"),
        entry.get("run_id"),
        entry.get("task_id"),
        try_number=entry.get("try_number"),
        updates=updates,
    )


def _finish_task_entry(entry: dict, status: str, message: str, *, error: str = "") -> None:
    _record_terminal(entry, status, send_telegram(message, final_failure=status == "failed"), error=error)


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
            "Workspace rules:\n"
            f"- Runtime source is {AIRFLOW_RUNTIME_WORKDIR}.\n"
            f"- Codex auto-heal workspace is {CODEX_WORKDIR}.\n"
            "- Inspect in the auto-heal workspace, then apply the same minimal verified fix to the runtime source.\n"
            "- Do not wait for approval for repository code fixes. Approval is required only for OneDrive writes or git commit/push.\n"
            "- If applying to runtime would overwrite unrelated local changes, stop and report the conflict.\n\n"
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
            "4. OneDrive/git이 아니면 운영 소스까지 반영하고 아래 형식의 한글 결과를 최종 응답으로 남기세요. Telegram 보고는 watcher가 담당합니다.\n\n"
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
        "Workspace rules:\n"
        f"- Runtime source is {AIRFLOW_RUNTIME_WORKDIR}.\n"
        f"- Codex auto-heal workspace is {CODEX_WORKDIR}.\n"
        "- Inspect in the auto-heal workspace, then apply the same minimal verified fix to the runtime source.\n"
        "- Do not wait for approval for repository code fixes. Approval is required only for OneDrive writes or git commit/push.\n"
        "- If applying to runtime would overwrite unrelated local changes, stop and report the conflict.\n\n"
        f"dag_id={entry['dag_id']}\n"
        f"task_id={entry['task_id']}\n"
        f"run_id={entry['run_id']}\n"
        f"try_number={entry['try_number']}\n"
        f"failure_class={entry.get('failure_class', '')}\n"
        f"error={entry.get('error', '')}\n\n"
        f"--- log ---\n{log_text}\n\n"
        "필수 지시:\n"
        "1. 원인을 먼저 파악하세요. unknown/transient/allocator_error도 조사 대상입니다.\n"
        "2. 코드/설계 문제면 최소 수정하고, 데이터/계정/권한 문제면 수정하지 말고 수동조치 사유를 보고하세요.\n"
        "3. modules.transform.utility.airflow_api.trigger_dag(dag_id) 재실행이 적절한지 판단하세요.\n"
        "4. OneDrive/git이 아니면 운영 소스까지 반영하고 아래 형식의 한글 결과를 최종 응답으로 남기세요. Telegram 보고는 watcher가 담당합니다.\n\n"
        "[자동복구 결과]\n"
        "상태: 성공/실패\n"
        "원인: <쉬운 한글 요약>\n"
        "수정파일: <경로 또는 없음>\n"
        "다음조치: <새 run_id 또는 수동조치>\n"
    )


def _claim_import_entry(signature: str) -> bool:
    if not QUEUE_PATH.exists():
        return False
    with _exclusive_file_lock(QUEUE_LOCK_PATH):
        rows = _load_queue_rows()
        claimed = False
        now = datetime.now(timezone.utc)
        for kind, row in rows:
            if kind != "json" or claimed or not isinstance(row, dict):
                continue
            if (
                row.get("kind") == "import_error"
                and row.get("signature") == signature
                and _claim_available(row)
            ):
                row["claimed_by"] = "codex"
                row["claimed_at"] = now.isoformat()
                row["claim_expires_at"] = (now + timedelta(seconds=CLAIM_LEASE_SECONDS)).isoformat()
                claimed = True
        if claimed:
            _write_queue_rows(rows)
        return claimed


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
        logger.info("heal_queue idle: pending=0")
        return False

    logger.info(
        "heal_queue pending entry: kind=%s dag_id=%s task_id=%s run_id=%s failure_class=%s",
        entry.get("kind"),
        entry.get("dag_id"),
        entry.get("task_id"),
        entry.get("run_id"),
        entry.get("failure_class"),
    )

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
                reported = send_telegram(
                    "[자동복구] Codex 실행 실패(import 오류)\n"
                    f"signature={signature}\n"
                    f"returncode={result.returncode}",
                    final_failure=True,
                )
                update_import_entry(signature, {
                    "result_status": "failed",
                    "result_finished_at": datetime.now(timezone.utc).isoformat(),
                    "result_reported_at": datetime.now(timezone.utc).isoformat() if reported else None,
                    "result_error": f"returncode={result.returncode}",
                })
            else:
                reported = send_telegram(
                    "[자동복구 결과]\n"
                    "상태: 자동조치 완료\n"
                    f"DAG import 오류: {entry.get('dag_id', '<unknown>')}\n"
                    f"작업 경로: {CODEX_WORKDIR}\n"
                    "다음조치: DAG 재파싱 또는 재실행 확인"
                )
                update_import_entry(signature, {
                    "result_status": "completed",
                    "result_finished_at": datetime.now(timezone.utc).isoformat(),
                    "result_reported_at": datetime.now(timezone.utc).isoformat() if reported else None,
                })
        except subprocess.TimeoutExpired:
            reported = send_telegram(
                "[자동복구] Codex 시간초과(import 오류)\n"
                f"signature={signature}\n"
                f"제한시간초={CODEX_TIMEOUT_SECONDS}\n"
                "수동확인필요=true",
                final_failure=True,
            )
            update_import_entry(signature, {
                "result_status": "failed",
                "result_finished_at": datetime.now(timezone.utc).isoformat(),
                "result_reported_at": datetime.now(timezone.utc).isoformat() if reported else None,
                "result_error": "codex_timeout",
            })
        except Exception as e:
            reported = send_telegram(
                "[자동복구] Codex 실행 오류(import 오류)\n"
                f"signature={signature}\n"
                f"오류={e}",
                final_failure=True,
            )
            update_import_entry(signature, {
                "result_status": "failed",
                "result_finished_at": datetime.now(timezone.utc).isoformat(),
                "result_reported_at": datetime.now(timezone.utc).isoformat() if reported else None,
                "result_error": str(e)[:500],
            })
        return True

    dag_id = entry.get("dag_id")
    run_id = entry.get("run_id")
    task_id = entry.get("task_id")
    try_number = entry.get("try_number")
    if not all([dag_id, run_id, task_id, try_number]):
        logger.warning("heal_queue 필수 필드 누락: %s", entry)
        return False

    log_text = get_task_log(dag_id, run_id, task_id, try_number)
    if not log_text:
        logger.warning("task log lookup returned empty: dag_id=%s task_id=%s run_id=%s", dag_id, task_id, run_id)
    from modules.transform.utility.safe_recovery import memory_failure, ALLOW
    if task_id in ALLOW.get(dag_id, set()) and memory_failure(log_text):
        if claim_heal_task(dag_id, run_id, task_id, "resource-controller", try_number=try_number,
                           updates={"failure_class": "memory_pressure", "skip_reason": "resource_controller_owned"}):
            _record_suppressed_terminal(entry, "skipped", reason="resource_controller_owned")
            return True
        return False
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
            _record_suppressed_terminal(entry, "skipped", reason="pending_retry")
            return True
        return False

    if failure_class == NOT_RUN_FAILURE_CLASS:
        return _handle_not_run_entry(entry, signature)

    if failure_class == "data_file_error":
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-data-file",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "data_file_error", "signature": signature},
        ):
            _record_terminal(entry, "skipped", _send_data_file_notification(entry))
            return True
        return False

    if failure_class not in _AUTO_EDIT_CLASSES:
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={"failure_class": failure_class, "skip_reason": "manual_only", "signature": signature},
        ):
            _record_suppressed_terminal(entry, "skipped", reason="manual_only")
            return True
        return False

    if _already_attempted_today(signature, entry):
        finished_at = datetime.now(timezone.utc).isoformat()
        if claim_heal_task(
            dag_id,
            run_id,
            task_id,
            "autoheal-skip",
            try_number=try_number,
            updates={
                "failure_class": failure_class,
                "skip_reason": "duplicate_incident_today",
                "signature": signature,
                "result_status": "skipped",
                "result_finished_at": finished_at,
                "notification_suppressed": True,
            },
        ):
            logger.info(
                "오늘 이미 처리한 동일 장애라 알림 없이 건너뜀: dag_id=%s task_id=%s",
                dag_id,
                task_id,
            )
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
            _finish_task_entry(
                entry,
                "failed",
                "[자동복구] Codex 실행 실패 - 수동확인 필요\n"
                f"dag_id={dag_id}\n"
                f"task_id={task_id}\n"
                f"run_id={run_id}\n"
                f"returncode={result.returncode}",
                error=f"returncode={result.returncode}",
            )
        else:
            _finish_task_entry(
                entry,
                "completed",
                "[자동복구 결과]\n"
                "상태: 자동조치 완료\n"
                f"dag_id={dag_id}\n"
                f"task_id={task_id}\n"
                f"작업 경로={CODEX_WORKDIR}\n"
                "다음조치=DAG 재실행 또는 다음 스케줄 확인",
            )
    except subprocess.TimeoutExpired:
        _finish_task_entry(
            entry,
            "failed",
            "[자동복구] Codex 시간초과 - 수동확인 필요\n"
            f"dag_id={dag_id}\n"
            f"task_id={task_id}\n"
            f"run_id={run_id}\n"
            f"제한시간초={CODEX_TIMEOUT_SECONDS}",
            error="codex_timeout",
        )
    except Exception as e:
        _finish_task_entry(
            entry,
            "failed",
            "[자동복구] Codex 실행 오류\n"
            f"dag_id={dag_id}\n"
            f"task_id={task_id}\n"
            f"run_id={run_id}\n"
            f"오류={e}",
            error=str(e),
        )
    return True


def _heartbeat_is_fresh(max_age_seconds: int) -> bool:
    try:
        payload = json.loads(HEARTBEAT_PATH.read_text(encoding="utf-8"))
        ts = _parse_utc(payload.get("ts"))
        if not ts or (datetime.now(timezone.utc) - ts).total_seconds() >= max_age_seconds:
            return False
        pid = payload.get("pid")
        if pid and not _pid_is_alive(pid):
            logger.info("heartbeat가 죽은 watcher pid를 가리켜 stale로 처리합니다: pid=%s", pid)
            return False
        return True
    except (OSError, ValueError, AttributeError):
        return False


def _pid_is_alive(pid: object) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    if pid_int == os.getpid():
        return True
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid_int)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except OSError:
        return False


def _acquire_watcher_lock() -> str:
    token = uuid.uuid4().hex
    WATCHER_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(str(WATCHER_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"token": token, "pid": os.getpid(), "backend": AUTOHEAL_BACKEND}, f)
            return token
        except FileExistsError:
            if _heartbeat_is_fresh(max(POLL_INTERVAL * 3, 180)):
                raise RuntimeError("다른 autoheal watcher의 heartbeat가 정상입니다")
            WATCHER_LOCK_PATH.unlink(missing_ok=True)
    raise RuntimeError(f"watcher lock 획득 실패: {WATCHER_LOCK_PATH}")


def _watcher_lock_owned(token: str) -> bool:
    try:
        payload = json.loads(WATCHER_LOCK_PATH.read_text(encoding="utf-8"))
        return payload.get("token") == token
    except (OSError, ValueError, AttributeError):
        return False


def _release_watcher_lock(token: str) -> None:
    if _watcher_lock_owned(token):
        WATCHER_LOCK_PATH.unlink(missing_ok=True)


def main() -> None:
    global _WATCHER_TOKEN
    try:
        _WATCHER_TOKEN = _acquire_watcher_lock()
    except RuntimeError as exc:
        logger.info("watch_heal_queue 시작 생략: %s", exc)
        return
    logger.info("watch_heal_queue start: %s", QUEUE_PATH)
    logger.info("AUTOHEAL_HEARTBEAT_PATH=%s", HEARTBEAT_PATH)
    logger.info("AIRFLOW_RUNTIME_WORKDIR=%s", AIRFLOW_RUNTIME_WORKDIR)
    logger.info("CODEX_WORKDIR=%s", CODEX_WORKDIR)
    logger.info("CODEX_TIMEOUT_SECONDS=%s", CODEX_TIMEOUT_SECONDS)
    logger.info("started_at=%s", datetime.now(timezone.utc).isoformat())
    _write_heartbeat("started")
    try:
        while _watcher_lock_owned(_WATCHER_TOKEN):
            last_result = "idle"
            last_error = ""
            try:
                processed = process_once()
                last_result = "processed" if processed else "idle"
            except Exception as e:
                last_result = "error"
                last_error = str(e)
                logger.exception("heal_queue 처리 실패: %s", e)
            _write_heartbeat(last_result, last_error)
            time.sleep(POLL_INTERVAL)
    finally:
        _release_watcher_lock(_WATCHER_TOKEN)


if __name__ == "__main__":
    main()
