"""Airflow task alert and auto-heal queue helpers."""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from modules.transform.utility.mail_recipients import MAIL_CMJ_PM

logger = logging.getLogger(__name__)

_ALERT_EMAILS = [MAIL_CMJ_PM]
HEAL_QUEUE_PATH = Path(os.getenv("HEAL_QUEUE_PATH", "/opt/airflow/logs/heal_queue.jsonl"))
_TELEGRAM_TRIGGER = "해결해라"

_TRANSIENT_PATTERNS = [
    r"timeout",
    r"timed out",
    r"connection.*(reset|refused|aborted|closed)",
    r"temporar(y|ily)",
    r"5\d\d",
    r"bad gateway",
    r"service unavailable",
    r"gateway timeout",
    r"chrome",
    r"chromedriver",
    r"selenium",
    r"webdriver",
    r"session (deleted|not created|invalid)",
    r"no such window",
    r"stale element",
    r"element.*not.*clickable",
    r"net::err_",
    r"browser.*(crash|closed|disconnected)",
    r"crash",
]

_DATA_OR_ACCOUNT_PATTERNS = [
    r"login failed",
    r"invalid.*(password|credential)",
    r"unauthorized",
    r"forbidden",
    r"permission",
    r"account",
    r"credential",
    r"password",
    r"no stores?",
    r"no data",
    r"failed for all accounts",
    r"성공\s*0/",
    r"로그인",
    r"계정",
    r"비밀번호",
    r"권한",
    r"매장.*없",
    r"데이터.*없",
]

_DATA_FILE_PATTERNS = [
    r"there is no item named '\[content_types\]\.xml' in the archive",
    r"there is no item named \"\[content_types\]\.xml\" in the archive",
    r"\[content_types\]\.xml",
    r"badzipfile",
    r"not a zip file",
    r"invalid xlsx",
    r"xlsx required parts missing",
    r"xlsx zip open failed",
]

_ALLOCATOR_ERROR_PATTERNS = [
    r"상품\s*슬롯\s*초과",
    r"slot\s*overflow",
    r"manual_item_block_size",
    r"item_id\s*배정\s*실패",
    r"store_seq=.*\d+\s*/\s*\d+",
]

_CODE_ERROR_PATTERNS = [
    r"syntaxerror",
    r"indentationerror",
    r"taberror",
    r"nameerror",
    r"modulenotfounderror",
    r"importerror",
    r"cannot import name",
    r"unexpected keyword argument",
    r"missing .*required .*argument",
    r"takes .* positional argument",
]


def _get_telegram_creds() -> tuple[str, str]:
    token = ""
    chat_id = ""
    try:
        from airflow.models import Variable

        token = Variable.get("TELEGRAM_BOT_TOKEN", default_var="")
        chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var="")
    except Exception:
        pass
    return token or os.getenv("TELEGRAM_BOT_TOKEN", ""), chat_id or os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram(text: str) -> bool:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram alert sent")
        return True
    except Exception as e:
        logger.warning("Telegram send failed (ignored): %s", e)
        return False


def send_telegram_chunks(text: str, limit: int = 4000) -> bool:
    lines = (text or "").split("\n")
    chunks: list[str] = []
    buf: list[str] = []
    cur = 0
    for line in lines:
        while len(line) > limit:
            if buf:
                chunks.append("\n".join(buf))
                buf = []
                cur = 0
            chunks.append(line[:limit])
            line = line[limit:]
        add = len(line) + (1 if buf else 0)
        if cur + add > limit:
            chunks.append("\n".join(buf))
            buf = [line]
            cur = len(line)
        else:
            buf.append(line)
            cur += add
    if buf:
        chunks.append("\n".join(buf))
    ok = True
    for chunk in chunks:
        ok = send_telegram(chunk) and ok
    return ok


def _send_email_alert(subject: str, body: str) -> None:
    try:
        from modules.transform.utility.mailer import send_email, text_to_html

        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info("Email alert sent: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("Email alert failed: %s", e)


def classify_failure(text: str) -> str:
    """Classify failures for alerting and auto-heal routing."""
    normalized = (text or "").lower()
    for pattern in _DATA_FILE_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return "data_file_error"
    for pattern in _ALLOCATOR_ERROR_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return "allocator_error"
    for pattern in _DATA_OR_ACCOUNT_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return "data_or_account"
    for pattern in _TRANSIENT_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return "transient"
    for pattern in _CODE_ERROR_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return "code_error"
    return "unknown"


def _append_heal_queue(entry: dict) -> bool:
    try:
        HEAL_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with HEAL_QUEUE_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info("heal_queue appended: %s", HEAL_QUEUE_PATH)
        return True
    except Exception as e:
        logger.warning("heal_queue append failed (ignored): %s", e)
        return False


def _is_airflow_temporary_run(run_id: object) -> bool:
    return "temporary_run" in str(run_id or "")


def enqueue_heal_task(context) -> bool:
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        logger.warning("heal_queue append skipped: missing task_instance")
        return False
    if _is_airflow_temporary_run(getattr(ti, "run_id", "")):
        logger.info("heal_queue append skipped: temporary run_id=%s", getattr(ti, "run_id", ""))
        return True

    exception = context.get("exception", "")
    error_text = str(exception or "")
    failure_class = classify_failure(error_text)
    state = str(getattr(ti, "state", "") or "")
    pending_retry = state == "up_for_retry"

    entry = {
        "kind": "task_failure",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "state": state,
        "pending_retry": pending_retry,
        "failure_class": failure_class,
        "auto_edit_allowed": failure_class == "code_error" and not pending_retry,
        "error": error_text,
        "log_url": getattr(ti, "log_url", ""),
        "ts": datetime.utcnow().isoformat(),
        "claimed_by": None,
    }
    return _append_heal_queue(entry)


def _handle_failure_callback(context, *, telegram_enabled: bool) -> None:
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        logger.warning("failure callback skipped: missing task_instance")
        return
    if _is_airflow_temporary_run(getattr(ti, "run_id", "")):
        logger.info("failure callback skipped: temporary run_id=%s", getattr(ti, "run_id", ""))
        return

    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
    failure_class = classify_failure(str(exception))

    subject = f"[Airflow 실패] {ti.dag_id} / {ti.task_id}"
    body = (
        "[DAG 실패]\n"
        f"dag_id={ti.dag_id}\n"
        f"task_id={ti.task_id}\n"
        f"run_id={ti.run_id}\n"
        f"try_number={ti.try_number}\n"
        f"execution_date={execution_date}\n"
        f"log_url={ti.log_url}\n"
        f"failure_class={failure_class}\n"
        f"error={exception}"
    )

    _send_email_alert(subject, body)
    if telegram_enabled:
        send_telegram(f"{body}\n{_TELEGRAM_TRIGGER}")
    if not enqueue_heal_task(context):
        if telegram_enabled:
            send_telegram(f"[Auto-Heal] heal_queue_write_failed=true\ndag_id={ti.dag_id}\ntask_id={ti.task_id}")


def on_failure_callback(context) -> None:
    _handle_failure_callback(context, telegram_enabled=True)


def on_failure_callback_no_telegram(context) -> None:
    """이메일과 자동복구 큐는 유지하되 태스크별 Telegram은 보내지 않는다."""
    _handle_failure_callback(context, telegram_enabled=False)


def on_retry_callback(context) -> None:
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        logger.warning("retry callback skipped: missing task_instance")
        return
    if _is_airflow_temporary_run(getattr(ti, "run_id", "")):
        logger.info("retry callback skipped: temporary run_id=%s", getattr(ti, "run_id", ""))
        return

    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception = context.get("exception", "알 수 없음")
    retry_number = ti.try_number - 1

    body = (
        "[DAG 재시도]\n"
        f"dag_id={ti.dag_id}\n"
        f"task_id={ti.task_id}\n"
        f"run_id={ti.run_id}\n"
        f"try_number={ti.try_number}\n"
        f"retry_number={retry_number}\n"
        f"execution_date={execution_date}\n"
        f"log_url={ti.log_url}\n"
        f"error={exception}"
    )
    send_telegram(body)
