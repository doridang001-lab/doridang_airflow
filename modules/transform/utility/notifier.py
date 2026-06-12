"""
Airflow DAG 실패/재시도 알림 유틸리티

사용법:
    from modules.transform.utility.notifier import on_failure_callback, on_retry_callback

    default_args = {
        ...
        "on_failure_callback": on_failure_callback,
        "on_retry_callback":   on_retry_callback,
    }
"""

import logging
import json
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.parse

logger = logging.getLogger(__name__)

_ALERT_EMAILS = ["a17019@kakao.com"]
HEAL_QUEUE_PATH = Path("C:/airflow/logs/heal_queue.jsonl")

# Telegram 메시지 끝에 붙는 트리거 문구 — Claude가 자동으로 오류 해결 시도
_TELEGRAM_TRIGGER = "해결해라"


def _get_telegram_creds() -> tuple[str, str]:
    """Airflow Variable에서 봇 토큰·채팅 ID 반환. 없으면 빈 문자열."""
    try:
        from airflow.models import Variable
        token   = Variable.get("TELEGRAM_BOT_TOKEN", default_var="")
        chat_id = Variable.get("TELEGRAM_CHAT_ID",   default_var="")
        return token, chat_id
    except Exception:
        return "", ""


def send_telegram(text: str) -> None:
    """Telegram Bot API로 메시지 전송."""
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram 자격증명 없음 — 전송 스킵")
        return
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram 알림 전송 완료")
    except Exception as e:
        logger.warning("Telegram 전송 실패 (무시): %s", e)


def _send_email_alert(subject: str, body: str) -> None:
    try:
        from modules.transform.utility.mailer import send_email, text_to_html
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info("이메일 알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("이메일 알림 발송 실패: %s", e)


def _append_heal_queue(entry: dict) -> None:
    try:
        HEAL_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with HEAL_QUEUE_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info("heal_queue 기록 완료: %s", HEAL_QUEUE_PATH)
    except Exception as e:
        logger.warning("heal_queue 기록 실패 (무시): %s", e)


def on_failure_callback(context) -> None:
    """Task 최종 실패 시 이메일 + Telegram 알림."""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")

    subject = f"[Airflow 실패] {ti.dag_id} / {ti.task_id}"
    body = (
        f"[DAG 실패]\n"
        f"dag_id={ti.dag_id}\n"
        f"task_id={ti.task_id}\n"
        f"run_id={ti.run_id}\n"
        f"try_number={ti.try_number}\n"
        f"execution_date={execution_date}\n"
        f"log_url={ti.log_url}\n"
        f"error={exception}"
    )

    _send_email_alert(subject, body)
    send_telegram(f"{body}\n{_TELEGRAM_TRIGGER}")
    _append_heal_queue({
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "error": str(exception),
        "ts": datetime.utcnow().isoformat(),
        "claimed_by": None,
    })


def on_retry_callback(context) -> None:
    """Task 재시도 시 Telegram 알림 (이메일 없음)."""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")
    retry_number   = ti.try_number - 1

    body = (
        f"[DAG 재시도]\n"
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
