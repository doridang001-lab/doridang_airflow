"""Airflow global Telegram failure alert plugin."""
from __future__ import annotations

import logging
import urllib.parse
import urllib.request

from airflow.plugins_manager import AirflowPlugin

try:
    from airflow.listeners import hookimpl
except ImportError:
    hookimpl = lambda f: f  # noqa: E731

logger = logging.getLogger(__name__)


def _send(text):
    try:
        from airflow.models import Variable

        token = Variable.get("TELEGRAM_BOT_TOKEN", default_var="")
        chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var="")
        if not token or not chat_id:
            return

        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(
            "https://api.telegram.org/bot" + token + "/sendMessage",
            data=payload,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("[TelegramPlugin] sent")
    except Exception as e:
        logger.warning("[TelegramPlugin] failed: %s", e)


def _has_custom_task_callback(ti) -> bool:
    task = getattr(ti, "task", None)
    if task is None:
        return False
    callback = getattr(task, "on_failure_callback", None)
    if callback is None:
        return False
    if isinstance(callback, (list, tuple, set)):
        return len(callback) > 0
    return True


def _is_final_retry(ti) -> bool:
    try_number = max(1, int(getattr(ti, "try_number", 1) or 1))
    task = getattr(ti, "task", None)
    retries = int(getattr(task, "retries", 0) or 0) if task else 0
    max_tries = retries + 1
    return try_number >= max_tries


class _Listener:
    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error=None, session=None):
        try:
            ti = task_instance

            if _has_custom_task_callback(ti):
                return
            if not _is_final_retry(ti):
                return

            ed = getattr(ti, "execution_date", None)
            ed_str = ed.strftime("%Y-%m-%d %H:%M") if ed else ""
            err_str = str(error) if error else "error unavailable"
            retry = max(0, int(getattr(ti, "try_number", 1) - 1)
            )
            log_url = getattr(ti, "log_url", "")
            msg = (
                "DAG: " + ti.dag_id + "\n"
                "Task: " + ti.task_id + "\n"
                "Retry: " + str(retry) + "\n"
                "Datetime: " + ed_str + "\n"
                "Error: " + err_str + "\n"
                "Log: " + log_url
            )
            _send(msg)
        except Exception as exc:
            logger.warning("[TelegramPlugin] callback error: %s", exc)


class TelegramNotifierPlugin(AirflowPlugin):
    name = "telegram_notifier"
    listeners = [_Listener()]
