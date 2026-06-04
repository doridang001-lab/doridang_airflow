"""Airflow global Telegram failure alert plugin — auto-applies to all DAGs"""
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


class _Listener:
    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error=None, session=None):
        try:
            ti = task_instance
            ed = getattr(ti, "execution_date", None)
            retry = max(0, getattr(ti, "try_number", 1) - 1)
            ed_str = ed.strftime("%Y-%m-%d %H:%M") if ed else ""
            err_str = str(error) if error else "알 수 없음"
            log_url = getattr(ti, "log_url", "")
            msg = (
                "DAG: " + ti.dag_id + "\n"
                "Task: " + ti.task_id + "\n"
                "재시도: " + str(retry) + "회차\n"
                "실행일시: " + ed_str + "\n"
                "에러: " + err_str + "\n"
                "로그: " + log_url + "\n"
                "해결해라"
            )
            _send(msg)
        except Exception as exc:
            logger.warning("[TelegramPlugin] callback error: %s", exc)


class TelegramNotifierPlugin(AirflowPlugin):
    name = "telegram_notifier"
    listeners = [_Listener()]
