"""
ToOrder 諛곕떖 怨꾩젙 愿由??꾪솴 ?섏쭛 ???뚮┝ 諛쒖넚 DAG

泥섎━ ?먮쫫:
    1. crawl_delivery_account  : ToOrder 諛곕떖 怨꾩젙 愿由?Excel ?ㅼ슫濡쒕뱶 (Selenium)
    2. filter_alert_targets    : ?ㅻ쪟 留ㅼ옣 ?꾪꽣留?+ ?대떦??留ㅽ븨
    3. save_to_csv             : OneDrive CSV ???+ ???寃利?    4. send_alert_emails       : ?대떦?먮퀎 HTML ?대찓??諛쒖넚

?ㅼ?以? 留ㅼ씪 09:35 KST (SMP_DELIVERY_ALERT_TIME)
TEST_MODE=True: 紐⑤뱺 ?대찓?쇱쓣 a17019@kakao.com ?⑤룆 ?섏떊?쇰줈 ?꾪솚
"""

import importlib
import os
import logging
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.common.config import ADMIN_EMAIL
from modules.transform.utility.mailer import send_email, text_to_html
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import SMP_DELIVERY_ALERT_TIME

# ============================================================
# TEST_MODE: True ???뚯뒪???섏떊??a17019@kakao.com)濡쒕쭔 諛쒖넚
#            False ???대떦????諛쒖넚 + CC
# ============================================================
TEST_MODE = False

# ============================================================
# Failure alert
# ============================================================
logger = logging.getLogger(__name__)


def _on_task_failure(context):
    """
    ?ㅽ뙣 利됱떆 ?뚮┝ 諛쒖넚.
    - 1?뚯감 ?ㅽ뙣: 利됱떆 ?뚮┝ (?ъ떆???덉젙 ?덈궡)
    - 理쒖쥌 ?ㅽ뙣: ?ъ떆???뚯쭊 ??異붽? ?뚮┝
    """
    try:
        ti = context.get("ti") or context.get("task_instance")
        task = context.get("task")
        exc = context.get("exception")

        try_number = getattr(ti, "try_number", None)
        retries = getattr(task, "retries", None)
        total_tries = (retries + 1) if isinstance(retries, int) else None

        is_first_failure = (try_number == 1)
        is_final_failure = (
            (total_tries is not None)
            and (try_number is not None)
            and (try_number >= total_tries)
        )
        if not (is_first_failure or is_final_failure):
            return

        status = "FIRST FAIL (will retry)" if (is_first_failure and not is_final_failure) else "FINAL FAIL"
        dag_name = getattr(ti, "dag_id", None) or context.get("dag").dag_id
        task_name = getattr(ti, "task_id", None) or (task.task_id if task else "unknown_task")
        run_id = context.get("run_id")
        logical_date = context.get("logical_date") or context.get("execution_date")
        log_url = getattr(ti, "log_url", None)

        subject = f"[AIRFLOW][TOORDER_DELIVERY] {status}: {dag_name}.{task_name}"
        body_text = "\n".join(
            [
                f"DAG: {dag_name}",
                f"Task: {task_name}",
                f"Run ID: {run_id}",
                f"Logical date: {logical_date}",
                f"Try: {try_number}/{total_tries or '?'}",
                f"Log URL: {log_url}",
                f"Exception: {repr(exc)}",
            ]
        )
        send_email(
            subject=subject,
            html_content=text_to_html(body_text),
            to_emails=ADMIN_EMAIL,
            **context,
        )
    except Exception:
        logger.exception("Failure alert callback failed")
    try:
        _ti2 = context.get("task_instance") or context.get("ti")
        _exc2 = context.get("exception", "?????놁쓬")
        _rd2 = getattr(_ti2, "execution_date", None)
        _ed2 = _rd2.strftime("%Y-%m-%d %H:%M") if _rd2 else ""
        _retry2 = getattr(_ti2, "try_number", 1) - 1
        send_telegram(
            f"DAG: {_ti2.dag_id}\nTask: {_ti2.task_id}\n?ъ떆?? {_retry2}?뚯감\n"
            f"?ㅽ뻾?쇱떆: {_ed2}\n?먮윭: {_exc2}\n濡쒓렇: {_ti2.log_url}\n?닿껐?대씪"
        )
        enqueue_heal_task(context)
    except Exception:
        pass

# ============================================================
# ?뚯씠?꾨씪??紐⑤뱢 ?숈쟻 ?꾪룷??# ============================================================
_pipeline = importlib.import_module(
    "modules.transform.pipelines.strategy.SMP_delivery_account_alert_01"
)

crawl_delivery_account_excel = _pipeline.crawl_delivery_account_excel
load_and_filter_alerts = _pipeline.load_and_filter_alerts
save_alerts_to_csv = _pipeline.save_alerts_to_csv
send_delivery_alert_emails = _pipeline.send_delivery_alert_emails
cleanup_downloaded_excel = _pipeline.cleanup_downloaded_excel

# ============================================================
# DAG ?뺤쓽
# ============================================================
with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder 諛곕떖 怨꾩젙 ?곌껐 ?ㅻ쪟 留ㅼ옣 ?쇱씪 ?뚮┝",
    schedule=SMP_DELIVERY_ALERT_TIME,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["strategy", "toorder", "delivery", "alert", "daily"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": _on_task_failure,
    },
) as dag:

    task_crawl = PythonOperator(
        task_id="crawl_delivery_account",
        python_callable=crawl_delivery_account_excel,
        execution_timeout=pendulum.duration(minutes=10),
    )

    task_filter = PythonOperator(
        task_id="filter_alert_targets",
        python_callable=load_and_filter_alerts,
        execution_timeout=pendulum.duration(minutes=10),
    )

    task_csv = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_alerts_to_csv,
        execution_timeout=pendulum.duration(minutes=5),
    )

    task_email = PythonOperator(
        task_id="send_alert_emails",
        python_callable=send_delivery_alert_emails,
        op_kwargs={"test_mode": TEST_MODE},
        execution_timeout=pendulum.duration(minutes=5),
    )

    task_cleanup = PythonOperator(
        task_id="cleanup_excel",
        python_callable=cleanup_downloaded_excel,
        trigger_rule="all_done",
        execution_timeout=pendulum.duration(minutes=2),
    )

    task_crawl >> task_filter >> task_csv >> task_email >> task_cleanup

