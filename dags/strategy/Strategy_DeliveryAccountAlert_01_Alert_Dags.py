"""
ToOrder 배달 계정 관리 현황 수집 및 알림 발송 DAG

처리 흐름:
    1. crawl_delivery_account  : ToOrder 배달 계정 관리 Excel 다운로드 (Selenium)
    2. filter_alert_targets    : 오류 매장 필터링 및 담당자 매핑
    3. save_to_csv             : CSV 저장 및 검증
    4. send_alert_emails       : 담당자별 HTML 이메일 발송

실행 주기: 매일 09:35 KST (SMP_DELIVERY_ALERT_TIME)
TEST_MODE=True: 모든 이메일을 관리자 수신자로만 발송
"""

import importlib
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
# TEST_MODE: True 이면 관리자에게만 발송, False 이면 담당자에게 발송 + 관리자 CC
# ============================================================
TEST_MODE = False

# ============================================================
# Failure alert
# ============================================================
logger = logging.getLogger(__name__)


def _on_task_failure(context):
    """
    실패 즉시 알림 발송.
    - 1회차 실패: 즉시 알림 (재시도 예정 안내)
    - 최종 실패: 재시도 소진 후 추가 알림
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
        _exc2 = context.get("exception", "예외 정보 없음")
        _rd2 = getattr(_ti2, "execution_date", None)
        _ed2 = _rd2.strftime("%Y-%m-%d %H:%M") if _rd2 else ""
        _retry2 = getattr(_ti2, "try_number", 1) - 1
        send_telegram(
            f"DAG: {_ti2.dag_id}\nTask: {_ti2.task_id}\n재시도: {_retry2}회차\n"
            f"실행일시: {_ed2}\n에러: {_exc2}\n로그: {_ti2.log_url}\n해결 필요"
        )
        enqueue_heal_task(context)
    except Exception:
        pass

# ============================================================
# 파이프라인 모듈 동적 임포트
# ============================================================
_pipeline = importlib.import_module(
    "modules.transform.pipelines.strategy.SMP_delivery_account_alert_01"
)

crawl_delivery_account_excel = _pipeline.crawl_delivery_account_excel
load_and_filter_alerts = _pipeline.load_and_filter_alerts
save_alerts_to_csv = _pipeline.save_alerts_to_csv
send_delivery_alert_emails = _pipeline.send_delivery_alert_emails
cleanup_downloaded_excel = _pipeline.cleanup_downloaded_excel

# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=Path(__file__).stem,
    description="ToOrder 배달 계정 연결 오류 매장 일일 알림",
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

