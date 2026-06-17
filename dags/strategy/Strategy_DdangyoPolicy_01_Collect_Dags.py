"""
?↔꺼???뺤콉 ?섏쭛 DAG

泥섎━ ?먮쫫:
1. Playwright 濡쒓렇??諛?怨듭??ы빆 紐⑸줉 ?뚯떛
2. 媛?怨듭? 蹂몃Ц ?섏쭛 (?щ줈洹몄씤 ???곸꽭 ?섏씠吏 ?묎렐)
3. ?뺤콉 ???앹꽦 (?뺤콉?좏삎 8醫?遺꾨쪟, ?ㅽ뻾 ?쒖븞 ?앹꽦)
4. CSV ?꾩쟻 ???(以묐났?쒓굅, ?뺣젹)

?ㅽ뻾 ?쒓컖: 留ㅼ씪 10:15 (KST, ?뺤콉 DAG 5遺?媛꾧꺽 遺꾩궛)
?곗씠???ㅽ궎留? policy_id, platform, collected_at, policy_date, title,
               category, content, content_summary, policy_type,
               recommended_action, source_url
"""

import logging
import pendulum
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from modules.common.config import ADMIN_EMAIL
from modules.transform.utility.mailer import send_email, text_to_html
from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import SMP_POLICY_DDANGYO_TIME
from modules.transform.pipelines.strategy.SMP_ddangyo_policy_collect import (
    login_and_collect_notices,
    collect_notice_bodies,
    build_policy_rows,
    save_policy_csv,
    write_policy_log,
)

dag_id = Path(__file__).stem
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

        # Airflow tries are 1-based. "retries" is the number of retries after the first try.
        total_tries = (retries + 1) if isinstance(retries, int) else None

        is_first_failure = (try_number == 1)
        is_final_failure = (
            (total_tries is not None)
            and (try_number is not None)
            and (try_number >= total_tries)
        )

        # Avoid spamming on intermediate retry attempts.
        if not (is_first_failure or is_final_failure):
            return

        status = "FIRST FAIL (will retry)" if (is_first_failure and not is_final_failure) else "FINAL FAIL"
        dag_name = getattr(ti, "dag_id", None) or context.get("dag").dag_id
        task_name = getattr(ti, "task_id", None) or (task.task_id if task else "unknown_task")
        run_id = context.get("run_id")
        logical_date = context.get("logical_date") or context.get("execution_date")
        log_url = getattr(ti, "log_url", None)

        subject = f"[AIRFLOW][DDANGYO_POLICY] {status}: {dag_name}.{task_name}"
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


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "on_failure_callback": _on_task_failure,
}


# ============================================================
# Task ?섑띁 ?⑥닔
# ============================================================

def task_login_and_collect(**context):
    """?붽린???뺤콉 ?섏쭛 ???↔꺼??濡쒓렇??諛?怨듭? 紐⑸줉 ?뚯떛"""
    notice_list = login_and_collect_notices(**context)
    if not notice_list:
        raise AirflowSkipException("?섏쭛??怨듭?媛 ?놁뒿?덈떎.")
    return notice_list


def task_collect_notice_bodies(**context):
    """媛?怨듭? 蹂몃Ц ?섏쭛"""
    notices_with_content = collect_notice_bodies(**context)
    if not notices_with_content:
        raise AirflowSkipException("蹂몃Ц???섏쭛??怨듭?媛 ?놁뒿?덈떎.")
    return notices_with_content


def task_build_policy_rows(**context):
    """?뺤콉 ???앹꽦 (?뺤콉?좏삎 8醫?遺꾨쪟)"""
    policy_rows = build_policy_rows(**context)
    if not policy_rows:
        raise AirflowSkipException("?앹꽦???뺤콉 ?됱씠 ?놁뒿?덈떎.")
    return policy_rows


def task_save_policy_csv(**context):
    """CSV ?꾩쟻 ???(以묐났?쒓굅, ?뺣젹)"""
    saved_path = save_policy_csv(**context)
    policy_rows = context["ti"].xcom_pull(task_ids="task_build_policy_rows", key="policy_rows") or []
    logger.info("Ddangyo policy collection complete | path=%s | rows=%s", saved_path, len(policy_rows))
    return saved_path


# ============================================================
# DAG ?뺤쓽
# ============================================================

with DAG(
    dag_id=dag_id,
    description="?↔꺼???뺤콉 蹂寃??섏쭛 (Playwright 湲곕컲)",
    schedule=SMP_POLICY_DDANGYO_TIME,
    start_date=pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "ddangyo", "policy", "daily"],
    default_args=default_args,
) as dag:

    t1 = PythonOperator(
        task_id="task_login_and_collect",
        python_callable=task_login_and_collect,
    )

    t2 = PythonOperator(
        task_id="task_collect_notice_bodies",
        python_callable=task_collect_notice_bodies,
    )

    t3 = PythonOperator(
        task_id="task_build_policy_rows",
        python_callable=task_build_policy_rows,
    )

    t4 = PythonOperator(
        task_id="task_save_policy_csv",
        python_callable=task_save_policy_csv,
    )

    t5 = PythonOperator(
        task_id="task_write_log",
        python_callable=write_policy_log,
        trigger_rule="all_done",
    )

    t1 >> t2 >> t3 >> t4 >> t5

