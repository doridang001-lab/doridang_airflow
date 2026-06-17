"""
?댁??ъ뒪(EasyPOS) 留ㅼ텧 ?곸닔利??섏쭛 DAG

泥섎━ ?먮쫫:
1. ?ㅽ뻾 ?좎쭨 寃곗젙 (conf['date_from'/'date_to'] ?먮뒗 conf['sale_date'] ?먮뒗 ?댁젣)
2. Playwright濡?EasyPOS ?뱀씪留ㅼ텧?댁뿭 ???곸닔利앸퀎 ?곹뭹?댁뿭 ?섏쭛
3. OneDrive easypos_sales_raw/ym={YYYY-MM}/receipts.csv ?곸옱

?좎쭨 踰붿쐞 ?ㅼ젙:
- DATE_FROM / DATE_TO ?곸닔(肄붾뱶)濡?湲곕낯媛??ㅼ젙 媛??(None?대㈃ ?댁젣 1??
- ?고???conf {"date_from": "YYYY-MM-DD", "date_to": "YYYY-MM-DD"} 濡?override
- conf {"sale_date": "YYYY-MM-DD"} (?⑥씪 ?좎쭨) ???명솚

?ㅼ?以? 留ㅼ씪 07:50 (DB_EASYPOS_SALES_TIME)
?섏쭛 寃쎈줈: ANALYTICS_DB/easypos_sales_raw/ym=YYYY-MM/receipts.csv
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_EASYPOS_SALES_TIME
from modules.transform.pipelines.db.DB_EasyPOS_Sales import (
    resolve_sale_dates,
    collect_receipts,
    save_to_raw,
    verify_missing,
)
from modules.transform.pipelines.db.DB_EasyPOS_Product import (
    download_easypos_product,
    save_easypos_product,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# ?좎쭨 踰붿쐞 ?ㅼ젙 (None?대㈃ ?댁젣留? ?ㅼ젙?섎㈃ ?대떦 湲곌컙 ?섏쭛)
# ?고???conf(date_from/date_to ?먮뒗 sale_date)媛 ?덉쑝硫?conf ?곗꽑
DATE_FROM = None   # ?? "2026-04-01"
DATE_TO   = None   # ?? "2026-04-03"

# LOOKBACK_DAYS: DATE_FROM/DATE_TO/conf 誘몄?????理쒓렐 N??以?raw ?뚯씪 ?녿뒗 ?좊쭔 ?섏쭛
# None ???댁젣 1?쇰쭔, int ??理쒓렐 N??raw ?뚯씪 議댁옱 ?뺤씤 ???꾨씫遺꾨쭔 ?섏쭛
LOOKBACK_DAYS: int | None = 7

_ALERT_EMAILS = ["a17019@kakao.com"]


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info(f"?뚮┝ 諛쒖넚 ?꾨즺: {_ALERT_EMAILS}")
    except Exception as e:
        logger.error(f"?뚮┝ 諛쒖넚 ?ㅽ뙣: {e}")


def _on_failure_callback(context):
    """Task 理쒖쥌 ?ㅽ뙣 ???대찓???뚮┝"""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "?????놁쓬")

    _send_alert(
        subject=f"[Airflow ?ㅽ뙣] {ti.dag_id} / {ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"?ㅽ뻾?쇱떆: {execution_date}\n"
            f"?먮윭: {exception}\n"
            f"濡쒓렇: {ti.log_url}"
        ),
    )
    try:
        _ti = context.get('task_instance')
        _exc = context.get('exception', '?????놁쓬')
        _rd = getattr(_ti, 'execution_date', None)
        _ed = _rd.strftime('%Y-%m-%d %H:%M') if _rd else ''
        _retry = getattr(_ti, 'try_number', 1) - 1
        send_telegram(
            f"DAG: {_ti.dag_id}\nTask: {_ti.task_id}\n?ъ떆?? {_retry}?뚯감\n"
            f"?ㅽ뻾?쇱떆: {_ed}\n?먮윭: {_exc}\n濡쒓렇: {_ti.log_url}\n?닿껐?대씪"
        )
        enqueue_heal_task(context)
    except Exception:
        pass


def _on_retry_callback(context):
    """Task ?ъ떆?????대찓???뚮┝"""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "?????놁쓬")
    retry_number   = ti.try_number - 1

    _send_alert(
        subject=f"[Airflow ?ъ떆??{retry_number}?? {ti.dag_id} / {ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"?ъ떆?? {retry_number}?뚯감\n"
            f"?ㅽ뻾?쇱떆: {execution_date}\n"
            f"?먮윭: {exception}\n"
            f"濡쒓렇: {ti.log_url}"
        ),
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
    "on_retry_callback": _on_retry_callback,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_EASYPOS_SALES_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "easypos", "selenium", "receipts"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_dates",
        python_callable=resolve_sale_dates,
        op_kwargs={"dag_date_from": DATE_FROM, "dag_date_to": DATE_TO, "lookback_days": LOOKBACK_DAYS},
    )

    t2 = PythonOperator(
        task_id="collect_receipts",
        python_callable=collect_receipts,
    )

    t3 = PythonOperator(
        task_id="save_to_raw",
        python_callable=save_to_raw,
    )

    t4 = PythonOperator(
        task_id="verify_missing",
        python_callable=verify_missing,
    )

    t5 = PythonOperator(
        task_id="download_easypos_product",
        python_callable=download_easypos_product,
    )

    t6 = PythonOperator(
        task_id="save_easypos_product",
        python_callable=save_easypos_product,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6

