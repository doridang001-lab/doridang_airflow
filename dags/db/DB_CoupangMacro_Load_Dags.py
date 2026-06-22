from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_CoupangMacro_load import load_coupang_macro_partition
from modules.transform.utility.mailer import send_email
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.schedule import DB_COUPANG_MACRO_TIME


dag_id = Path(__file__).stem
logger = logging.getLogger(__name__)
DEFAULT_NOTIFY_EMAIL = "a17019@kakao.com"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}


def send_failure_email_if_any(**context) -> str:
    """Send Coupang macro failure email when dag_run.conf contains failures."""
    conf = context["dag_run"].conf or {}
    failures = conf.get("failures", [])
    collected_at = conf.get("collected_at", "-")
    to_email = conf.get("notify_email", DEFAULT_NOTIFY_EMAIL)

    if not failures:
        logger.info("쿠팡 실패 항목 없음 - 이메일 전송 생략")
        return "skipped: no failures"

    logger.info("쿠팡 실패 %d개 -> %s 알림 전송", len(failures), to_email)

    rows_html = ""
    for failure in failures:
        account = failure.get("account", "-")
        stores = failure.get("stores", "-")
        orders = "OK" if failure.get("ordersOk") else "FAIL"
        cmg = "OK" if failure.get("cmgOk") else "FAIL"
        note = failure.get("note", "")
        stores_display = str(stores).replace("|", "<br>")
        rows_html += (
            "<tr>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;color:#555;font-size:12px'>{account}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;font-weight:600'>{stores_display}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;text-align:center'>{orders}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;text-align:center'>{cmg}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;color:#888;font-size:12px'>{note}</td>"
            "</tr>"
        )

    html_body = f"""
<div style='font-family:Malgun Gothic,Arial,sans-serif;background:#ffffff;color:#1a1a1a;padding:24px;border-radius:8px;border:1px solid #e0e0e0;max-width:700px'>
  <h2 style='color:#d32f2f;margin:0 0 6px 0;font-size:18px'>쿠팡이츠 수집 실패 알림</h2>
  <p style='color:#666;font-size:13px;margin:0 0 20px 0'>수집일시: {collected_at} &nbsp;|&nbsp; 실패 계정 <strong>{len(failures)}개</strong></p>
  <table style='width:100%;border-collapse:collapse;font-size:13px;border:1px solid #ddd'>
    <thead>
      <tr style='background:#f5f5f5;color:#333'>
        <th style='padding:10px 14px;text-align:left;border-bottom:2px solid #ddd'>계정 ID</th>
        <th style='padding:10px 14px;text-align:left;border-bottom:2px solid #ddd'>매장</th>
        <th style='padding:10px 14px;text-align:center;border-bottom:2px solid #ddd'>주문서</th>
        <th style='padding:10px 14px;text-align:center;border-bottom:2px solid #ddd'>마케팅</th>
        <th style='padding:10px 14px;text-align:left;border-bottom:2px solid #ddd'>비고</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <p style='margin-top:16px;font-size:12px;color:#888;border-top:1px solid #eee;padding-top:12px'>
    runner.html에서 수동 재시도하거나 watchdog 재시도 로그를 확인하세요.
  </p>
</div>
"""

    subject = f"[쿠팡이츠] 수집 실패 {len(failures)}개 - 확인 필요"
    send_email(subject=subject, html_content=html_body, to_emails=to_email)
    logger.info("쿠팡 실패 이메일 전송 완료 -> %s", to_email)
    return f"sent: {len(failures)} failures"


with DAG(
    dag_id=dag_id,
    description="쿠팡이츠 매크로 원본 적재 및 실패 알림 통합 DAG",
    schedule=DB_COUPANG_MACRO_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=3,
    default_args=default_args,
    tags=["db", "coupang_macro", "partition"],
) as dag:

    load_task = PythonOperator(
        task_id="load_coupang_macro_partition",
        python_callable=load_coupang_macro_partition,
    )

    notify_task = PythonOperator(
        task_id="send_failure_email_if_any",
        python_callable=send_failure_email_if_any,
    )

    load_task >> notify_task
