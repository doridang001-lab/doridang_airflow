"""쿠팡 수집 실패 이메일 알림 DAG

Chrome 확장 runner.js 가 재시도 후에도 실패가 남을 경우
Airflow REST API (POST /api/v1/dags/DB_Coupang_Notify/dagRuns) 로 트리거된다.

conf 예시:
{
  "failures": [
    {"account": "hosig4", "stores": "도리당 광명철산점", "ordersOk": true, "cmgOk": false, "note": "매장목록 로드 실패"},
    ...
  ],
  "collected_at": "2026-06-05T13:08:15.000Z",
  "notify_email": "a17019@kakao.com"
}
"""

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.mailer import send_email

logger = logging.getLogger(__name__)

DEFAULT_NOTIFY_EMAIL = "a17019@kakao.com"


def _send_failure_email(**context):
    conf = context["dag_run"].conf or {}
    failures = conf.get("failures", [])
    collected_at = conf.get("collected_at", "-")
    to_email = conf.get("notify_email", DEFAULT_NOTIFY_EMAIL)

    if not failures:
        logger.info("실패 항목 없음 — 이메일 전송 생략")
        return

    logger.info(f"실패 {len(failures)}개 → {to_email} 알림 전송")

    # ── 이메일 본문 구성 ──
    rows_html = ""
    for f in failures:
        account = f.get("account", "-")
        stores  = f.get("stores", "-")
        orders  = "✅" if f.get("ordersOk") else "❌"
        cmg     = "✅" if f.get("cmgOk")    else "❌"
        note    = f.get("note", "")
        # 매장명 줄바꿈 (| 구분자 → 줄바꿈)
        stores_display = stores.replace("|", "<br>")
        rows_html += (
            f"<tr>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;color:#555;font-size:12px'>{account}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;font-weight:600'>{stores_display}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;text-align:center;font-size:16px'>{orders}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;text-align:center;font-size:16px'>{cmg}</td>"
            f"<td style='padding:8px 14px;border-bottom:1px solid #eee;color:#888;font-size:12px'>{note}</td>"
            f"</tr>"
        )

    html_body = f"""
<div style='font-family:Malgun Gothic,Arial,sans-serif;background:#ffffff;color:#1a1a1a;padding:24px;border-radius:8px;border:1px solid #e0e0e0;max-width:700px'>
  <h2 style='color:#d32f2f;margin:0 0 6px 0;font-size:18px'>⚠️ 쿠팡이츠 수집 실패 알림</h2>
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
    runner.html 에서 수동 재시도 가능 — <strong>↩ 실패 재시도</strong> 버튼 클릭
  </p>
</div>
"""

    subject = f"[쿠팡이츠] 수집 실패 {len(failures)}개 — 수동 재시도 필요"

    try:
        send_email(
            subject=subject,
            html_content=html_body,
            to_emails=to_email,
        )
        logger.info(f"이메일 전송 완료 → {to_email}")
    except Exception as e:
        logger.error(f"이메일 전송 실패: {e}")
        raise


with DAG(
    dag_id="DB_Coupang_Notify",
    description="쿠팡이츠 수집 실패 이메일 알림 (Chrome 확장 runner.js 트리거)",
    schedule=None,            # REST API 트리거 전용 — 스케줄 없음
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=3,        # 연속 실패 시 복수 알림 허용
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["coupang", "notify", "email"],
) as dag:

    notify = PythonOperator(
        task_id="send_failure_email",
        python_callable=_send_failure_email,
    )
