"""
unified_sales 일별 생성 DAG (okpos + unionpos + easypos + posfeed)

처리 흐름:
1. 날짜 결정
2. okpos 적재
3. unionpos 상품 마스터 보정
4. unionpos 적재
5. easypos 적재
6. posfeed 적재
7. platform 재분류
8. 최종 검증
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import send_telegram
from modules.transform.pipelines.db.DB_UnifiedSales import (
    backfill_okpos as pipeline_backfill_okpos,
    backfill_unionpos as pipeline_backfill_unionpos,
    upsert_fin_product_grp_from_unionpos as pipeline_upsert_unionpos_products,
    reclassify_hall_platform as pipeline_reclassify,
    run_lookback_okpos as pipeline_lookback_okpos,
    run_lookback_unionpos as pipeline_lookback_unionpos,
    run_okpos as pipeline_run_okpos,
    run_unionpos as pipeline_run_unionpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_easypos import (
    backfill_easypos as pipeline_backfill_easypos,
    run_easypos as pipeline_run_easypos,
    run_lookback_easypos as pipeline_lookback_easypos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import (
    backfill_posfeed as pipeline_backfill_posfeed,
    generate_posfeed_whitelist_draft as pipeline_generate_whitelist_draft,
    report_posfeed_exclusions as pipeline_report_posfeed_exclusions,
    run_posfeed as pipeline_run_posfeed,
    run_lookback_posfeed as pipeline_lookback_posfeed,
    sync_posfeed_blacklist as pipeline_sync_posfeed_blacklist,
)
from modules.transform.pipelines.db.DB_UnifiedSales_toorder import (
    backfill_toorder as pipeline_backfill_toorder,
    run_toorder as pipeline_run_toorder,
    run_lookback_toorder as pipeline_lookback_toorder,
)
from modules.transform.pipelines.db.DB_UnifiedSales_validate import (
    validate_sales,
    validate_monthly_sales,
    build_daily_summary as pipeline_build_daily_summary,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

_ALERT_EMAILS = ["a17019@kakao.com"]


def _send_alert(subject: str, body: str) -> None:
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        send_email(subject=subject, html_content=text_to_html(body), to_emails=_ALERT_EMAILS)
        logger.info("알림 발송 완료: %s", _ALERT_EMAILS)
    except Exception as e:
        logger.error("알림 발송 실패: %s", e)


def _on_failure_callback(context):
    """Task 최종 실패 시 이메일 + Telegram 알림"""
    ti             = context.get("task_instance")
    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    exception      = context.get("exception", "알 수 없음")
    body = (
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"실행일시: {execution_date}\n"
        f"에러: {exception}\n"
        f"로그: {ti.log_url}"
    )
    _send_alert(subject=f"[Airflow 실패] {ti.dag_id} / {ti.task_id}", body=body)
    send_telegram(body + "\n해결해라")


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}

# LOOKBACK_DAYS:
# - int  : 최근 N일 누락분 보충
# - None : 전체 소스 CSV 백필
LOOKBACK_DAYS: int | None = None


def resolve_date(**context) -> str:
    """conf['sale_date'] → XCom push (정정 모드). 없으면 None push."""
    dag_run   = context.get("dag_run")
    conf      = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = conf.get("sale_date") if isinstance(conf, dict) else None

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)

    if sale_date:
        logger.info("정정 모드 → %s", sale_date)
        return f"정정 날짜: {sale_date}"
    if LOOKBACK_DAYS is None:
        logger.info("LOOKBACK_DAYS=None → 전체 소스 백필 모드")
        return "전체 소스 백필 모드"
    logger.info("Lookback %d일 모드", LOOKBACK_DAYS)
    return f"Lookback {LOOKBACK_DAYS}일 모드"


def build_okpos(**context) -> str:
    """okpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback 7일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_okpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_okpos(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_okpos()
    return pipeline_lookback_okpos(days=LOOKBACK_DAYS)


def build_unionpos(**context) -> str:
    """unionpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback N일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_unionpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_unionpos(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_unionpos()
    return pipeline_lookback_unionpos(days=LOOKBACK_DAYS)


def sync_unionpos_products(**context) -> str:
    """UnionPOS 영수증 품목을 기반으로 fin_product_grp.csv 미등록 상품을 선반영한다."""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        ym = str(sale_date)[:7]
        return pipeline_upsert_unionpos_products(ym=ym, dry_run=False)

    # lookback/백필 모드: 최근 7일이 월 경계를 넘을 수 있어 당월+전월을 선반영
    kst_now = pendulum.now("Asia/Seoul")
    ym_now = kst_now.format("YYYY-MM")
    ym_prev = kst_now.subtract(months=1).format("YYYY-MM")

    msg = []
    msg.append(pipeline_upsert_unionpos_products(ym=ym_now, dry_run=False))
    # 월초에는 전월 데이터를 lookback으로 만질 가능성이 높아서 항상 함께 수행
    msg.append(pipeline_upsert_unionpos_products(ym=ym_prev, dry_run=False))
    return " | ".join(msg)


def build_easypos(**context) -> str:
    """easypos -> unified_sales 저장

    conf['sale_date'] 있으면 특정일자 overwrite, 없으면 lookback/백필.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_easypos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_easypos(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_easypos()
    return pipeline_lookback_easypos(days=LOOKBACK_DAYS)


def build_posfeed(**context) -> str:
    """posfeed → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 lookback/백필.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_posfeed()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_posfeed(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_posfeed()
    return pipeline_lookback_posfeed(days=LOOKBACK_DAYS)


def build_toorder(**context) -> str:
    """toorder CSV → unified_sales 저장."""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if conf.get("backfill"):
        return pipeline_backfill_toorder()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_run_toorder(sale_date, overwrite=True)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_toorder()
    return pipeline_lookback_toorder(days=LOOKBACK_DAYS)


def report_posfeed_exclusions(**context) -> str:
    """posfeed 블랙리스트 제외 내역을 ym별 상세·집계 CSV로 저장."""
    return pipeline_report_posfeed_exclusions(**context)


def sync_posfeed_blacklist(**context) -> str:
    """fin_product_posfeed_whitelist.csv의 N 항목을 기존 parquet에 소급 적용 (매 실행마다)."""
    return pipeline_sync_posfeed_blacklist()


def generate_posfeed_whitelist_draft(**context) -> str:
    """posfeed item_name 목록을 draft CSV에 append (매 실행마다 신규 항목만 추가, 기존 항목 유지)."""
    return pipeline_generate_whitelist_draft()


def build_daily_summary(**context) -> str:
    """unified_sales → 일별×store×brand×order_type×platform 요약 parquet (LLM broadcast)."""
    return pipeline_build_daily_summary()


def reclassify_platform(**context) -> str:
    """unified_sales의 포스/제휴사주문 platform을 테이블명 기반으로 재분류."""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if not sale_date:
        return "Lookback 모드: 재분류 스킵 (신규 데이터에 이미 적용됨)"
    return pipeline_reclassify(sale_date, overwrite=True)


with DAG(
    dag_id=dag_id,
    schedule="20 10 * * *",  # 매일 10:30
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "toorder", "okpos", "unionpos", "easypos", "posfeed", "unified_sales"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_date",
        python_callable=resolve_date,
    )

    t3 = PythonOperator(
        task_id="build_okpos",
        python_callable=build_okpos,
    )

    t3a = PythonOperator(
        task_id="sync_unionpos_products",
        python_callable=sync_unionpos_products,
    )

    t4 = PythonOperator(
        task_id="build_unionpos",
        python_callable=build_unionpos,
    )

    t5 = PythonOperator(
        task_id="build_easypos",
        python_callable=build_easypos,
    )

    t5a = PythonOperator(
        task_id="build_posfeed",
        python_callable=build_posfeed,
    )

    t5a2 = PythonOperator(
        task_id="build_toorder",
        python_callable=build_toorder,
    )

    t5a3 = PythonOperator(
        task_id="report_posfeed_exclusions",
        python_callable=report_posfeed_exclusions,
    )

    t5b = PythonOperator(
        task_id="sync_posfeed_blacklist",
        python_callable=sync_posfeed_blacklist,
    )

    t5c = PythonOperator(
        task_id="generate_posfeed_whitelist_draft",
        python_callable=generate_posfeed_whitelist_draft,
    )

    t6 = PythonOperator(
        task_id="reclassify_platform",
        python_callable=reclassify_platform,
    )

    t7 = PythonOperator(
        task_id="validate_sales",
        python_callable=validate_sales,
    )

    t8 = PythonOperator(
        task_id="validate_monthly_sales",
        python_callable=validate_monthly_sales,
    )

    t9 = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    # 순차 실행: 같은 날짜 parquet에 동시 write 방지
    t1 >> t3 >> t3a >> t4 >> t5 >> t5a >> t5a3 >> t5a2 >> t5b >> t5c >> t6 >> t7 >> t8 >> t9
