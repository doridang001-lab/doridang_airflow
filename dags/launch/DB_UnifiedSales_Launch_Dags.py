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
8. 테스트 매장 배달의민족 source 최종 정리
9. 최종 검증
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_LAUNCH_UNIFIED_TIME
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
from modules.transform.pipelines.db.DB_UnifiedSales_validate import (
    validate_sales,
    validate_monthly_sales,
    build_daily_summary as pipeline_build_daily_summary,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

def _on_failure_callback(context):
    """Task 최종 실패 시 Telegram 알림"""
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
    send_telegram(body + "\n해결해라")
    enqueue_heal_task(context)


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
# LOOKBACK_DAYS: int | None = 7
LOOKBACK_DAYS: int | None = 1

# 배민 직수집 교정 대상 테스트 매장 (검증 완료 후 확대)
TEST_STORES: list[str] = ["송파삼전점"]
TARGET_STORES: list[str] = ["송파삼전점"]

def resolve_date(**context) -> str:
    """conf['sale_date'] → XCom push (정정 모드). 없으면 None push."""
    dag_run   = context.get("dag_run")
    conf      = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = conf.get("sale_date") if isinstance(conf, dict) else None
    if not sale_date:
        sale_date = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)

    logger.info("Launch 집계 날짜 → %s", sale_date)
    return f"Launch 집계 날짜: {sale_date}"


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
        return pipeline_run_okpos(sale_date, overwrite=True, target_stores=TARGET_STORES)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_okpos()
    return pipeline_lookback_okpos(days=LOOKBACK_DAYS, target_stores=TARGET_STORES)


def build_unionpos(**context) -> str:
    """unionpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback N일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    return "송파삼전점 Launch 대상 아님 - UnionPOS 스킵"


def sync_unionpos_products(**context) -> str:
    """UnionPOS 영수증 품목을 기반으로 fin_product_grp.csv 미등록 상품을 선반영한다."""
    return "송파삼전점 Launch 대상 아님 - UnionPOS 상품 보정 스킵"


def build_easypos(**context) -> str:
    """easypos -> unified_sales 저장

    conf['sale_date'] 있으면 특정일자 overwrite, 없으면 lookback/백필.
    conf['backfill'] 있으면 전체 backfill.
    """
    return "송파삼전점 Launch 대상 아님 - EasyPOS 스킵"


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
        return pipeline_run_posfeed(sale_date, overwrite=True, target_stores=TARGET_STORES)
    if LOOKBACK_DAYS is None:
        return pipeline_backfill_posfeed()
    return pipeline_lookback_posfeed(days=LOOKBACK_DAYS, target_stores=TARGET_STORES)


def reconcile_baemin(**context) -> str:
    """TEST_STORES의 배민 직수집 데이터로 UnifiedSales 배달의민족 행 교정."""
    if not TEST_STORES:
        return "TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (
        reconcile_baemin_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return reconcile_baemin_for_test_stores(
        stores=TEST_STORES,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,
    )


def reconcile_coupang(**context) -> str:
    """TEST_STORES의 쿠팡 직수집 데이터로 UnifiedSales 쿠팡이츠 행 교정."""
    stores = [store for store in TEST_STORES if store != "법흥리점"]
    if not stores:
        return "쿠팡수동 대상 TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
        reconcile_coupang_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return reconcile_coupang_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,
    )


def report_posfeed_exclusions(**context) -> str:
    """posfeed 블랙리스트 제외 내역을 ym별 상세·집계 CSV로 저장."""
    return "송파삼전점 Launch 범위 밖 - posfeed exclusion report 스킵"


def sync_posfeed_blacklist(**context) -> str:
    """fin_product_grp.csv의 posfeed N 항목을 기존 parquet에 소급 적용 (매 실행마다)."""
    return "송파삼전점 Launch 범위 밖 - posfeed blacklist 동기화 스킵"


def generate_posfeed_whitelist_draft(**context) -> str:
    """posfeed item_name 목록을 draft CSV에 append (매 실행마다 신규 항목만 추가, 기존 항목 유지)."""
    return "송파삼전점 Launch 범위 밖 - posfeed whitelist draft 스킵"


def build_daily_summary(**context) -> str:
    """unified_sales → 일별×store×brand×order_type×platform 요약 parquet (LLM broadcast)."""
    return "송파삼전점 Launch 범위 밖 - daily summary 스킵"


def reclassify_platform(**context) -> str:
    """unified_sales의 포스/제휴사주문 platform을 테이블명 기반으로 재분류."""
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if not sale_date:
        return "Lookback 모드: 재분류 스킵 (신규 데이터에 이미 적용됨)"
    return pipeline_reclassify(sale_date, overwrite=True)


def enforce_baemin_manual_only(**context) -> str:
    """최종 방어: TEST_STORES의 배달의민족은 배민수동만 남긴다."""
    if not TEST_STORES:
        return "TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (
        enforce_baemin_manual_only_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return enforce_baemin_manual_only_for_test_stores(
        stores=TEST_STORES,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,
    )


def enforce_coupang_manual_only(**context) -> str:
    """최종 방어: TEST_STORES의 쿠팡이츠는 쿠팡수동만 남긴다."""
    stores = [store for store in TEST_STORES if store != "법흥리점"]
    if not stores:
        return "쿠팡수동 대상 TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
        enforce_coupang_manual_only_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return enforce_coupang_manual_only_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,
    )


with DAG(
    dag_id=dag_id,
    schedule=DB_LAUNCH_UNIFIED_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "unionpos", "easypos", "posfeed", "unified_sales"],
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

    t_baemin = PythonOperator(
        task_id="reconcile_baemin",
        python_callable=reconcile_baemin,
    )

    t_coupang = PythonOperator(
        task_id="reconcile_coupang",
        python_callable=reconcile_coupang,
    )

    t6 = PythonOperator(
        task_id="reclassify_platform",
        python_callable=reclassify_platform,
    )

    t6a = PythonOperator(
        task_id="enforce_baemin_manual_only",
        python_callable=enforce_baemin_manual_only,
    )

    t6b = PythonOperator(
        task_id="enforce_coupang_manual_only",
        python_callable=enforce_coupang_manual_only,
    )

    t7 = PythonOperator(
        task_id="validate_sales",
        python_callable=validate_sales,
        op_kwargs={"target_stores": TARGET_STORES},
    )

    t8 = PythonOperator(
        task_id="validate_monthly_sales",
        python_callable=validate_monthly_sales,
        op_kwargs={"target_stores": TARGET_STORES},
    )

    t9 = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    # 순차 실행: 같은 날짜 parquet에 동시 write 방지
    t1 >> t3 >> t3a >> t4 >> t5 >> t5a >> t5a3 >> t5b >> t5c >> t_baemin >> t_coupang >> t6 >> t6a >> t6b >> t7 >> t8 >> t9
