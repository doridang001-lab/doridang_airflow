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

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIFIED_SALES_TIME
from modules.transform.pipelines.db.DB_CoupangMacro_load import (
    load_coupang_macro_partition,
    move_coupang_down_to_collect,
)
from modules.transform.pipelines.db.DB_UnifiedSales import (
    ADD_TEST_STORES,
    DELIVERY_MANUAL_TEST_STORES,
    FULL_RECALC_STORES,
    TOORDER_MANUAL_STORES,
    backfill_okpos as pipeline_backfill_okpos,
    backfill_okpos_stores as pipeline_backfill_okpos_stores,
    backfill_toorder_manual_stores as pipeline_backfill_toorder,
    backfill_unionpos as pipeline_backfill_unionpos,
    backfill_unionpos_stores as pipeline_backfill_unionpos_stores,
    enforce_manual_delivery_sources_for_test_stores as pipeline_enforce_manual_delivery_sources,
    purge_manual_delivery_sources_for_non_test_stores as pipeline_purge_non_test_manual_delivery_sources,
    quarantine_conflict_copies as pipeline_quarantine_conflict_copies,
    upsert_fin_product_grp_from_unionpos as pipeline_upsert_unionpos_products,
    reclassify_hall_platform as pipeline_reclassify,
    refresh_store_meta_in_unified_sales as pipeline_refresh_store_meta,
    run_lookback_okpos as pipeline_lookback_okpos,
    run_lookback_toorder_manual_stores as pipeline_lookback_toorder,
    run_lookback_unionpos as pipeline_lookback_unionpos,
    run_okpos as pipeline_run_okpos,
    run_toorder_manual_stores as pipeline_run_toorder,
    run_unionpos as pipeline_run_unionpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_easypos import (
    backfill_easypos as pipeline_backfill_easypos,
    backfill_easypos_stores as pipeline_backfill_easypos_stores,
    run_easypos as pipeline_run_easypos,
    run_lookback_easypos as pipeline_lookback_easypos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import (
    backfill_posfeed as pipeline_backfill_posfeed,
    backfill_posfeed_stores as pipeline_backfill_posfeed_stores,
    generate_posfeed_whitelist_draft as pipeline_generate_whitelist_draft,
    report_posfeed_exclusions as pipeline_report_posfeed_exclusions,
    run_posfeed as pipeline_run_posfeed,
    run_lookback_posfeed as pipeline_lookback_posfeed,
    sync_posfeed_blacklist as pipeline_sync_posfeed_blacklist,
)
from modules.transform.pipelines.db.DB_UnifiedSales_validate import (
    validate_sales as pipeline_validate_sales,
    validate_monthly_sales as pipeline_validate_monthly_sales,
    build_daily_summary as pipeline_build_daily_summary,
)

logger = logging.getLogger(__name__)

dag_id = "DB_UnifiedSales"
TODAY_DAG_ID = "DB_UnifiedSales_Today_Dags"
TOTAL_DAG_ID = "DB_UnifiedSales_Total_Macro_Dags"
LEGACY_TOTAL_DAG_ID = "DB_UnifiedSales_Total_Dags"
BLOCKING_DAG_IDS = (TODAY_DAG_ID, TOTAL_DAG_ID, LEGACY_TOTAL_DAG_ID)
BLOCKING_STATES = ("running",)

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
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": _on_failure_callback,
}

# LOOKBACK_DAYS:
# - int  : 최근 N일 처리 (OKPOS는 원천 기준 교체, 그 외 채널은 기존 lookback 정책)
# - None : 전체기간 백필
#
# 기본 스케줄/수동 재실행은 최근 구간만 만진다.
# FULL_RECALC_STORES는 기본 실행 범위를 유지한 뒤 지정 매장만 전체기간 추가 복구한다.
LOOKBACK_DAYS: int | None = 7

# 배민·쿠팡 직수집 교정 대상 테스트 매장.
# 공통 저장/비교 로직도 같은 목록으로 배달 POS source를 제외한다.
TEST_STORES: list[str] = DELIVERY_MANUAL_TEST_STORES
AUTO_ENFORCE_TEST_STORES: list[str] = [store for store in ADD_TEST_STORES if str(store).strip()]
FULL_RECALC_TARGET_STORES: list[str] = [store for store in FULL_RECALC_STORES if str(store).strip()]


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


def _assert_no_blocking_run_active() -> None:
    with create_session() as session:
        rows = (
            session.query(DagRun.dag_id, DagRun.run_id, DagRun.state)
            .filter(DagRun.dag_id.in_(BLOCKING_DAG_IDS))
            .filter(DagRun.state.in_(BLOCKING_STATES))
            .order_by(DagRun.execution_date.desc())
            .all()
        )
    if not rows:
        return
    active = ", ".join(f"{dag_id}:{run_id}:{state}" for dag_id, run_id, state in rows)
    raise AirflowException(
        "UnifiedSales 관련 DAG 실행 중이어서 parquet 동시 write 방지를 위해 "
        f"DB_UnifiedSales 실행을 재시도합니다. active_runs={active}"
    )


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_today_mode(context) -> bool:
    return _truthy(_conf(context).get("today_mode"))


def _is_partial_store_mode(context) -> bool:
    return _truthy(_conf(context).get("partial_store_mode"))


def _is_repair_mode(context) -> bool:
    return _truthy(_conf(context).get("repair_mode"))


def _is_backfill_mode(context) -> bool:
    return _truthy(_conf(context).get("backfill"))


def _is_full_backfill_requested(context) -> bool:
    conf = _conf(context)
    return _truthy(conf.get("backfill")) or _truthy(conf.get("backfill_all"))


def _has_full_recalc_targets(context) -> bool:
    return (
        bool(FULL_RECALC_TARGET_STORES)
        and not _is_partial_store_mode(context)
        and not _is_today_mode(context)
        and not _is_full_backfill_requested(context)
        and LOOKBACK_DAYS is not None
    )


def _require_lookback_days() -> int | None:
    if LOOKBACK_DAYS is None:
        logger.info("LOOKBACK_DAYS=None: 전체기간 백필 모드로 처리합니다.")
        return None
    return LOOKBACK_DAYS


def _manual_delivery_target_stores(context) -> list[str]:
    store_scope = _xcom_store_scope(context)
    if store_scope:
        return _manual_delivery_store_scope(store_scope)
    return list(TEST_STORES)


def _manual_delivery_lookback_days(context) -> int | None:
    if _is_full_backfill_requested(context):
        return None
    return _require_lookback_days()


def _lookback_target_dates(days: int) -> list[str]:
    today = pendulum.now("Asia/Seoul")
    return [today.subtract(days=i).format("YYYY-MM-DD") for i in range(1, days + 1)]


def _full_recalc_stores() -> list[str]:
    return list(FULL_RECALC_TARGET_STORES)


def _manual_delivery_store_scope(stores: list[str]) -> list[str]:
    test_store_set = {str(store).strip() for store in TEST_STORES if str(store).strip()}
    return [store for store in stores if str(store).strip() in test_store_set]


def _conf_stores(context, *, default_to_test_stores: bool = False) -> list[str]:
    raw_stores = _conf(context).get("stores")
    if raw_stores is None:
        return list(TEST_STORES) if default_to_test_stores else []
    if isinstance(raw_stores, str):
        stores = [part.strip() for part in raw_stores.split(",")]
    elif isinstance(raw_stores, (list, tuple, set)):
        stores = [str(store).strip() for store in raw_stores]
    else:
        raise ValueError("conf['stores']는 문자열 또는 리스트여야 합니다.")
    return [store for store in stores if store]


def _resolve_store_scope(context, sale_date: str | None) -> list[str]:
    if _is_partial_store_mode(context):
        return _conf_stores(context, default_to_test_stores=True)
    return []


def _xcom_store_scope(context) -> list[str]:
    ti = context.get("ti")
    if not ti:
        return []
    stores = ti.xcom_pull(task_ids="resolve_date", key="stores") or []
    return [str(store).strip() for store in stores if str(store).strip()]


def _is_store_scoped_mode(context) -> bool:
    return _is_partial_store_mode(context) or bool(_xcom_store_scope(context))


def _append_full_recalc_result(context, normal_result: str, full_recalc_func) -> str:
    if not _has_full_recalc_targets(context):
        return normal_result
    stores = _full_recalc_stores()
    full_result = full_recalc_func(stores)
    return f"{normal_result} | FULL_RECALC_STORES 추가 전체복구 stores={stores}: {full_result}"


def _append_manual_full_recalc_result(context, normal_result: str, full_recalc_func) -> str:
    if not _has_full_recalc_targets(context):
        return normal_result
    stores = _manual_delivery_store_scope(_full_recalc_stores())
    if not stores:
        return normal_result
    full_result = full_recalc_func(stores)
    return f"{normal_result} | FULL_RECALC_STORES 수동배달 추가 전체복구 stores={stores}: {full_result}"


def resolve_date(**context) -> str:
    """conf['sale_date'] → XCom push (정정 모드). 없으면 None push."""
    _assert_no_blocking_run_active()
    conf      = _conf(context)
    requested_sale_date = conf.get("sale_date") if isinstance(conf, dict) else None
    sale_date = requested_sale_date
    store_scope = _resolve_store_scope(context, sale_date)

    if _is_today_mode(context) and not requested_sale_date:
        raise ValueError("today_mode=true 실행에는 conf['sale_date']가 필요합니다.")
    if _is_partial_store_mode(context):
        if not sale_date:
            raise ValueError("partial_store_mode=true 실행에는 conf['sale_date']가 필요합니다.")
        if not store_scope:
            raise ValueError("partial_store_mode=true 실행에는 TEST_STORES 또는 conf['stores']가 필요합니다.")
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)
        ti.xcom_push(key="stores", value=store_scope)

    if sale_date:
        logger.info("정정 모드 → %s", sale_date)
        if _is_partial_store_mode(context):
            return (
                f"매장 부분 정정 날짜: {sale_date} | "
                f"stores={store_scope}"
            )
        if store_scope:
            return f"매장 제한 정정 날짜: {sale_date} | stores={store_scope}"
        if _has_full_recalc_targets(context):
            return f"정정 날짜: {sale_date} | FULL_RECALC_STORES 추가 전체복구 stores={_full_recalc_stores()}"
        return f"정정 날짜: {sale_date}"
    if _is_backfill_mode(context):
        logger.info("dag_run.conf backfill=true → 전체 소스 백필 모드")
        return "전체 소스 백필 모드"
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        logger.info("LOOKBACK_DAYS=None: 전체기간 백필 모드")
        return "전체기간 백필 모드"
    if _has_full_recalc_targets(context):
        return f"Lookback {lookback_days}일 모드 | FULL_RECALC_STORES 추가 전체복구 stores={_full_recalc_stores()}"
    logger.info("Lookback %d일 모드", lookback_days)
    return f"Lookback {lookback_days}일 모드"


def ingest_pc2(**context) -> str:
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - pc2 inbox 적재 스킵"
    if _is_today_mode(context):
        return "today_mode=true - pc2 inbox 적재 스킵"
    try:
        from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import ingest_baemin_pc2_inbox
    except ModuleNotFoundError as exc:
        if exc.name != "modules.transform.pipelines.db.DB_Beamin_pc2_distribute":
            raise
        logger.warning("DB_Beamin_pc2_distribute 모듈 없음: pc2 inbox 적재 생략")
        return "pc2 inbox 적재 생략: 모듈 없음"

    return ingest_baemin_pc2_inbox(**context)


def ingest_manual_baemin_orders(**context) -> str:
    """영업관리부_수집의 배민 수동 CSV를 baemin 원데이터 파티션으로 선적재한다."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 배민 수동 CSV 선적재 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 배민 수동 CSV 선적재 스킵"
    from modules.transform.pipelines.db.DB_BaeminManual_load import (
        load_manual_baemin_files,
    )

    return load_manual_baemin_files(**context)


def cleanup_manual_baemin_orders(**context) -> str:
    """UnifiedSales DAG 안에서 적재 성공한 배민 수동 CSV를 보관 처리한다."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 배민 수동 CSV cleanup 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 배민 수동 CSV cleanup 스킵"
    from modules.transform.pipelines.db.DB_BaeminManual_load import (
        cleanup_manual_baemin_files,
    )

    return cleanup_manual_baemin_files(**context)


def quarantine_conflicts(**context) -> str:
    """OneDrive 충돌본을 집계 전에 격리한다."""
    return pipeline_quarantine_conflict_copies()


def build_okpos(**context) -> str:
    """okpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 LOOKBACK_DAYS 범위 교체.
    FULL_RECALC_STORES가 있으면 기본 실행 후 지정 매장만 전체기간 추가 복구.
    conf['backfill'] 또는 conf['backfill_all'] 있으면 전체 매장 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _is_full_backfill_requested(context):
        return pipeline_backfill_okpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = _xcom_store_scope(context)
        if stores:
            result = pipeline_run_okpos(sale_date, overwrite=False, stores=stores)
        else:
            result = pipeline_run_okpos(sale_date, overwrite=True)
        return _append_full_recalc_result(context, result, pipeline_backfill_okpos_stores)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_okpos()
    result = pipeline_lookback_okpos(days=lookback_days)
    return _append_full_recalc_result(context, result, pipeline_backfill_okpos_stores)


def build_unionpos(**context) -> str:
    """unionpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback N일 누락 append.
    FULL_RECALC_STORES가 있으면 기본 실행 후 지정 매장만 전체기간 추가 복구.
    conf['backfill'] 또는 conf['backfill_all'] 있으면 전체 매장 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _is_full_backfill_requested(context):
        return pipeline_backfill_unionpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = _xcom_store_scope(context)
        if stores:
            result = pipeline_run_unionpos(sale_date, overwrite=False, stores=stores)
        else:
            result = pipeline_run_unionpos(sale_date, overwrite=True)
        return _append_full_recalc_result(context, result, pipeline_backfill_unionpos_stores)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_unionpos()
    result = pipeline_lookback_unionpos(days=lookback_days)
    return _append_full_recalc_result(context, result, pipeline_backfill_unionpos_stores)


def sync_unionpos_products(**context) -> str:
    """UnionPOS 영수증 품목을 기반으로 fin_product_grp_input.csv 미등록 상품을 선반영한다."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - unionpos 상품 마스터 선반영 스킵"

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        ym = str(sale_date)[:7]
        return pipeline_upsert_unionpos_products(ym=ym, dry_run=False)

    # lookback/백필 모드: 최근 구간이 월 경계를 넘을 수 있어 당월+전월을 선반영
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
    FULL_RECALC_STORES가 있으면 기본 실행 후 지정 매장만 전체기간 추가 복구.
    conf['backfill'] 또는 conf['backfill_all'] 있으면 전체 매장 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _is_full_backfill_requested(context):
        return pipeline_backfill_easypos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = _xcom_store_scope(context)
        if stores:
            result = pipeline_run_easypos(sale_date, overwrite=False, stores=stores)
        else:
            result = pipeline_run_easypos(sale_date, overwrite=True)
        return _append_full_recalc_result(context, result, pipeline_backfill_easypos_stores)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_easypos()
    result = pipeline_lookback_easypos(days=lookback_days)
    return _append_full_recalc_result(context, result, pipeline_backfill_easypos_stores)


def build_posfeed(**context) -> str:
    """posfeed → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 lookback/백필.
    FULL_RECALC_STORES가 있으면 기본 실행 후 지정 매장만 전체기간 추가 복구.
    conf['backfill'] 또는 conf['backfill_all'] 있으면 전체 매장 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _is_full_backfill_requested(context):
        return pipeline_backfill_posfeed()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = _xcom_store_scope(context)
        if stores:
            result = pipeline_run_posfeed(
                sale_date,
                overwrite=False,
                persist_item_ids=not _is_today_mode(context),
                stores=stores,
            )
        else:
            result = pipeline_run_posfeed(
                sale_date,
                overwrite=True,
                persist_item_ids=not _is_today_mode(context),
            )
        return _append_full_recalc_result(context, result, pipeline_backfill_posfeed_stores)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_posfeed()
    result = pipeline_lookback_posfeed(days=lookback_days)
    return _append_full_recalc_result(context, result, pipeline_backfill_posfeed_stores)


def build_toorder(**context) -> str:
    """POS 없는 수동매장의 배민·쿠팡 제외 채널을 toorder에서 적재."""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _is_full_backfill_requested(context):
        return pipeline_backfill_toorder()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = _xcom_store_scope(context)
        if stores:
            result = pipeline_run_toorder(sale_date, stores=stores, overwrite=False)
        else:
            result = pipeline_run_toorder(sale_date, overwrite=True)
        return _append_full_recalc_result(
            context,
            result,
            lambda stores: pipeline_backfill_toorder(stores=stores),
        )
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_toorder()
    result = pipeline_lookback_toorder(lookback_days)
    return _append_full_recalc_result(
        context,
        result,
        lambda stores: pipeline_backfill_toorder(stores=stores),
    )


def reconcile_baemin(**context) -> str:
    """TEST_STORES의 배민 직수집 데이터로 UnifiedSales 배달의민족 행 교정."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 배민 reconcile 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 배민 reconcile 스킵"
    stores = _manual_delivery_target_stores(context)
    if not stores:
        return "TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (
        reconcile_baemin_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    result = reconcile_baemin_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )
    return _append_manual_full_recalc_result(
        context,
        result,
        lambda scoped_stores: reconcile_baemin_for_test_stores(
            stores=scoped_stores,
            sale_date=None,
            lookback_days=None,
        ),
    )


def reconcile_coupang(**context) -> str:
    """TEST_STORES의 쿠팡 직수집 데이터로 UnifiedSales 쿠팡이츠 행 교정."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 쿠팡 reconcile 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 쿠팡 reconcile 스킵"
    stores = _manual_delivery_target_stores(context)
    if not stores:
        return "쿠팡수동 대상 TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
        reconcile_coupang_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    result = reconcile_coupang_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )
    return _append_manual_full_recalc_result(
        context,
        result,
        lambda scoped_stores: reconcile_coupang_for_test_stores(
            stores=scoped_stores,
            sale_date=None,
            lookback_days=None,
        ),
    )


def report_posfeed_exclusions(**context) -> str:
    """posfeed 블랙리스트 제외 내역을 ym별 상세·집계 CSV로 저장."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - posfeed 제외 리포트 스킵"
    if _is_today_mode(context):
        return "today_mode=true - posfeed 제외 리포트 스킵"
    return pipeline_report_posfeed_exclusions(**context)


def sync_posfeed_blacklist(**context) -> str:
    """fin_product_grp_input.csv의 posfeed 제외 항목을 기존 parquet에 제한적으로 소급 적용."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - posfeed blacklist 동기화 스킵"
    if _is_today_mode(context):
        return "today_mode=true - posfeed blacklist 동기화 스킵"

    conf = _conf(context)
    if _truthy(conf.get("sync_posfeed_blacklist_all")):
        return pipeline_sync_posfeed_blacklist()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        return pipeline_sync_posfeed_blacklist(target_dates=[str(sale_date)])

    if _truthy(conf.get("backfill")):
        return "backfill 모드: 전체 posfeed blacklist 소급은 sync_posfeed_blacklist_all=true일 때만 실행"

    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_sync_posfeed_blacklist()
    return pipeline_sync_posfeed_blacklist(target_dates=_lookback_target_dates(lookback_days))


def generate_posfeed_whitelist_draft(**context) -> str:
    """posfeed 전체 whitelist draft/LLM은 명시적으로 요청한 경우에만 실행한다."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - posfeed whitelist draft 스킵"
    if _is_today_mode(context):
        return "today_mode=true - posfeed whitelist draft 스킵"
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    conf = _conf(context)
    if sale_date:
        return f"정정 모드({sale_date}): 전체 posfeed whitelist draft 생성 스킵"
    if not conf.get("backfill"):
        return "기본/lookback 모드: 전체 posfeed whitelist draft 생성 스킵"
    if not conf.get("posfeed_whitelist_llm"):
        return "backfill 모드: posfeed_whitelist_llm 미지정으로 전체 LLM 생성 스킵"
    return pipeline_generate_whitelist_draft(enable_llm=True)


def build_daily_summary(**context) -> str:
    """unified_sales → 일별×store×brand×order_type×platform 요약 parquet (LLM broadcast)."""
    return pipeline_build_daily_summary()


def refresh_store_meta(**context) -> str:
    """sales_employee.csv 기준으로 unified_sales 매장 메타를 최신화."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - 매장 메타 전체 갱신 스킵"
    return pipeline_refresh_store_meta()


def enforce_manual_delivery_sources(**context) -> str:
    """TEST_STORES 배달 플랫폼은 수동 source만 남기도록 전체 파일을 정리."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - 수동 배달 source 전체 enforce 스킵"
    if _is_today_mode(context):
        if AUTO_ENFORCE_TEST_STORES:
            return pipeline_enforce_manual_delivery_sources(stores=AUTO_ENFORCE_TEST_STORES)
        return "today_mode=true - 수동 배달 source enforce 스킵"
    result = pipeline_enforce_manual_delivery_sources(stores=_manual_delivery_target_stores(context))
    return _append_manual_full_recalc_result(
        context,
        result,
        lambda scoped_stores: pipeline_enforce_manual_delivery_sources(stores=scoped_stores),
    )


def purge_non_test_manual_delivery_sources(**context) -> str:
    """TEST_STORES에서 제외된 매장의 과거 배민수동/쿠팡수동 행을 제거."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - 비테스트 수동 배달 source purge 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 비테스트 수동 배달 source purge 스킵"
    return pipeline_purge_non_test_manual_delivery_sources(stores=TEST_STORES)


def reclassify_platform(**context) -> str:
    """unified_sales의 포스/제휴사주문 platform을 테이블명 기반으로 재분류."""
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - 재분류 스킵"
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if not sale_date:
        return "Lookback 모드: 재분류 스킵 (신규 데이터에 이미 적용됨)"
    return pipeline_reclassify(sale_date, overwrite=True)


def enforce_baemin_manual_only(**context) -> str:
    """최종 방어: TEST_STORES의 배달의민족은 배민수동만 남긴다."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 배민 manual only enforce 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 배민 manual only enforce 스킵"
    stores = _manual_delivery_target_stores(context)
    if not stores:
        return "TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (
        enforce_baemin_manual_only_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    result = enforce_baemin_manual_only_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )
    return _append_manual_full_recalc_result(
        context,
        result,
        lambda scoped_stores: enforce_baemin_manual_only_for_test_stores(
            stores=scoped_stores,
            sale_date=None,
            lookback_days=None,
        ),
    )


def enforce_coupang_manual_only(**context) -> str:
    """최종 방어: TEST_STORES의 쿠팡이츠는 쿠팡수동만 남긴다."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 쿠팡 manual only enforce 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 쿠팡 manual only enforce 스킵"
    stores = _manual_delivery_target_stores(context)
    if not stores:
        return "쿠팡수동 대상 TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
        enforce_coupang_manual_only_for_test_stores,
    )

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    result = enforce_coupang_manual_only_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )
    return _append_manual_full_recalc_result(
        context,
        result,
        lambda scoped_stores: enforce_coupang_manual_only_for_test_stores(
            stores=scoped_stores,
            sale_date=None,
            lookback_days=None,
        ),
    )


def validate_sales_task(**context) -> str:
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - validate_sales 스킵"
    if _is_today_mode(context):
        return "today_mode=true - validate_sales 스킵"
    if _is_repair_mode(context):
        return "repair_mode=true - validate_sales 알림 스킵"
    return pipeline_validate_sales()


def validate_monthly_sales_task(**context) -> str:
    if _is_store_scoped_mode(context):
        return "매장 제한 실행 - validate_monthly_sales 스킵"
    if _is_repair_mode(context):
        return "repair_mode=true - validate_monthly_sales 알림 스킵"
    return pipeline_validate_monthly_sales()


def send_today_success_alert(**context) -> str:
    if not _is_today_mode(context):
        return "today_mode 아님 - 성공 알림 스킵"

    ti = context.get("ti")
    sale_date = ti.xcom_pull(task_ids="resolve_date", key="sale_date") if ti else None
    completed_at = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", "")
    body = (
        "[도리당] Today UnifiedSales 완료\n"
        f"DAG: {dag_id}\n"
        f"sale_date: {sale_date or '-'}\n"
        f"run_id: {run_id}\n"
        f"완료시각: {completed_at} KST"
    )
    send_telegram(body)
    return f"Today UnifiedSales 성공 알림 발송: {sale_date or '-'}"


with DAG(
    dag_id=dag_id,
    schedule=DB_UNIFIED_SALES_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "okpos", "unionpos", "easypos", "posfeed", "unified_sales"],
) as dag:

    t1 = PythonOperator(
        task_id="resolve_date",
        python_callable=resolve_date,
        retries=24,
        retry_delay=timedelta(minutes=10),
    )

    t_move_coupang = PythonOperator(
        task_id="move_coupang_to_collect",
        python_callable=move_coupang_down_to_collect,
    )

    t_load_coupang = PythonOperator(
        task_id="load_coupang_macro_partition",
        python_callable=load_coupang_macro_partition,
    )

    t_ingest_pc2 = PythonOperator(
        task_id="ingest_baemin_pc2_inbox",
        python_callable=ingest_pc2,
    )

    t_ingest_manual_baemin = PythonOperator(
        task_id="ingest_manual_baemin_orders",
        python_callable=ingest_manual_baemin_orders,
    )

    t_cleanup_manual_baemin = PythonOperator(
        task_id="cleanup_manual_baemin_orders",
        python_callable=cleanup_manual_baemin_orders,
    )

    t_quarantine = PythonOperator(
        task_id="quarantine_conflicts",
        python_callable=quarantine_conflicts,
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

    t_toorder = PythonOperator(
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

    t6b2 = PythonOperator(
        task_id="enforce_manual_delivery_sources",
        python_callable=enforce_manual_delivery_sources,
    )

    t6b3 = PythonOperator(
        task_id="purge_non_test_manual_delivery_sources",
        python_callable=purge_non_test_manual_delivery_sources,
    )

    t6c = PythonOperator(
        task_id="refresh_store_meta",
        python_callable=refresh_store_meta,
    )

    t7 = PythonOperator(
        task_id="validate_sales",
        python_callable=validate_sales_task,
    )

    t8 = PythonOperator(
        task_id="validate_monthly_sales",
        python_callable=validate_monthly_sales_task,
    )

    t9 = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )

    t10 = PythonOperator(
        task_id="send_today_success_alert",
        python_callable=send_today_success_alert,
    )

    # 순차 실행: 같은 날짜 parquet에 동시 write 방지
    t_move_coupang >> t_load_coupang >> t_ingest_manual_baemin >> t_cleanup_manual_baemin >> t1 >> t_quarantine >> t_ingest_pc2 >> t3 >> t3a >> t4 >> t5 >> t5a >> t_toorder >> t5a3 >> t5b >> t5c >> t_baemin >> t_coupang >> t6 >> t6a >> t6b >> t6b2 >> t6b3 >> t6c >> t7 >> t8 >> t9 >> t10
