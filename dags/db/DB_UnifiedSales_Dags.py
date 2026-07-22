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
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import enqueue_heal_task, send_telegram
from modules.transform.utility.schedule import DB_UNIFIED_SALES_TIME
from modules.transform.pipelines.db.DB_UnifiedSales import (
    ADD_TEST_STORES,
    DELIVERY_MANUAL_TEST_STORES,
    TOORDER_MANUAL_STORES,
    backfill_okpos as pipeline_backfill_okpos,
    backfill_toorder_manual_stores as pipeline_backfill_toorder,
    backfill_unionpos as pipeline_backfill_unionpos,
    enforce_manual_delivery_sources_for_test_stores as pipeline_enforce_manual_delivery_sources,
    purge_manual_delivery_sources_for_non_test_stores as pipeline_purge_non_test_manual_delivery_sources,
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
    validate_sales as pipeline_validate_sales,
    validate_monthly_sales as pipeline_validate_monthly_sales,
    build_daily_summary as pipeline_build_daily_summary,
)

logger = logging.getLogger(__name__)

dag_id = "DB_UnifiedSales"

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
# 전체 백필은 dag_run.conf {"backfill": true}로만 실행한다.
LOOKBACK_DAYS: int | None = 14

# 배민·쿠팡 직수집 교정 대상 테스트 매장.
# 공통 저장/비교 로직도 같은 목록으로 배달 POS source를 제외한다.
TEST_STORES: list[str] = DELIVERY_MANUAL_TEST_STORES
AUTO_ENFORCE_TEST_STORES: list[str] = [store for store in ADD_TEST_STORES if str(store).strip()]


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return conf if isinstance(conf, dict) else {}


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


def _require_lookback_days() -> int | None:
    if LOOKBACK_DAYS is None:
        logger.info("LOOKBACK_DAYS=None: 전체기간 백필 모드로 처리합니다.")
        return None
    return LOOKBACK_DAYS


def _manual_delivery_target_stores(context) -> list[str]:
    if _is_backfill_mode(context) and AUTO_ENFORCE_TEST_STORES:
        return list(AUTO_ENFORCE_TEST_STORES)
    return list(TEST_STORES)


def _manual_delivery_lookback_days(context) -> int | None:
    if _is_backfill_mode(context) and AUTO_ENFORCE_TEST_STORES:
        return None
    return _require_lookback_days()


def _lookback_target_dates(days: int) -> list[str]:
    today = pendulum.now("Asia/Seoul")
    return [today.subtract(days=i).format("YYYY-MM-DD") for i in range(days)]


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


def resolve_date(**context) -> str:
    """conf['sale_date'] → XCom push (정정 모드). 없으면 None push."""
    conf      = _conf(context)
    sale_date = conf.get("sale_date") if isinstance(conf, dict) else None

    if _is_today_mode(context) and not sale_date:
        raise ValueError("today_mode=true 실행에는 conf['sale_date']가 필요합니다.")
    if _is_partial_store_mode(context):
        stores = _conf_stores(context, default_to_test_stores=True)
        if not sale_date:
            raise ValueError("partial_store_mode=true 실행에는 conf['sale_date']가 필요합니다.")
        if not stores:
            raise ValueError("partial_store_mode=true 실행에는 TEST_STORES 또는 conf['stores']가 필요합니다.")

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="sale_date", value=sale_date)
        ti.xcom_push(
            key="stores",
            value=_conf_stores(context, default_to_test_stores=_is_partial_store_mode(context)),
        )

    if sale_date:
        logger.info("정정 모드 → %s", sale_date)
        if _is_partial_store_mode(context):
            return (
                f"매장 부분 정정 날짜: {sale_date} | "
                f"stores={_conf_stores(context, default_to_test_stores=True)}"
            )
        return f"정정 날짜: {sale_date}"
    if _is_backfill_mode(context):
        logger.info("dag_run.conf backfill=true → 전체 소스 백필 모드")
        return "전체 소스 백필 모드"
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        logger.info("LOOKBACK_DAYS=None: 전체기간 백필 모드")
        return "전체기간 백필 모드"
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


def build_okpos(**context) -> str:
    """okpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 LOOKBACK_DAYS 범위 교체.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_okpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        if _is_partial_store_mode(context):
            stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
            return pipeline_run_okpos(sale_date, overwrite=False, stores=stores)
        return pipeline_run_okpos(sale_date, overwrite=True)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_okpos()
    return pipeline_lookback_okpos(days=lookback_days)


def build_unionpos(**context) -> str:
    """unionpos → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 Lookback N일 누락 append.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_unionpos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        if _is_partial_store_mode(context):
            stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
            return pipeline_run_unionpos(sale_date, overwrite=False, stores=stores)
        return pipeline_run_unionpos(sale_date, overwrite=True)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_unionpos()
    return pipeline_lookback_unionpos(days=lookback_days)


def sync_unionpos_products(**context) -> str:
    """UnionPOS 영수증 품목을 기반으로 fin_product_grp_input.csv 미등록 상품을 선반영한다."""
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
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_easypos()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        if _is_partial_store_mode(context):
            stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
            return pipeline_run_easypos(sale_date, overwrite=False, stores=stores)
        return pipeline_run_easypos(sale_date, overwrite=True)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_easypos()
    return pipeline_lookback_easypos(days=lookback_days)


def build_posfeed(**context) -> str:
    """posfeed → unified_sales 저장.

    conf['sale_date'] 있으면 정정(overwrite), 없으면 lookback/백필.
    conf['backfill'] 있으면 전체 backfill.
    """
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_posfeed()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
        if _is_partial_store_mode(context):
            return pipeline_run_posfeed(
                sale_date,
                overwrite=False,
                persist_item_ids=not _is_today_mode(context),
                stores=stores,
            )
        return pipeline_run_posfeed(
            sale_date,
            overwrite=True,
            persist_item_ids=not _is_today_mode(context),
        )
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_posfeed()
    return pipeline_lookback_posfeed(days=lookback_days)


def build_toorder(**context) -> str:
    """POS 없는 수동매장의 배민·쿠팡 제외 채널을 toorder에서 적재."""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_toorder()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        if _is_partial_store_mode(context):
            stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
            return pipeline_run_toorder(sale_date, stores=stores, overwrite=False)
        return pipeline_run_toorder(sale_date, overwrite=True)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_toorder()
    return pipeline_lookback_toorder(lookback_days)


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
    return reconcile_baemin_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
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
    return reconcile_coupang_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )


def report_posfeed_exclusions(**context) -> str:
    """posfeed 블랙리스트 제외 내역을 ym별 상세·집계 CSV로 저장."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - posfeed 제외 리포트 스킵"
    if _is_today_mode(context):
        return "today_mode=true - posfeed 제외 리포트 스킵"
    return pipeline_report_posfeed_exclusions(**context)


def sync_posfeed_blacklist(**context) -> str:
    """fin_product_grp_input.csv의 posfeed 제외 항목을 기존 parquet에 제한적으로 소급 적용."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - posfeed blacklist 동기화 스킵"
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
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - posfeed whitelist draft 스킵"
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
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 매장 메타 전체 갱신 스킵"
    return pipeline_refresh_store_meta()


def enforce_manual_delivery_sources(**context) -> str:
    """TEST_STORES 배달 플랫폼은 수동 source만 남기도록 전체 파일을 정리."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 수동 배달 source enforce 스킵"
    if _is_today_mode(context):
        if AUTO_ENFORCE_TEST_STORES:
            return pipeline_enforce_manual_delivery_sources(stores=AUTO_ENFORCE_TEST_STORES)
        return "today_mode=true - 수동 배달 source enforce 스킵"
    return pipeline_enforce_manual_delivery_sources(stores=_manual_delivery_target_stores(context))


def purge_non_test_manual_delivery_sources(**context) -> str:
    """TEST_STORES에서 제외된 매장의 과거 배민수동/쿠팡수동 행을 제거."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 비테스트 수동 배달 source purge 스킵"
    if _is_today_mode(context):
        return "today_mode=true - 비테스트 수동 배달 source purge 스킵"
    return pipeline_purge_non_test_manual_delivery_sources(stores=TEST_STORES)


def reclassify_platform(**context) -> str:
    """unified_sales의 포스/제휴사주문 platform을 테이블명 기반으로 재분류."""
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - 재분류 스킵"
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
    return enforce_baemin_manual_only_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
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
    return enforce_coupang_manual_only_for_test_stores(
        stores=stores,
        sale_date=sale_date,
        lookback_days=_manual_delivery_lookback_days(context),
    )


def validate_sales_task(**context) -> str:
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - validate_sales 스킵"
    if _is_today_mode(context):
        return "today_mode=true - validate_sales 스킵"
    if _is_repair_mode(context):
        return "repair_mode=true - validate_sales 알림 스킵"
    return pipeline_validate_sales()


def validate_monthly_sales_task(**context) -> str:
    if _is_partial_store_mode(context):
        return "partial_store_mode=true - validate_monthly_sales 스킵"
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
    t_ingest_manual_baemin >> t_cleanup_manual_baemin >> t1 >> t_ingest_pc2 >> t3 >> t3a >> t4 >> t5 >> t5a >> t_toorder >> t5a3 >> t5b >> t5c >> t_baemin >> t_coupang >> t6 >> t6a >> t6b >> t6b2 >> t6b3 >> t6c >> t7 >> t8 >> t9 >> t10
