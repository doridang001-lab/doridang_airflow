"""수집 결손 감시 DAG.

배민·쿠팡·posfeed·OKPOS·투오더 일매출의 최근 7일 날짜별 건수를 세어
결손(0건)·급감(전주 동일 요일 대비 50% 미만)을 텔레그램으로 알린다.

2026-09-11: 투오더 6일 결측, 배민·OKPOS 1일 결손이 아무 알림 없이 지나간 뒤 신설.

conf:
    days (int)     → 감시 창 일수 (기본 7)
    dry_run (bool) → 알림 없이 로그만
"""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_CollectionFreshness import check_collection_freshness
from modules.transform.utility.dag_defaults import DEFAULT_DAGRUN_TIMEOUT
from modules.transform.utility.schedule import DB_COLLECTION_FRESHNESS_TIME

dag_id = Path(__file__).stem

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # 파티션 전수 읽기가 배민 9월 118파일 기준 약 1분. 마운트가 멈추면 무한정 붙잡히므로 상한을 둔다.
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id=dag_id,
    description="수집원별 날짜 결손·급감 감시 (배민/쿠팡/posfeed/OKPOS/투오더)",
    schedule=DB_COLLECTION_FRESHNESS_TIME,
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    default_args=default_args,
    is_paused_upon_creation=False,
    tags=["db", "monitoring", "freshness"],
    doc_md=__doc__,
) as dag:
    PythonOperator(
        task_id="check_collection_freshness",
        python_callable=check_collection_freshness,
    )
