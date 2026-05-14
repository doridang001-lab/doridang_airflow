"""
통합 상품 마스터 DAG

처리 요약:
1) OKPOS 상품조회.xlsx 로드
2) fin_product_grp.csv와 비교 → 신규/변경 감지
3) LLM(Ollama)으로 수동분류 + is_main_candidate 자동 분류
4) CSV에 append (llm_check=Y)
5) 이메일 알림 → 사용자가 검토 후 llm_check 직접 N으로 수정

스케줄: DB_FIN_PRODUCT_TIME (매일 10:35, OKPOS Product 완료 후)
"""

import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import DB_FIN_PRODUCT_TIME
from modules.transform.pipelines.db.DB_FinProduct import (
    load_okpos_product_xlsx,
    detect_product_changes,
    classify_with_llm,
    update_product_master,
    send_alert_email,
    finalize_unionpos_pending,
    apply_review_approvals,
)

logger = logging.getLogger(__name__)

dag_id = Path(__file__).stem

# ------------------------------------------------------------
# Runtime switches (edit here)
# ------------------------------------------------------------
# LLM 분류 on/off (OFF면 신규/변경은 '기타/N'로만 적재되고, 사용자가 수동분류를 보강하면 됨)
ENABLE_LLM: bool = True

# OKPOS '구분' 필터 (현재는 홀만 진행)
ALLOWED_GUBUN = ["홀"]

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=dag_id,
    schedule=DB_FIN_PRODUCT_TIME,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["db", "product", "llm", "master"],
) as dag:

    t1 = PythonOperator(
        task_id="load_okpos_product_xlsx",
        python_callable=load_okpos_product_xlsx,
        op_kwargs={"allowed_gubun": ALLOWED_GUBUN},
    )

    t2 = PythonOperator(
        task_id="detect_product_changes",
        python_callable=detect_product_changes,
    )

    t3 = PythonOperator(
        task_id="classify_with_llm",
        python_callable=classify_with_llm,
        op_kwargs={"enable_llm": ENABLE_LLM},
    )

    t4 = PythonOperator(
        task_id="update_product_master",
        python_callable=update_product_master,
    )

    t5 = PythonOperator(
        task_id="send_alert_email",
        python_callable=send_alert_email,
    )

    t6 = PythonOperator(
        task_id="finalize_unionpos_pending",
        python_callable=finalize_unionpos_pending,
        op_kwargs={"enable_llm": ENABLE_LLM},
    )

    t7 = PythonOperator(
        task_id="apply_review_approvals",
        python_callable=apply_review_approvals,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
