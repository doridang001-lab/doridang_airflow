"""
투오더 종합보고서 일일 자동 다운로드 DAG

처리 흐름:
1. dag_run.conf 유무에 따라 단일(전일) 또는 범위 날짜 리스트 결정
2. 날짜 리스트에 해당하는 종합보고서 크롤링 다운로드
3. 다운로드된 파일을 LOCAL_DB/toorder_sales_report 로 이동

conf 키:
    start_date (str): 범위 시작일 "YYYY-MM-DD" (end_date와 함께 지정)
    end_date   (str): 범위 종료일 "YYYY-MM-DD" (start_date와 함께 지정)

실행 시각: 매일 09:00 (KST)
"""

import logging
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.extract.crawling_toorder_sales_report import (
    generate_date_range,
    run_crawling_date_range,
)
from modules.load.load_df_glob import move_download_files
from modules.transform.utility.paths import DOWN_DIR, LOCAL_DB
from modules.transform.utility.schedule import SMD_TOORDER_SALES_REPORT_TIME

logger = logging.getLogger(__name__)

# ============================================================
# 설정
# ============================================================

TOORDER_ID = "doridang1"
TOORDER_PW = "ehfl0109!!"


# ============================================================
# 태스크 함수
# ============================================================

def get_target_dates(**context) -> list:
    """
    dag_run.conf 유무에 따라 처리할 날짜 리스트를 결정하고 XCom에 push한다.

    conf에 start_date / end_date가 모두 있으면 범위 모드,
    없으면 KST 기준 전일 날짜 1건으로 동작한다.

    XCom key: date_list
    """
    conf = context.get("dag_run").conf or {}

    if conf.get("start_date") and conf.get("end_date"):
        date_list = generate_date_range(conf["start_date"], conf["end_date"])
        logger.info(
            "[범위 모드] %s ~ %s: %d일",
            conf["start_date"],
            conf["end_date"],
            len(date_list),
        )
    else:
        kst = pendulum.timezone("Asia/Seoul")
        yesterday = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")
        date_list = [yesterday]
        logger.info("[일별 모드] 대상 날짜: %s", yesterday)

    context["ti"].xcom_push(key="date_list", value=date_list)
    return date_list


def crawl_reports(**context) -> str:
    """
    투오더 종합보고서를 날짜 리스트 기준으로 크롤링 다운로드한다.

    XCom pull:  task_id='get_target_dates', key='date_list'
    XCom push:  key='downloaded_files' (성공한 파일 경로 리스트)
    """
    date_list = context["ti"].xcom_pull(
        task_ids="get_target_dates", key="date_list"
    )

    results = run_crawling_date_range(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        date_list=date_list,
        download_dir=DOWN_DIR,
    )

    downloaded = [r["file"] for r in results if r["success"] and r["file"]]
    failed = [r for r in results if not r["success"]]

    logger.info("[크롤링 완료] %d/%d건 성공", len(downloaded), len(date_list))

    if failed:
        for f in failed:
            logger.warning(
                "다운로드 실패 - 날짜: %s, 사유: %s", f["date"], f["error"]
            )

    context["ti"].xcom_push(key="downloaded_files", value=downloaded)
    return f"{len(downloaded)}건 다운로드 완료"


def move_files(**context) -> str:
    """
    다운로드 폴더의 종합보고서 파일을 LOCAL_DB/toorder_sales_report 로 이동한다.

    대상 패턴: 종합보고서_채널별(일)매출보고서_*.xlsx
    """
    dest_dir = str(LOCAL_DB / "toorder_sales_report")
    logger.info("파일 이동 시작: %s → %s", str(DOWN_DIR), dest_dir)

    move_download_files(
        patterns=["종합보고서_채널별(일)매출보고서_*.xlsx"],
        dest_dir=dest_dir,
        base_dir=str(DOWN_DIR),
    )

    logger.info("파일 이동 완료")
    return f"이동 대상 경로: {dest_dir}"


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=Path(__file__).stem,
    description="투오더 종합보고서 일일 자동 다운로드 (전일 또는 범위)",
    schedule=SMD_TOORDER_SALES_REPORT_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "toorder", "sales_report", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:

    t1 = PythonOperator(
        task_id="get_target_dates",
        python_callable=get_target_dates,
    )

    t2 = PythonOperator(
        task_id="crawl_reports",
        python_callable=crawl_reports,
    )

    t3 = PythonOperator(
        task_id="move_files",
        python_callable=move_files,
    )

    t1 >> t2 >> t3
