"""
쿠팡이츠 CMG 마케팅 데이터 파티션 저장 및 OneDrive 백업 DAG

Schedule: 매주 월요일 15:30 (데이터 수집 후)
Dataset: coupangeats_cmg_*.csv
Output: C:/Users/민준/OneDrive - 주식회사 도리당/data/analytics/coupang_marketing/
        brand=도리당/store={매장명}/ym={YYYY-MM}/data.csv
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.sales.SMD_CoupangEats_CMG_Partition import (
    load_coupangeats_cmg_partition,
)
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.notifier import on_failure_callback

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
CMG_MARKETING_DIR = ANALYTICS_DB / "coupang_marketing"

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    'Sales_CoupangEats_CMG_Partition_Dags',
    default_args=default_args,
    description='쿠팡이츠 CMG 파티션 저장 및 OneDrive 백업',
    schedule_interval='30 15 * * 1',  # 매주 월요일 15:30
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['sales', 'coupang', 'partition'],
)


def _get_recent_cmg_dates(days_back: int = 7) -> list[str]:
    """CMG 파티션에서 최근 N일 이내 조회일자 목록 반환 (최신순)."""
    cutoff = (pendulum.now(KST) - timedelta(days=days_back)).format("YYYY-MM-DD")
    dates: set[str] = set()
    for f in CMG_MARKETING_DIR.glob("brand=*/store=*/ym=*/data.csv"):
        try:
            df = pd.read_csv(f, usecols=["조회일자"], dtype=str)
            for d in df["조회일자"].dropna().unique():
                if str(d).strip() >= cutoff:
                    dates.add(str(d).strip())
        except Exception:
            pass
    return sorted(dates, reverse=True)


def validate_cmg(**context) -> str:
    from modules.transform.pipelines.db.DB_Coupang_02_validate import validate_cmg_vs_toorder
    from modules.transform.utility.mailer import send_email

    dates = _get_recent_cmg_dates(days_back=7)
    if not dates:
        logger.warning("CMG 검증 대상 날짜 없음 (최근 7일 데이터 없음)")
        return "검증 스킵: 데이터 없음"

    all_mismatches: dict[str, list] = {}
    for target_date in dates:
        result = validate_cmg_vs_toorder(target_date, retry=1)
        if result.get("matched") is True:
            logger.info("CMG 검증 통과: %s", target_date)
            continue
        if result.get("matched") is None:
            logger.warning("CMG 검증 스킵 (%s): %s", target_date, result.get("reason"))
            continue
        all_mismatches[target_date] = result

    if not all_mismatches:
        return f"검증 통과 ({len(dates)}일)"

    rows = []
    for target_date, result in sorted(all_mismatches.items(), reverse=True):
        for store in result.get("mismatches", []):
            sr = result["store_results"][store]
            diff = sr["cmg"] - sr["toorder"]
            rows.append(
                f"<tr><td>{target_date}</td><td>{store}</td>"
                f"<td style='text-align:right'>{sr['toorder']:,}원</td>"
                f"<td style='text-align:right'>{sr['cmg']:,}원</td>"
                f"<td style='color:red;text-align:right'>{diff:+,}원</td></tr>"
            )

    total_dates = len(all_mismatches)
    html = f"""
    <h3>⚠️ 쿠팡이츠 CMG vs ToOrder 매출 불일치</h3>
    <p>불일치 날짜: <b>{total_dates}일</b></p>
    <table border='1' cellpadding='4' style='border-collapse:collapse'>
      <tr><th>날짜</th><th>매장명</th><th>ToOrder</th><th>CMG 전체매출</th><th>차이</th></tr>
      {''.join(rows)}
    </table>
    <p style='color:gray;font-size:12px'>확장에서 재수집 후 DAG를 재실행하면 재검증됩니다.</p>
    """
    send_email(
        subject=f"[쿠팡CMG검증실패] {total_dates}일 불일치",
        html_content=html,
        to_emails="a17019@kakao.com",
    )
    logger.warning("CMG 검증 실패 이메일 발송: %d일", total_dates)
    return f"검증 실패 이메일 발송 ({total_dates}일)"


task_partition = PythonOperator(
    task_id='load_coupangeats_cmg_partition',
    python_callable=load_coupangeats_cmg_partition,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_cmg',
    python_callable=validate_cmg,
    trigger_rule='all_done',
    dag=dag,
)

task_partition >> task_validate
