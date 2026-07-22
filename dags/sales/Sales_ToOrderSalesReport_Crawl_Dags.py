"""
투오더 종합보고서 일일 자동 다운로드 DAG

처리 흐름:
1. dag_run.conf 유무에 따라 날짜 리스트 결정
2. 날짜 리스트에 해당하는 종합보고서 크롤링 다운로드
3. 다운로드된 파일을 ANALYTICS_DB/toorder_daily_sales/ 로 변환·저장

conf 키:
    없음            → lookback 모드: 최근 LOOKBACK_DAYS일 중 CSV 없는 날짜만 수집
    sale_date (str) → 정정 모드: 해당 날짜 1일 강제 덮어쓰기 "YYYY-MM-DD"
    backfill (bool) → 전체 백필: BACKFILL_START ~ 어제 중 누락 날짜 전체 수집
    start_date (str) + end_date (str) → 범위 모드: 지정 구간 전체 수집

실행 시각: 매일 09:00 (KST)
"""

import logging
import re
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.extract.crawling_toorder_sales_report import (
    generate_date_range,
    run_crawling_date_range,
)
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR
from modules.transform.utility.schedule import SMD_TOORDER_SALES_REPORT_TIME
from modules.transform.utility.notifier import on_failure_callback
from modules.transform.utility.account import get_default_account

logger = logging.getLogger(__name__)

# ============================================================
# 설정
# ============================================================

TOORDER_ID, TOORDER_PW = get_default_account("toorder")

LOOKBACK_DAYS = 7
BACKFILL_START = "2026-01-01"


# ============================================================
# 내부 유틸리티
# ============================================================

def _get_missing_dates(since: str, until: str) -> list:
    """since ~ until 사이에서 CSV가 없는 날짜 리스트를 반환한다."""
    dest = ANALYTICS_DB / "toorder_daily_sales"
    existing = {p.stem[-8:] for p in dest.glob("toorder_daily_sales_*.csv")}
    all_dates = generate_date_range(since, until)
    return [d for d in all_dates if d.replace("-", "") not in existing]


# ============================================================
# 태스크 함수
# ============================================================

def get_target_dates(**context) -> list:
    """
    dag_run.conf에 따라 처리할 날짜 리스트를 결정하고 XCom에 push한다.

    - conf 없음        → lookback: 최근 LOOKBACK_DAYS일 중 누락 날짜만
    - sale_date        → 정정 모드: 해당 1일 강제 수집
    - backfill=true    → 전체 백필: BACKFILL_START ~ 어제 중 누락 날짜
    - start_date+end_date → 범위 모드: 지정 구간 전체
    """
    conf = context.get("dag_run").conf or {}
    kst = pendulum.timezone("Asia/Seoul")
    yesterday = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")

    if conf.get("backfill"):
        date_list = _get_missing_dates(BACKFILL_START, yesterday)
        logger.info("[backfill 모드] %s ~ %s 중 누락 %d일", BACKFILL_START, yesterday, len(date_list))

    elif conf.get("sale_date"):
        date_list = [conf["sale_date"]]
        logger.info("[정정 모드] 대상 날짜: %s", conf["sale_date"])

    elif conf.get("start_date") and conf.get("end_date"):
        date_list = generate_date_range(conf["start_date"], conf["end_date"])
        logger.info(
            "[범위 모드] %s ~ %s: %d일",
            conf["start_date"],
            conf["end_date"],
            len(date_list),
        )

    else:
        since = pendulum.now(kst).subtract(days=LOOKBACK_DAYS).format("YYYY-MM-DD")
        date_list = _get_missing_dates(since, yesterday)
        logger.info("[lookback 모드] 최근 %d일 중 누락 %d일", LOOKBACK_DAYS, len(date_list))

    if not date_list:
        logger.info("수집 대상 없음 (모든 날짜 CSV 존재)")

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
    다운로드 폴더의 종합보고서 파일(Sheet1)을 표준 포맷으로 변환해
    ANALYTICS_DB/toorder_daily_sales/ 루트 아래에
    toorder_daily_sales_YYYYMMDD.csv 형태로 저장한다.
    같은 날짜 파일이 이미 있으면 덮어쓴다.

    대상 패턴: 종합보고서_채널별(일)매출보고서_*.xlsx
    """
    pattern = "종합보고서_채널별(일)매출보고서_*.xlsx"
    files = sorted(Path(DOWN_DIR).glob(pattern))
    base_dest_dir = ANALYTICS_DB / "toorder_daily_sales"
    base_dest_dir.mkdir(parents=True, exist_ok=True)

    if not files:
        logger.warning("변환 대상 파일 없음: %s/%s", str(DOWN_DIR), pattern)
        return f"변환 대상 없음: {DOWN_DIR}"

    converted_count = 0
    output_files = []

    for src_path in files:
        try:
            raw = pd.read_excel(src_path, sheet_name=0, header=None)

            # 행 구조: 0=리포트 타이틀, 1=기간, 2=실제 헤더, 3~데이터
            date_text = str(raw.iat[1, 0]) if len(raw.index) > 1 else ""
            m = re.search(r"(\d{4}-\d{2}-\d{2})", date_text)
            order_date = m.group(1) if m else ""

            header_row = raw.iloc[2].fillna("").astype(str).str.strip()
            store_positions = [i for i, v in header_row.items() if v == "매장"]
            if len(store_positions) < 2:
                raise ValueError("'매장' 헤더 블록을 찾지 못했습니다")

            start_idx, end_idx = store_positions[0], store_positions[1]
            block = raw.iloc[3:, start_idx:end_idx].copy()
            block.columns = header_row.iloc[start_idx:end_idx].tolist()
            block = block.rename(columns={"매장": "매장명"})
            block = block[block["매장명"].notna()].copy()
            block["매장명"] = block["매장명"].astype(str).str.strip()

            meta_cols = {"매장명", "매장 태그", "오픈일", "합 계", "합계"}
            value_cols = [c for c in block.columns if c not in meta_cols]

            long_df = block.melt(
                id_vars=["매장명"],
                value_vars=value_cols,
                var_name="플랫폼명",
                value_name="금액_raw",
            )
            long_df["매출액"] = pd.to_numeric(
                long_df["금액_raw"], errors="coerce"
            ).fillna(0)
            long_df["플랫폼명"] = long_df["플랫폼명"].replace({"요기배달": "요기요"})

            out_df = (
                long_df.groupby(["매장명", "플랫폼명"], as_index=False)["매출액"]
                .sum()
                .assign(수집채널="토더", 주문일자=order_date)
                [["수집채널", "주문일자", "매장명", "플랫폼명", "매출액"]]
            )
            out_df = out_df[out_df["매출액"] > 0].reset_index(drop=True)

            ymd = order_date.replace("-", "") if order_date else src_path.stem[-8:]
            output_path = base_dest_dir / f"toorder_daily_sales_{ymd}.csv"
            if output_path.exists():
                logger.info("기존 파일 덮어쓰기: %s", output_path)
            out_df.to_csv(output_path, index=False, encoding="utf-8-sig")

            src_path.unlink()

            converted_count += 1
            output_files.append(str(output_path))
            logger.info(
                "✅ 변환 완료: %s → %s (rows=%d)",
                src_path.name,
                output_path.name,
                len(out_df),
            )
            logger.info("🗑️ 원본 삭제 완료: %s", src_path)
        except Exception as exc:
            logger.error("❌ 변환 실패: %s (%s)", src_path.name, exc)

    context["ti"].xcom_push(key="converted_files", value=output_files)
    logger.info("변환 완료: %d/%d", converted_count, len(files))
    return f"변환 완료: {converted_count}/{len(files)} (저장 루트: {base_dest_dir})"


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
    "on_failure_callback": on_failure_callback,
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
