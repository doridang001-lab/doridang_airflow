"""
AI Daily Collection - 투오더 일별매출보고서 다운로드 DAG

- 이동: https://ceo.toorder.co.kr/dashboard/sales-report/date
- 오늘 날짜 선택
- 엑셀 다운로드(보고서 생성)
- 파일명 변경: 종합보고서_일별매출보고서 -> 종합보고서_일별매출보고서_YYMMDD.xlsx
- 저장 위치(OneDrive analytics): analytics/ai_daily_collection
- 통합 파일 누적: analytics/ai_daily_collection/종합보고서_일별매출보고서_통합.xlsx
- 수동 다운로드 파일(E:\\down\\종합보고서_일별매출보고서_*.xlsx)도 동일 형식으로 누적
"""

import os
import logging
from pathlib import Path
import shutil
from typing import List

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.utility.schedule import AI_DAILY_COLLECTION_TIME

from modules.extract.crawling_toorder_sales_report_daily_date import run_crawling_single_date
from modules.transform.ai_daily_collection_integrator import (
    AI_SALES_ALERT_TREND_CHART_CID,
    ingest_pending_ai_daily_collection_xlsx_files,
    rebuild_ai_daily_collection_compat_outputs,
    build_ai_score_sheet,
    build_llm_ai_diagnosis,
    save_llm_diagnosis_to_db,
    build_ai_sales_alert_html,
    build_ai_sales_alert_trend_chart_png,
    export_daily_summary_csv,
    load_ai_daily_collection_daily_totals,
)
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR
from modules.transform.utility.mailer import send_email


DAG_ID = "DB_Sales_Alert_01_Score_AI_Daily_Collection"

TOORDER_ID = os.getenv("TOORDER_ID", "doridang15")
TOORDER_PW = os.getenv("TOORDER_PW", "ehfl5233!")

DEST_DIR = ANALYTICS_DB / "ai_daily_collection"
DOWNLOAD_DIR = DOWN_DIR / "toorder_sales_report_date"
INTEGRATED_XLSX = DEST_DIR / "종합보고서_일별매출보고서_통합.xlsx"
DAILY_SUMMARY_CSV = DEST_DIR / "종합보고서_일별매출보고서_일별합계.csv"

logger = logging.getLogger(__name__)

ALERT_TO_EMAILS = ["a17019@kakao.com", "bulu1017@kakao.com"] # 이메일 수신자 리스트 (조민준 pm, 오나영 차장)
POWERBI_DASHBOARD_URL = "https://app.powerbi.com/groups/me/reports/07a40f9d-e54a-40db-9b5e-6a2648206cab/a640e3550e57c3103a85?experience=power-bi"
FINAL_CLEANUP_TARGETS = [
    Path(r"E:\down\종합보고서_일별매출보고서_260513_raw.xlsx"),
    Path(r"E:\down\종합보고서_일별매출보고서_260513_raw_bak_091051.xlsx"),
]


def collect_ai_daily_sales_report(**context) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")

    DEST_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    result = run_crawling_single_date(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        target_date=today,
        download_dir=DOWNLOAD_DIR,
    )

    if not result.get("success"):
        raise RuntimeError(f"toorder 일별매출보고서 다운로드 실패({today}): {result.get('error')}")

    src = Path(result["file"])
    if not src.exists():
        raise FileNotFoundError(f"다운로드 파일이 존재하지 않습니다: {src}")

    dest = DEST_DIR / src.name
    shutil.move(str(src), str(dest))
    logger.info("다운로드 파일 저장 완료: %s", dest)
    return str(dest)


def append_ai_daily_sales_report_to_integrated(**context) -> str:
    """
    1) 당일 다운로드된 엑셀(DEST_DIR)을 통합 파일에 upsert
    2) E:\\down\\종합보고서_일별매출보고서_*.xlsx(수동 다운로드)도 통합 파일에 upsert
    """
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst).format("YYYY-MM-DD")

    # upstream(collect) 결과가 있으면 우선 처리
    ti = context.get("ti")
    downloaded_path = None
    if ti:
        downloaded_path = ti.xcom_pull(task_ids="collect_ai_daily_sales_report")

    sources: List[Path] = []
    if downloaded_path:
        p = Path(downloaded_path)
        if p.exists():
            sources.append(p)

    # 수동 다운로드 파일: DEST_DIR의 번호형(_1~_99), 일별크롤(_YYMMDD) 제외
    for pat in ["종합보고서_일별매출보고서_[0-9].xlsx", "종합보고서_일별매출보고서_[0-9][0-9].xlsx"]:
        sources.extend(sorted(DEST_DIR.glob(pat)))
    # DOWN_DIR: 하위호환
    sources.extend(sorted(DOWN_DIR.glob("종합보고서_일별매출보고서_*.xlsx")))

    # 중복 제거(경로 기준)
    uniq: List[Path] = []
    seen = set()
    for p in sources:
        key = str(p).lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)

    if not uniq:
        raise FileNotFoundError(f"처리할 리포트 파일이 없습니다 (today={today})")

    # legacy 통합파일이 존재하면 신규 파일명으로 1회 마이그레이션(신규 파일이 없을 때만)
    if not INTEGRATED_XLSX.exists():
        legacy_candidates = [
            p
            for p in sorted(DEST_DIR.glob("*통합.xlsx"))
            if p.exists() and p.name != INTEGRATED_XLSX.name
        ]
        if legacy_candidates:
            shutil.copy(str(legacy_candidates[0]), str(INTEGRATED_XLSX))
            logger.info("통합 파일명 마이그레이션: %s -> %s", legacy_candidates[0], INTEGRATED_XLSX)

    total_rows = 0
    processed: List[str] = []
    for src in uniq:
        report_date, rows = parse_toorder_daily_report(src)
        total_rows += upsert_integrated_rows(INTEGRATED_XLSX, rows)
        processed.append(f"{src.name} ({report_date}, {len(rows)} rows)")

    logger.info("통합 파일 업데이트 완료: %s (upsert_rows=%s)", INTEGRATED_XLSX, total_rows)
    for item in processed:
        logger.info("처리 완료: %s", item)
    return f"integrated={INTEGRATED_XLSX} processed={processed} total_rows={total_rows}"


def append_integrated_daily_totals(**context) -> str:
    count = build_daily_summary_sheet(INTEGRATED_XLSX)
    logger.info("날짜별합계 시트 생성 완료: %s (dates=%s)", INTEGRATED_XLSX, count)
    return f"integrated={INTEGRATED_XLSX} dates={count}"


def export_daily_summary_csv_task(**context) -> str:
    rows = export_daily_summary_csv(INTEGRATED_XLSX, DAILY_SUMMARY_CSV)
    logger.info("일별합계 CSV 저장 완료: %s (%d rows)", DAILY_SUMMARY_CSV, rows)
    return f"csv={DAILY_SUMMARY_CSV} rows={rows}"


def build_ai_score_sheet_task(**context) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    target_date = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")
    result = build_ai_score_sheet(INTEGRATED_XLSX, target_date=target_date)
    logger.info(
        "AI 점수/진단 시트 생성 완료: %s (target_date=%s grade=%s total_score=%s)",
        INTEGRATED_XLSX,
        target_date,
        result.get("grade"),
        result.get("total_score"),
    )
    return (
        f"integrated={INTEGRATED_XLSX} target_date={target_date} "
        f"grade={result.get('grade')} total_score={result.get('total_score')}"
    )


def append_ai_daily_sales_report_to_integrated(**context) -> str:
    extra_dirs: List[Path] = [DOWN_DIR]

    ti = context.get("ti")
    downloaded_path = ti.xcom_pull(task_ids="collect_ai_daily_sales_report") if ti else None
    if downloaded_path:
        downloaded_dir = Path(downloaded_path).parent
        if downloaded_dir not in extra_dirs:
            extra_dirs.append(downloaded_dir)

    processed = ingest_pending_ai_daily_collection_xlsx_files(
        base_dir=DEST_DIR,
        extra_dirs=extra_dirs,
        rename_to_raw=False,
    )
    total_rows = sum(int(item.get("upsert_rows", 0)) for item in processed)
    logger.info("ai_daily_collection parquet sync complete: files=%d upsert_rows=%d", len(processed), total_rows)
    return f"integrated={INTEGRATED_XLSX} processed_files={len(processed)} total_rows={total_rows}"


def append_integrated_daily_totals(**context) -> str:
    del context
    outputs = rebuild_ai_daily_collection_compat_outputs(
        base_dir=DEST_DIR,
        integrated_path=INTEGRATED_XLSX,
        csv_path=DAILY_SUMMARY_CSV,
    )
    logger.info("ai_daily_collection compatibility outputs rebuilt: %s", outputs)
    return f"integrated={outputs['integrated_xlsx']} csv={outputs['daily_summary_csv']}"


def _build_recent_daily_mtd_avg_trend(target_date: str, days: int = 30) -> List[dict]:
    totals_df = load_ai_daily_collection_daily_totals(base_dir=DEST_DIR)
    if totals_df.empty:
        return []

    work_df = totals_df.copy()
    work_df["sale_date"] = work_df["sale_date"].astype(str)
    work_df = work_df[work_df["sale_date"] <= target_date].copy()
    if work_df.empty:
        return []

    work_df = work_df.sort_values("sale_date").reset_index(drop=True)
    work_df["month"] = work_df["sale_date"].str.slice(0, 7)
    work_df["cum_sales"] = work_df.groupby("month")["sales"].cumsum()

    trend: List[dict] = []
    prev_avg_sales = None
    for row in work_df.tail(days).itertuples(index=False):
        sale_date = str(row.sale_date)
        date_obj = pendulum.parse(sale_date)
        day_count = int(date_obj.day)
        avg_sales = int(round(int(row.cum_sales) / day_count)) if day_count > 0 else 0
        growth_pct = None
        if prev_avg_sales not in (None, 0):
            growth_pct = ((avg_sales / prev_avg_sales) - 1) * 100
        trend.append(
            {
                "sale_date": sale_date,
                "label": f"{date_obj.month}/{date_obj.day}",
                "range_label": f"{date_obj.month}/1~{date_obj.day}",
                "total_sales": int(row.cum_sales),
                "day_count": day_count,
                "avg_sales": avg_sales,
                "avg_growth_pct_vs_prev_day": growth_pct,
            }
        )
        prev_avg_sales = avg_sales
    return trend


def send_ai_sales_alert_email_task(**context) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    target_date = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")

    # 통합 엑셀에는 룰 기반 AI_진단 시트를 남기되, 이메일 문구는 LLM(Ollama)로 고품질 생성(실패 시 룰 폴백)
    scorecard = build_ai_score_sheet(INTEGRATED_XLSX, target_date=target_date)
    llm_diag = build_llm_ai_diagnosis(scorecard, enable_llm=True)

    try:
        save_result = save_llm_diagnosis_to_db(scorecard, llm_diag)
        logger.info("LLM 진단 DB 저장: %s", save_result)
    except Exception as exc:
        logger.warning("LLM 진단 DB 저장 실패 (이메일 발송 계속): %s", exc)

    scorecard_for_email = dict(scorecard)
    scorecard_for_email.update({k: llm_diag[k] for k in ("situation", "cause", "suggest")})
    scorecard_for_email["llm_used"] = llm_diag.get("llm_used", "N")
    scorecard_for_email["daily_avg_trend"] = _build_recent_daily_mtd_avg_trend(target_date)
    trend_chart_png = build_ai_sales_alert_trend_chart_png(scorecard_for_email["daily_avg_trend"])

    html = build_ai_sales_alert_html(
        scorecard_for_email,
        dashboard_url=POWERBI_DASHBOARD_URL,
        trend_chart_cid=AI_SALES_ALERT_TREND_CHART_CID if trend_chart_png else None,
    )

    subject = f"[AI \uc9c4\ub2e8][{target_date}] {scorecard.get('grade')} ({scorecard.get('total_score')}/8)"
    inline_images = []
    if trend_chart_png:
        inline_images.append(
            {
                "content_id": AI_SALES_ALERT_TREND_CHART_CID,
                "filename": "ai-daily-collection-trend.png",
                "mime_subtype": "png",
                "data": trend_chart_png,
            }
        )
    send_email(subject=subject, html_content=html, to_emails=ALERT_TO_EMAILS, inline_images=inline_images)
    logger.info("AI 진단 이메일 발송 완료: to=%s subject=%s", ALERT_TO_EMAILS, subject)
    return f"sent={ALERT_TO_EMAILS} subject={subject}"


def cleanup_final_report_files(**context) -> str:
    del context
    results = []
    for target_path in FINAL_CLEANUP_TARGETS:
        if target_path.exists():
            target_path.unlink()
            logger.info("final cleanup: %s deleted", target_path)
            results.append(f"{target_path}=deleted")
        else:
            logger.info("final cleanup: %s not found", target_path)
            results.append(f"{target_path}=not found")
    return "; ".join(results)


with DAG(
    dag_id=DAG_ID,
    description="투오더 일별매출보고서(오늘) 다운로드 + 통합 엑셀(누적) 업데이트",
    schedule=AI_DAILY_COLLECTION_TIME,  # 필요 시 cron으로 변경
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "toorder", "report", "ai_daily_collection"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:
    t1 = PythonOperator(
        task_id="collect_ai_daily_sales_report",
        python_callable=collect_ai_daily_sales_report,
    )

    t2 = PythonOperator(
        task_id="sync_xlsx_to_parquet",
        python_callable=append_ai_daily_sales_report_to_integrated,
    )

    t3 = PythonOperator(
        task_id="rebuild_compatibility_outputs",
        python_callable=append_integrated_daily_totals,
    )

    t4 = PythonOperator(
        task_id="export_daily_summary_csv",
        python_callable=export_daily_summary_csv_task,
    )

    t5 = PythonOperator(
        task_id="build_ai_score_sheet",
        python_callable=build_ai_score_sheet_task,
    )

    t6 = PythonOperator(
        task_id="send_ai_sales_alert_email",
        python_callable=send_ai_sales_alert_email_task,
    )

    t7 = PythonOperator(
        task_id="cleanup_final_report_files",
        python_callable=cleanup_final_report_files,
    )

    t1 >> t2 >> t3 >> t5 >> t6 >> t7
