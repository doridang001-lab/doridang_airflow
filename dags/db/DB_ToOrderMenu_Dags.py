"""
투오더 메뉴별 판매량 분석 DAG

수집 데이터:
1. 메뉴별 판매량 (aggregate): 카테고리·메뉴명·판매량·매출액·매출비중
2. 매장별 메뉴 판매량 (store detail): 매장명·메뉴명·매장태그·오픈일·판매량·매출액

저장 경로:
- ANALYTICS_DB/toorder_menu/menu_summary/menu_summary_{YYMMDD}.parquet
- ANALYTICS_DB/toorder_menu/store_detail/store_detail_{YYMMDD}.parquet

conf 키:
    없음               → 어제 1일 수집
    sale_date (str)    → 지정 날짜 1일 수집 ("YYYY-MM-DD")
    backfill (bool)    → 최근 7일 범위 수집 (단일 파일로 저장)
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.extract.crawling_toorder_menu import run_toorder_menu_crawl, OPTION_ANALYSIS_URL
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR
from modules.transform.utility.schedule import DB_TOORDER_MENU_TIME

logger = logging.getLogger(__name__)


# ============================================================
# 설정
# ============================================================

TOORDER_ID = "doridang15"
TOORDER_PW = "ehfl5233!"

BACKFILL_DAYS = 7
EXCEL_HEADER_ROW = 3   # 4번째 행(0-indexed=3)이 헤더

TOORDER_MENU_DB   = ANALYTICS_DB / "toorder_menu"
TOORDER_OPTION_DB = TOORDER_MENU_DB / "option_detail"


# ============================================================
# 태스크 함수
# ============================================================

def get_date_range(**context) -> dict:
    """
    dag_run.conf에 따라 수집 날짜 범위를 결정하고 XCom에 push한다.

    - conf 없음        → 어제 1일
    - sale_date        → 지정 날짜 1일
    - backfill=true    → 최근 BACKFILL_DAYS일 범위 (단일 다운로드)
    """
    conf = context.get("dag_run").conf or {}
    kst = pendulum.timezone("Asia/Seoul")
    yesterday = pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")
    collect_date = pendulum.now(kst).format("YYYY-MM-DD")

    if conf.get("backfill"):
        start_date = pendulum.now(kst).subtract(days=BACKFILL_DAYS).format("YYYY-MM-DD")
        end_date = yesterday
        logger.info("[backfill] %s ~ %s", start_date, end_date)
    elif conf.get("sale_date"):
        start_date = end_date = conf["sale_date"]
        logger.info("[정정] %s", start_date)
    else:
        start_date = end_date = yesterday
        logger.info("[기본] %s", start_date)

    context["ti"].xcom_push(key="start_date", value=start_date)
    context["ti"].xcom_push(key="end_date", value=end_date)
    context["ti"].xcom_push(key="collect_date", value=collect_date)
    return {"start": start_date, "end": end_date}


def crawl_menu(**context) -> str:
    """투오더 메뉴 분석 페이지를 크롤링하고 결과를 XCom에 push한다."""
    ti = context["ti"]
    start_date = ti.xcom_pull(task_ids="get_date_range", key="start_date")
    end_date = ti.xcom_pull(task_ids="get_date_range", key="end_date")

    result = run_toorder_menu_crawl(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        start_date=start_date,
        end_date=end_date,
        download_dir=DOWN_DIR,
    )

    if result.get("error"):
        raise RuntimeError(f"크롤링 실패: {result['error']}")

    ti.xcom_push(key="all_menu_names", value=result.get("menu_names", []))
    ti.xcom_push(
        key="store_files",
        value=[(m, str(p)) for m, p in result["store_files"]],
    )

    store_cnt = len(result["store_files"])
    menu_cnt = len(result.get("menu_names", []))
    logger.info("크롤링 완료 | 전체 메뉴 %d개, 매장별 %d건", menu_cnt, store_cnt)
    return f"전체 메뉴 {menu_cnt}개, 매장별 {store_cnt}건"


def _parse_store_excels(store_files_raw: list, collect_date: str) -> "pd.DataFrame":
    """store 파일 목록 [(menu_name, file_str)] → 표준 DataFrame 변환 (공통 로직)."""
    frames = []
    for menu_name, file_str in store_files_raw:
        fpath = Path(file_str)
        if not fpath.exists():
            continue
        try:
            df = pd.read_excel(fpath, header=EXCEL_HEADER_ROW)
            df = df.dropna(how="all").iloc[:, :5]
            df.columns = ["카테고리", "매장명", "판매량", "매출액", "매출비중"]
            df = df[df["매장명"].notna()].reset_index(drop=True)
            df.insert(1, "메뉴명", menu_name)
            df["수집날짜"] = collect_date
            frames.append(df)
            fpath.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("[%s] store Excel 파싱 실패: %s", menu_name, exc)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def save_parquet(**context) -> str:
    """다운로드된 Excel 파일을 표준 컬럼으로 변환해 store_detail parquet으로 저장한다."""
    ti = context["ti"]
    collect_date = ti.xcom_pull(task_ids="get_date_range", key="collect_date")
    store_files_raw = ti.xcom_pull(task_ids="crawl_menu", key="store_files") or []

    yymmdd = datetime.strptime(collect_date, "%Y-%m-%d").strftime("%y%m%d")

    store_df = _parse_store_excels(store_files_raw, collect_date)
    store_saved = 0
    if not store_df.empty:
        out_dir = TOORDER_MENU_DB / "store_detail"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"store_detail_{yymmdd}.parquet"
        store_df.to_parquet(out_path, index=False, engine="pyarrow")
        store_saved = len(store_df)
        logger.info("store_detail 저장: %s (%d행)", out_path.name, store_saved)

    return f"store_detail {store_saved}행 저장 완료"


def retry_missing(**context) -> str:
    """
    menu_summary vs store_detail parquet을 비교해 누락 메뉴를 감지하고
    해당 메뉴만 Selenium으로 재시도한 뒤 store_detail에 병합한다.
    """
    ti = context["ti"]
    collect_date = ti.xcom_pull(task_ids="get_date_range", key="collect_date")
    start_date = ti.xcom_pull(task_ids="get_date_range", key="start_date")
    end_date = ti.xcom_pull(task_ids="get_date_range", key="end_date")

    if not collect_date:
        return "collect_date XCom 없음, 스킵"

    yymmdd = datetime.strptime(collect_date, "%Y-%m-%d").strftime("%y%m%d")
    store_path = TOORDER_MENU_DB / "store_detail" / f"store_detail_{yymmdd}.parquet"

    # ── 1. 누락 메뉴 감지 (XCom menu_names vs store_detail parquet)
    all_menus = set(ti.xcom_pull(task_ids="crawl_menu", key="all_menu_names") or [])
    if not all_menus:
        logger.info("메뉴 목록 XCom 없음, 재시도 스킵")
        return "메뉴 목록 XCom 없음, 스킵"

    if store_path.exists():
        store_menus = set(pd.read_parquet(store_path)["메뉴명"].dropna().unique())
        missing = list(all_menus - store_menus)
    else:
        missing = list(all_menus)

    if not missing:
        logger.info("누락 메뉴 없음 (%d개 완료)", len(all_menus))
        return f"누락 없음 ({len(all_menus)}개 완료)"

    logger.info("누락 메뉴 %d개 재시도: %s", len(missing), missing[:5])

    # ── 2. 누락 메뉴만 Selenium 재시도
    result = run_toorder_menu_crawl(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        start_date=start_date,
        end_date=end_date,
        download_dir=DOWN_DIR,
        target_menus=missing,
    )

    if result.get("error"):
        raise RuntimeError(f"재시도 크롤링 실패: {result['error']}")

    # ── 3. 새 Excel → DataFrame
    new_df = _parse_store_excels(result.get("store_files", []), collect_date)
    if new_df.empty:
        logger.warning("재시도 결과 없음 (%d개 대상)", len(missing))
        return f"재시도 {len(missing)}건 모두 실패"

    # ── 4. 기존 parquet에 병합 (재시도 메뉴 stale 행 교체 후 concat)
    if store_path.exists():
        existing = pd.read_parquet(store_path)
        retried = set(new_df["메뉴명"].unique())
        existing = existing[~existing["메뉴명"].isin(retried)]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    store_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(store_path, index=False, engine="pyarrow")

    ok = len(result["store_files"])
    logger.info("재시도 완료: %d/%d건 성공, store_detail %d행", ok, len(missing), len(combined))
    return f"재시도 완료: {ok}/{len(missing)}건 성공"


def crawl_option(**context) -> str:
    """투오더 옵션별 판매량 분석 페이지를 크롤링한다."""
    ti = context["ti"]
    start_date = ti.xcom_pull(task_ids="get_date_range", key="start_date")
    end_date = ti.xcom_pull(task_ids="get_date_range", key="end_date")

    result = run_toorder_menu_crawl(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        start_date=start_date,
        end_date=end_date,
        download_dir=DOWN_DIR,
        page_url=OPTION_ANALYSIS_URL,
    )

    if result.get("error"):
        raise RuntimeError(f"옵션 크롤링 실패: {result['error']}")

    ti.xcom_push(key="all_option_names", value=result.get("menu_names", []))
    ti.xcom_push(
        key="option_files",
        value=[(m, str(p)) for m, p in result["store_files"]],
    )

    option_cnt = len(result["store_files"])
    logger.info("옵션 크롤링 완료 | 전체 %d개, 매장별 %d건",
                len(result.get("menu_names", [])), option_cnt)
    return f"옵션 전체 {len(result.get('menu_names', []))}개, 매장별 {option_cnt}건"


def save_option_parquet(**context) -> str:
    """옵션 Excel 파일을 option_detail parquet으로 저장한다."""
    ti = context["ti"]
    collect_date = ti.xcom_pull(task_ids="get_date_range", key="collect_date")
    option_files_raw = ti.xcom_pull(task_ids="crawl_option", key="option_files") or []

    yymmdd = datetime.strptime(collect_date, "%Y-%m-%d").strftime("%y%m%d")

    df = _parse_store_excels(option_files_raw, collect_date)
    saved = 0
    if not df.empty:
        df = df.rename(columns={"메뉴명": "옵션명"})
        TOORDER_OPTION_DB.mkdir(parents=True, exist_ok=True)
        out_path = TOORDER_OPTION_DB / f"option_detail_{yymmdd}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        saved = len(df)
        logger.info("option_detail 저장: %s (%d행)", out_path.name, saved)

    return f"option_detail {saved}행 저장 완료"


def retry_missing_option(**context) -> str:
    """옵션 누락 수집을 감지하고 재시도한다."""
    ti = context["ti"]
    collect_date = ti.xcom_pull(task_ids="get_date_range", key="collect_date")
    start_date = ti.xcom_pull(task_ids="get_date_range", key="start_date")
    end_date = ti.xcom_pull(task_ids="get_date_range", key="end_date")

    if not collect_date:
        return "collect_date XCom 없음, 스킵"

    yymmdd = datetime.strptime(collect_date, "%Y-%m-%d").strftime("%y%m%d")
    store_path = TOORDER_OPTION_DB / f"option_detail_{yymmdd}.parquet"

    all_options = set(ti.xcom_pull(task_ids="crawl_option", key="all_option_names") or [])
    if not all_options:
        return "옵션 목록 XCom 없음, 스킵"

    if store_path.exists():
        store_options = set(pd.read_parquet(store_path)["옵션명"].dropna().unique())
        missing = list(all_options - store_options)
    else:
        missing = list(all_options)

    if not missing:
        return f"누락 없음 ({len(all_options)}개 완료)"

    logger.info("옵션 누락 %d개 재시도: %s", len(missing), missing[:5])

    result = run_toorder_menu_crawl(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        start_date=start_date,
        end_date=end_date,
        download_dir=DOWN_DIR,
        page_url=OPTION_ANALYSIS_URL,
        target_menus=missing,
    )

    if result.get("error"):
        raise RuntimeError(f"옵션 재시도 크롤링 실패: {result['error']}")

    new_df = _parse_store_excels(result.get("store_files", []), collect_date)
    if new_df.empty:
        return f"재시도 {len(missing)}건 모두 실패"

    new_df = new_df.rename(columns={"메뉴명": "옵션명"})

    if store_path.exists():
        existing = pd.read_parquet(store_path)
        retried = set(new_df["옵션명"].unique())
        existing = existing[~existing["옵션명"].isin(retried)]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    TOORDER_OPTION_DB.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(store_path, index=False, engine="pyarrow")

    ok = len(result["store_files"])
    logger.info("옵션 재시도 완료: %d/%d건 성공, option_detail %d행", ok, len(missing), len(combined))
    return f"옵션 재시도 완료: {ok}/{len(missing)}건 성공"


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id=Path(__file__).stem,
    description="투오더 메뉴별 판매량 분석 자동 수집 (집계 + 매장별 상세)",
    schedule=DB_TOORDER_MENU_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "toorder", "menu_analysis", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:

    t1 = PythonOperator(task_id="get_date_range",       python_callable=get_date_range)
    t2 = PythonOperator(task_id="crawl_menu",           python_callable=crawl_menu)
    t3 = PythonOperator(task_id="save_parquet",         python_callable=save_parquet)
    t4 = PythonOperator(task_id="retry_missing",        python_callable=retry_missing,
                        trigger_rule="all_done")
    t5 = PythonOperator(task_id="crawl_option",         python_callable=crawl_option)
    t6 = PythonOperator(task_id="save_option_parquet",  python_callable=save_option_parquet)
    t7 = PythonOperator(task_id="retry_missing_option", python_callable=retry_missing_option,
                        trigger_rule="all_done")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
