"""
토오더 메뉴/옵션 상세 판매 분석 DAG.

수집 대상
1. 메뉴별 매장 상세 판매 분석
2. 옵션별 상세 판매 분석

저장 경로
- ANALYTICS_DB/toorder_menu/toorder_menu_detail_{YYMMDD}.parquet
- ANALYTICS_DB/toorder_option/toorder_option_detail_{YYMMDD}.parquet
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

from modules.extract.crawling_toorder_menu import (
    OPTION_ANALYSIS_URL,
    run_toorder_menu_crawl,
)
from modules.extract.crawling_toorder_sales_report import generate_date_range
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR
from modules.transform.utility.schedule import DB_TOORDER_MENU_TIME

logger = logging.getLogger(__name__)


TOORDER_ID = "doridang15"
TOORDER_PW = os.getenv("TOORDER_PW", "ehfl5233!")

MANUAL_DATE_RANGE = None
BACKFILL_DAYS = 7
EXCEL_HEADER_ROW = 3

TOORDER_MENU_DB = ANALYTICS_DB / "toorder_menu"
TOORDER_OPTION_DB = ANALYTICS_DB / "toorder_option"
TOORDER_MENU_DOWN = DOWN_DIR / "toorder_menu"
TOORDER_OPTION_DOWN = DOWN_DIR / "toorder_option"

MENU_GROUP_LABEL = "메뉴별"
OPTION_GROUP_LABEL = "옵션별"
SKIP_RETRY_REASONS = {"button_not_found", "dialog_timeout", "download_timeout"}


def _build_date_key(target_date: str) -> str:
    return datetime.strptime(target_date, "%Y-%m-%d").strftime("%y%m%d")


def _normalize_date(value: str, kst: pendulum.tz.timezone.Timezone) -> str:
    return pendulum.parse(str(value).strip(), strict=False).in_timezone(kst).format("YYYY-MM-DD")


def _parse_date_range_conf(date_range: str, kst: pendulum.tz.timezone.Timezone) -> List[str]:
    raw = str(date_range).strip()
    if "~" not in raw:
        raise ValueError("date_range 형식 오류: 'YYYY-MM-DD~YYYY-MM-DD' 형식이 필요합니다.")

    start_raw, end_raw = [part.strip() for part in raw.split("~", 1)]
    start_date = _normalize_date(start_raw, kst)
    end_date = _normalize_date(end_raw, kst)
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    return generate_date_range(start_date, end_date)


def get_target_dates(**context) -> List[str]:
    conf = context.get("dag_run").conf or {}
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst)
    yesterday = today.subtract(days=1).format("YYYY-MM-DD")

    if MANUAL_DATE_RANGE:
        if not isinstance(MANUAL_DATE_RANGE, (list, tuple)) or len(MANUAL_DATE_RANGE) != 2:
            raise ValueError("MANUAL_DATE_RANGE는 None 또는 [시작일, 종료일] 형식이어야 합니다.")
        start_date = _normalize_date(MANUAL_DATE_RANGE[0], kst)
        end_date = _normalize_date(MANUAL_DATE_RANGE[1], kst)
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        date_list = generate_date_range(start_date, end_date)
        logger.info("[MANUAL_DATE_RANGE] %s ~ %s (%d일)", start_date, end_date, len(date_list))
    elif conf.get("date_range"):
        date_list = _parse_date_range_conf(conf["date_range"], kst)
        logger.info("[date_range] %s -> %d일", conf["date_range"], len(date_list))
    elif conf.get("sale_date"):
        date_list = [_normalize_date(conf["sale_date"], kst)]
        logger.info("[sale_date] %s", date_list[0])
    elif conf.get("backfill"):
        start_date = today.subtract(days=BACKFILL_DAYS).format("YYYY-MM-DD")
        date_list = generate_date_range(start_date, yesterday)
        logger.info("[backfill] %s ~ %s (%d일)", start_date, yesterday, len(date_list))
    else:
        date_list = [yesterday]
        logger.info("[default] %s", yesterday)

    context["ti"].xcom_push(key="date_list", value=date_list)
    return date_list


def choose_analysis_type(**context) -> str | List[str]:
    conf = context.get("dag_run").conf or {}
    analysis_type = str(conf.get("analysis_type", "all")).strip().lower()
    if analysis_type == "menu":
        logger.info("분석 타입: 메뉴별")
        return "crawl_menu"
    if analysis_type == "option":
        logger.info("분석 타입: 옵션별")
        return "crawl_option"

    logger.info("분석 타입: 전체")
    return ["crawl_menu", "crawl_option"]


def _crawl_analysis_for_dates(
    *,
    task_id: str,
    download_dir: Path,
    page_url: str | None = None,
    **context,
) -> str:
    ti = context["ti"]
    date_list = ti.xcom_pull(task_ids="get_target_dates", key="date_list") or []
    results_by_date: Dict[str, Dict[str, Any]] = {}

    for target_date in date_list:
        kwargs: Dict[str, Any] = {
            "toorder_id": TOORDER_ID,
            "toorder_pw": TOORDER_PW,
            "start_date": target_date,
            "end_date": target_date,
            "download_dir": download_dir,
        }
        if page_url:
            kwargs["page_url"] = page_url

        result = run_toorder_menu_crawl(**kwargs)
        if result.get("error"):
            raise RuntimeError(f"{task_id} {target_date} 수집 실패: {result['error']}")

        results_by_date[target_date] = {
            "menu_names": result.get("menu_names", []),
            "store_files": [(name, str(path)) for name, path in result.get("store_files", [])],
            "skipped_details": result.get("skipped_details", []),
        }
        logger.info(
            "[%s] %s 완료 | 전체 %d개, 상세 파일 %d개, 스킵 %d개",
            task_id,
            target_date,
            len(result.get("menu_names", [])),
            len(result.get("store_files", [])),
            len(result.get("skipped_details", [])),
        )

    ti.xcom_push(key="results_by_date", value=results_by_date)
    return f"{task_id} {len(date_list)}일 수집 완료"


def crawl_menu(**context) -> str:
    return _crawl_analysis_for_dates(task_id="crawl_menu", download_dir=TOORDER_MENU_DOWN, **context)


def crawl_option(**context) -> str:
    return _crawl_analysis_for_dates(
        task_id="crawl_option",
        download_dir=TOORDER_OPTION_DOWN,
        page_url=OPTION_ANALYSIS_URL,
        **context,
    )


def _normalize_header_cell(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _flatten_duplicate_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    flattened: List[str] = []
    for idx, header in enumerate(headers):
        base = header or f"unnamed_{idx}"
        count = seen.get(base, 0)
        flattened.append(base if count == 0 else f"{base}.{count}")
        seen[base] = count + 1
    return flattened


def _find_first_matching_column(columns: List[str], candidates: List[str]) -> str | None:
    for candidate in candidates:
        for col in columns:
            if col == candidate:
                return col
    for candidate in candidates:
        for col in columns:
            if candidate in col:
                return col
    return None


def _find_header_row_by_tokens(raw_df: pd.DataFrame, expected_tokens: tuple[str, ...]) -> int:
    best_row = EXCEL_HEADER_ROW
    best_score = -1
    probe_rows = min(len(raw_df), 12)
    for row_idx in range(probe_rows):
        values = [_normalize_header_cell(v) for v in raw_df.iloc[row_idx].tolist()]
        score = sum(1 for v in values if any(token in v for token in expected_tokens))
        if score > best_score:
            best_score = score
            best_row = row_idx
    return best_row


def _load_store_detail_excel(fpath: Path, menu_name: str, collect_date: str) -> pd.DataFrame:
    raw_df = pd.read_excel(fpath, header=None, dtype=str)
    raw_df = raw_df.dropna(how="all").dropna(axis=1, how="all")
    if raw_df.empty:
        return pd.DataFrame()

    header_row = _find_header_row_by_tokens(
        raw_df,
        ("매장", "판매량", "매출액", "매출비중", "매출 비중", "카테고리"),
    )
    headers = [_normalize_header_cell(v) for v in raw_df.iloc[header_row].tolist()]
    headers = _flatten_duplicate_headers(headers)

    data_df = raw_df.iloc[header_row + 1 :].copy()
    data_df.columns = headers
    data_df = data_df.dropna(how="all").reset_index(drop=True)
    if data_df.empty:
        return pd.DataFrame()

    columns = list(data_df.columns)
    category_col = _find_first_matching_column(columns, ["카테고리"])
    store_col = _find_first_matching_column(columns, ["매장명", "매장 명", "매장"])
    qty_col = _find_first_matching_column(columns, ["판매량", "판매 수량"])
    sales_col = _find_first_matching_column(columns, ["매출액", "매출"])
    share_col = _find_first_matching_column(columns, ["매출비중", "매출 비중"])
    menu_title_col = _find_first_matching_column(columns, ["메뉴 명", "메뉴명"])

    if store_col is None:
        if menu_title_col is not None:
            raise ValueError(f"상세 매장 파일이 아니라 집계 파일로 보입니다. columns={columns[:12]}")
        raise ValueError(f"매장명 컬럼을 찾지 못했습니다. columns={columns[:12]}")

    parsed = pd.DataFrame(index=data_df.index)
    parsed["카테고리"] = data_df[category_col] if category_col else pd.NA
    parsed["메뉴명"] = menu_name
    parsed["매장명"] = data_df[store_col]
    parsed["판매량"] = data_df[qty_col] if qty_col else pd.NA
    parsed["매출액"] = data_df[sales_col] if sales_col else pd.NA
    parsed["매출비중"] = data_df[share_col] if share_col else pd.NA

    parsed["매장명"] = parsed["매장명"].astype(str).str.strip()
    parsed = parsed[
        parsed["매장명"].ne("")
        & parsed["매장명"].ne("nan")
        & ~parsed["매장명"].isin(["합계", "총계", "소계"])
    ].reset_index(drop=True)
    parsed["수집일자"] = collect_date
    return parsed


def _parse_store_excels(store_files_raw: list, collect_date: str) -> pd.DataFrame:
    frames = []
    for menu_name, file_str in store_files_raw:
        fpath = Path(file_str)
        if not fpath.exists():
            continue
        try:
            df = _load_store_detail_excel(fpath, menu_name, collect_date)
            logger.info("[%s] 상세 파싱 완료 | rows=%d", menu_name, len(df))
            frames.append(df)
            fpath.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("[%s] 상세 Excel 파싱 실패: %s", menu_name, exc)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_option_detail_excel(fpath: Path, menu_name: str, collect_date: str) -> pd.DataFrame:
    raw_df = pd.read_excel(fpath, header=None, dtype=str)
    raw_df = raw_df.dropna(how="all").dropna(axis=1, how="all")
    if raw_df.empty:
        return pd.DataFrame()

    header_row = _find_header_row_by_tokens(
        raw_df,
        ("카테고리", "메뉴", "옵션", "판매량", "매출액", "매출 비중", "매출비중"),
    )
    headers = [_normalize_header_cell(v) for v in raw_df.iloc[header_row].tolist()]
    headers = _flatten_duplicate_headers(headers)

    data_df = raw_df.iloc[header_row + 1 :].copy()
    data_df.columns = headers
    data_df = data_df.dropna(how="all").reset_index(drop=True)
    if data_df.empty:
        return pd.DataFrame()

    columns = list(data_df.columns)
    category_col = _find_first_matching_column(columns, ["카테고리"])
    detail_menu_col = _find_first_matching_column(columns, ["메뉴 명", "메뉴명", "하위 메뉴"])
    option_col = _find_first_matching_column(columns, ["하위 옵션", "옵션 명", "옵션명"])
    qty_col = _find_first_matching_column(columns, ["판매량", "판매 수량"])
    sales_col = _find_first_matching_column(columns, ["매출액", "매출"])
    share_col = _find_first_matching_column(columns, ["매출비중", "매출 비중"])

    if detail_menu_col is None and option_col is None:
        raise ValueError(f"옵션 상세 컬럼을 찾지 못했습니다. columns={columns[:12]}")

    parsed = pd.DataFrame(index=data_df.index)
    parsed["카테고리"] = data_df[category_col] if category_col else pd.NA
    parsed["수집대상명"] = menu_name
    parsed["메뉴명"] = data_df[detail_menu_col] if detail_menu_col else menu_name
    parsed["옵션명"] = data_df[option_col] if option_col else pd.NA
    parsed["판매량"] = data_df[qty_col] if qty_col else pd.NA
    parsed["매출액"] = data_df[sales_col] if sales_col else pd.NA
    parsed["매출비중"] = data_df[share_col] if share_col else pd.NA

    parsed["메뉴명"] = parsed["메뉴명"].astype(str).str.strip()
    parsed["옵션명"] = parsed["옵션명"].astype(str).str.strip()
    parsed = parsed[
        parsed["메뉴명"].ne("")
        & parsed["메뉴명"].ne("nan")
        & ~parsed["메뉴명"].isin(["합계", "총계", "소계"])
    ].reset_index(drop=True)
    parsed["수집일자"] = collect_date
    return parsed


def _parse_option_excels(store_files_raw: list, collect_date: str) -> pd.DataFrame:
    frames = []
    for menu_name, file_str in store_files_raw:
        fpath = Path(file_str)
        if not fpath.exists():
            continue
        try:
            df = _load_option_detail_excel(fpath, menu_name, collect_date)
            logger.info("[%s] 옵션 상세 파싱 완료 | rows=%d", menu_name, len(df))
            frames.append(df)
            fpath.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("[%s] 옵션 Excel 파싱 실패: %s", menu_name, exc)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _save_detail_parquet_for_dates(
    *,
    source_task_id: str,
    output_dir: Path,
    output_prefix: str,
    parse_func,
    label: str,
    **context,
) -> str:
    ti = context["ti"]
    date_list = ti.xcom_pull(task_ids="get_target_dates", key="date_list") or []
    results_by_date = ti.xcom_pull(task_ids=source_task_id, key="results_by_date") or {}

    output_dir.mkdir(parents=True, exist_ok=True)

    saved_files = 0
    saved_rows = 0
    for target_date in date_list:
        result_for_date = results_by_date.get(target_date, {})
        store_files = result_for_date.get("store_files") or []
        if not store_files:
            logger.info("[%s][%s] %s 저장할 상세 데이터 없음", source_task_id, target_date, label)
            continue

        detail_df = parse_func(store_files, target_date)
        if detail_df.empty:
            logger.info("[%s][%s] %s 파싱 결과 없음", source_task_id, target_date, label)
            continue

        out_path = output_dir / f"{output_prefix}_{_build_date_key(target_date)}.parquet"
        detail_df.to_parquet(out_path, index=False, engine="pyarrow")
        saved_files += 1
        saved_rows += len(detail_df)
        logger.info("[%s][%s] %s 저장 완료: %s (%d행)", source_task_id, target_date, label, out_path.name, len(detail_df))

    return f"{source_task_id} 상세 parquet 저장 완료: {saved_files}개 파일, {saved_rows}행"


def save_menu_detail_parquet(**context) -> str:
    return _save_detail_parquet_for_dates(
        source_task_id="crawl_menu",
        output_dir=TOORDER_MENU_DB,
        output_prefix="toorder_menu_detail",
        parse_func=_parse_store_excels,
        label=MENU_GROUP_LABEL,
        **context,
    )


def save_option_detail_parquet(**context) -> str:
    return _save_detail_parquet_for_dates(
        source_task_id="crawl_option",
        output_dir=TOORDER_OPTION_DB,
        output_prefix="toorder_option_detail",
        parse_func=_parse_option_excels,
        label=OPTION_GROUP_LABEL,
        **context,
    )


def _get_retry_candidate_names(
    *,
    result_for_date: Dict[str, Any],
    existing_names: set[str],
    source_task_id: str,
    target_date: str,
) -> List[str]:
    all_names = {str(name).strip() for name in result_for_date.get("menu_names", []) if str(name).strip()}
    skipped_details = result_for_date.get("skipped_details", []) or []
    excluded_names = {
        str(item.get("menu_name", "")).strip()
        for item in skipped_details
        if str(item.get("reason", "")).strip() in SKIP_RETRY_REASONS and str(item.get("menu_name", "")).strip()
    }
    retry_candidates = sorted(all_names - existing_names - excluded_names)
    logger.info(
        "[%s][%s] retry candidate summary | all=%d existing=%d skipped_excluded=%d retry_candidates=%d",
        source_task_id,
        target_date,
        len(all_names),
        len(existing_names),
        len(excluded_names),
        len(retry_candidates),
    )
    return retry_candidates


def _retry_missing_for_dates(
    *,
    source_task_id: str,
    download_dir: Path,
    output_dir: Path,
    output_prefix: str,
    parse_func,
    compare_column: str,
    page_url: str | None = None,
    **context,
) -> str:
    ti = context["ti"]
    date_list = ti.xcom_pull(task_ids="get_target_dates", key="date_list") or []
    results_by_date = ti.xcom_pull(task_ids=source_task_id, key="results_by_date") or {}

    retried_dates = 0
    recovered_files = 0

    for target_date in date_list:
        result_for_date = results_by_date.get(target_date, {})
        all_names = {str(name).strip() for name in result_for_date.get("menu_names", []) if str(name).strip()}
        if not all_names:
            logger.info("[%s][%s] 비교 대상 없음, 재시도 생략", source_task_id, target_date)
            continue

        union_path = output_dir / f"{output_prefix}_{_build_date_key(target_date)}.parquet"
        existing = pd.read_parquet(union_path) if union_path.exists() else pd.DataFrame()
        existing_names = (
            set(existing[compare_column].dropna().astype(str).unique())
            if not existing.empty and compare_column in existing.columns
            else set()
        )
        missing = _get_retry_candidate_names(
            result_for_date=result_for_date,
            existing_names=existing_names,
            source_task_id=source_task_id,
            target_date=target_date,
        )
        if not missing:
            logger.info("[%s][%s] 누락 없음 (%d개)", source_task_id, target_date, len(all_names))
            continue

        kwargs: Dict[str, Any] = {
            "toorder_id": TOORDER_ID,
            "toorder_pw": TOORDER_PW,
            "start_date": target_date,
            "end_date": target_date,
            "download_dir": download_dir,
            "target_menus": missing,
        }
        if page_url:
            kwargs["page_url"] = page_url

        logger.info("[%s][%s] 누락 %d개 재시도", source_task_id, target_date, len(missing))
        retry_result = run_toorder_menu_crawl(**kwargs)
        if retry_result.get("error"):
            raise RuntimeError(f"{source_task_id} {target_date} 재시도 실패: {retry_result['error']}")

        new_df = parse_func(retry_result.get("store_files", []), target_date)
        if new_df.empty:
            logger.warning("[%s][%s] 재시도 결과 없음", source_task_id, target_date)
            continue

        if not existing.empty:
            new_names = set(new_df[compare_column].dropna().astype(str).unique())
            if compare_column in existing.columns:
                existing = existing.loc[~existing[compare_column].astype(str).isin(new_names)]
            combined = pd.concat([existing, new_df], ignore_index=True, sort=False)
        else:
            combined = new_df

        output_dir.mkdir(parents=True, exist_ok=True)
        combined.to_parquet(union_path, index=False, engine="pyarrow")
        retried_dates += 1
        recovered_files += len(retry_result.get("store_files", []))
        logger.info(
            "[%s][%s] 재시도 완료: %d개 복구, 현재 %d행",
            source_task_id,
            target_date,
            len(retry_result.get("store_files", [])),
            len(combined),
        )

    return f"{source_task_id} 재시도 완료: {retried_dates}일, {recovered_files}개 파일 복구"


def retry_missing_menu(**context) -> str:
    return _retry_missing_for_dates(
        source_task_id="crawl_menu",
        download_dir=TOORDER_MENU_DOWN,
        output_dir=TOORDER_MENU_DB,
        output_prefix="toorder_menu_detail",
        parse_func=_parse_store_excels,
        compare_column="메뉴명",
        **context,
    )


def retry_missing_option(**context) -> str:
    return _retry_missing_for_dates(
        source_task_id="crawl_option",
        download_dir=TOORDER_OPTION_DOWN,
        output_dir=TOORDER_OPTION_DB,
        output_prefix="toorder_option_detail",
        parse_func=_parse_option_excels,
        compare_column="수집대상명",
        page_url=OPTION_ANALYSIS_URL,
        **context,
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="토오더 메뉴/옵션별 판매 분석 일별 수집",
    schedule=DB_TOORDER_MENU_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["01_crawling", "toorder", "detail_analysis", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    t1 = PythonOperator(task_id="get_target_dates", python_callable=get_target_dates)
    t2 = BranchPythonOperator(task_id="select_analysis_type", python_callable=choose_analysis_type)
    t3 = PythonOperator(task_id="crawl_menu", python_callable=crawl_menu)
    t4 = PythonOperator(task_id="crawl_option", python_callable=crawl_option)
    t5 = PythonOperator(task_id="save_menu_detail_parquet", python_callable=save_menu_detail_parquet)
    t6 = PythonOperator(
        task_id="retry_missing_menu",
        python_callable=retry_missing_menu,
        trigger_rule="all_done",
    )
    t7 = PythonOperator(task_id="save_option_detail_parquet", python_callable=save_option_detail_parquet)
    t8 = PythonOperator(
        task_id="retry_missing_option",
        python_callable=retry_missing_option,
        trigger_rule="all_done",
    )

    t1 >> t2
    t2 >> [t3, t4]
    t3 >> t5 >> t6
    t4 >> t7 >> t8
