"""
unified_sales 일별/월별 검증 파이프라인.

검증 방식:
1. 대상일 1일을 결정한다.
2. MART_DB/unified_sales_grp/unified_sales_*.parquet 에서 sale_date+store 기준 total_price를 집계한다.
3. ToOrder 일별 store/platform parquet에서 같은 기준으로 집계한다.
4. 수동 교정 기준을 쓰는 TEST_STORES는 토더 기준 검증에서 제외한다.
5. outer join 으로 비교하고 오차율 2% 이상 매장을 Telegram으로 알림 발송한다.
6. 일별 결과: MART_DB/unified_sales_grp_error_list/unified_sales_error_YYYY-MM-DD.csv
7. 월별 결과: MART_DB/unified_sales_grp_error_list/unified_sales_monthly_YYYY-MM.csv
   (올해 데이터가 있는 모든 ym에 대해 생성, 알림은 어제 기준 달만)
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    COLLECT_DB,
    LLM_OUTPUT_DIR,
    LOCAL_DB,
    MART_DB,
    ONEDRIVE_DB,
)

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

_DOCKER_TO_WIN = [
    ("/opt/airflow/onedrive_mart", str(MART_DB)),
    ("/opt/airflow/analytics", str(ANALYTICS_DB)),
    ("/opt/airflow/Repository", str(ONEDRIVE_DB)),
    ("/opt/airflow/Collect_Data", str(COLLECT_DB)),
    ("/opt/airflow/Local_DB", str(LOCAL_DB)),
    ("/opt/airflow/onedrive_llm", str(LLM_OUTPUT_DIR)),
]
_DYNAMIC_DOCKER_TO_WIN = [
    ("/opt/airflow/onedrive_mart", str(MART_DB)),
    ("/opt/airflow/analytics", str(ANALYTICS_DB)),
    ("/opt/airflow/Repository", str(ONEDRIVE_DB)),
    ("/opt/airflow/Collect_Data", str(COLLECT_DB)),
    ("/opt/airflow/Local_DB", str(LOCAL_DB)),
    ("/opt/airflow/onedrive_llm", str(LLM_OUTPUT_DIR)),
]


def _to_win_file_uri(path: Path) -> tuple[str, str]:
    """Docker 경로를 Windows 경로 문자열과 file:/// URI로 변환한다."""
    posix = path.as_posix()
    for docker_prefix, win_prefix in _DYNAMIC_DOCKER_TO_WIN:
        if posix.startswith(docker_prefix):
            rel = posix[len(docker_prefix):]
            win_str = win_prefix + rel.replace("/", "\\")
            uri_path = win_prefix.replace("\\", "/") + rel
            href = "file:///" + quote(uri_path, safe="/:@!$&'()*+,;=")
            return win_str, href
    return str(path), path.as_uri()
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
VALIDATION_DIR = MART_DB / "unified_sales_grp_error_list"
VALIDATION_FILE_PREFIX = "unified_sales_error_"
MONTHLY_FILE_PREFIX = "unified_sales_monthly_"

_COL_LABELS = {
    "sale_date": "날짜",
    "ym": "월",
    "store": "매장",
    "excel_total": "토더합계",
    "unified_total": "DB합계",
    "difference": "차이",
    "error_rate": "오차율(%)",
    "status": "상태",
}

BAEMIN_MANUAL_TEST_STORES = {"해운대중동점", "법흥리점", "송파삼전점"}
COUPANG_MANUAL_TEST_STORES = {"해운대중동점", "송파삼전점"}
VALIDATION_EXCLUDED_TEST_STORES = BAEMIN_MANUAL_TEST_STORES | COUPANG_MANUAL_TEST_STORES
VALIDATION_SCOPE_NOTE = "기준: TEST_STORES 제외, 나머지 매장 토더↔unified 비교"


def _apply_validation_scope(df: pd.DataFrame) -> pd.DataFrame:
    """TEST_STORES는 수동 교정 기준이 달라 토더 기준 검증에서 제외한다."""
    if df.empty or "store" not in df.columns:
        return df

    scoped = df.copy()
    scoped["store"] = scoped["store"].astype(str).str.strip()
    return scoped[~scoped["store"].isin(VALIDATION_EXCLUDED_TEST_STORES)].copy()


def _filter_target_stores(df: pd.DataFrame, target_stores: list[str] | None = None) -> pd.DataFrame:
    if df.empty or not target_stores or "store" not in df.columns:
        return df

    targets = {str(store).strip() for store in target_stores if str(store).strip()}
    if not targets:
        return df

    scoped = df.copy()
    scoped["store"] = scoped["store"].astype(str).str.strip()
    return scoped[scoped["store"].isin(targets)].copy()


def _resolve_target_date(**context) -> str:
    """conf.sale_date 또는 resolve_date XCom 을 우선 사용하고, 없으면 KST 기준 전일을 사용한다."""
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    if isinstance(conf, dict):
        sale_date = conf.get("sale_date")
        if sale_date:
            return str(sale_date)

    ti = context.get("ti")
    if ti:
        sale_date = ti.xcom_pull(task_ids="resolve_date", key="sale_date")
        if sale_date:
            return str(sale_date)

    for key in ("data_interval_end", "logical_date", "execution_date"):
        dt = context.get(key)
        if dt is None:
            continue
        try:
            return dt.in_timezone("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")
        except Exception:
            pass
        try:
            return (dt.astimezone(KST) - timedelta(days=1)).date().isoformat()
        except Exception:
            pass
        try:
            parsed = pd.Timestamp(dt)
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize(KST)
            else:
                parsed = parsed.tz_convert(KST)
            return (parsed - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception:
            pass

    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# HTML 테이블 빌더
# ---------------------------------------------------------------------------

def _build_html_table(df: pd.DataFrame) -> str:
    """DataFrame → 스타일드 HTML 테이블. error_rate ≥ 2인 행은 배경 강조."""
    th_style = (
        "background:#2c3e50;color:#fff;padding:8px 12px;"
        "text-align:center;font-size:13px;white-space:nowrap;"
    )
    td_style_base = "padding:7px 11px;font-size:13px;text-align:right;border-bottom:1px solid #e0e0e0;"
    td_style_str = "padding:7px 11px;font-size:13px;text-align:left;border-bottom:1px solid #e0e0e0;"

    cols = [c for c in df.columns if c in _COL_LABELS]
    headers = "".join(f'<th style="{th_style}">{_COL_LABELS.get(c, c)}</th>' for c in cols)

    rows_html = []
    for i, (_, row) in enumerate(df.iterrows()):
        is_error = float(row.get("error_rate", 0)) >= 2
        row_bg = "#fff3f3" if is_error else ("#f9f9f9" if i % 2 else "#ffffff")
        cells = []
        for c in cols:
            val = row[c]
            if c in ("sale_date", "ym", "store", "status"):
                style = td_style_str + f"background:{row_bg};"
                if c == "status" and is_error:
                    style += "color:#c0392b;font-weight:bold;"
            else:
                style = td_style_base + f"background:{row_bg};"
                if c == "error_rate" and is_error:
                    style += "color:#c0392b;font-weight:bold;"
                if c == "difference" and isinstance(val, (int, float)) and val != 0:
                    val = f"{val:+,}"
                elif isinstance(val, (int, float)):
                    val = f"{val:,}" if c != "error_rate" else f"{val:.2f}"
            cells.append(f'<td style="{style}">{val}</td>')
        rows_html.append(f'<tr>{"".join(cells)}</tr>')

    return (
        '<table style="border-collapse:collapse;width:100%;font-family:Malgun Gothic,Arial,sans-serif;">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(rows_html)}</tbody>'
        "</table>"
    )


def _format_telegram_rows(df: pd.DataFrame, *, date_col: str, limit: int = 20) -> str:
    lines = []
    for _, row in df.head(limit).iterrows():
        lines.append(
            f"- {row.get(date_col, '')} {row.get('store', '')}: "
            f"오차율 {float(row.get('error_rate', 0)):.2f}% / "
            f"차이 {int(row.get('difference', 0)):,}"
        )
    if len(df) > limit:
        lines.append(f"... 외 {len(df) - limit}건")
    return "\n".join(lines)


def _build_telegram_message(title: str, target_label: str, error_rows: pd.DataFrame, csv_path: Path, *, date_col: str) -> str:
    win_path, _ = _to_win_file_uri(csv_path)
    rows_text = _format_telegram_rows(error_rows, date_col=date_col)
    return (
        f"{title}\n"
        f"{target_label}\n"
        f"{VALIDATION_SCOPE_NOTE}\n"
        f"오차율 2% 이상: {len(error_rows)}건\n"
        f"CSV: {win_path}\n"
        f"{rows_text}"
    )


# ---------------------------------------------------------------------------
# 일별 검증
# ---------------------------------------------------------------------------

def _load_parquet_totals(target_date: str) -> pd.DataFrame:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 파일이 없습니다: {UNIFIED_ROOT}")

    parts = []
    for file_path in files:
        try:
            frame = pd.read_parquet(file_path, columns=["sale_date", "store", "total_price"])
        except Exception as exc:
            logger.warning("parquet 로드 실패: %s | %s", file_path, exc)
            continue
        parts.append(frame)

    if not parts:
        return pd.DataFrame(columns=["sale_date", "store", "unified_total"])

    df = pd.concat(parts, ignore_index=True)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["sale_date"] == target_date].copy()
    if df.empty:
        return pd.DataFrame(columns=["sale_date", "store", "unified_total"])

    df = _apply_validation_scope(df)
    if df.empty:
        return pd.DataFrame(columns=["sale_date", "store", "unified_total"])

    df["store"] = df["store"].astype(str).str.strip()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["sale_date", "store"], as_index=False)["total_price"]
        .sum()
        .rename(columns={"total_price": "unified_total"})
    )
    grouped["unified_total"] = grouped["unified_total"].round().astype(int)
    return grouped


def _load_excel_totals(target_date: str) -> pd.DataFrame:
    path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
    if not path.exists():
        logger.warning("토더 parquet 없음: %s", path)
        return pd.DataFrame(columns=["sale_date", "store", "excel_total"])

    df = pd.read_parquet(path, columns=["date", "store", "price"])
    df["sale_date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["sale_date"] == target_date].copy()
    if df.empty:
        logger.warning("토더 parquet 데이터 없음: %s", target_date)
        return pd.DataFrame(columns=["sale_date", "store", "excel_total"])

    df = _apply_validation_scope(df)
    if df.empty:
        return pd.DataFrame(columns=["sale_date", "store", "excel_total"])

    grouped = (
        df.groupby(["sale_date", "store"], as_index=False)["price"]
        .sum()
        .rename(columns={"price": "excel_total"})
    )
    df["store"] = df["store"].astype(str).str.strip()
    grouped["store"] = grouped["store"].astype(str).str.strip()
    grouped["excel_total"] = grouped["excel_total"].round().astype(int)
    return grouped


def _compute_diff(excel: pd.DataFrame, parquet: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(excel, parquet, on=["sale_date", "store"], how="outer")
    merged["excel_total"] = pd.to_numeric(merged["excel_total"], errors="coerce").fillna(0).round().astype(int)
    merged["unified_total"] = pd.to_numeric(merged["unified_total"], errors="coerce").fillna(0).round().astype(int)
    merged["difference"] = merged["unified_total"] - merged["excel_total"]
    merged["error_rate"] = merged.apply(
        lambda row: round(abs(row["difference"]) / abs(row["excel_total"]) * 100, 2)
        if row["excel_total"] != 0
        else 0.0,
        axis=1,
    )
    merged["status"] = merged["error_rate"].apply(lambda v: "error" if v >= 2 else "ok")
    if "sale_date" in merged.columns:
        merged["sale_date"] = merged["sale_date"].fillna("")
    return merged[
        ["sale_date", "store", "excel_total", "unified_total", "difference", "error_rate", "status"]
    ].sort_values(["sale_date", "store"]).reset_index(drop=True)


def _save_validation_csv(diff: pd.DataFrame, target_date: str) -> Path:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    file_path = VALIDATION_DIR / f"{VALIDATION_FILE_PREFIX}{target_date}.csv"
    diff.to_csv(file_path, index=False, encoding="utf-8-sig")
    return file_path


def _send_alert(target_date: str, error_rows: pd.DataFrame, csv_path: Path, **context) -> None:
    display = (
        error_rows[["sale_date", "store", "excel_total", "unified_total", "difference", "error_rate", "status"]]
        .sort_values(["error_rate", "store"], ascending=[False, True])
    )
    message = _build_telegram_message(
        title="[도리당] unified_sales 일별 검증 알림",
        target_label=f"대상일: {target_date}",
        error_rows=display,
        csv_path=csv_path,
        date_col="sale_date",
    )
    send_telegram(message)


def validate_sales(target_stores: list[str] | None = None, **context) -> str:
    """대상일 1일 기준 unified_sales 와 일별매출보고서를 비교한다."""
    target_date = _resolve_target_date(**context)
    logger.info("unified_sales 검증 대상일: %s", target_date)

    parquet = _filter_target_stores(_load_parquet_totals(target_date=target_date), target_stores)
    excel = _filter_target_stores(_load_excel_totals(target_date=target_date), target_stores)
    diff = _compute_diff(excel=excel, parquet=parquet)
    csv_path = _save_validation_csv(diff=diff, target_date=target_date)
    logger.info("검증 결과 저장: %s | rows=%d", csv_path, len(diff))

    error_rows = diff[(diff["error_rate"] >= 2) & (diff["difference"].abs() >= 100000)].copy()
    if error_rows.empty:
        return f"검증 완료: {target_date} | 오차율 2% 이상(±10만원↑) 없음 | CSV: {csv_path}"

    _send_alert(target_date=target_date, error_rows=error_rows, csv_path=csv_path, **context)
    return f"검증 경고: {target_date} | 오차율 2% 이상 {len(error_rows)}건 | CSV: {csv_path}"


# ---------------------------------------------------------------------------
# 월별 검증
# ---------------------------------------------------------------------------

def _get_parquet_year_months(year: str) -> list:
    """unified_sales parquet에서 해당 연도의 모든 ym 값을 추출한다."""
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    ym_set: set = set()
    for file_path in files:
        try:
            df = pd.read_parquet(file_path, columns=["sale_date"])
            dates = pd.to_datetime(df["sale_date"], errors="coerce").dt.strftime("%Y-%m")
            ym_set.update(dates.dropna().unique())
        except Exception as exc:
            logger.warning("parquet ym 추출 실패: %s | %s", file_path, exc)
    return sorted(ym for ym in ym_set if str(ym).startswith(year))


def _load_parquet_monthly_totals(target_ym: str) -> pd.DataFrame:
    """전체 parquet → ym 기준 필터 → store 기준 합산."""
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        return pd.DataFrame(columns=["ym", "store", "unified_total"])

    parts = []
    for file_path in files:
        try:
            frame = pd.read_parquet(file_path, columns=["sale_date", "store", "total_price"])
        except Exception as exc:
            logger.warning("parquet 로드 실패: %s | %s", file_path, exc)
            continue
        parts.append(frame)

    if not parts:
        return pd.DataFrame(columns=["ym", "store", "unified_total"])

    df = pd.concat(parts, ignore_index=True)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["ym"] = df["sale_date"].str[:7]
    df = df[df["ym"] == target_ym].copy()
    if df.empty:
        return pd.DataFrame(columns=["ym", "store", "unified_total"])

    df = _apply_validation_scope(df)
    if df.empty:
        return pd.DataFrame(columns=["ym", "store", "unified_total"])

    df["store"] = df["store"].astype(str).str.strip()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["ym", "store"], as_index=False)["total_price"]
        .sum()
        .rename(columns={"total_price": "unified_total"})
    )
    grouped["unified_total"] = grouped["unified_total"].round().astype(int)
    return grouped


def _load_excel_monthly_totals(target_ym: str) -> pd.DataFrame:
    """ToOrder parquet 전체 로드 → ym 기준 필터 → store 기준 합산."""
    path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
    if not path.exists():
        logger.warning("토더 parquet 없음 (월별): %s", path)
        return pd.DataFrame(columns=["ym", "store", "excel_total"])

    df = pd.read_parquet(path, columns=["date", "store", "price"])
    df["ym"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
    df = df[df["ym"] == target_ym].copy()
    if df.empty:
        logger.warning("토더 parquet 데이터 없음 (월별): %s", target_ym)
        return pd.DataFrame(columns=["ym", "store", "excel_total"])

    df = _apply_validation_scope(df)
    if df.empty:
        return pd.DataFrame(columns=["ym", "store", "excel_total"])

    df["store"] = df["store"].astype(str).str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["ym", "store"], as_index=False)["price"]
        .sum()
        .rename(columns={"price": "excel_total"})
    )
    grouped["excel_total"] = grouped["excel_total"].round().astype(int)
    return grouped


def _compute_monthly_diff(excel: pd.DataFrame, parquet: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(excel, parquet, on=["ym", "store"], how="outer")
    merged["excel_total"] = pd.to_numeric(merged["excel_total"], errors="coerce").fillna(0).round().astype(int)
    merged["unified_total"] = pd.to_numeric(merged["unified_total"], errors="coerce").fillna(0).round().astype(int)
    merged["difference"] = merged["unified_total"] - merged["excel_total"]
    merged["error_rate"] = merged.apply(
        lambda row: round(abs(row["difference"]) / abs(row["excel_total"]) * 100, 2)
        if row["excel_total"] != 0
        else 0.0,
        axis=1,
    )
    merged["status"] = merged["error_rate"].apply(lambda v: "error" if v >= 2 else "ok")
    if "ym" in merged.columns:
        merged["ym"] = merged["ym"].fillna("")
    return merged[
        ["ym", "store", "excel_total", "unified_total", "difference", "error_rate", "status"]
    ].sort_values(["ym", "store"]).reset_index(drop=True)


def _save_monthly_comparison_csv(diff: pd.DataFrame, target_ym: str) -> Path:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    file_path = VALIDATION_DIR / f"{MONTHLY_FILE_PREFIX}{target_ym}.csv"
    diff.to_csv(file_path, index=False, encoding="utf-8-sig")
    return file_path


def _send_monthly_alert(target_ym: str, error_rows: pd.DataFrame, csv_path: Path, **context) -> None:
    display = (
        error_rows[["ym", "store", "excel_total", "unified_total", "difference", "error_rate", "status"]]
        .sort_values(["error_rate", "store"], ascending=[False, True])
    )
    message = _build_telegram_message(
        title="[도리당] unified_sales 월별 검증 알림",
        target_label=f"대상월: {target_ym}",
        error_rows=display,
        csv_path=csv_path,
        date_col="ym",
    )
    send_telegram(message)


def validate_monthly_sales(target_stores: list[str] | None = None, **context) -> str:
    """올해 데이터가 있는 모든 ym에 대해 월별 CSV를 생성하고, 어제 기준 달만 알림을 발송한다."""
    target_date = _resolve_target_date(**context)
    current_year = target_date[:4]
    current_ym = target_date[:7]
    logger.info("unified_sales 월별 검증 시작: year=%s, alert_ym=%s", current_year, current_ym)

    all_ym = _get_parquet_year_months(current_year)
    if not all_ym:
        return f"월별 검증: {current_year} 데이터 없음"

    saved = []
    alert_sent = False
    for ym in all_ym:
        parquet = _filter_target_stores(_load_parquet_monthly_totals(ym), target_stores)
        excel = _filter_target_stores(_load_excel_monthly_totals(ym), target_stores)
        diff = _compute_monthly_diff(excel=excel, parquet=parquet)
        csv_path = _save_monthly_comparison_csv(diff=diff, target_ym=ym)
        saved.append(ym)
        logger.info("월별 검증 저장: %s | rows=%d", csv_path, len(diff))

        if ym == current_ym:
            error_rows = diff[diff["error_rate"] >= 2].copy()
            if not error_rows.empty:
                _send_monthly_alert(ym, error_rows, csv_path, **context)
                alert_sent = True

    summary = f"월별 검증 완료: {', '.join(saved)} | CSV {len(saved)}개 저장"
    if alert_sent:
        summary += f" | {current_ym} 오차 알림 발송"
    return summary


# ---------------------------------------------------------------------------
# 일별 요약 Parquet (단일 출력)
# ---------------------------------------------------------------------------

DAILY_SUMMARY_PATH = MART_DB / "unified_sales_grp" / "daily_summary.parquet"


def _status(v: int) -> str:
    if v <= 10_000_000:  return "1천 이하"
    if v <= 20_000_000:  return "1천 초과 ~ 2천 이하"
    if v <= 30_000_000:  return "2천 초과 ~ 3천 이하"
    if v <= 50_000_000:  return "3천초과 ~ 5천 이하"
    return "5천 초과"


def build_daily_summary() -> str:
    """unified_sales parquet → 일별×store×brand×order_type×platform 집계 parquet.

    월별 집계(expected, lag/rolling, LLM)는 내부 계산 후 일별 행에 broadcast.
    출력: daily_summary.parquet 단일 파일.
    """
    import calendar

    files = sorted(UNIFIED_ROOT.glob("unified_sales*.parquet"))
    if not files:
        logger.warning("unified_sales parquet 없음: %s", UNIFIED_ROOT)
        return "parquet 없음"

    df = pd.concat(
        [pd.read_parquet(f, columns=[
            "sale_date", "ym", "store", "brand", "region", "담당자", "실오픈일",
            "order_type", "platform", "total_price", "order_cnt",
        ]) for f in files],
        ignore_index=True,
    )
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    df["order_cnt"] = pd.to_numeric(df["order_cnt"], errors="coerce").fillna(0)
    df["day"] = df["sale_date"].dt.day
    df["ym"] = df["sale_date"].dt.strftime("%Y-%m")

    today_ym = datetime.now(KST).strftime("%Y-%m")

    # ── 1. 일별 집계 ─────────────────────────────────────────────────────────
    daily = (
        df.groupby(["sale_date", "ym", "store", "brand", "order_type", "platform"], sort=False)
        .agg(
            region=("region", "first"),
            담당자=("담당자", "first"),
            실오픈일=("실오픈일", "first"),
            total_price=("total_price", "sum"),
            order_cnt=("order_cnt", "sum"),
        )
        .reset_index()
    )
    daily["total_price"] = daily["total_price"].round(0).astype(int)
    daily["order_cnt"] = daily["order_cnt"].round(0).astype(int)

    # ── 2. 월별 집계 (store×brand×order_type×platform×ym) ────────────────────
    monthly = (
        df.groupby(["ym", "store", "brand", "order_type", "platform"], sort=False)
        .agg(
            month_sales=("total_price", "sum"),
            month_order_cnt=("order_cnt", "sum"),
            actual_days=("sale_date", "nunique"),
            max_sale_date=("sale_date", "max"),
        )
        .reset_index()
    )
    monthly["avg_daily_sales"] = (monthly["month_sales"] / monthly["actual_days"]).round(2)
    monthly["avg_daily_order_cnt"] = (monthly["month_order_cnt"] / monthly["actual_days"]).round(2)

    past_mask = monthly["ym"] < today_ym
    curr_mask = monthly["ym"] == today_ym
    monthly.loc[past_mask, "expected_month_sales"] = monthly.loc[past_mask, "month_sales"]
    monthly.loc[past_mask, "expected_month_order_cnt"] = monthly.loc[past_mask, "month_order_cnt"]
    if curr_mask.any():
        curr_idx = monthly[curr_mask].index
        last_days = monthly.loc[curr_idx, "max_sale_date"].apply(
            lambda d: calendar.monthrange(d.year, d.month)[1]
        )
        monthly.loc[curr_idx, "expected_month_sales"] = (
            monthly.loc[curr_idx, "avg_daily_sales"] * last_days
        ).round(0)
        monthly.loc[curr_idx, "expected_month_order_cnt"] = (
            monthly.loc[curr_idx, "avg_daily_order_cnt"] * last_days
        ).round(0)
    monthly["expected_month_sales"] = monthly["expected_month_sales"].fillna(0).round(0).astype(int)
    monthly["expected_month_order_cnt"] = monthly["expected_month_order_cnt"].fillna(0).round(0).astype(int)
    # status는 store 단위에서 계산 (채널별 expected 기준 아님)

    # ── 3. lag/rolling (store×order_type×platform 기준) ──────────────────────
    monthly_s = monthly.sort_values(["store", "brand", "order_type", "platform", "ym"]).reset_index(drop=True)
    grp_key = ["store", "brand", "order_type", "platform"]
    monthly_s["prev_expected_month_sales"] = (
        monthly_s.groupby(grp_key)["expected_month_sales"]
        .shift(1).fillna(0).round(0).astype(int)
    )
    monthly_s["prev_month_order_cnt"] = (
        monthly_s.groupby(grp_key)["month_order_cnt"]
        .shift(1).fillna(0).round(0).astype(int)
    )
    monthly_s["avg_3m_expected_sales"] = (
        monthly_s.groupby(grp_key)["expected_month_sales"]
        .transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
        .fillna(0).round(0).astype(int)
    )
    monthly_s["avg_3m_order_cnt"] = (
        monthly_s.groupby(grp_key)["expected_month_order_cnt"]
        .transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
        .fillna(0).round(0).astype(int)
    )

    def _agg_expected(grp_df: pd.DataFrame, keys: list) -> pd.DataFrame:
        """keys 기준 월별 집계 + expected 계산 + lag/rolling 반환."""
        g = grp_df.groupby(keys + ["ym"], sort=False).agg(
            tp=("total_price","sum"), oc=("order_cnt","sum"),
            actual_days=("sale_date","nunique"), ms=("sale_date","max")
        ).reset_index()
        g["ad"] = (g["tp"] / g["actual_days"]).round(0).astype(int)
        g["ado"] = (g["oc"] / g["actual_days"]).round(2)
        p = g["ym"] < today_ym; c = g["ym"] == today_ym
        g.loc[p, "es"] = g.loc[p, "tp"]; g.loc[p, "eo"] = g.loc[p, "oc"]
        if c.any():
            ci = g[c].index
            ld = g.loc[ci, "ms"].apply(lambda d: calendar.monthrange(d.year, d.month)[1])
            g.loc[ci, "es"] = (g.loc[ci, "ad"] * ld).round(0)
            g.loc[ci, "eo"] = (g.loc[ci, "ado"] * ld).round(0)
        g["es"] = g["es"].fillna(0).round(0).astype(int)
        g["eo"] = g["eo"].fillna(0).round(0).astype(int)
        gs = g.sort_values(keys + ["ym"]).reset_index(drop=True)
        # prev_es/eo: shift(1) 대신 날짜 기반 join → 월 공백이 있어도 정확한 전달 값 사용
        _prev = gs[keys + ["ym", "es", "oc"]].copy()
        _prev["_next_ym"] = _prev["ym"].apply(
            lambda y: (pd.Period(y, "M") + 1).strftime("%Y-%m")
        )
        gs = gs.merge(
            _prev[keys + ["_next_ym", "es", "oc"]].rename(
                columns={"_next_ym": "ym", "es": "prev_es", "oc": "prev_eo"}
            ),
            on=keys + ["ym"], how="left",
        )
        gs["prev_es"] = gs["prev_es"].fillna(0).astype(int)
        gs["prev_eo"] = gs["prev_eo"].fillna(0).astype(int)
        gs["avg3_s"]  = gs.groupby(keys)["es"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).astype(int)
        gs["avg3_o"]  = gs.groupby(keys)["eo"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).astype(int)
        # 일평균 기반 비교 (partial month 보정)
        gs["prev_ad"]  = gs.groupby(keys)["ad"].shift(1).fillna(0).astype(int)
        gs["avg3_ad"]  = gs.groupby(keys)["ad"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).astype(int)
        gs["prev_ado"] = gs.groupby(keys)["ado"].shift(1).fillna(0).round(2)
        gs["avg3_ado"] = gs.groupby(keys)["ado"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).round(2)
        return gs

    # ── 5. 브랜드별 집계 (store_month_df용) ──────────────────────────────────
    bdf = _agg_expected(df, ["store", "brand"])

    # ── 6. 주별 집계 (store×brand×order_type×platform×week_start) ────────────
    from datetime import timedelta
    df["week_start"] = df["sale_date"] - pd.to_timedelta(df["sale_date"].dt.weekday, unit="D")
    today_dt = datetime.now(KST)
    today_ws = (today_dt - timedelta(days=today_dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    today_ws = pd.Timestamp(today_ws)

    weekly = (
        df.groupby(["week_start", "store", "brand", "order_type", "platform"], sort=False)
        .agg(week_sales=("total_price","sum"), week_order_cnt=("order_cnt","sum"),
             days_cnt=("sale_date","nunique"))
        .reset_index()
    )
    weekly["week_sales"] = weekly["week_sales"].round(0).astype(int)
    weekly["week_order_cnt"] = weekly["week_order_cnt"].round(0).astype(int)
    weekly["avg_dw"] = (weekly["week_sales"] / weekly["days_cnt"]).round(2)
    weekly["avg_dw_o"] = (weekly["week_order_cnt"] / weekly["days_cnt"]).round(2)

    w_past = weekly["week_start"] < today_ws
    w_curr = weekly["week_start"] == today_ws
    weekly.loc[w_past, "expected_week_sales"] = weekly.loc[w_past, "week_sales"]
    weekly.loc[w_past, "expected_week_order_cnt"] = weekly.loc[w_past, "week_order_cnt"]
    if w_curr.any():
        weekly.loc[w_curr, "expected_week_sales"] = (weekly.loc[w_curr, "avg_dw"] * 7).round(0)
        weekly.loc[w_curr, "expected_week_order_cnt"] = (weekly.loc[w_curr, "avg_dw_o"] * 7).round(0)
    weekly["expected_week_sales"] = weekly["expected_week_sales"].fillna(0).round(0).astype(int)
    weekly["expected_week_order_cnt"] = weekly["expected_week_order_cnt"].fillna(0).round(0).astype(int)

    wk_grp = ["store", "brand", "order_type", "platform"]
    weekly_s = weekly.sort_values(wk_grp + ["week_start"]).reset_index(drop=True)
    weekly_s["prev_expected_week_sales"] = weekly_s.groupby(wk_grp)["expected_week_sales"].shift(1).fillna(0).astype(int)
    weekly_s["prev_week_order_cnt"] = weekly_s.groupby(wk_grp)["week_order_cnt"].shift(1).fillna(0).astype(int)
    weekly_s["avg_3w_expected_sales"] = weekly_s.groupby(wk_grp)["expected_week_sales"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).astype(int)
    weekly_s["avg_3w_order_cnt"] = weekly_s.groupby(wk_grp)["expected_week_order_cnt"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).fillna(0).astype(int)

    daily["week_start"] = daily["sale_date"] - pd.to_timedelta(daily["sale_date"].dt.weekday, unit="D")

    # ── 8a. 매장 종합 월별 expected (store 전체 tp / store max_day × 말일) ────
    # bdf["es"] = store 전체 집계 기준 expected → 채널별 max_day 불일치 오차 없음
    store_month_df = (
        bdf[["ym", "store", "brand", "es", "eo", "prev_es", "prev_eo"]]
        .rename(columns={
            "es":      "store_expected_month_sales",
            "eo":      "store_expected_month_order_cnt",
            "prev_es": "store_prev_expected_month_sales",
            "prev_eo": "store_prev_expected_month_order_cnt",
        })
    )
    # status = 현재월 store 전체(브랜드 합산) expected 기준 → 전 기간 동일 적용
    # → PowerBI 슬라이서로 "현재 1천 이하 매장" 필터 시 전체 이력이 정확히 걸림
    curr_store_es = (
        bdf[bdf["ym"] == today_ym]
        .groupby("store", as_index=False)["es"]
        .sum()
    )
    curr_store_es["status"] = curr_store_es["es"].apply(_status)
    status_map = curr_store_es.set_index("store")["status"].to_dict()
    store_month_df["status"] = store_month_df["store"].map(status_map).fillna(
        store_month_df["store_expected_month_sales"].apply(lambda v: _status(int(v)))
    )

    # ── 8b. 매장 종합 주별 expected (채널별 expected SUM → store×brand×week) ──
    store_weekly_df = (
        weekly_s.groupby(["week_start", "store", "brand"])[["expected_week_sales", "expected_week_order_cnt"]]
        .sum().reset_index()
        .rename(columns={
            "expected_week_sales":      "store_expected_week_sales",
            "expected_week_order_cnt":  "store_expected_week_order_cnt",
        })
    )

    # ── 9. 일별에 모든 컬럼 broadcast merge ──────────────────────────────────
    m_cols = ["ym", "store", "brand", "order_type", "platform",
              "expected_month_sales", "expected_month_order_cnt",
              "prev_expected_month_sales", "avg_3m_expected_sales",
              "prev_month_order_cnt", "avg_3m_order_cnt"]
    daily = daily.merge(monthly_s[m_cols],    on=["ym","store","brand","order_type","platform"], how="left")
    daily = daily.merge(store_month_df,        on=["ym","store","brand"],                         how="left")

    # 전월 이전: expected_month_sales = total_price (일별 실매출) → SUM 집계 정합성 보장
    past_daily_mask = daily["ym"] < today_ym
    daily.loc[past_daily_mask, "expected_month_sales"] = daily.loc[past_daily_mask, "total_price"]
    daily.loc[past_daily_mask, "expected_month_order_cnt"] = daily.loc[past_daily_mask, "order_cnt"]

    w_cols = ["week_start", "store", "brand", "order_type", "platform",
              "expected_week_sales", "expected_week_order_cnt",
              "prev_expected_week_sales", "avg_3w_expected_sales",
              "prev_week_order_cnt", "avg_3w_order_cnt"]
    daily = daily.merge(weekly_s[w_cols],     on=["week_start","store","brand","order_type","platform"], how="left")
    daily = daily.merge(store_weekly_df,       on=["week_start","store","brand"],                         how="left")

    for col in ["llm_total_summary","llm_total_reason","llm_total_action",
                "llm_brand_summary","llm_brand_reason","llm_brand_action"]:
        daily[col] = ""
    daily["llm_summary"] = daily["llm_total_summary"]
    daily["llm_reason"] = daily["llm_total_reason"]
    daily["llm_action"] = daily["llm_total_action"]

    # ── 10. 최종 컬럼 순서 & 저장 ────────────────────────────────────────────
    daily = daily[[
        # legacy daily_summary schema: keep these first for position-based consumers.
        "sale_date", "ym", "store", "brand", "region", "담당자", "실오픈일",
        "order_type", "platform", "total_price", "order_cnt",
        "expected_month_sales", "expected_month_order_cnt",
        "prev_expected_month_sales", "avg_3m_expected_sales",
        "prev_month_order_cnt", "avg_3m_order_cnt",
        "status",
        "llm_summary", "llm_reason", "llm_action",
        # 추가 분석 컬럼은 legacy 컬럼 뒤에 배치
        "week_start",
        "store_expected_month_sales", "store_expected_month_order_cnt",
        "store_prev_expected_month_sales", "store_prev_expected_month_order_cnt",
        "llm_total_summary", "llm_total_reason", "llm_total_action",
        "llm_brand_summary", "llm_brand_reason", "llm_brand_action",
        "expected_week_sales", "expected_week_order_cnt",
        "prev_expected_week_sales", "avg_3w_expected_sales",
        "prev_week_order_cnt", "avg_3w_order_cnt",
        "store_expected_week_sales", "store_expected_week_order_cnt",
    ]]
    DAILY_SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(DAILY_SUMMARY_PATH, index=False, engine="pyarrow")
    logger.info("일별 요약 저장: %s (%d행)", DAILY_SUMMARY_PATH, len(daily))
    return f"일별 요약 {len(daily)}행 → {DAILY_SUMMARY_PATH}"
