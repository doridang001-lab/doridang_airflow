"""
unified_sales 일별/월별 검증 파이프라인.

검증 방식:
1. 대상일 1일을 결정한다.
2. MART_DB/unified_sales_grp/unified_sales_*.parquet 에서 sale_date+store 기준 total_price를 집계한다.
3. AI daily collection parquet에서 같은 기준으로 집계한다.
4. outer join 으로 비교하고 오차율 2% 이상 매장을 HTML 테이블로 메일 발송한다.
5. 일별 결과: MART_DB/unified_sales_grp_error_list/unified_sales_error_YYYY-MM-DD.csv
6. 월별 결과: MART_DB/unified_sales_grp_error_list/unified_sales_monthly_YYYY-MM.csv
   (올해 데이터가 있는 모든 ym에 대해 생성, 알림은 어제 기준 달만)
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.ai_daily_collection_integrator import load_ai_daily_collection_detail_df
from modules.transform.utility.mailer import send_email
from modules.transform.utility.paths import ANALYTICS_DB, MART_DB

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
VALIDATION_DIR = MART_DB / "unified_sales_grp_error_list"
VALIDATION_FILE_PREFIX = "unified_sales_error_"
MONTHLY_FILE_PREFIX = "unified_sales_monthly_"

_COL_LABELS = {
    "sale_date": "날짜",
    "ym": "월",
    "store": "매장",
    "excel_total": "엑셀합계",
    "unified_total": "DB합계",
    "difference": "차이",
    "error_rate": "오차율(%)",
    "status": "상태",
}


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

    for key in ("logical_date", "data_interval_end", "execution_date"):
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


def _build_email_html(title: str, subtitle: str, table_html: str, csv_path: Path) -> str:
    return f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:24px;line-height:1.6;background:#f4f6f8;">
<div style="max-width:900px;margin:auto;background:#fff;border-radius:8px;padding:24px;
            box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <h2 style="margin-top:0;color:#2c3e50;border-bottom:2px solid #2c3e50;padding-bottom:8px;">{title}</h2>
  <p style="color:#555;">{subtitle}</p>
  {table_html}
  <p style="color:#999;font-size:12px;margin-top:16px;">결과 파일: {csv_path}</p>
  <p style="color:#bbb;font-size:11px;">이 메일은 자동으로 발송되었습니다.</p>
</div>
</body>
</html>"""


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
            frame = pd.read_parquet(file_path, columns=["sale_date", "source", "store", "platform", "total_price"])
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

    df["store"] = df["store"].astype(str).str.strip()
    df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
    df["platform"] = df["platform"].fillna("").astype(str).str.strip()

    # 법흥리점: 토더(toorder) + 홀 주문 제외
    beopheung_mask = df["store"] == "법흥리점"
    exclude_mask = beopheung_mask & (
        (df["source"] == "toorder") | (df["platform"] == "홀")
    )
    df = df[~exclude_mask].copy()
    logger.info("법흥리점 토더/홀 제외: %d행", int(exclude_mask.sum()))

    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["sale_date", "store"], as_index=False)["total_price"]
        .sum()
        .rename(columns={"total_price": "unified_total"})
    )
    grouped["unified_total"] = grouped["unified_total"].round().astype(int)
    return grouped


def _load_excel_totals(target_date: str) -> pd.DataFrame:
    df = load_ai_daily_collection_detail_df(
        base_dir=ANALYTICS_DB / "ai_daily_collection",
        target_date=target_date,
    )
    if df.empty:
        logger.warning("AI daily collection 데이터 없음: %s", target_date)
        return pd.DataFrame(columns=["sale_date", "store", "excel_total"])

    grouped = (
        df.groupby(["sale_date", "store"], as_index=False)["sales"]
        .sum()
        .rename(columns={"sales": "excel_total"})
    )
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
    table_html = _build_html_table(display)
    html = _build_email_html(
        title=f"[도리당] unified_sales 일별 검증 알림",
        subtitle=f"대상일: <b>{target_date}</b> &nbsp;|&nbsp; 오차율 2% 이상 매장 수: <b>{len(error_rows)}</b>",
        table_html=table_html,
        csv_path=csv_path,
    )
    send_email(
        subject=f"[도리당] unified_sales 검증 알림 ({target_date})",
        html_content=html,
        to_emails="a17019@kakao.com",
        **context,
    )


def validate_sales(**context) -> str:
    """대상일 1일 기준 unified_sales 와 일별매출보고서를 비교한다."""
    target_date = _resolve_target_date(**context)
    logger.info("unified_sales 검증 대상일: %s", target_date)

    parquet = _load_parquet_totals(target_date=target_date)
    excel = _load_excel_totals(target_date=target_date)
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
            frame = pd.read_parquet(file_path, columns=["sale_date", "source", "store", "platform", "total_price"])
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

    df["store"] = df["store"].astype(str).str.strip()
    df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
    df["platform"] = df["platform"].fillna("").astype(str).str.strip()

    # 법흥리점: 토더(toorder) + 홀 주문 제외
    beopheung_mask = df["store"] == "법흥리점"
    exclude_mask = beopheung_mask & (
        (df["source"] == "toorder") | (df["platform"] == "홀")
    )
    df = df[~exclude_mask].copy()
    logger.info("법흥리점 토더/홀 제외 (월별): %d행", int(exclude_mask.sum()))
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["ym", "store"], as_index=False)["total_price"]
        .sum()
        .rename(columns={"total_price": "unified_total"})
    )
    grouped["unified_total"] = grouped["unified_total"].round().astype(int)
    return grouped


def _load_excel_monthly_totals(target_ym: str) -> pd.DataFrame:
    """AI daily collection 전체 로드 → ym 기준 필터 → store 기준 합산."""
    df = load_ai_daily_collection_detail_df(
        base_dir=ANALYTICS_DB / "ai_daily_collection",
        target_date=None,
    )
    if df.empty:
        logger.warning("AI daily collection 데이터 없음 (월별): %s", target_ym)
        return pd.DataFrame(columns=["ym", "store", "excel_total"])

    df["ym"] = df["sale_date"].astype(str).str[:7]
    df = df[df["ym"] == target_ym].copy()
    if df.empty:
        return pd.DataFrame(columns=["ym", "store", "excel_total"])

    df["store"] = df["store"].astype(str).str.strip()
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
    grouped = (
        df.groupby(["ym", "store"], as_index=False)["sales"]
        .sum()
        .rename(columns={"sales": "excel_total"})
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
    table_html = _build_html_table(display)
    html = _build_email_html(
        title="[도리당] unified_sales 월별 검증 알림",
        subtitle=f"대상월: <b>{target_ym}</b> &nbsp;|&nbsp; 오차율 2% 이상 매장 수: <b>{len(error_rows)}</b>",
        table_html=table_html,
        csv_path=csv_path,
    )
    send_email(
        subject=f"[도리당] unified_sales 월별 검증 알림 ({target_ym})",
        html_content=html,
        to_emails="a17019@kakao.com",
        **context,
    )


def validate_monthly_sales(**context) -> str:
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
        parquet = _load_parquet_monthly_totals(ym)
        excel = _load_excel_monthly_totals(ym)
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
