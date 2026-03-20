"""
📧 투오더 VOC 주간 알림 파이프라인

기준:
- 작성일자 == 어제
- pct_change_30d < 0

입력:
- LOCAL_DB/temp/toorder_voc_store_summary_*.parquet (최신 파일)
- LOCAL_DB/영업관리부_DB/sales_employee.csv (담당자/이메일 매핑)
"""

from pathlib import Path
import re
import pandas as pd
import pendulum

from modules.transform.utility.paths import LOCAL_DB


def _normalize_store_name(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"^도리당\s*", "", s)
    s = re.sub(r"\s+", "", s)
    return s


def _find_latest_store_summary_parquet() -> Path | None:
    temp_dir = LOCAL_DB / "temp"
    candidates = sorted(
        temp_dir.glob("toorder_voc_store_summary_*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_employee_mapping() -> pd.DataFrame:
    employee_path = LOCAL_DB / "영업관리부_DB" / "sales_employee.csv"
    if not employee_path.exists():
        return pd.DataFrame(columns=["_store_key", "담당자", "email"])

    last_error = None
    for encoding in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            emp = pd.read_csv(employee_path, encoding=encoding)
            break
        except Exception as e:
            last_error = e
    else:
        print(f"[VOC ALERT] 직원 파일 로드 실패: {last_error}")
        return pd.DataFrame(columns=["_store_key", "담당자", "email"])

    required = ["매장명"]
    if not all(c in emp.columns for c in required):
        return pd.DataFrame(columns=["_store_key", "담당자", "email"])

    keep_cols = [c for c in ["매장명", "담당자", "email"] if c in emp.columns]
    emp = emp[keep_cols].copy()
    if "담당자" not in emp.columns:
        emp["담당자"] = ""
    if "email" not in emp.columns:
        emp["email"] = ""

    emp["_store_key"] = emp["매장명"].astype(str).map(_normalize_store_name)
    emp = emp[emp["_store_key"].ne("")]
    emp = emp.drop_duplicates(subset=["_store_key"], keep="first")
    return emp[["_store_key", "담당자", "email"]]


def filter_voc_alerts(output_xcom_key: str = "voc_alert_targets", **context):
    ti = context["task_instance"]

    parquet_path = _find_latest_store_summary_parquet()
    if parquet_path is None:
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (store_summary parquet 없음)"

    df = pd.read_parquet(parquet_path)
    if df.empty:
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 데이터 비어있음)"

    if "작성일자" not in df.columns or "pct_change_30d" not in df.columns:
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (필수 컬럼 없음: 작성일자/pct_change_30d)"

    df = df.copy()
    df["작성일자"] = pd.to_datetime(df["작성일자"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["pct_change_30d"] = pd.to_numeric(df["pct_change_30d"], errors="coerce")

    yesterday = (pendulum.now("Asia/Seoul") - pendulum.duration(days=1)).to_date_string()

    alert_df = df[(df["pct_change_30d"] < 0) & (df["작성일자"] == yesterday)].copy()

    if alert_df.empty:
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"0건 (기준일={yesterday}, 감소 매장 없음)"

    if "매장명" in alert_df.columns:
        mapping = _load_employee_mapping()
        if not mapping.empty:
            alert_df["_store_key"] = alert_df["매장명"].astype(str).map(_normalize_store_name)
            alert_df = alert_df.merge(mapping, how="left", on="_store_key")
            alert_df.drop(columns=["_store_key"], inplace=True, errors="ignore")

    out_dir = LOCAL_DB / "영업관리부_DB"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "toorder_voc_alert_targets.csv"
    alert_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    ti.xcom_push(key=output_xcom_key, value=str(out_path))
    return f"VOC 알림대상 {len(alert_df):,}건 저장 ({yesterday})"


def send_voc_alert_email(
    input_xcom_key: str = "voc_alert_targets",
    smtp_conn_id: str = "doridang_conn_smtp_gmail",
    **context,
):
    from airflow.hooks.base import BaseHook
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    ti = context["task_instance"]
    csv_path = ti.xcom_pull(task_ids="filter_voc_alerts", key=input_xcom_key)

    if not csv_path:
        return "발송 스킵 (알림 대상 없음)"

    path = Path(csv_path)
    if not path.exists():
        return f"발송 스킵 (파일 없음: {path})"

    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty:
        return "발송 스킵 (알림 대상 0건)"

    recipients = []
    if "email" in df.columns:
        recipients = [e for e in df["email"].fillna("").astype(str).str.strip().unique().tolist() if e]

    if not recipients:
        return "발송 스킵 (수신자 이메일 없음)"

    show_cols = [c for c in ["작성일자", "매장명", "부정리뷰수", "sum_30d_recent", "sum_30d_prev", "pct_change_30d", "담당자"] if c in df.columns]
    body_df = df[show_cols].copy() if show_cols else df.copy()

    if "pct_change_30d" in body_df.columns:
        body_df["pct_change_30d"] = pd.to_numeric(body_df["pct_change_30d"], errors="coerce").round(2)

    html_table = body_df.to_html(index=False, border=1)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[도리당][VOC] 전일 대비 30일 추세 하락 알림 ({len(df):,}건)"

    conn = BaseHook.get_connection(smtp_conn_id)
    msg["From"] = conn.login
    msg["To"] = ", ".join(recipients)

    html = f"""
    <html><body>
    <h3>전일 기준 VOC 하락 알림</h3>
    <p>조건: 작성일자=어제, pct_change_30d &lt; 0</p>
    {html_table}
    </body></html>
    """
    msg.attach(MIMEText(html, "html", "utf-8"))

    server = smtplib.SMTP(conn.host, conn.port, timeout=30)
    server.starttls()
    server.login(conn.login, conn.password)
    server.send_message(msg)
    server.quit()

    return f"VOC 알림 메일 발송 완료: {len(recipients)}명 / {len(df):,}건"
