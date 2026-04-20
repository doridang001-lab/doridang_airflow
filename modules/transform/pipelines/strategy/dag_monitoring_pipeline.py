"""
DAG 모니터링 파이프라인

당일 실행된 모든 DAG의 성공/실패를 Airflow 메타DB에서 수집하고
실패 판단 규칙을 적용하여 결과를 OneDrive에 저장합니다.
"""

import io
import logging
from pathlib import Path

import pandas as pd

from modules.common.config import ADMIN_EMAIL
from modules.transform.utility.paths import MART_DB
from modules.transform.utility.onedrive import save_to_onedrive_csv

logger = logging.getLogger(__name__)

# 히트맵 기준 시각 소스: 실제 실행 시각(start_date) 우선
# 필요 시 "execution_date"로 변경 가능
HEATMAP_TIME_SOURCE = "start_date"  # "start_date" | "execution_date"


def _parse_payload_timestamp(value):
    """XCom JSON payload에서 복원된 timestamp를 안전하게 파싱한다."""
    if value is None:
        return pd.NaT

    if isinstance(value, pd.Timestamp):
        return value

    if isinstance(value, str):
        text = value.strip()
        if text == "" or text.lower() in {"nat", "none", "null"}:
            return pd.NaT
        if text.isdigit():
            try:
                return pd.to_datetime(int(text), unit="ms", utc=True, errors="coerce")
            except Exception:
                return pd.NaT
        return pd.to_datetime(text, utc=True, errors="coerce")

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return pd.NaT
        return pd.to_datetime(value, unit="ms", utc=True, errors="coerce")

    return pd.to_datetime(value, utc=True, errors="coerce")


def collect_dag_runs(**context) -> str:
    """
    실행 당일 dag_run + task_instance 수집.
    target_date는 Airflow logical_date 기준 (YYYY-MM-DD).

    XCom push key: 'dag_runs_and_tasks'
    반환: XCom push된 target_date 문자열
    """
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    logical_date = context["logical_date"]
    logical_kst = logical_date.in_timezone("Asia/Seoul")
    target_date = logical_kst.strftime("%Y-%m-%d")

    logger.info(f"collect_dag_runs 시작: target_date={target_date}")

    # Airflow 메타DB(datetime with timezone) 바인딩을 위해 timezone-aware 경계값 사용
    date_start = logical_kst.start_of("day")
    date_end = date_start.add(days=1)

    dag_run_rows = []
    task_instance_rows = []

    _EXCLUDE_PREFIXES = ("Strategy_DagMonitoring",)

    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= date_start)
            .filter(DagRun.execution_date < date_end)
            .all()
        )
        dag_runs = [dr for dr in dag_runs if not dr.dag_id.startswith(_EXCLUDE_PREFIXES)]

        logger.info(f"수집된 dag_run 수: {len(dag_runs)}")

        for dr in dag_runs:
            dag_run_rows.append({
                "dag_id": dr.dag_id,
                "run_id": dr.run_id,
                "state": dr.state,
                "execution_date": str(dr.execution_date),
                "start_date": str(dr.start_date),
                "end_date": str(dr.end_date),
            })

            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dr.dag_id)
                .filter(TaskInstance.run_id == dr.run_id)
                .all()
            )

            for ti in task_instances:
                task_instance_rows.append({
                    "dag_id": ti.dag_id,
                    "run_id": ti.run_id,
                    "task_id": ti.task_id,
                    "state": ti.state,
                    "start_date": str(ti.start_date),
                    "end_date": str(ti.end_date),
                    "duration": ti.duration,
                })

    dag_runs_df = pd.DataFrame(dag_run_rows)
    task_instances_df = pd.DataFrame(task_instance_rows)

    if dag_runs_df.empty:
        logger.warning(f"target_date={target_date} 에 실행된 dag_run이 없습니다.")
        dag_runs_df = pd.DataFrame(
            columns=["dag_id", "run_id", "state", "execution_date", "start_date", "end_date"]
        )

    if task_instances_df.empty:
        logger.warning("수집된 task_instance가 없습니다.")
        task_instances_df = pd.DataFrame(
            columns=["dag_id", "run_id", "task_id", "state", "start_date", "end_date", "duration"]
        )

    if not dag_runs_df.empty:
        dag_runs_df["execution_date"] = pd.to_datetime(dag_runs_df["execution_date"], errors="coerce")
        dag_runs_df["start_date"] = pd.to_datetime(dag_runs_df["start_date"], errors="coerce")
        dag_runs_df = dag_runs_df[~dag_runs_df["run_id"].astype(str).str.startswith("__airflow_temporary_run_")]
        dag_runs_df = (
            dag_runs_df.sort_values(["dag_id", "execution_date", "start_date"], ascending=[True, False, False])
            .drop_duplicates(subset=["dag_id"], keep="first")
            .reset_index(drop=True)
        )

        if not task_instances_df.empty:
            valid_run_keys = set(zip(dag_runs_df["dag_id"], dag_runs_df["run_id"]))
            task_instances_df = task_instances_df[
                task_instances_df.apply(
                    lambda row: (row["dag_id"], row["run_id"]) in valid_run_keys,
                    axis=1,
                )
            ].reset_index(drop=True)

    logger.info(
        f"수집 완료: dag_runs={len(dag_runs_df)}건, task_instances={len(task_instances_df)}건"
    )

    payload = {
        "target_date": target_date,
        "dag_runs": dag_runs_df.to_json(orient="records", force_ascii=False),
        "task_instances": task_instances_df.to_json(orient="records", force_ascii=False),
    }
    context["ti"].xcom_push(key="dag_runs_and_tasks", value=payload)
    return target_date


def apply_failure_rules(**context) -> str:
    """
    XCom에서 dag_runs / task_instances를 받아 실패 판단 로직 3단계 적용.

    반환 DataFrame 컬럼:
    dag_id, execution_date, dag_run_state, status, fail_type, detail,
    duration_sec, total_tasks, failed_tasks, skipped_tasks

    판정 기준:
    - 1차(FAIL): dag_run.state == 'failed'
    - 2차(FAIL): skipped_tasks/total_tasks >= 0.5 OR upstream_failed 상태 존재
    - 3차(WARN): 0 < duration_sec < 10 OR all tasks skipped
    - 나머지: OK

    priority: FAIL > WARN > OK
    fail_type: 'dag_failed' / 'high_skip_ratio' / 'upstream_failed' / 'short_duration' / 'all_skipped' / None

    XCom push key: 'monitoring_results'
    """
    import json

    logger.info("apply_failure_rules 시작")

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="collect_dag_runs", key="dag_runs_and_tasks")

    target_date = payload["target_date"]
    dag_runs_df = pd.read_json(io.StringIO(payload["dag_runs"]), orient="records", convert_dates=False)
    task_instances_df = pd.read_json(io.StringIO(payload["task_instances"]), orient="records", convert_dates=False)

    results = []

    for _, dr in dag_runs_df.iterrows():
        dag_id = dr["dag_id"]
        run_id = dr["run_id"]
        dag_run_state = dr["state"]
        execution_date = dr["execution_date"]

        if not task_instances_df.empty:
            ti_df = task_instances_df[
                (task_instances_df["dag_id"] == dag_id)
                & (task_instances_df["run_id"] == run_id)
            ]
        else:
            ti_df = pd.DataFrame()

        total_tasks = len(ti_df)
        success_tasks = int((ti_df["state"] == "success").sum()) if not ti_df.empty else 0
        failed_tasks = int((ti_df["state"] == "failed").sum()) if not ti_df.empty else 0
        skipped_tasks = int((ti_df["state"] == "skipped").sum()) if not ti_df.empty else 0
        upstream_failed_tasks = (
            int((ti_df["state"] == "upstream_failed").sum()) if not ti_df.empty else 0
        )

        # duration 계산 (초)
        try:
            start = _parse_payload_timestamp(dr.get("start_date"))
            end = _parse_payload_timestamp(dr.get("end_date"))
            if pd.notna(start) and pd.notna(end):
                duration_sec = (end - start).total_seconds()
            else:
                duration_sec = 0.0
        except Exception:
            duration_sec = 0.0

        # KST 시각(히트맵용): 기본은 실제 실행 시각(start_date) 우선
        # 5분 버킷은 내림이 아닌 반올림으로 계산해 특정 분(:25)으로 과도하게 몰리는 현상을 완화한다.
        try:
            ts = pd.NaT
            preferred_col = "start_date" if HEATMAP_TIME_SOURCE == "start_date" else "execution_date"
            fallback_col = "execution_date" if preferred_col == "start_date" else "start_date"

            ts_val = dr.get(preferred_col)
            if pd.notna(ts_val):
                ts = _parse_payload_timestamp(ts_val)
            if pd.isna(ts):
                ts_val = dr.get(fallback_col)
                if pd.notna(ts_val):
                    ts = _parse_payload_timestamp(ts_val)

            if pd.isna(ts):
                raise ValueError("start_date/execution_date 모두 파싱 실패")

            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")

            ts_kst = ts.tz_convert("Asia/Seoul")

            minute_rounded = int(round(ts_kst.minute / 5.0) * 5)
            hour = int(ts_kst.hour)
            if minute_rounded == 60:
                minute_rounded = 0
                hour = (hour + 1) % 24

            start_kst_hour = hour
            start_kst_min = minute_rounded
        except Exception:
            start_kst_hour = -1
            start_kst_min = -1

        status = "OK"
        fail_type = None
        detail = "정상 실행"

        # 1차: dag_run.state == 'failed'
        if dag_run_state == "failed":
            status = "FAIL"
            fail_type = "dag_failed"
            detail = f"DAG 실행 실패 (failed tasks: {failed_tasks}건)"

        intentional_skip_run = (
            total_tasks > 0
            and skipped_tasks > 0
            and failed_tasks == 0
            and upstream_failed_tasks == 0
            and dag_run_state == "success"
            and (success_tasks + skipped_tasks) == total_tasks
        )

        # 2차: 스킵 비율 >= 0.5 OR upstream_failed 존재
        if status != "FAIL":
            if total_tasks > 0 and (skipped_tasks / total_tasks) >= 0.5 and not intentional_skip_run:
                status = "FAIL"
                fail_type = "high_skip_ratio"
                skip_pct = round(skipped_tasks / total_tasks * 100, 1)
                detail = f"스킵 비율 과다 ({skipped_tasks}/{total_tasks}, {skip_pct}%)"
            elif upstream_failed_tasks > 0:
                status = "FAIL"
                fail_type = "upstream_failed"
                detail = f"upstream_failed 태스크 존재 ({upstream_failed_tasks}건)"

        # 3차: 짧은 실행 시간 OR 전체 스킵
        if status == "OK":
            if total_tasks > 0 and skipped_tasks == total_tasks:
                status = "WARN"
                fail_type = "all_skipped"
                detail = f"전체 태스크 스킵 ({total_tasks}건)"
            elif intentional_skip_run:
                detail = f"정상 실행 (의도적 스킵 포함: {skipped_tasks}/{total_tasks}건)"
            elif 0 < duration_sec < 10:
                status = "WARN"
                fail_type = "short_duration"
                detail = f"실행 시간 과소 ({duration_sec:.1f}초)"

        results.append({
            "dag_id": dag_id,
            "execution_date": execution_date,
            "dag_run_state": dag_run_state,
            "status": status,
            "fail_type": fail_type,
            "detail": detail,
            "duration_sec": duration_sec,
            "total_tasks": total_tasks,
            "success_tasks": success_tasks,
            "failed_tasks": failed_tasks,
            "skipped_tasks": skipped_tasks,
            "start_hour_kst": start_kst_hour,
            "start_min_kst": start_kst_min,
        })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        results_df = pd.DataFrame(
            columns=[
                "dag_id", "execution_date", "dag_run_state", "status",
                "fail_type", "detail", "duration_sec", "total_tasks",
                "success_tasks", "failed_tasks", "skipped_tasks",
                "start_hour_kst", "start_min_kst",
            ]
        )

    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0
    logger.info(f"판정 완료: FAIL={fail_count}, WARN={warn_count}, OK={ok_count}")

    payload_out = {
        "target_date": target_date,
        "results": results_df.to_json(orient="records", force_ascii=False),
    }
    context["ti"].xcom_push(key="monitoring_results", value=payload_out)
    return target_date


def _build_report_html(results_df: pd.DataFrame, target_date: str) -> str:
    """
    히트맵 + 실패/경고 요약을 담은 HTML 이메일 본문 생성.

    구조:
    - 헤더 (날짜 + 요약 카드)
    - 실패/경고 상세 테이블 (FAIL/WARN 있을 때만)
    - 시간별 히트맵 테이블 (X축: 0~23시, Y축: DAG)
    """
    logger.info("_build_report_html 시작")

    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0
    total_count = len(results_df)

    # ── 색상 상수 ──────────────────────────────────────────────
    C_FAIL = "#e74c3c"
    C_WARN = "#f39c12"
    C_OK = "#27ae60"
    C_EMPTY = "#dfe6e9"
    C_HEADER_BG = "#2c3e50"

    # ── 요약 카드 ──────────────────────────────────────────────
    def _card(color: str, label: str, count: int) -> str:
        return (
            f'<div style="display:inline-block;background:{color};color:#fff;'
            f'border-radius:6px;padding:10px 20px;margin:4px;font-size:15px;font-weight:bold;">'
            f'{label}<br><span style="font-size:22px;">{count}건</span></div>'
        )

    summary_html = (
        _card(C_FAIL, "FAIL", fail_count)
        + _card(C_WARN, "WARN", warn_count)
        + _card(C_OK, "OK", ok_count)
        + _card("#7f8c8d", "전체", total_count)
    )

    # ── 실패/경고 상세 테이블 ────────────────────────────────
    alert_html = ""
    if not results_df.empty and (fail_count > 0 or warn_count > 0):
        alert_rows = results_df[results_df["status"].isin(["FAIL", "WARN"])].sort_values(
            ["status", "dag_id"]
        )
        rows_html = ""
        for _, row in alert_rows.iterrows():
            color = C_FAIL if row["status"] == "FAIL" else C_WARN
            badge = (
                f'<span style="background:{color};color:#fff;border-radius:4px;'
                f'padding:2px 8px;font-size:12px;font-weight:bold;">{row["status"]}</span>'
            )
            dur = f'{row["duration_sec"]:.1f}s' if row["duration_sec"] else "-"
            dag_url = f'http://localhost:8080/dags/{row["dag_id"]}/grid'
            dag_link = (
                f'<a href="{dag_url}" style="color:{color};font-weight:bold;text-decoration:none;"'
                f' title="Airflow에서 열기">'
                f'{row["dag_id"]} ↗</a>'
            )
            rows_html += (
                f'<tr>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #ecf0f1;">{dag_link}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #ecf0f1;">{badge}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #ecf0f1;">{row["detail"]}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #ecf0f1;text-align:right;">{dur}</td>'
                f'</tr>'
            )
        th_style = (
            'style="padding:8px 10px;background:#34495e;color:#fff;'
            'text-align:left;font-weight:bold;"'
        )
        alert_html = f"""
        <h3 style="color:#2c3e50;margin-top:24px;">⚠️ 실패/경고 상세</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr>
              <th {th_style}>DAG</th>
              <th {th_style}>상태</th>
              <th {th_style}>상세</th>
              <th {th_style} style="text-align:right;">실행시간</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        """

    # ── 공통 범례/색상 ────────────────────────────────────────
    heatmap_html = ""
    if not results_df.empty:
        STATUS_COLOR = {"OK": C_OK, "FAIL": C_FAIL, "WARN": C_WARN}
        STATUS_MARKER = {"OK": "O", "FAIL": "F", "WARN": "W"}

        def _legend(color: str, label: str) -> str:
            return (
                f'<span style="display:inline-block;background:{color};color:#fff;'
                f'border-radius:3px;padding:2px 8px;font-size:11px;margin-right:6px;">{label}</span>'
            )

        legend_html = (
            _legend(C_OK, "OK")
            + _legend(C_FAIL, "FAIL")
            + _legend(C_WARN, "WARN")
            + _legend(C_EMPTY, "미실행")
        )

        def _dag_cell(dag_id_str: str, dag_label: str, status: str) -> str:
            dag_url = f'http://localhost:8080/dags/{dag_id_str}/grid'
            base_style = (
                'padding:4px 8px;border-bottom:1px solid #ecf0f1;'
                'font-size:12px;white-space:nowrap;'
            )
            if status in ("FAIL", "WARN"):
                cell_color = C_FAIL if status == "FAIL" else C_WARN
                return (
                    f'<td style="{base_style}">'
                    f'<a href="{dag_url}" style="color:{cell_color};font-weight:bold;'
                    f'text-decoration:none;" title="Airflow에서 열기">{dag_label} ↗</a>'
                    f'</td>'
                )
            return f'<td style="{base_style}">{dag_label}</td>'

        # ── ① 시간별 히트맵 (0~23시 전체) ─────────────────────
        hour_headers = "".join(
            f'<th style="padding:4px 6px;background:{C_HEADER_BG};color:#fff;'
            f'font-size:11px;text-align:center;min-width:24px;">{h:02d}</th>'
            for h in range(24)
        )
        hourly_header_row = (
            f'<tr><th style="padding:6px 10px;background:{C_HEADER_BG};color:#fff;'
            f'text-align:left;min-width:200px;">DAG</th>{hour_headers}</tr>'
        )
        def _safe_int(val, default: int = -1) -> int:
            try:
                return int(val)
            except (TypeError, ValueError):
                return default

        hourly_data_rows = ""
        for _, row in results_df.sort_values("dag_id").iterrows():
            dag_id_str = str(row["dag_id"])
            dag_label = dag_id_str if len(dag_id_str) <= 35 else dag_id_str[:33] + "…"
            h_run = _safe_int(row.get("start_hour_kst"))
            status = row["status"]
            cells = ""
            for h in range(24):
                if h == h_run and h_run >= 0:
                    bg = STATUS_COLOR.get(status, C_OK)
                    marker = STATUS_MARKER.get(status, "?")
                    cells += (
                        f'<td title="{status}" bgcolor="{bg}" '
                        f'style="background:{bg};text-align:center;font-size:10px;'
                        f'font-weight:bold;color:#111;border:1px solid #ffffff;'
                        f'min-width:24px;height:18px;">{marker}</td>'
                    )
                else:
                    cells += (
                        f'<td title="미실행" bgcolor="{C_EMPTY}" '
                        f'style="background:{C_EMPTY};text-align:center;font-size:10px;'
                        f'color:#7f8c8d;border:1px solid #ffffff;min-width:24px;height:18px;">·</td>'
                    )
            hourly_data_rows += (
                f'<tr>{_dag_cell(dag_id_str, dag_label, status)}{cells}</tr>'
            )

        hourly_heatmap = (
            f'<h3 style="color:#2c3e50;margin-top:28px;">🗓️ 시간별 실행 히트맵 (KST)</h3>'
            f'<p style="font-size:12px;color:#7f8c8d;margin:4px 0 8px;">{legend_html}</p>'
            f'<div style="overflow-x:auto;">'
            f'<table style="border-collapse:collapse;font-size:12px;min-width:600px;">'
            f'<thead>{hourly_header_row}</thead>'
            f'<tbody>{hourly_data_rows}</tbody>'
            f'</table></div>'
        )

        # ── ② 5분 단위 히트맵 (시간대별 분리) ─────────────────
        MIN_SLOTS = list(range(0, 60, 5))
        active_hours = sorted([
            int(h) for h in results_df["start_hour_kst"].dropna().unique()
            if int(h) >= 0
        ])

        minute_sections = ""
        if not active_hours:
            minute_sections = (
                '<p style="font-size:12px;color:#7f8c8d;">'
                '실행 시각(start_date/execution_date) 정보가 없어 5분 히트맵을 생성하지 못했습니다.'
                '</p>'
            )
        else:
            for hour in active_hours:
                hour_df = results_df[results_df["start_hour_kst"] == hour].sort_values("dag_id")
                min_headers = "".join(
                    f'<th style="padding:4px 5px;background:{C_HEADER_BG};color:#fff;'
                    f'font-size:10px;text-align:center;min-width:38px;">{hour:02d}:{m:02d}</th>'
                    for m in MIN_SLOTS
                )
                header_row = (
                    f'<tr><th style="padding:6px 10px;background:{C_HEADER_BG};color:#fff;'
                    f'text-align:left;min-width:200px;">DAG</th>{min_headers}</tr>'
                )
                data_rows = ""
                for _, row in hour_df.iterrows():
                    dag_id_str = str(row["dag_id"])
                    dag_label = dag_id_str if len(dag_id_str) <= 35 else dag_id_str[:33] + "…"
                    m_run = _safe_int(row.get("start_min_kst"))
                    status = row["status"]
                    cells = ""
                    for m in MIN_SLOTS:
                        if m == m_run and m_run >= 0:
                            bg = STATUS_COLOR.get(status, C_OK)
                            marker = STATUS_MARKER.get(status, "?")
                            cells += (
                                f'<td title="{status}" bgcolor="{bg}" '
                                f'style="background:{bg};text-align:center;font-size:10px;'
                                f'font-weight:bold;color:#111;border:1px solid #ffffff;'
                                f'min-width:38px;height:18px;">{marker}</td>'
                            )
                        else:
                            cells += (
                                f'<td title="미실행" bgcolor="{C_EMPTY}" '
                                f'style="background:{C_EMPTY};text-align:center;font-size:10px;'
                                f'color:#7f8c8d;border:1px solid #ffffff;min-width:38px;height:18px;">·</td>'
                            )
                    data_rows += (
                        f'<tr>{_dag_cell(dag_id_str, dag_label, status)}{cells}</tr>'
                    )
                minute_sections += (
                    f'<h4 style="color:#34495e;margin:20px 0 6px;">'
                    f'🕐 {hour:02d}시 실행 ({len(hour_df)}개 DAG)</h4>'
                    f'<div style="overflow-x:auto;">'
                    f'<table style="border-collapse:collapse;font-size:12px;">'
                    f'<thead>{header_row}</thead>'
                    f'<tbody>{data_rows}</tbody>'
                    f'</table></div>'
                )

        minute_heatmap = (
            f'<h3 style="color:#2c3e50;margin-top:36px;">⏱️ 5분 단위 실행 히트맵 (KST)</h3>'
            f'<p style="font-size:12px;color:#7f8c8d;margin:4px 0 8px;">{legend_html}</p>'
            f'{minute_sections}'
        )

        heatmap_html = hourly_heatmap + minute_heatmap

    # ── 최종 조립 ──────────────────────────────────────────────
    no_data_msg = (
        '<p style="color:#7f8c8d;">실행된 DAG가 없습니다.</p>' if results_df.empty else ""
    )

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;padding:20px;color:#2c3e50;">
  <div style="background:{C_HEADER_BG};color:#fff;padding:16px 20px;border-radius:8px;margin-bottom:20px;">
    <h2 style="margin:0;font-size:18px;">📊 DAG 모니터링 리포트</h2>
    <p style="margin:4px 0 0;font-size:13px;opacity:0.8;">{target_date} · 매일 16:30 KST 자동 발송</p>
  </div>
  <div style="margin-bottom:16px;">{summary_html}</div>
  {no_data_msg}
  {alert_html}
  {heatmap_html}
  <p style="margin-top:30px;font-size:11px;color:#bdc3c7;border-top:1px solid #ecf0f1;padding-top:10px;">
    이 메일은 Airflow DAG 모니터링 파이프라인이 자동 발송했습니다.
  </p>
</body>
</html>"""

    return html


def save_monitoring_results(**context) -> str:
    """
    XCom에서 monitoring_results를 받아 OneDrive CSV로 저장.
    경로: {MART_DB}/dags_monitoring/dags_monitoring_{YYYYMMDD}.csv
    save_to_onedrive_csv 사용 (pk_col="dag_id", if_exists="replace")

    XCom push key: 'results_file_path'
    반환: 저장된 파일 경로 문자열
    """
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="apply_failure_rules", key="monitoring_results")

    target_date = payload["target_date"]
    results_df = pd.read_json(io.StringIO(payload["results"]), orient="records", convert_dates=False)

    logger.info(f"save_monitoring_results 시작: target_date={target_date}")

    date_str = target_date.replace("-", "")
    file_path = str(MART_DB / "dags_monitoring" / f"dags_monitoring_{date_str}.csv")

    logger.info(f"저장 경로: {file_path}")

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    save_to_onedrive_csv(
        df=results_df,
        file_path=file_path,
        pk_col="dag_id",
        if_exists="replace",
    )

    logger.info(f"저장 완료: {file_path}")

    context["ti"].xcom_push(key="results_file_path", value=file_path)
    return file_path


def send_monitoring_alert_email(**context) -> str:
    """
    monitoring_results XCom에서 판정 결과를 받아 이메일 발송.

    - FAIL/WARN 유무와 관계없이 항상 발송 (히트맵 리포트 포함)
    - Subject: FAIL > WARN > 정상 접두사
    - 수신자: ADMIN_EMAIL (관리자)
    - mailer.py send_email 사용, HTML 본문은 _build_report_html 생성
    """
    from modules.transform.utility.mailer import send_email

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="apply_failure_rules", key="monitoring_results")

    target_date = payload["target_date"]
    results_df = pd.read_json(io.StringIO(payload["results"]), orient="records", convert_dates=False)

    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0

    logger.info(f"[{target_date}] FAIL={fail_count}, WARN={warn_count}, OK={ok_count} - 이메일 발송 시작")

    html_content = _build_report_html(results_df, target_date)

    if fail_count > 0:
        subject_prefix = "🔴 FAIL"
    elif warn_count > 0:
        subject_prefix = "🟡 WARN"
    else:
        subject_prefix = "✅ 정상"

    subject = (
        f"[DAG 모니터링 {subject_prefix}] {target_date} "
        f"- FAIL {fail_count}건 / WARN {warn_count}건 / OK {ok_count}건"
    )

    to_emails = ADMIN_EMAIL

    result = send_email(
        subject=subject,
        html_content=html_content,
        to_emails=to_emails,
        **context,
    )

    logger.info(f"이메일 발송 완료: subject={subject}")
    return result
