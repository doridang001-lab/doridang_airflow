"""
DAG 모니터링 파이프라인

당일 실행된 모든 DAG의 성공/실패를 Airflow 메타DB에서 수집하고
실패 판단 규칙을 적용하여 결과를 OneDrive에 저장합니다.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import MART_DB
from modules.transform.utility.onedrive import save_to_onedrive_csv

logger = logging.getLogger(__name__)


def collect_dag_runs(target_date: str) -> tuple:
    """
    target_date (YYYY-MM-DD) 당일 실행된 dag_run + task_instance 수집.

    반환: (dag_runs_df, task_instances_df)

    dag_runs_df 컬럼: dag_id, run_id, state, execution_date, start_date, end_date
    task_instances_df 컬럼: dag_id, run_id, task_id, state, start_date, end_date, duration
    """
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    logger.info(f"collect_dag_runs 시작: target_date={target_date}")

    date_start = datetime.strptime(target_date, "%Y-%m-%d")
    date_end = date_start + timedelta(days=1)

    dag_run_rows = []
    task_instance_rows = []

    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= date_start)
            .filter(DagRun.execution_date < date_end)
            .all()
        )

        logger.info(f"수집된 dag_run 수: {len(dag_runs)}")

        for dr in dag_runs:
            dag_run_rows.append({
                "dag_id": dr.dag_id,
                "run_id": dr.run_id,
                "state": dr.state,
                "execution_date": dr.execution_date,
                "start_date": dr.start_date,
                "end_date": dr.end_date,
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
                    "start_date": ti.start_date,
                    "end_date": ti.end_date,
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

    logger.info(
        f"수집 완료: dag_runs={len(dag_runs_df)}건, task_instances={len(task_instances_df)}건"
    )
    return dag_runs_df, task_instances_df


def apply_failure_rules(
    dag_runs_df: pd.DataFrame, task_instances_df: pd.DataFrame
) -> pd.DataFrame:
    """
    실패 판단 로직 3단계 적용.

    반환 DataFrame 컬럼:
    dag_id, execution_date, dag_run_state, status, fail_type, detail,
    duration_sec, total_tasks, failed_tasks, skipped_tasks

    판정 기준:
    - 1차(FAIL): dag_run.state == 'failed'
    - 2차(FAIL): skipped_tasks/total_tasks >= 0.5 OR upstream_failed 상태 존재
    - 3차(WARN): duration_sec < 10 AND duration_sec > 0 OR all tasks skipped
    - 나머지: OK

    priority: FAIL > WARN > OK
    fail_type: 'dag_failed' / 'high_skip_ratio' / 'upstream_failed' / 'short_duration' / 'all_skipped' / None
    """
    logger.info("apply_failure_rules 시작")

    results = []

    for _, dr in dag_runs_df.iterrows():
        dag_id = dr["dag_id"]
        run_id = dr["run_id"]
        dag_run_state = dr["state"]
        execution_date = dr["execution_date"]

        # 해당 dag_run의 task_instance 필터링
        if not task_instances_df.empty:
            ti_df = task_instances_df[
                (task_instances_df["dag_id"] == dag_id)
                & (task_instances_df["run_id"] == run_id)
            ]
        else:
            ti_df = pd.DataFrame()

        total_tasks = len(ti_df)
        failed_tasks = int((ti_df["state"] == "failed").sum()) if not ti_df.empty else 0
        skipped_tasks = int((ti_df["state"] == "skipped").sum()) if not ti_df.empty else 0
        upstream_failed_tasks = (
            int((ti_df["state"] == "upstream_failed").sum()) if not ti_df.empty else 0
        )

        # duration 계산 (초)
        if pd.notna(dr.get("start_date")) and pd.notna(dr.get("end_date")):
            try:
                duration_sec = (dr["end_date"] - dr["start_date"]).total_seconds()
            except Exception:
                duration_sec = 0.0
        else:
            duration_sec = 0.0

        status = "OK"
        fail_type = None
        detail = "정상 실행"

        # 1차: dag_run.state == 'failed'
        if dag_run_state == "failed":
            status = "FAIL"
            fail_type = "dag_failed"
            detail = f"DAG 실행 실패 (failed tasks: {failed_tasks}건)"

        # 2차: 스킵 비율 >= 0.5 OR upstream_failed 존재
        if status != "FAIL":
            if total_tasks > 0 and (skipped_tasks / total_tasks) >= 0.5:
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
            "failed_tasks": failed_tasks,
            "skipped_tasks": skipped_tasks,
        })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        results_df = pd.DataFrame(
            columns=[
                "dag_id", "execution_date", "dag_run_state", "status",
                "fail_type", "detail", "duration_sec", "total_tasks",
                "failed_tasks", "skipped_tasks",
            ]
        )

    fail_count = int((results_df["status"] == "FAIL").sum()) if not results_df.empty else 0
    warn_count = int((results_df["status"] == "WARN").sum()) if not results_df.empty else 0
    ok_count = int((results_df["status"] == "OK").sum()) if not results_df.empty else 0
    logger.info(f"판정 완료: FAIL={fail_count}, WARN={warn_count}, OK={ok_count}")

    return results_df


def format_email_body(results_df: pd.DataFrame, target_date: str) -> str:
    """
    결과 DataFrame을 이메일 텍스트 본문으로 변환.

    형식:
    [2026-04-03 DAG 실행 점검]

    FAIL (N건)
    - dag_id → detail
    ...

    WARN (N건)
    - dag_id → detail
    ...

    정상: N건

    FAIL/WARN 없으면 "모든 DAG 정상 실행 완료" 출력.
    """
    logger.info("format_email_body 시작")

    if results_df.empty:
        return f"[{target_date} DAG 실행 점검]\n\n실행된 DAG가 없습니다."

    fail_df = results_df[results_df["status"] == "FAIL"]
    warn_df = results_df[results_df["status"] == "WARN"]
    ok_count = int((results_df["status"] == "OK").sum())

    lines = [f"[{target_date} DAG 실행 점검]", ""]

    if fail_df.empty and warn_df.empty:
        lines.append("모든 DAG 정상 실행 완료")
        lines.append("")
        lines.append(f"정상: {ok_count}건")
        return "\n".join(lines)

    if not fail_df.empty:
        lines.append(f"FAIL ({len(fail_df)}건)")
        for _, row in fail_df.iterrows():
            lines.append(f"- {row['dag_id']} → {row['detail']}")
        lines.append("")

    if not warn_df.empty:
        lines.append(f"WARN ({len(warn_df)}건)")
        for _, row in warn_df.iterrows():
            lines.append(f"- {row['dag_id']} → {row['detail']}")
        lines.append("")

    lines.append(f"정상: {ok_count}건")

    return "\n".join(lines)


def save_monitoring_results(results_df: pd.DataFrame, target_date: str) -> str:
    """
    OneDrive CSV로 저장.
    경로: {MART_DB}/dags_monitoring/dags_monitoring_{YYYYMMDD}.csv
    save_to_onedrive_csv 사용 (pk_col="dag_id", if_exists="replace")
    저장된 파일 경로 반환.
    """
    logger.info(f"save_monitoring_results 시작: target_date={target_date}")

    date_str = target_date.replace("-", "")
    file_path = str(MART_DB / "dags_monitoring" / f"dags_monitoring_{date_str}.csv")

    logger.info(f"저장 경로: {file_path}")

    # 디렉터리 생성
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    save_to_onedrive_csv(
        df=results_df,
        file_path=file_path,
        pk_col="dag_id",
        if_exists="replace",
    )

    logger.info(f"저장 완료: {file_path}")
    return file_path
