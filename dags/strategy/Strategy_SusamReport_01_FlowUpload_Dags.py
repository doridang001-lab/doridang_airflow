"""전일 수삼 CSV를 저장하고 표 텍스트를 매일 Flow 고정 게시글 댓글에 업로드한다.

conf 없는 예약/수동 실행: KST 전일
단일 재처리 예시: {"date": "2026-07-03"}
재업로드 예시: {"date": "2026-07-03", "force": true}
범위 재업로드 예시: {"start": "2026-07-01", "end": "2026-07-19", "force": true}
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.session import create_session

from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT
from modules.transform.utility.paths import LOCAL_DB
from modules.transform.utility.schedule import SMP_SUSAM_REPORT_TIME

logger = logging.getLogger(__name__)

DAG_ID = Path(__file__).stem
POST_URL_VARIABLE = "FLOW_SUSAM_POST_URL"
DEBUGGER_VARIABLE = "FLOW_SUSAM_CHROME_DEBUGGER"
CHROMEDRIVER_VARIABLE = "FLOW_SUSAM_CHROMEDRIVER_PATH"
KST = pendulum.timezone("Asia/Seoul")
UNIFIED_DAG_ID = "DB_UnifiedSales"
SOURCE_STABILITY_SECONDS = 60 * 10
FLOW_CHROME_RELEASE_PATH = LOCAL_DB / "flow_susam_chrome_release.json"


def _validate_date(value: str, field: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise AirflowException(f"dag_run.conf.{field}는 YYYY-MM-DD 형식이어야 합니다.") from exc
    if parsed.strftime("%Y-%m-%d") != value:
        raise AirflowException(f"dag_run.conf.{field}는 YYYY-MM-DD 형식이어야 합니다.")
    return parsed


def _resolve_targets(
    conf: dict | None,
    *,
    data_interval_end=None,
    now=None,
) -> tuple[list[str], str, list[str], bool]:
    """CLI 날짜 인자, 표시값, 대상 날짜 목록, 기본 전일 모드 여부를 반환한다."""
    conf = conf if isinstance(conf, dict) else {}
    date_str = str(conf.get("date") or "").strip()
    start_str = str(conf.get("start") or "").strip()
    end_str = str(conf.get("end") or "").strip()

    if date_str:
        if start_str or end_str:
            raise AirflowException("date와 start/end는 함께 사용할 수 없습니다.")
        _validate_date(date_str, "date")
        return ["--date", date_str], date_str, [date_str], False

    if start_str or end_str:
        if not start_str or not end_str:
            raise AirflowException("start와 end는 함께 사용해야 합니다.")
        start_date = _validate_date(start_str, "start")
        end_date = _validate_date(end_str, "end")
        if start_date > end_date:
            raise AirflowException("dag_run.conf.start는 end보다 늦을 수 없습니다.")
        dates = []
        current = pendulum.date(start_date.year, start_date.month, start_date.day)
        last = pendulum.date(end_date.year, end_date.month, end_date.day)
        while current <= last:
            dates.append(current.format("YYYY-MM-DD"))
            current = current.add(days=1)
        return ["--start", start_str, "--end", end_str], f"{start_str}~{end_str}", dates, False

    base = data_interval_end or now or pendulum.now(KST)
    target = pendulum.instance(base).in_timezone(KST).subtract(days=1).format("YYYY-MM-DD")
    return ["--date", target], target, [target], True


def _context_targets(context) -> tuple[list[str], str, list[str], bool]:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    return _resolve_targets(conf, data_interval_end=context.get("data_interval_end"))


def _daily_runs_ready(runs) -> bool:
    daily_runs = [run for run in runs if not str(getattr(run, "run_id", "")).startswith("today__")]
    active = [run for run in daily_runs if getattr(run, "state", None) in {"queued", "running"}]
    terminal = [run for run in daily_runs if getattr(run, "state", None) in {"success", "failed"}]
    logger.info(
        "UnifiedSales 당일 상태 | daily=%d active=%d terminal=%d",
        len(daily_runs),
        len(active),
        len(terminal),
    )
    return bool(terminal) and not active


def _source_updated_today(path: Path, *, now=None) -> bool:
    """예약 실행이 이전 생성본을 재사용하지 않도록 KST 당일 갱신 여부를 확인한다."""
    current = pendulum.instance(now).in_timezone(KST) if now else pendulum.now(KST)
    return path.stat().st_mtime >= current.start_of("day").timestamp()


def _wait_for_source_ready(**context) -> bool:
    _date_args, target_label, target_dates, default_mode = _context_targets(context)
    missing = []
    for date_str in target_dates:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        path = UNIFIED_ROOT / f"unified_sales_{parsed:%y%m%d}.parquet"
        if not path.is_file():
            missing.append(str(path))
    if missing:
        logger.info("UnifiedSales 원천 대기 | target=%s missing=%s", target_label, missing)
        return False
    if not default_mode:
        logger.info("명시 날짜 원천 확인 완료 | target=%s", target_label)
        return True

    current_now = pendulum.now(KST)
    stale = []
    for date_str in target_dates:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        path = UNIFIED_ROOT / f"unified_sales_{parsed:%y%m%d}.parquet"
        if not _source_updated_today(path, now=current_now):
            stale.append(str(path))
    if stale:
        logger.info("UnifiedSales 당일 갱신 대기 | target=%s stale=%s", target_label, stale)
        return False

    unstable = []
    current_timestamp = current_now.timestamp()
    for date_str in target_dates:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        path = UNIFIED_ROOT / f"unified_sales_{parsed:%y%m%d}.parquet"
        age_seconds = current_timestamp - path.stat().st_mtime
        if age_seconds < SOURCE_STABILITY_SECONDS:
            unstable.append({"path": str(path), "age_seconds": int(age_seconds)})
    if unstable:
        logger.info("UnifiedSales 원천 안정화 대기 | target=%s files=%s", target_label, unstable)
        return False

    day_start = current_now.start_of("day").in_timezone("UTC")
    day_end = day_start.add(days=1)
    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == UNIFIED_DAG_ID)
            .filter(DagRun.start_date.isnot(None))
            .filter(DagRun.start_date >= day_start)
            .filter(DagRun.start_date < day_end)
            .order_by(DagRun.start_date.desc())
            .all()
        )
    ready = _daily_runs_ready(runs)
    logger.info("전일 원천 최종 준비 여부 | target=%s ready=%s", target_label, ready)
    return ready


def _wait_for_flow_chrome() -> bool:
    debugger_address = Variable.get(DEBUGGER_VARIABLE).strip()
    if not debugger_address:
        raise AirflowException(f"Airflow Variable {DEBUGGER_VARIABLE}가 비어 있습니다.")
    endpoint = f"http://{debugger_address}/json/version"
    try:
        with urlopen(endpoint, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        ready = response.status == 200 and str(payload.get("Browser") or "").startswith("Chrome/")
    except Exception as exc:
        logger.info("Flow Chrome 대기 | endpoint=%s error=%r", endpoint, exc)
        return False
    logger.info("Flow Chrome 준비 확인 | endpoint=%s browser=%s", endpoint, payload.get("Browser"))
    return ready


def _upload_daily_report(**context):
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    date_args, target_label, _target_dates, _default_mode = _context_targets(context)

    post_url = Variable.get(POST_URL_VARIABLE)
    debugger_address = Variable.get(DEBUGGER_VARIABLE)
    chromedriver_path = Variable.get(CHROMEDRIVER_VARIABLE, default_var="").strip()
    if chromedriver_path:
        os.environ["FLOW_CHROMEDRIVER_PATH"] = chromedriver_path

    from scripts.analysis.susam_report import main

    args = [
        *date_args,
        "--post-url",
        post_url,
        "--debugger-address",
        debugger_address,
    ]
    if conf.get("force") is True:
        args.append("--force")

    result = main(args)
    if result["failed"]:
        raise AirflowException(f"일별 상품 매출 Flow 업로드 실패: {result['failed']}")
    logger.info("일별 상품 매출 Flow 업로드 DAG 완료 | target=%s result=%s", target_label, result)
    return result


def _signal_flow_chrome_release(**context) -> str:
    """Windows 제한시간 실행기가 Flow Chrome을 즉시 정리하도록 성공 마커를 기록한다."""
    payload = {
        "completed_at": pendulum.now(KST).to_iso8601_string(),
        "dag_id": DAG_ID,
        "run_id": context.get("run_id") or getattr(context.get("dag_run"), "run_id", ""),
    }
    FLOW_CHROME_RELEASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = FLOW_CHROME_RELEASE_PATH.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(FLOW_CHROME_RELEASE_PATH)
    logger.info("Flow Chrome 종료 마커 기록 | path=%s run_id=%s", FLOW_CHROME_RELEASE_PATH, payload["run_id"])
    return str(FLOW_CHROME_RELEASE_PATH)


with DAG(
    dag_id=DAG_ID,
    description="매일 전일 수삼 CSV를 저장하고 표 텍스트를 Flow 댓글에 업로드",
    schedule=SMP_SUSAM_REPORT_TIME,
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["strategy", "flow", "daily-sales"],
) as dag:
    wait_for_source_ready = PythonSensor(
        task_id="wait_for_source_ready",
        python_callable=_wait_for_source_ready,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 2,
    )

    wait_for_flow_chrome = PythonSensor(
        task_id="wait_for_flow_chrome",
        python_callable=_wait_for_flow_chrome,
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 10,
    )

    upload_daily_report = PythonOperator(
        task_id="upload_daily_report",
        python_callable=_upload_daily_report,
    )

    release_flow_chrome = PythonOperator(
        task_id="release_flow_chrome",
        python_callable=_signal_flow_chrome_release,
    )

    wait_for_source_ready >> wait_for_flow_chrome >> upload_daily_report >> release_flow_chrome
