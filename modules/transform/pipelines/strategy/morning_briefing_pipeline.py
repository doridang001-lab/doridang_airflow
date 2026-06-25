"""
출근길 AI 브리핑 파이프라인

수집:
    - 오늘 일정 (기본 Notion Calendar, MORNING_BRIEFING_INCLUDE_CALENDAR=0이면 제외)
    - 어제 FAIL/WARN DAG (모니터링 CSV)
    - git status / branch / log
    - daily_summary.parquet 신선도

생성:
    - gpt-oss로 각 FAIL DAG 원인 분석 (Step A)
    - gpt-oss로 전체 우선순위 브리핑 (Step B)

발송:
    - Telegram (modules.transform.utility.telegram)
"""

import csv
import io
import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, date, timedelta
from pathlib import Path

import pendulum

logger = logging.getLogger(__name__)

# morning_briefing_pipeline.py는 modules/transform/pipelines/strategy/ 에 위치
# → parent×5 = airflow 프로젝트 루트
_GIT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
_INCLUDE_CALENDAR = os.getenv("MORNING_BRIEFING_INCLUDE_CALENDAR", "1").strip() not in {"0", "false", "False", "no", "NO"}
_CALENDAR_SOURCE = os.getenv("MORNING_BRIEFING_CALENDAR_SOURCE", "cache").strip().lower()
_CALENDAR_CACHE_PATH = Path(
    os.getenv("MORNING_BRIEFING_CALENDAR_CACHE", "/opt/airflow/config/notion_calendar_cache.json")
    if os.getenv("AIRFLOW_HOME")
    else os.getenv("MORNING_BRIEFING_CALENDAR_CACHE", str(_GIT_ROOT / "config" / "notion_calendar_cache.json"))
)

_WINDOWS_SCHEDULED_TASKS = [
    {
        "label": "쿠팡 자동 수집",
        "task_name": "CoupangAutoCollect",
        "duplicate": False,
    },
    {
        "label": "WSL 에러 감지",
        "task_name": "telegram_claude_loop",
        "duplicate": False,
    },
    {
        "label": "WSL 에러 감지 중복 후보",
        "task_name": "TelegramClaudeLoop",
        "duplicate": True,
    },
    {
        "label": "AGENTS.md 업그레이드",
        "task_name": "Airflow_Codex_MD_Improver_Weekly",
        "duplicate": False,
    },
]


# ============================================================
# 수집 헬퍼
# ============================================================

def _collect_calendar_events() -> list[dict]:
    """오늘 일정 수집. 기본은 Notion Calendar, 필요 시 Google Calendar로 전환."""
    if not _INCLUDE_CALENDAR:
        logger.info("Calendar 수집 비활성화(MORNING_BRIEFING_INCLUDE_CALENDAR=0)")
        return []
    if _CALENDAR_SOURCE in {"cache", "cached", "notion_cache"}:
        events = _collect_cached_calendar_events()
        if events:
            return events
        logger.info("Calendar cache 비어 있음 — Notion Calendar 직접 수집 시도")
        return _collect_notion_calendar_events()
    if _CALENDAR_SOURCE in {"notion", "notion_calendar"}:
        return _collect_notion_calendar_events()
    if _CALENDAR_SOURCE not in {"google", "google_calendar"}:
        logger.warning("알 수 없는 Calendar source=%s — 일정 수집 건너뜀", _CALENDAR_SOURCE)
        return []

    return _collect_google_calendar_events()


def _collect_cached_calendar_events() -> list[dict]:
    """호스트 수집 스크립트가 저장한 Notion Calendar 캐시를 읽는다."""
    if not _CALENDAR_CACHE_PATH.exists():
        logger.warning("Calendar cache 없음: %s", _CALENDAR_CACHE_PATH)
        return []
    try:
        payload = json.loads(_CALENDAR_CACHE_PATH.read_text(encoding="utf-8"))
        today = pendulum.now("Asia/Seoul").to_date_string()
        if payload.get("date") != today:
            logger.warning("Calendar cache 날짜 불일치: cache=%s today=%s", payload.get("date"), today)
            return []
        events = payload.get("events") or []
        return [
            {
                "time": str(ev.get("time") or "종일"),
                "summary": str(ev.get("summary") or ev.get("title") or "").strip(),
                "status": str(ev.get("status") or "").strip(),
            }
            for ev in events
            if str(ev.get("summary") or ev.get("title") or "").strip()
        ]
    except Exception as e:
        logger.warning("Calendar cache 읽기 실패: %s", e)
        return []


def _collect_notion_calendar_events() -> list[dict]:
    """Notion Calendar 오늘 일정 수집. 실패 시 빈 목록 반환."""
    driver = None
    notion_briefing = None
    try:
        from modules.transform.pipelines.private import private_morning_briefing as notion_briefing

        target_date = pendulum.now("Asia/Seoul").to_date_string()
        driver = notion_briefing._launch_browser()
        events_by_day = notion_briefing._collect_calendar(driver, target_date=target_date)
        events = events_by_day.get("today", []) if isinstance(events_by_day, dict) else []
        return [
            {
                "time": (ev.get("time") or "종일"),
                "summary": (ev.get("title") or "").strip(),
                "status": (ev.get("status") or "").strip(),
            }
            for ev in events
            if (ev.get("title") or "").strip()
        ]
    except Exception as e:
        logger.warning("Notion Calendar 수집 실패: %s", e)
        return []
    finally:
        if driver is not None and notion_briefing is not None:
            try:
                notion_briefing._quit_browser(driver)
            except Exception:
                pass


def _collect_google_calendar_events() -> list[dict]:
    """Google Calendar 오늘 일정 수집. token.json 없으면 빈 목록 반환."""

    token_path = _GIT_ROOT / "config" / "calendar_token.json"
    if not token_path.exists():
        logger.warning("calendar_token.json 없음 — Calendar 수집 건너뜀 (scripts/generate_calendar_token.py 실행 필요)")
        return []
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_authorized_user_file(str(token_path))
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")

        service = build("calendar", "v3", credentials=creds)
        tz = pendulum.timezone("Asia/Seoul")
        today_start = pendulum.today(tz).isoformat()
        today_end = pendulum.tomorrow(tz).isoformat()

        result = service.events().list(
            calendarId="primary",
            timeMin=today_start,
            timeMax=today_end,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        events = []
        for e in result.get("items", []):
            start = e["start"].get("dateTime", e["start"].get("date", ""))
            time_label = (
                pendulum.parse(start).in_timezone(tz).strftime("%H:%M")
                if "T" in start
                else "종일"
            )
            events.append({"time": time_label, "summary": e.get("summary", "(제목없음)")})
        return events
    except Exception as e:
        logger.warning(f"Calendar 수집 실패: {e}")
        return []


def _get_previous_day_window_kst() -> tuple[pendulum.DateTime, pendulum.DateTime, str]:
    target_day = pendulum.now("Asia/Seoul").subtract(days=1)
    start_kst = target_day.start_of("day")
    end_kst = start_kst.add(days=1)
    return start_kst, end_kst, target_day.strftime("%Y-%m-%d")


def _normalize_run_dir_name(name: str) -> str:
    return name.replace("", ":")


def _find_run_log_dir(logs_base: Path, dag_id: str, run_id: str) -> Path | None:
    dag_dir = logs_base / f"dag_id={dag_id}"
    if not dag_dir.exists():
        return None
    expected = f"run_id={run_id}"
    for run_dir in dag_dir.glob("run_id=*"):
        if _normalize_run_dir_name(run_dir.name) == expected:
            return run_dir
    return None


def _find_latest_attempt_log(run_log_dir: Path | None, task_id: str) -> Path | None:
    if run_log_dir is None:
        return None
    task_dir = run_log_dir / f"task_id={task_id}"
    if not task_dir.exists():
        return None
    attempts = sorted(task_dir.glob("attempt=*.log"))
    return attempts[-1] if attempts else None


def _extract_error_details(log_path: Path | None) -> tuple[str, str]:
    if log_path is None or not log_path.exists():
        return "", ""

    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not lines:
        return "", ""

    summary = ""
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if "traceback" in lowered:
            continue
        if "error" in lowered or "exception" in lowered or stripped.startswith("KeyError"):
            summary = stripped[-220:]
            break

    traceback_start = None
    for index in range(len(lines) - 1, -1, -1):
        if "Traceback" in lines[index]:
            traceback_start = index
            break

    excerpt_lines = lines[traceback_start:traceback_start + 40] if traceback_start is not None else lines[-40:]
    excerpt = "\n".join(line.rstrip() for line in excerpt_lines if line.strip()).strip()
    if not summary and excerpt:
        summary = excerpt.splitlines()[-1][-220:]
    return summary[:220], excerpt[:2000]


def _dag_run_sort_key(dag_run) -> pendulum.DateTime:
    return dag_run.end_date or dag_run.start_date or dag_run.execution_date


def _to_pendulum_utc(value) -> pendulum.DateTime | None:
    if value is None:
        return None
    try:
        return pendulum.instance(value).in_timezone("UTC")
    except Exception:
        try:
            return pendulum.parse(str(value)).in_timezone("UTC")
        except Exception:
            return None


def _dag_run_completed_at(dag_run) -> pendulum.DateTime | None:
    return _to_pendulum_utc(getattr(dag_run, "end_date", None) or getattr(dag_run, "start_date", None))


def _dag_run_logical_at(dag_run) -> pendulum.DateTime | None:
    return _to_pendulum_utc(getattr(dag_run, "execution_date", None))


def _is_excluded_briefing_dag(dag_id: str) -> bool:
    lowered = (dag_id or "").lower()
    if dag_id == "Private_MorningBriefing_Dags":
        return True
    return lowered.startswith("tmp_") or lowered.endswith("_test") or "_test_" in lowered


def _is_noise_log_message(message: str) -> bool:
    text = (message or "").lower()
    noise_patterns = [
        "received sigterm",
        "::endgroup:",
        "local_task_job_runner.py",
        "taskinstance.py:3093",
    ]
    return any(pattern in text for pattern in noise_patterns)


def _collect_previous_day_dag_results() -> tuple[list[dict], list[dict]]:
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    import os

    start_kst, end_kst, _target_label = _get_previous_day_window_kst()
    start_utc = start_kst.in_timezone("UTC")
    end_utc = end_kst.in_timezone("UTC")
    logs_base = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
    excluded_dags = {"Private_MorningBriefing_Dags"}

    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= start_utc)
            .filter(DagRun.execution_date < end_utc)
            .all()
        )

        latest_runs: dict[str, object] = {}
        for dag_run in dag_runs:
            if dag_run.dag_id in excluded_dags:
                continue
            sort_key = _dag_run_sort_key(dag_run)
            current = latest_runs.get(dag_run.dag_id)
            if current is None:
                latest_runs[dag_run.dag_id] = dag_run
                continue
            current_key = _dag_run_sort_key(current)
            if sort_key and current_key:
                if sort_key >= current_key:
                    latest_runs[dag_run.dag_id] = dag_run
            elif sort_key and not current_key:
                latest_runs[dag_run.dag_id] = dag_run

        failures: list[dict] = []
        log_errors: list[dict] = []

        for dag_id, dag_run in sorted(latest_runs.items()):
            if str(getattr(dag_run, "state", "")) != "failed":
                continue

            run_id = str(dag_run.run_id)
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_id)
                .filter(TaskInstance.run_id == run_id)
                .all()
            )
            failed_tasks = [ti for ti in task_instances if str(getattr(ti, "state", "")) in {"failed", "upstream_failed"}]
            run_log_dir = _find_run_log_dir(logs_base, dag_id, run_id)

            error_summary = ""
            error_excerpt = ""
            message_entries: list[dict] = []

            for ti in failed_tasks:
                log_path = _find_latest_attempt_log(run_log_dir, str(ti.task_id))
                summary, excerpt = _extract_error_details(log_path)
                if summary and not error_summary:
                    error_summary = summary
                if excerpt and not error_excerpt:
                    error_excerpt = excerpt
                if summary and not _is_noise_log_message(summary):
                    entry = {"msg": summary[:200], "level": "ERROR"}
                    if entry not in message_entries:
                        message_entries.append(entry)

            if not error_summary:
                error_summary = f"?? ?? ?? run: failed task {len(failed_tasks)}?"

            failures.append({
                "dag_id": dag_id,
                "status": "FAIL",
                "fail_type": "dag_failed",
                "error_summary": error_summary,
                "error_excerpt": error_excerpt,
            })

            if message_entries:
                log_errors.append({
                    "dag_id": dag_id,
                    "messages": message_entries[:3],
                })

    if not failures and not log_errors:
        # DB 조회 결과가 없을 때는 기존 dags_monitoring CSV로 fallback
        try:
            import csv
            from modules.transform.utility.paths import MART_DB

            yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
            csv_path = MART_DB / "dags_monitoring" / f"dags_monitoring_{yesterday}.csv"
            if csv_path.exists():
                with csv_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        status = (row.get("status", "") or "").upper()
                        if status not in {"FAIL", "WARN"}:
                            continue
                        excerpt = str(row.get("error_excerpt", ""))
                        failures.append(
                            {
                                "dag_id": str(row.get("dag_id", "")),
                                "status": status,
                                "fail_type": str(row.get("fail_type", "")),
                                "error_summary": str(row.get("error_summary", ""))[:500],
                                "error_excerpt": excerpt[:2000] if excerpt not in ("nan", "None", "") else "",
                            }
                        )
            else:
                logger.warning(f"모니터링 CSV 없음: {csv_path}")
        except Exception:
            logger.warning("dags_monitoring CSV fallback 실패")

    return failures, log_errors


def _collect_previous_day_dag_results_v2() -> tuple[list[dict], list[dict]]:
    """전일 KST 기준 최종 미해결 run만 실패로 판단한다.

    실패 후 더 최신 성공 run이 있거나 retry 최종 상태가 성공이면 운영 적재가
    회복된 것으로 보고 브리핑의 FAIL/로그오류에서 제외한다. 오래된 manual run이
    뒤늦게 종료되어 end_date만 전일에 찍힌 경우도 제외한다.
    """
    from airflow.utils.session import create_session
    import os

    with create_session() as session:
        from airflow.models import DagRun, TaskInstance

        start_kst, end_kst, _ = _get_previous_day_window_kst()
        start_utc = start_kst.in_timezone("UTC")
        # 새벽/아침 브리핑 시 전일 logical date의 스케줄 run이 KST 자정 이후 끝나는 경우가 있어
        # 완료 시각 확인 범위는 다음날 오전까지 허용한다.
        end_utc = end_kst.add(hours=12).in_timezone("UTC")
        logs_base = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))

        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= start_utc)
            .filter(DagRun.execution_date < end_utc)
            .filter(DagRun.state.in_(["success", "failed", "upstream_failed"]))
            .all()
        )

        latest_runs: dict[str, object] = {}
        for dr in dag_runs:
            if _is_excluded_briefing_dag(dr.dag_id):
                continue

            logical_at = _dag_run_logical_at(dr)
            completed_at = _dag_run_completed_at(dr)
            if not logical_at or logical_at < start_utc or logical_at >= end_utc:
                continue

            # 브리핑 시점까지 완료된 run만 최종 판정에 사용한다. 아직 running인 run은
            # 실패로 단정하지 않는다.
            if not completed_at or completed_at < start_utc or completed_at >= end_utc:
                continue

            sort_key = completed_at or logical_at
            current = latest_runs.get(dr.dag_id)
            current_key = _dag_run_completed_at(current) if current is not None else None
            if current is None or (current_key is not None and sort_key >= current_key) or current_key is None:
                latest_runs[dr.dag_id] = dr

        failures: list[dict] = []
        log_errors: list[dict] = []
        for dr in sorted(latest_runs.values(), key=lambda item: item.dag_id):
            if str(getattr(dr, "state", "")) not in {"failed", "upstream_failed"}:
                continue

            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id
            ).all()
            failed_tasks = [ti for ti in task_instances if str(getattr(ti, "state", "")) in {"failed", "upstream_failed"}]
            run_log_dir = _find_run_log_dir(logs_base, dr.dag_id, str(dr.run_id))

            msg_entries: list[dict] = []
            error_summary = ""
            error_excerpt = ""
            for ti in failed_tasks:
                log_path = _find_latest_attempt_log(run_log_dir, str(ti.task_id))
                summary, excerpt = _extract_error_details(log_path)
                if summary:
                    if not error_summary:
                        error_summary = summary
                    if not _is_noise_log_message(summary):
                        msg_entries.append({"msg": summary[:200], "level": "ERROR"})
                if excerpt and not error_excerpt:
                    error_excerpt = excerpt

            if not error_summary:
                error_summary = f"실패 run: failed task {len(failed_tasks)}개"
            if msg_entries:
                log_errors.append({"dag_id": dr.dag_id, "messages": msg_entries[:3]})
            failures.append({
                "dag_id": dr.dag_id,
                "status": "FAIL",
                "fail_type": "dag_failed",
                "error_summary": error_summary,
                "error_excerpt": error_excerpt,
            })

    return failures, log_errors


def _collect_dag_failures() -> list[dict]:
    failures, _ = _collect_previous_day_dag_results_v2()
    return failures


def _collect_git_status() -> dict:
    """git status --short 기준으로 실제 미커밋 변경사항을 수집한다."""
    git_dir = _GIT_ROOT / ".git"
    if not git_dir.exists():
        return {"status": "(git 없음)", "log": "(git 없음)", "unmerged_branches": "(git 없음)", "dirty_count": 0}
    if not shutil.which("git"):
        return {"status": "(git 명령 없음)", "log": "(git 명령 없음)", "unmerged_branches": "(git 명령 없음)", "dirty_count": 0}
    try:
        def _run_git(args: list[str]) -> str:
            result = subprocess.run(
                ["git", *args],
                cwd=str(_GIT_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
            )
            if result.returncode != 0:
                raise RuntimeError((result.stderr or result.stdout or "").strip()[:200])
            return result.stdout.strip()

        branch = _run_git(["branch", "--show-current"]) or _run_git(["rev-parse", "--short", "HEAD"])
        status_text = _run_git(["status", "--short"])
        dirty_lines = [line for line in status_text.splitlines() if line.strip()]
        log_line = _run_git(["log", "-1", "--pretty=format:%h %s"])

        try:
            unmerged_text = _run_git(["branch", "--no-merged", "main", "--format=%(refname:short)"])
            unmerged = [
                line.strip()
                for line in unmerged_text.splitlines()
                if line.strip() and line.strip() != branch
            ]
        except Exception:
            unmerged = []

        return {
            "status": status_text if dirty_lines else "(변경사항 없음)",
            "log": f"최근: {log_line[:80]}" if log_line else "(커밋 없음)",
            "branch": branch,
            "unmerged_branches": "\n".join(unmerged) if unmerged else "(없음)",
            "dirty_count": len(dirty_lines),
        }
    except Exception as e:
        logger.warning(f"git 파싱 실패: {e}")
        return {"status": "(git 파싱 오류)", "log": "(git 파싱 오류)", "unmerged_branches": "(git 파싱 오류)", "dirty_count": 0}


def _collect_data_freshness() -> str:
    """daily_summary.parquet 최신 수정 시각."""
    from modules.transform.utility.paths import MART_DB

    ds_path = MART_DB / "unified_sales_grp" / "daily_summary.parquet"
    if ds_path.exists():
        return datetime.fromtimestamp(ds_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return "파일 없음"


def _collect_log_warnings() -> list[dict]:
    _, log_errors = _collect_previous_day_dag_results_v2()
    return log_errors


def _collect_scheduled_dags() -> list[str]:
    """오늘 활성화된 DAG 목록 (Airflow 메타DB)."""
    try:
        from airflow.models.dag import DagModel
        from airflow.utils.db import create_session

        with create_session() as session:
            rows = (
                session.query(DagModel.dag_id, DagModel.schedule_interval)
                .filter(DagModel.is_paused.is_(False), DagModel.is_active.is_(True))
                .order_by(DagModel.dag_id)
                .all()
            )
        return [
            f"{r.dag_id} ({r.schedule_interval})"
            for r in rows
            if r.dag_id != "Private_MorningBriefing_Dags"
        ]
    except Exception as e:
        logger.warning(f"DAG 목록 수집 실패: {e}")
        return []


def _task_result_label(value: str) -> tuple[str, str]:
    raw = (value or "").strip()
    if raw == "0":
        return "ok", "최근 성공"
    if raw == "267011":
        return "not_run", "아직 실행 전"
    if raw == "267009":
        return "running", "실행 중"
    if raw in {"", "N/A"}:
        return "unknown", "결과 없음"
    return "failed", f"Last Result {raw}"


def _clean_schtasks_time(value: str) -> str:
    raw = (value or "").strip()
    if not raw or raw == "N/A":
        return raw
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    time_match = re.search(r"\d{1,2}:\d{2}:\d{2}", raw)
    if date_match and time_match:
        return f"{date_match.group(0)} {time_match.group(0)}"
    return raw.replace("\ufffd", "").strip()


def _collect_windows_scheduled_tasks() -> list[dict]:
    """Windows 작업 스케줄러 상태 수집. Windows 호스트 접근이 안 되면 확인 불가로 반환."""
    base_rows = []
    for spec in _WINDOWS_SCHEDULED_TASKS:
        task_name = spec["task_name"]
        base_rows.append({
            "label": spec["label"],
            "task_name": task_name,
            "duplicate": spec["duplicate"],
            "status": "unavailable",
            "status_label": "확인 불가",
            "last_run": "",
            "last_result": "",
            "next_run": "",
            "task_to_run": "",
            "error": "",
        })

    try:
        encoding = "mbcs" if os.name == "nt" else None
        result = subprocess.run(
            ["schtasks", "/query", "/fo", "CSV", "/v"],
            capture_output=True,
            text=True,
            encoding=encoding,
            errors="replace",
            timeout=20,
        )
    except FileNotFoundError:
        for row in base_rows:
            row["error"] = "schtasks 없음"
        return base_rows
    except Exception as e:
        for row in base_rows:
            row["error"] = str(e)[:120]
        return base_rows

    if result.returncode != 0:
        error = (result.stderr or result.stdout or "").strip()[:120]
        for row in base_rows:
            row["error"] = error
        return base_rows

    try:
        task_rows = list(csv.DictReader(io.StringIO(result.stdout)))
    except Exception as e:
        for row in base_rows:
            row["error"] = f"CSV 파싱 실패: {str(e)[:100]}"
        return base_rows

    by_name = {}
    for task_row in task_rows:
        raw_name = (task_row.get("TaskName") or "").strip()
        normalized = raw_name.lstrip("\\")
        if normalized:
            by_name[normalized.lower()] = task_row

    rows = []
    for base in base_rows:
        task_row = by_name.get(base["task_name"].lower())
        if not task_row:
            base.update({
                "status": "missing",
                "status_label": "등록 없음",
            })
            rows.append(base)
            continue

        status, status_label = _task_result_label(task_row.get("Last Result", ""))
        base.update({
            "status": status,
            "status_label": status_label,
            "last_run": _clean_schtasks_time(task_row.get("Last Run Time", "")),
            "last_result": task_row.get("Last Result", ""),
            "next_run": _clean_schtasks_time(task_row.get("Next Run Time", "")),
            "task_to_run": task_row.get("Task To Run", "")[:160],
        })
        rows.append(base)

    return rows


def _format_scheduled_task_line(task: dict) -> str:
    if task.get("duplicate"):
        prefix = "⚠️"
    else:
        prefix = {
            "ok": "✅",
            "running": "🟡",
            "not_run": "⚪",
            "missing": "❌",
            "failed": "❌",
            "unavailable": "⚪",
            "unknown": "⚪",
        }.get(task.get("status"), "⚪")

    parts = [f"{prefix} {task['label']}"]
    if task.get("duplicate"):
        parts.append(f"중복 후보({task['task_name']})")
    else:
        parts.append(task.get("status_label") or "상태 없음")

    if task.get("last_run"):
        parts.append(f"최근 {task['last_run']}")
    if task.get("next_run") and task["next_run"] != "N/A":
        parts.append(f"다음 {task['next_run']}")
    if task.get("error") and task.get("status") in {"missing", "unavailable"}:
        parts.append(task["error"])

    return " - ".join(parts)


def _should_include_scheduled_task_summary() -> bool:
    return pendulum.now("Asia/Seoul").day_of_week == 0


# ============================================================
# Task 함수
# ============================================================

def collect_briefing_data(**context):
    """브리핑에 필요한 모든 데이터 수집 후 XCom 저장."""
    calendar = _collect_calendar_events()
    failures, log_warnings = _collect_previous_day_dag_results_v2()
    git = _collect_git_status()
    freshness = _collect_data_freshness()
    scheduled = _collect_scheduled_dags()
    scheduled_tasks = _collect_windows_scheduled_tasks()

    payload = {
        "calendar": calendar,
        "failures": failures,
        "git": git,
        "freshness": freshness,
        "scheduled": scheduled[:10],
        "log_warnings": log_warnings,
        "scheduled_tasks": scheduled_tasks,
    }
    context["ti"].xcom_push(key="briefing_data", value=json.dumps(payload, ensure_ascii=False))
    msg = f"수집 완료 — 일정 {len(calendar)}건 / 실패 {len(failures)}건 / 로그오류 DAG {len(log_warnings)}건"
    logger.info(msg)
    return msg


def _llm_call(prompt: str, system: str, num_predict: int = 300) -> str:
    """Ollama 직접 호출 — num_predict 조절 가능."""
    from modules.transform.utility.qwen_client import get_ollama_client_with_candidates

    client, candidates = get_ollama_client_with_candidates()
    messages = [{"role": "system", "content": system}, {"role": "user", "content": prompt}]
    for model in candidates:
        try:
            resp = client.chat(
                model=model,
                messages=messages,
                stream=False,
                think=False,
                options={"num_predict": num_predict, "temperature": 0},
            )
            content = getattr(getattr(resp, "message", None), "content", None) or resp.get("message", {}).get("content", "")
            if content and content.strip():
                return content.strip()
        except Exception as e:
            logger.warning(f"LLM 실패 ({model}): {e}")
    return "(LLM 응답 없음)"


def _analyze_fail_dag(dag_id: str, error_excerpt: str) -> str:
    """gpt-oss로 FAIL DAG 원인 한 줄 분석."""
    if not error_excerpt:
        return "문제: 최종 실패 run 존재 / 원인: 로그 excerpt 없음 / 조치: Airflow task log 직접 확인"
    system = (
        "You are an Airflow expert. Analyze ONLY unresolved final failed DAG runs. "
        "Reply ONLY in Korean, one line: '문제: X / 원인: Y / 조치: Z'. "
        "Do not recommend Airflow/Python version upgrades unless the log proves that is the direct fix. "
        "Do not treat retry history or already successful reruns as current incidents."
    )
    prompt = f"DAG: {dag_id}\n에러:\n{error_excerpt[:1500]}"
    try:
        return _llm_call(prompt, system, num_predict=150)
    except Exception as e:
        logger.warning(f"LLM 분석 실패 ({dag_id}): {e}")
        return f"분석 실패: {str(e)[:80]}"


def generate_briefing(**context):
    """gpt-oss로 우선순위 브리핑 생성 후 XCom 저장."""
    ti = context["ti"]
    data = json.loads(ti.xcom_pull(task_ids="collect_briefing_data", key="briefing_data"))

    # Step A — 각 FAIL DAG 원인 분석
    fail_lines = []
    for f in data["failures"]:
        if f["error_excerpt"]:
            analysis = _analyze_fail_dag(f["dag_id"], f["error_excerpt"])
            fail_lines.append(f"• {f['dag_id']}: {analysis}")
        else:
            label = f["error_summary"] or f["fail_type"] or f["status"]
            fail_lines.append(f"• {f['dag_id']} [{f['status']}]: {label}")

    # Step B — 전체 우선순위 브리핑 (로그 오류 요약을 LLM에 넘겨 우선순위에 반영)
    cal_text = "\n".join(
        f"  {e['time']} {e['summary']}" + (f" ({e.get('status')})" if e.get("status") else "")
        for e in data["calendar"]
    ) or "  (없음)"
    fail_text = "\n".join(fail_lines) or "  (없음)"
    git = data["git"]

    # 로그 오류: DAG명 + 첫 번째 메시지만 (LLM 컨텍스트용)
    log_ctx = "\n".join(
        f"  {w['dag_id']}: [{w['messages'][0].get('level', '?')}] {w['messages'][0].get('msg', '')[:120]}"
        for w in data.get("log_warnings", [])
    ) or "  (없음)"

    b_prompt = (
        "[오늘 일정]\n"
        f"{cal_text}\n\n"
        "[미해결 실패 DAG]\n"
        f"{fail_text}\n\n"
        "[미해결 DAG와 연결된 로그 오류]\n"
        f"{log_ctx}\n\n"
        "[Git]\n"
        f"미커밋: {git.get('dirty_count', 0)}건 | 상태: {git['status'][:100]} | 최근커밋: {git['log']}\n\n"
        "[데이터 신선도]\n"
        f"daily_summary 최신: {data['freshness']}"
    )
    b_system = (
        "너는 데이터 엔지니어의 아침 업무 비서야. "
        "아래 구조화된 정보를 보고 오늘 가장 먼저 처리해야 할 작업을 번호 목록으로 정리해줘. "
        "무조건 한국어로만 답변해. 영어 금지. "
        "미해결 실패 DAG가 '(없음)'이면 DAG 장애 해결 작업을 우선순위에 넣지 마. "
        "재실행 성공, retry 성공, 오래된 manual 실패, test/tmp DAG, 참고용 로그는 오늘 조치로 쓰지 마. "
        "로그 오류는 미해결 실패 DAG와 직접 연결될 때만 장애 조치로 표현해. "
        "근거 없이 Airflow 또는 Python 버전 업데이트를 권고하지 마. "
        "각 항목은 실제 실행 가능한 동사형 작업으로 작성해. "
        "최대 5줄."
    )
    priority_text = _llm_call(b_prompt, b_system, num_predict=400)

    # 메시지 조합
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d (%a)")
    fail_cnt = sum(1 for f in data["failures"] if f.get("status") == "FAIL")
    warn_cnt = sum(1 for f in data["failures"] if f.get("status") == "WARN")
    log_warnings = data.get("log_warnings", [])
    log_err_cnt = sum(
        1 for w in log_warnings for m in w["messages"] if m.get("level") == "ERROR"
    )
    log_warn_msg_cnt = sum(
        1 for w in log_warnings for m in w["messages"] if m.get("level") != "ERROR"
    )

    sections = []

    # 헤더
    sections.append(
        f"[AI 브리핑] {today}\n"
        f"FAIL {fail_cnt} / WARN {warn_cnt} / 로그오류 {log_err_cnt} / 로그경고 {log_warn_msg_cnt}"
    )

    # 우선순위
    sections.append(f"🎯 오늘 우선순위\n{priority_text}")

    # 일정
    if data["calendar"]:
        cal_lines = "\n".join(
            f"  {e['time']}  {e['summary']}" + (f" ({e.get('status')})" if e.get("status") else "")
            for e in data["calendar"]
        )
        sections.append(f"📅 오늘 일정\n{cal_lines}")

    # 실패/경고 DAG (어제)
    if fail_lines:
        sections.append("❌ 실패/경고 DAG\n" + "\n".join(fail_lines))

    # 로그 오류 DAG — DAG명 + 핵심 메시지 한 줄, 최대 5개
    if log_warnings:
        shown = log_warnings[:5]
        lw_lines = [
            f"  • {w['dag_id']}: [{w['messages'][0].get('level', '?')}] {w['messages'][0].get('msg', '')[:80]}"
            for w in shown
        ]
        total = len(log_warnings)
        extra = f"\n  ...외 {total - 5}건" if total > 5 else ""
        sections.append(f"⚠️ 로그 메시지 ({log_err_cnt}오류/{log_warn_msg_cnt}경고)\n" + "\n".join(lw_lines) + extra)

    # Windows 작업 스케줄러 요약 — 주 1회 월요일만 표시
    scheduled_tasks = data.get("scheduled_tasks") or []
    if scheduled_tasks and _should_include_scheduled_task_summary():
        task_lines = [_format_scheduled_task_line(task) for task in scheduled_tasks]
        sections.append("🗓 작업 스케줄러\n" + "\n".join(task_lines))

    # 미커밋 + 신선도
    dirty_count = int(git.get("dirty_count") or 0)
    if dirty_count > 0:
        sections.append(f"📋 미커밋 {dirty_count}건\n  daily_summary 최신: {data['freshness']}")
    else:
        sections.append(f"✅ 미커밋 없음\n  daily_summary 최신: {data['freshness']}")

    message = "\n\n".join(sections)
    ti.xcom_push(key="briefing_message", value=message)
    logger.info("브리핑 생성 완료")
    return "브리핑 생성 완료"


def send_briefing(**context):
    """Telegram으로 브리핑 전송 (notifier.send_telegram 재사용)."""
    from modules.transform.utility.notifier import send_telegram
    from modules.transform.utility.paths import MART_DB

    ti = context["ti"]
    message = ti.xcom_pull(task_ids="generate_briefing", key="briefing_message")

    if not message:
        logger.warning("브리핑 메시지 없음 — 전송 건너뜀")
        return "전송 건너뜀"

    # 출처 추가
    message_with_source = f"{message}\n\n출처: Private_MorningBriefing_Dags"

    send_telegram(message_with_source)
    logger.info("Telegram 전송 완료")

    today_str = pendulum.now("Asia/Seoul").strftime("%Y%m%d")
    log_dir = MART_DB / "briefing_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"briefing_{today_str}.md"
    log_file.write_text(message, encoding="utf-8")
    logger.info(f"브리핑 로그 저장: {log_file}")

    return "Telegram 전송 완료"
