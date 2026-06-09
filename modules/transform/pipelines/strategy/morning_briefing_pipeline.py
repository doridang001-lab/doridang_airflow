"""
출근길 AI 브리핑 파이프라인

수집:
    - Google Calendar 오늘 일정
    - 어제 FAIL/WARN DAG (모니터링 CSV)
    - git status / branch / log
    - daily_summary.parquet 신선도

생성:
    - gpt-oss로 각 FAIL DAG 원인 분석 (Step A)
    - gpt-oss로 전체 우선순위 브리핑 (Step B)

발송:
    - Telegram (modules.transform.utility.telegram)
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum

logger = logging.getLogger(__name__)

# morning_briefing_pipeline.py는 modules/transform/pipelines/strategy/ 에 위치
# → parent×5 = airflow 프로젝트 루트
_GIT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


# ============================================================
# 수집 헬퍼
# ============================================================

def _collect_calendar_events() -> list[dict]:
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


def _collect_dag_failures() -> list[dict]:
    """어제 dags_monitoring CSV에서 FAIL/WARN DAG 목록 반환."""
    from modules.transform.utility.paths import MART_DB

    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    csv_path = MART_DB / "dags_monitoring" / f"dags_monitoring_{yesterday}.csv"

    if not csv_path.exists():
        logger.warning(f"모니터링 CSV 없음: {csv_path}")
        return []

    import pandas as pd

    df = pd.read_csv(csv_path)
    df = df[df["status"].isin(["FAIL", "WARN"])]

    failures = []
    for _, row in df.iterrows():
        excerpt = str(row.get("error_excerpt", ""))
        failures.append({
            "dag_id": str(row.get("dag_id", "")),
            "status": str(row.get("status", "")),
            "fail_type": str(row.get("fail_type", "")),
            "error_summary": str(row.get("error_summary", ""))[:500],
            "error_excerpt": excerpt[:2000] if excerpt not in ("nan", "None", "") else "",
        })
    return failures


def _collect_git_status() -> dict:
    """.git 폴더 직접 파싱 — git 바이너리/gitpython 없이 브랜치·커밋 수집.
    loose refs(refs/heads/) + packed-refs 모두 처리."""
    git_dir = _GIT_ROOT / ".git"
    if not git_dir.exists():
        return {"status": "(git 없음)", "log": "(git 없음)", "unmerged_branches": "(git 없음)"}
    try:
        # 현재 브랜치
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        current_branch = head.replace("ref: refs/heads/", "") if head.startswith("ref:") else head[:7]

        # 브랜치 해시 수집: loose refs + packed-refs 합산
        branch_hashes: dict[str, str] = {}

        # 1) loose refs
        refs_dir = git_dir / "refs" / "heads"
        if refs_dir.exists():
            for p in refs_dir.rglob("*"):
                if p.is_file():
                    branch_name = str(p.relative_to(refs_dir)).replace("\\", "/")
                    branch_hashes[branch_name] = p.read_text(encoding="utf-8").strip()

        # 2) packed-refs (git pack-refs 이후 loose 파일이 없어지는 경우)
        packed_refs_path = git_dir / "packed-refs"
        if packed_refs_path.exists():
            for line in packed_refs_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                parts = line.split()
                if len(parts) == 2 and parts[1].startswith("refs/heads/"):
                    bname = parts[1][len("refs/heads/"):]
                    if bname not in branch_hashes:  # loose ref 우선
                        branch_hashes[bname] = parts[0]

        main_hash = branch_hashes.get("main", "")
        unmerged = [b for b, h in branch_hashes.items() if b != "main" and h != main_hash]
        unmerged_str = "\n".join(unmerged) if unmerged else "(없음)"

        # 최근 커밋 메시지 (COMMIT_EDITMSG = 마지막 커밋)
        commit_msg_path = git_dir / "COMMIT_EDITMSG"
        last_msg = commit_msg_path.read_text(encoding="utf-8", errors="ignore").strip() if commit_msg_path.exists() else ""
        log = f"최근: {last_msg[:80]}" if last_msg else "(커밋 없음)"

        # 진행 중인 git 작업 확인
        dirty_indicators = ["MERGE_HEAD", "CHERRY_PICK_HEAD", "REBASE_HEAD"]
        in_progress = [f for f in dirty_indicators if (git_dir / f).exists()]
        status = f"브랜치: {current_branch}" + (f" ({', '.join(in_progress)} 진행 중)" if in_progress else "")

        return {"status": status, "log": log, "unmerged_branches": unmerged_str}
    except Exception as e:
        logger.warning(f"git 파싱 실패: {e}")
        return {"status": "(git 파싱 오류)", "log": "(git 파싱 오류)", "unmerged_branches": "(git 파싱 오류)"}


def _collect_data_freshness() -> str:
    """daily_summary.parquet 최신 수정 시각."""
    from modules.transform.utility.paths import MART_DB

    ds_path = MART_DB / "unified_sales_grp" / "daily_summary.parquet"
    if ds_path.exists():
        return datetime.fromtimestamp(ds_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return "파일 없음"


def _collect_log_warnings() -> list[dict]:
    """어제~오늘 태스크 로그에서 WARNING/ERROR 줄 추출, DAG별 대표 메시지 반환.

    로그 구조: /opt/airflow/logs/dag_id=X/run_id=Y/task_id=Z/attempt=N.log
    """
    import os

    logs_base = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
    if not logs_base.exists():
        logger.warning(f"로그 경로 없음: {logs_base}")
        return []

    cutoff = datetime.now() - timedelta(hours=26)  # 어제~오늘
    # 라이브러리 수준 노이즈는 건너뜀
    skip_keywords = [
        "DeprecationWarning", "UserWarning", "FutureWarning",
        "PendingDeprecationWarning", "ResourceWarning",
        "is deprecated", "will be deprecated",
        "InsecureRequestWarning",
    ]
    # 로거 이름 기준 무시 패턴 (Airflow 내부 잡음)
    skip_loggers = [
        "connectionpool", "urllib3", "paramiko", "botocore",
        "google.auth", "httpx",
    ]

    dag_warnings: dict[str, list[str]] = {}

    try:
        for dag_dir in sorted(logs_base.glob("dag_id=*")):
            dag_id = dag_dir.name.replace("dag_id=", "")
            if dag_id == "Strategy_MorningBriefing_Dags":
                continue

            # run_id 폴더를 최신순 정렬, 최근 2개만
            run_dirs = sorted(
                dag_dir.glob("run_id=*"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )[:2]

            for run_dir in run_dirs:
                if datetime.fromtimestamp(run_dir.stat().st_mtime) < cutoff:
                    continue
                for task_dir in run_dir.glob("task_id=*"):
                    for log_file in task_dir.glob("attempt=*.log"):
                        try:
                            text = log_file.read_text(encoding="utf-8", errors="ignore")
                            for line in text.splitlines():
                                is_warn = "} WARNING -" in line
                                is_err = "} ERROR -" in line
                                if not (is_warn or is_err):
                                    continue
                                if any(sk in line for sk in skip_keywords):
                                    continue
                                if any(sl in line for sl in skip_loggers):
                                    continue
                                # "{module} WARNING - 메시지" 에서 메시지만 추출
                                marker = "} WARNING - " if is_warn else "} ERROR - "
                                msg = line.split(marker, 1)[-1].strip()[:200]
                                if not msg:
                                    continue
                                seen = dag_warnings.get(dag_id, [])
                                if msg not in seen:
                                    seen.append(msg)
                                    dag_warnings[dag_id] = seen
                        except Exception:
                            continue
    except Exception as e:
        logger.warning(f"로그 스캔 실패: {e}")
        return []

    return [
        {"dag_id": dag_id, "messages": msgs[:3]}   # DAG당 최대 3줄
        for dag_id, msgs in dag_warnings.items()
    ]


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
            if r.dag_id != "Strategy_MorningBriefing_Dags"
        ]
    except Exception as e:
        logger.warning(f"DAG 목록 수집 실패: {e}")
        return []


# ============================================================
# Task 함수
# ============================================================

def collect_briefing_data(**context):
    """브리핑에 필요한 모든 데이터 수집 후 XCom 저장."""
    calendar = _collect_calendar_events()
    failures = _collect_dag_failures()
    git = _collect_git_status()
    freshness = _collect_data_freshness()
    scheduled = _collect_scheduled_dags()
    log_warnings = _collect_log_warnings()

    payload = {
        "calendar": calendar,
        "failures": failures,
        "git": git,
        "freshness": freshness,
        "scheduled": scheduled[:10],
        "log_warnings": log_warnings,
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
    system = (
        "You are an Airflow expert. Analyze the error and reply ONLY in Korean, "
        "one line: '문제: X / 원인: Y / 조치: Z'."
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
        if f["status"] == "FAIL" and f["error_excerpt"]:
            analysis = _analyze_fail_dag(f["dag_id"], f["error_excerpt"])
            fail_lines.append(f"• {f['dag_id']}: {analysis}")
        else:
            label = f["error_summary"] or f["fail_type"] or f["status"]
            fail_lines.append(f"• {f['dag_id']} [{f['status']}]: {label}")

    # Step B — 전체 우선순위 브리핑 (로그 오류 요약을 LLM에 넘겨 우선순위에 반영)
    cal_text = "\n".join(f"  {e['time']} {e['summary']}" for e in data["calendar"]) or "  (없음)"
    fail_text = "\n".join(fail_lines) or "  (없음)"
    git = data["git"]

    # 로그 오류: DAG명 + 첫 번째 메시지만 (LLM 컨텍스트용)
    log_ctx = "\n".join(
        f"  {w['dag_id']}: {w['messages'][0][:120]}" for w in data.get("log_warnings", [])
    ) or "  (없음)"

    b_prompt = (
        f"오늘 일정:\n{cal_text}\n\n"
        f"실패/경고 DAG (어제):\n{fail_text}\n\n"
        f"로그 오류 DAG:\n{log_ctx}\n\n"
        f"미커밋: {git['status'][:100]} | 최근커밋: {git['log']}\n"
        f"daily_summary 최신: {data['freshness']}"
    )
    b_system = (
        "너는 데이터 엔지니어의 아침 업무 비서야. "
        "아래 정보를 보고 오늘 가장 먼저 처리해야 할 것을 번호 목록으로 정리해줘. "
        "무조건 한국어로만 답변해. 영어 금지. 최대 5줄."
    )
    priority_text = _llm_call(b_prompt, b_system, num_predict=400)

    # 메시지 조합
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d (%a)")
    fail_cnt = sum(1 for f in data["failures"] if f["status"] == "FAIL")
    warn_cnt = sum(1 for f in data["failures"] if f["status"] == "WARN")
    log_warn_cnt = len(data.get("log_warnings", []))

    sections = []

    # 헤더
    sections.append(f"[AI 브리핑] {today}\nFAIL {fail_cnt} / WARN {warn_cnt} / 로그오류 {log_warn_cnt}")

    # 우선순위
    sections.append(f"🎯 오늘 우선순위\n{priority_text}")

    # 일정
    if data["calendar"]:
        cal_lines = "\n".join(f"  {e['time']}  {e['summary']}" for e in data["calendar"])
        sections.append(f"📅 오늘 일정\n{cal_lines}")

    # 실패/경고 DAG (어제)
    if fail_lines:
        sections.append("❌ 실패/경고 DAG\n" + "\n".join(fail_lines))

    # 로그 오류 DAG — DAG명 + 핵심 메시지 한 줄, 최대 5개
    log_warnings = data.get("log_warnings", [])
    if log_warnings:
        shown = log_warnings[:5]
        lw_lines = [f"  • {w['dag_id']}: {w['messages'][0][:80]}" for w in shown]
        extra = f"\n  ...외 {log_warn_cnt - 5}건" if log_warn_cnt > 5 else ""
        sections.append(f"⚠️ 로그 오류 ({log_warn_cnt}건)\n" + "\n".join(lw_lines) + extra)

    # 미커밋 + 신선도
    uncommitted = [l for l in git["status"].split("\n") if l.strip()]
    if uncommitted and git["status"] not in ("(변경사항 없음)", "(git 미지원)", "(git 오류)"):
        sections.append(f"📋 미커밋 {len(uncommitted)}건\n  daily_summary 최신: {data['freshness']}")
    else:
        sections.append(f"✅ 미커밋 없음\n  daily_summary 최신: {data['freshness']}")

    message = "\n\n".join(sections)
    ti.xcom_push(key="briefing_message", value=message)
    logger.info("브리핑 생성 완료")
    return "브리핑 생성 완료"


def send_briefing(**context):
    """Telegram으로 브리핑 전송 (notifier.send_telegram 재사용)."""
    from modules.transform.utility.notifier import send_telegram

    ti = context["ti"]
    message = ti.xcom_pull(task_ids="generate_briefing", key="briefing_message")

    if not message:
        logger.warning("브리핑 메시지 없음 — 전송 건너뜀")
        return "전송 건너뜀"

    send_telegram(message)
    logger.info("Telegram 전송 완료")
    return "Telegram 전송 완료"
