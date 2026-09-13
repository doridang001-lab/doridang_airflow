"""Flow parquet lookup tools for Doridang bot."""

from __future__ import annotations

import inspect
import logging
import re
import time
import threading
from pathlib import Path
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from modules.transform.doridang_bot import conversation
from modules.transform.utility import flow_task_status as status_rules
from modules.transform.utility.paths import FLOW_COMMENT_PARQUET, FLOW_POST_PARQUET, FLOW_PROJECT_PARQUET

logger = logging.getLogger(__name__)

ALLOWED_PROJECTS = {
    "2926716": "[브랜드 전략기획부] 직영점 성장전략(온라인 유입)",
    "2926717": "[브랜드 전략기획부] 브랜드 바이럴",
    "2926713": "[브랜드 전략기획부] 회사 현황판 구축",
}
LEADER_TEAM_MEMBERS = ["조민준", "황유경", "차보령"]

# 사람 기준 3종. auto = 담당자(worker) 우선, 비어 있으면 작성자(author_name)로 대체.
# get_worker_status와 get_team_status가 같은 기준을 쓰지 않으면 같은 사람의 숫자가 어긋난다.
BASIS_AUTO = "auto"
BASIS_AUTHOR = "author"
BASIS_WORKER = "worker"

TOOL_FUNCTIONS: dict[str, Callable[..., Any]] = {}
_CACHE_TTL_SEC = 60
_PARQUET_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_REQUEST_TABLES = ContextVar("flow_request_tables", default=None)
_REQUEST_TODAY = ContextVar("flow_request_today", default=None)
_SNAPSHOT_LOCK = threading.Lock()
_SNAPSHOT_CACHE = None


@contextmanager
def request_snapshot():
    """한 요청의 재조회가 캐시 갱신으로 서로 다른 표를 읽지 않게 고정한다."""
    global _SNAPSHOT_CACHE
    paths = {"projects": FLOW_PROJECT_PARQUET, "posts": FLOW_POST_PARQUET, "comments": FLOW_COMMENT_PARQUET}

    def signature():
        return tuple((str(file), file.stat().st_mtime_ns, file.stat().st_size)
                     for path in paths.values()
                     for file in sorted(Path(path).rglob("*.parquet") if Path(path).is_dir() else [Path(path)]))

    with _SNAPSHOT_LOCK:
        before = signature()
        if _SNAPSHOT_CACHE is not None and _SNAPSHOT_CACHE[0] == before:
            tables = _SNAPSHOT_CACHE[1]
        else:
            for attempt in range(2):
                tables = {key: pd.read_parquet(path) for key, path in paths.items()}
                after = signature()
                if before == after:
                    break
                before = after
            else:
                raise RuntimeError("수집 데이터가 갱신 중입니다. 다시 조회해 주세요.")
            _SNAPSHOT_CACHE = (after, tables)
    token = _REQUEST_TABLES.set(tables)
    today_token = _REQUEST_TODAY.set(datetime.now().strftime("%Y%m%d"))
    try:
        yield
    finally:
        _REQUEST_TABLES.reset(token)
        _REQUEST_TODAY.reset(today_token)


def request_today():
    return _REQUEST_TODAY.get() or datetime.now().strftime("%Y%m%d")


def deadline_state(value, status, today=None):
    raw = str(value or "").strip().replace("-", "")
    if not raw or raw in {"nan", "None", "NaT"}:
        return "기한 미등록"
    try:
        if not re.fullmatch(r"\d{8}", raw):
            raise ValueError(raw)
        datetime.strptime(raw, "%Y%m%d")
    except ValueError:
        return "기한 형식 확인 필요"
    if not status_rules.tracks_due(str(status or "")):
        return "현재 상태는 기한 경과 집계 대상 아님"
    return "기한 경과" if raw < (today or request_today()) else "기한 경과 아님"


def list_projects() -> dict[str, Any]:
    posts = _load_posts()
    comments = _load_comments()
    projects = []
    for project_id, project_name in ALLOWED_PROJECTS.items():
        project_posts = posts[posts["project_id"].eq(project_id)]
        project_comments = comments[comments["project_id"].eq(project_id)]
        projects.append({
            "project_id": project_id,
            "project_name": project_name,
            "project_url": _project_url(project_id),
            "posts": int(len(project_posts)),
            "comments": int(len(project_comments)),
            "message": "수집된 게시글이 없습니다" if project_posts.empty else "",
        })
    return {"projects": projects}


def get_project_status(project_id: str) -> dict[str, Any]:
    denied = _deny_if_not_allowed(project_id)
    if denied:
        return denied

    posts = _project_posts(project_id)
    if posts.empty:
        return _empty_project(project_id)

    tasks = _task_posts(posts)
    author_counts = _value_counts(tasks.get("author_name"))
    worker_counts = _value_counts(tasks.get("worker"))
    overdue = _overdue_posts(posts)
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "project_url": _project_url(project_id),
        "author_counts": author_counts,
        "worker_counts": worker_counts,
        "overdue_count": int(len(overdue)),
        "overdue_posts": _post_records(overdue, limit=20),
        "posts": _post_records(_sort_for_progress(_open_posts(posts)), limit=20),
        "open_count": int(len(_open_posts(posts))),
        **_task_summary(posts),
    }


def search_posts(project_id: str, keyword: str) -> dict[str, Any]:
    denied = _deny_if_not_allowed(project_id)
    if denied:
        return denied
    keyword = (keyword or "").strip()
    if not keyword:
        return {"project_id": project_id, "message": "검색어가 비어 있습니다", "posts": []}

    posts = _project_posts(project_id)
    if posts.empty:
        return _empty_project(project_id)

    title = _string_series(posts["title"])
    content = _string_series(posts["content_text"])
    matches = posts[
        title.str.contains(keyword, case=False, na=False, regex=False)
        | content.str.contains(keyword, case=False, na=False, regex=False)
    ]
    matches = matches.sort_values(["post_date", "post_id"], ascending=[False, False])
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "keyword": keyword,
        "count": int(len(matches)),
        "posts": _post_records(matches, limit=20),
    }


def find_posts(keyword: str) -> dict[str, Any]:
    keyword = (keyword or "").strip()
    if not keyword:
        return {"keyword": keyword, "count": 0, "posts": []}

    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    title = _string_series(allowed["title"])
    content = _string_series(allowed["content_text"])
    exact_title = title.str.strip().eq(keyword)
    contains_title = title.str.contains(keyword, case=False, na=False, regex=False)
    contains_content = content.str.contains(keyword, case=False, na=False, regex=False)
    matches = allowed[exact_title | contains_title | contains_content].copy()
    if matches.empty:
        return {"keyword": keyword, "count": 0, "posts": []}
    matches["_match_rank"] = 2
    matches.loc[contains_title, "_match_rank"] = 1
    matches.loc[exact_title, "_match_rank"] = 0
    matches = matches.sort_values(["_match_rank", "post_date", "post_id"], ascending=[True, False, False])
    return {
        "keyword": keyword,
        "count": int(len(matches)),
        "posts": _post_records(matches, limit=20),
    }


def resolve_post_reference(question: str, worker: str | None = None, project_id: str | None = None) -> list[str]:
    """질문에 명시된 업무 제목을 실제 자료에서 찾는다. 같은 제목은 임의 선택하지 않는다."""
    normalized = re.sub(r"[^\w]", "", question).lower()
    posts = _load_posts()
    posts = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
    if worker:
        posts = posts[_person_mask(posts, worker, BASIS_AUTO)]
    if project_id:
        posts = posts[posts["project_id"].eq(project_id)]
    matches = []
    for row in posts[["post_id", "title"]].to_dict("records"):
        title = re.sub(r"[^\w]", "", str(row.get("title") or "")).lower()
        if len(title) >= 5 and title in normalized:
            matches.append((len(title), str(row["post_id"])))
    longest = max((length for length, _ in matches), default=0)
    return list(dict.fromkeys(post_id for length, post_id in matches if length == longest))


def get_post_thread(post_id: str) -> dict[str, Any]:
    post_id = str(post_id or "").strip()
    posts = _load_posts()
    comments = _load_comments()
    selected = posts[_string_series(posts["post_id"]).eq(post_id)]
    if selected.empty:
        return {"post_id": post_id, "message": "게시글을 찾을 수 없습니다"}

    project_id = str(selected.iloc[0]["project_id"])
    denied = _deny_if_not_allowed(project_id)
    if denied:
        return denied

    parent = str(selected.iloc[0].get("parent_post_id") or "").strip()
    root_id = parent if parent and parent.lower() != "nan" and parent != "0" else post_id
    thread = posts[
        _string_series(posts["project_id"]).eq(project_id)
        & (_string_series(posts["post_id"]).eq(root_id)
        | _string_series(posts["parent_post_id"]).eq(root_id))
    ]
    thread_comments = comments[_string_series(comments["project_id"]).eq(project_id)
                               & _string_series(comments["post_id"]).isin(_string_series(thread["post_id"]))]
    thread = thread.sort_values(["depth", "post_date", "post_id"], ascending=[True, True, True])
    thread_comments = thread_comments.sort_values(["written_at", "comment_id"], ascending=[True, True])
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "root_post_id": root_id,
        "posts": _post_records(thread, limit=100),
        "comments": _comment_records(thread_comments.tail(200), limit=200),
    }


def get_recent_activity(project_id: str, days: int = 7) -> dict[str, Any]:
    denied = _deny_if_not_allowed(project_id)
    if denied:
        return denied
    days = max(1, min(int(days or 7), 90))
    comments = _project_comments(project_id)
    if comments.empty:
        return _empty_project(project_id)

    cutoff = datetime.now() - timedelta(days=days)
    comments = comments.copy()
    comments["_written_dt"] = pd.to_datetime(_string_series(comments["written_at"]), format="%Y%m%d%H%M%S", errors="coerce")
    system = comments[_bool_series(comments["is_system"]) & (comments["_written_dt"] >= cutoff)]
    changes = []
    for _, row in system.sort_values("_written_dt", ascending=False).iterrows():
        change = _parse_status_change(str(row.get("sys_code") or ""), str(row.get("content_text") or ""))
        if not change:
            continue
        changes.append({
            "written_at": _format_dt(row.get("_written_dt")),
            "post_id": str(row.get("post_id") or ""),
            "before": change[0],
            "after": change[1],
            "content_text": str(row.get("content_text") or ""),
        })
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "days": days,
        "status_changes": changes[:50],
        "count": len(changes),
    }


def search_conversation_log(keyword: str) -> dict[str, Any]:
    return {"keyword": keyword, "matches": conversation.search_log(keyword)}


def get_worker_status(worker: str, basis: str = BASIS_AUTO) -> dict[str, Any]:
    worker = _normalize_worker_query(worker)
    if not worker:
        return {"message": "담당자 이름이 비어 있습니다", "posts": []}

    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
    basis = _normalize_basis(basis)
    basis_label = _basis_label(basis)
    matches = allowed[_person_mask(allowed, worker, basis)].copy()
    if matches.empty:
        return {
            "worker": worker,
            "basis": basis,
            "basis_label": basis_label,
            "message": f"허용된 3개 프로젝트의 수집 데이터에서 해당 {basis_label} 기준 게시글을 찾지 못했습니다.",
            "allowed_projects": ALLOWED_PROJECTS,
            "posts": [],
        }

    matches["project_name"] = matches["project_id"].map(ALLOWED_PROJECTS).fillna(matches.get("project_name", ""))
    by_project = []
    for project_id, project_posts in matches.groupby("project_id", sort=False):
        by_project.append({
            "project_id": str(project_id),
            "project_name": ALLOWED_PROJECTS.get(str(project_id), ""),
            "project_url": _project_url(str(project_id)),
            "overdue_count": int(len(_overdue_posts(project_posts))),
            "posts": _post_records(_sort_for_progress(_open_posts(project_posts)), limit=20),
            **_task_summary(project_posts),
        })

    return {
        "worker": worker,
        "basis": basis,
        "basis_label": basis_label,
        "basis_note": _basis_note(basis),
        "overdue_count": int(len(_overdue_posts(matches))),
        "projects": by_project,
        # 진행상황 질문이므로 지금 열려 있는 업무만 보여준다. 완료 건수는 status_counts에 남는다
        "posts": _post_records(_sort_for_progress(_open_posts(matches)), limit=30),
        "open_count": int(len(_open_posts(matches))),
        "record_posts": _post_records(_record_posts(matches), limit=10),
        **_task_summary(matches),
    }


def get_team_status(basis: str = BASIS_AUTO) -> dict[str, Any]:
    basis = _normalize_basis(basis)
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()

    if basis == BASIS_WORKER:
        roster = _discover_person_names(allowed, basis)
    else:
        roster = list(LEADER_TEAM_MEMBERS)

    # get_worker_status와 동일한 마스크를 써야 같은 사람의 숫자가 어긋나지 않는다.
    masks = {name: _person_mask(allowed, name, basis) for name in roster}
    members = [_member_status_record(name, allowed[masks[name]]) for name in roster]

    if basis == BASIS_WORKER:
        ordered = sorted(
            members,
            key=lambda item: (int(item.get("overdue_count", 0)), int(item.get("active_count", 0)), int(item.get("post_count", 0))),
            reverse=True,
        )
        risk_source = allowed
    else:
        ordered = members
        risk_source = allowed[pd.concat(list(masks.values()), axis=1).any(axis=1)] if masks else allowed.head(0)

    return {
        "basis": basis,
        "basis_label": _basis_label(basis),
        "basis_note": _basis_note(basis),
        "member_count": len(ordered),
        **_task_summary(allowed),
        "team_members": [] if basis == BASIS_WORKER else list(LEADER_TEAM_MEMBERS),
        "members": ordered,
        "priority_posts": _post_records(_priority_action_posts(risk_source), limit=20),
        "risk_posts": _post_records(_risk_posts(risk_source), limit=20),
    }


def get_risk_status(project_id: str | None = None, priority_only: bool = False) -> dict[str, Any]:
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    if project_id:
        denied = _deny_if_not_allowed(project_id)
        if denied:
            return denied
        allowed = allowed[allowed["project_id"].eq(str(project_id))]

    risks = _priority_action_posts(allowed) if priority_only else _risk_posts(allowed)
    return {
        "project_id": str(project_id or ""),
        "project_name": ALLOWED_PROJECTS.get(str(project_id or ""), "전체 허용 프로젝트"),
        "project_url": _project_url(str(project_id or "")),
        "priority_only": bool(priority_only),
        "post_count": int(len(risks)),
        "status_counts": _value_counts(risks.get("task_status")) if not risks.empty else {},
        "posts": _post_records(risks, limit=30),
    }


def filter_posts(
    status: str | None = None,
    due: str | None = None,
    project_id: str | None = None,
    worker: str | None = None,
    basis: str = BASIS_AUTO,
) -> dict[str, Any]:
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    if project_id:
        denied = _deny_if_not_allowed(project_id)
        if denied:
            return denied
        allowed = allowed[allowed["project_id"].eq(str(project_id))]

    status = (status or "").strip()
    due = (due or "").strip()
    basis = _normalize_basis(basis)
    matches = allowed
    # "황유경 것 중에 기한 지난 건" 같은 후속 질문을 한 번에 답하기 위한 사람 조건
    worker = _normalize_worker_query(worker or "")
    if worker:
        matches = matches[_person_mask(matches, worker, basis)]
    if status == "미완료":
        matches = _open_posts(matches)
    elif status:
        # 사용자가 "회의록 보여줘"처럼 직접 지정하면 비업무도 조회한다
        matches = matches[_status_series(matches).eq(status)]
    else:
        matches = _task_posts(matches)
    if due == "none":
        due_series = _string_series(matches["end_dt"]).str.strip()
        matches = matches[due_series.eq("")]
    elif due == "overdue":
        matches = _overdue_posts(matches)
    elif re.fullmatch(r"\d{8}", due):
        due_series = _string_series(matches["end_dt"]).str.replace("-", "", regex=False).str.strip()
        matches = matches[due_series.eq(due)]

    matches = _sort_for_progress(matches)
    return {
        "project_id": str(project_id or ""),
        "project_name": ALLOWED_PROJECTS.get(str(project_id or ""), "전체 허용 프로젝트"),
        "project_url": _project_url(str(project_id or "")),
        "status_filter": status,
        "due_filter": due,
        "worker_filter": worker,
        "basis": basis,
        "basis_label": _basis_label(basis),
        "posts": _post_records(matches, limit=50),
        **_task_summary(matches),
    }


def get_topic_status(keyword: str) -> dict[str, Any]:
    keyword = (keyword or "").strip()
    if not keyword:
        return {"message": "검색 주제가 비어 있습니다", "posts": []}

    keywords = _expand_topic_keywords(keyword)
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    haystack = _string_series(allowed["title"]) + "\n" + _string_series(allowed["content_text"]) + "\n" + _string_series(allowed["task_nm"])
    mask = haystack.apply(lambda value: any(term in value for term in keywords))
    matches = allowed[mask].copy()
    if matches.empty:
        return {
            "keyword": keyword,
            "keywords": keywords,
            "message": "허용된 3개 프로젝트의 수집 데이터에서 관련 게시글을 찾지 못했습니다.",
            "posts": [],
        }

    by_project = []
    for project_id, project_posts in matches.groupby("project_id", sort=False):
        by_project.append({
            "project_id": str(project_id),
            "project_name": ALLOWED_PROJECTS.get(str(project_id), ""),
            "project_url": _project_url(str(project_id)),
            "posts": _post_records(_sort_for_progress(_task_posts(project_posts)), limit=20),
            **_task_summary(project_posts),
        })

    return {
        "keyword": keyword,
        "keywords": keywords,
        "projects": by_project,
        "posts": _post_records(_sort_for_progress(_task_posts(matches)), limit=40),
        **_task_summary(matches),
    }


def detect_project_id(text: str) -> str | None:
    text = (text or "").strip()
    for project_id in ALLOWED_PROJECTS:
        if project_id in text:
            return project_id
    aliases = {
        "2926716": ("직영점 성장전략", "온라인 유입", "성장전략"),
        "2926717": ("브랜드 바이럴", "바이럴"),
        "2926713": ("회사 현황판", "현황판 구축", "현황판"),
    }
    for project_id, keywords in aliases.items():
        if any(keyword in text for keyword in keywords):
            return project_id
    return None


def detect_worker_name(text: str) -> str | None:
    text = (text or "").strip()
    if not text:
        return None

    for worker in sorted(_known_worker_names(), key=len, reverse=True):
        if worker and worker in text:
            return worker

    matched = re.search(r"([가-힣]{2,4})\s*(?:PM|pm|실장|팀장|대표|매니저|님)", text)
    if matched:
        return _normalize_worker_query(matched.group(1))
    return None


def detect_author_basis(text: str) -> bool:
    text = (text or "").strip()
    return any(keyword in text for keyword in ["작성자", "쓴 사람", "올린 사람", "등록자", "작성 기준"])


def detect_topic_keyword(text: str) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    topic_markers = ["마케팅", "실적", "성과", "광고", "유입", "체험단", "바이럴", "인스타", "메타", "플레이스"]
    if any(marker in text for marker in topic_markers):
        if any(marker in text for marker in ["마케팅", "실적", "성과"]):
            return "마케팅 실적"
        for marker in topic_markers:
            if marker in text:
                return marker
    return None


def detect_team_status_intent(text: str) -> bool:
    text = (text or "").strip()
    return any(keyword in text for keyword in ["각 팀원", "팀원별", "팀원 별", "담당자별", "누가 뭐", "전체 담당자"])


def detect_risk_intent(text: str) -> bool:
    text = (text or "").strip()
    normalized = text.replace(" ", "")
    if normalized in {"상태", "상태확인", "피드백건확인용", "상태피드백건확인용"}:
        return True
    return any(keyword in text for keyword in ["위험 업무", "리스크", "기한 지난", "기한 경과", "기한 없음", "막힌 업무", "막힌", "보류", "대기", "피드백", "결제중", "결재중"])


def basis_label(basis: str = BASIS_AUTO) -> str:
    """사람 기준 표시 문구. 답변에 어느 기준으로 센 숫자인지 밝히기 위해 쓴다."""
    return _basis_label(basis)


def basis_note(basis: str = BASIS_AUTO) -> str:
    """기준의 정의를 한 줄로 설명한다. 같은 사람의 숫자가 왜 그런지 밝히기 위해 쓴다."""
    return _basis_note(basis)


# Flow 수집은 하루 6회(schedule.SMP_FLOW_COLLECT_TIME)다.
# 하루 넘게 갱신이 없으면 그 프로젝트는 수집이 멈춘 것으로 본다.
STALE_AFTER_DAYS = 1


def data_freshness() -> dict[str, Any]:
    """답변 대상 3개 프로젝트의 수집 신선도.

    전체 파티션(109개 프로젝트)에서 max를 뽑으면 답하지도 않는 프로젝트의
    수집 시각으로 "오늘 데이터"라고 표시하게 된다. 반드시 허용 프로젝트만 본다.
    """
    empty = {"latest": "", "oldest": "", "age_days": None, "stale": False}
    try:
        posts = _load_posts()
        allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
        series = _string_series(allowed.get("collected_at")).str.strip()
        series = series[series != ""]
    except Exception:
        logger.warning("collected_at 조회 실패", exc_info=True)
        return empty
    if series.empty:
        return empty

    latest_raw, oldest_raw = series.max(), series.min()
    age_days = None
    try:
        latest_ts = pd.Timestamp(latest_raw)
        now = datetime.now(tz=latest_ts.tz) if latest_ts.tz is not None else datetime.now()
        age_days = max(0, (now - latest_ts.to_pydatetime()).days)
    except Exception:
        logger.warning("collected_at 파싱 실패: %r", latest_raw)

    return {
        "latest": _safe_format_dt(latest_raw),
        "oldest": _safe_format_dt(oldest_raw),
        "age_days": age_days,
        "stale": bool(age_days is not None and age_days >= STALE_AFTER_DAYS),
    }


def data_as_of() -> str:
    """답변 대상 데이터의 최신 collected_at."""
    return data_freshness()["latest"]


def _safe_format_dt(value: Any) -> str:
    try:
        return _format_dt(value)
    except Exception:
        return str(value or "")


def tool_schemas() -> list[dict[str, Any]]:
    return [
        _schema("list_projects", "허용된 3개 Flow 프로젝트와 게시글/댓글 수를 조회합니다.", {}),
        _schema("get_project_status", "프로젝트의 업무 상태, 담당자, 기한 경과 게시글을 집계합니다.", {
            "project_id": {"type": "string", "description": "허용된 project_id"},
        }, ["project_id"]),
        _schema("search_posts", "프로젝트 게시글 제목과 본문에서 키워드를 검색합니다.", {
            "project_id": {"type": "string"},
            "keyword": {"type": "string"},
        }, ["project_id", "keyword"]),
        _schema("find_posts", "허용된 3개 프로젝트 전체에서 제목/본문으로 게시글을 검색합니다.", {
            "keyword": {"type": "string"},
        }, ["keyword"]),
        _schema("get_post_thread", "게시글과 하위 글, 댓글 스레드를 조회합니다.", {
            "post_id": {"type": "string"},
        }, ["post_id"]),
        _schema("get_recent_activity", "최근 N일간 시스템 댓글의 상태 변경 이력을 조회합니다.", {
            "project_id": {"type": "string"},
            "days": {"type": "integer", "description": "1부터 90 사이 일수"},
        }, ["project_id"]),
        _schema("get_worker_status", "허용된 3개 프로젝트 안에서 사람 이름으로 업무 진행상황을 조회합니다.", {
            "worker": {"type": "string", "description": "담당자 이름. 예: 황유경"},
            "basis": {"type": "string", "description": "auto(기본) | worker | author. auto는 담당자 우선, 없으면 작성자"},
        }, ["worker"]),
        _schema("get_team_status", "허용된 3개 프로젝트 전체에서 팀원별 업무 진행상황을 집계합니다.", {
            "basis": {"type": "string", "description": "auto(기본) | worker | author. get_worker_status와 같은 기준을 쓴다"},
        }),
        _schema("get_risk_status", "허용된 3개 프로젝트에서 위험 업무 또는 피드백/보완 업무를 조회합니다.", {
            "project_id": {"type": "string", "description": "선택 project_id"},
            "priority_only": {"type": "boolean", "description": "true면 피드백/보완만 조회"},
        }),
        _schema("filter_posts", "상태/기한/담당자 조건으로 업무를 필터링합니다. 특정 사람의 지연 업무처럼 조건이 겹칠 때 씁니다.", {
            "status": {"type": "string", "description": "진행 업무: 진행 | 대기 | 보류 | 피드백 | 보완 | 완료. 진행 업무 아님: 모니터링 | 업무단위 | 회의록 | 액션 — 사용자가 직접 물을 때만 쓴다"},
            "due": {"type": "string", "description": "none(기한 없음) | overdue(기한 경과) | YYYYMMDD"},
            "project_id": {"type": "string", "description": "선택 project_id"},
            "worker": {"type": "string", "description": "선택 담당자 이름. 예: 황유경"},
        }),
        _schema("get_topic_status", "허용된 3개 프로젝트 전체에서 주제/키워드 관련 게시글을 검색하고 상태를 집계합니다.", {
            "keyword": {"type": "string", "description": "검색 주제. 예: 마케팅 실적"},
        }, ["keyword"]),
        _schema("search_conversation_log", "이전 대화 log.md에서 키워드를 검색합니다.", {
            "keyword": {"type": "string"},
        }, ["keyword"]),
    ]


def execute_tool(name: str, arguments: dict[str, Any]) -> Any:
    func = TOOL_FUNCTIONS.get(name)
    if not func:
        return {"error": f"알 수 없는 tool입니다: {name}"}
    return func(**_filter_arguments(func, arguments or {}))


def _register() -> None:
    for func in [
        list_projects,
        get_project_status,
        search_posts,
        find_posts,
        get_post_thread,
        get_recent_activity,
        get_worker_status,
        get_team_status,
        get_risk_status,
        filter_posts,
        get_topic_status,
        search_conversation_log,
    ]:
        TOOL_FUNCTIONS[func.__name__] = func


def accepted_arguments(name: str) -> set[str]:
    """tool이 실제로 받는 인자 이름. 라우터가 조건을 잃지 않게 확인하는 데 쓴다."""
    func = TOOL_FUNCTIONS.get(name)
    if not func:
        return set()
    signature = inspect.signature(func)
    return {
        key
        for key, parameter in signature.parameters.items()
        if parameter.kind in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}
    }


def _filter_arguments(func: Callable[..., Any], arguments: dict[str, Any]) -> dict[str, Any]:
    signature = inspect.signature(func)
    allowed = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}
    }
    dropped = sorted(set(arguments) - allowed)
    if dropped:
        # 조용히 버리면 사용자가 요청한 조건이 흔적 없이 사라진다.
        # "진행만 정리해줘"가 status 인자를 잃고 전체를 답하던 버그가 여기서 났다.
        logger.warning("%s가 받지 못하는 인자를 버림: %s", getattr(func, "__name__", func), dropped)
    return {key: value for key, value in arguments.items() if key in allowed}


def _load_projects() -> pd.DataFrame:
    df = _cached_read_parquet("projects", FLOW_PROJECT_PARQUET)
    df["project_id"] = df["project_id"].astype(str)
    return df


def _load_posts() -> pd.DataFrame:
    df = _cached_read_parquet("posts", FLOW_POST_PARQUET)
    df["project_id"] = df["project_id"].astype(str)
    return df


def _load_comments() -> pd.DataFrame:
    df = _cached_read_parquet("comments", FLOW_COMMENT_PARQUET)
    df["project_id"] = df["project_id"].astype(str)
    return df


def _cached_read_parquet(key: str, path: Any) -> pd.DataFrame:
    tables = _REQUEST_TABLES.get()
    if tables is not None and key in tables:
        return tables[key].copy()
    now = time.time()
    cached = _PARQUET_CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL_SEC:
        if tables is not None:
            tables[key] = cached[1].copy()
        return cached[1].copy()
    df = pd.read_parquet(path)
    _PARQUET_CACHE[key] = (now, df)
    if tables is not None:
        tables[key] = df.copy()
    return df.copy()


def _project_posts(project_id: str) -> pd.DataFrame:
    return _load_posts()[lambda df: df["project_id"].eq(str(project_id))]


def _project_comments(project_id: str) -> pd.DataFrame:
    return _load_comments()[lambda df: df["project_id"].eq(str(project_id))]


def _deny_if_not_allowed(project_id: str) -> dict[str, Any] | None:
    project_id = str(project_id or "").strip()
    if project_id in ALLOWED_PROJECTS:
        return None
    return {
        "allowed": False,
        "message": "답변 범위 밖입니다. 허용된 3개 프로젝트만 조회할 수 있습니다.",
        "allowed_projects": ALLOWED_PROJECTS,
    }


def _empty_project(project_id: str) -> dict[str, Any]:
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "project_url": _project_url(project_id),
        "post_count": 0,
        "comment_count": 0,
        "message": "수집된 게시글이 없습니다",
    }


def _project_url(project_id: str) -> str:
    project_id = str(project_id or "").strip()
    if not project_id:
        return ""
    projects = _load_projects()
    if projects.empty or "project_url" not in projects.columns:
        return f"https://flow.team/main.act?projectId={project_id}"
    matched = projects[_string_series(projects["project_id"]).eq(project_id)]
    if matched.empty:
        return f"https://flow.team/main.act?projectId={project_id}"
    return str(matched.iloc[0].get("project_url") or f"https://flow.team/main.act?projectId={project_id}")


def _value_counts(series: pd.Series | None) -> dict[str, int]:
    if series is None:
        return {}
    cleaned = _string_series(series).str.strip().replace("", "(공백)")
    return {str(key): int(value) for key, value in cleaned.value_counts().items()}


def _status_series(posts: pd.DataFrame) -> pd.Series:
    return _string_series(posts.get("task_status")).str.strip()


def _task_mask(posts: pd.DataFrame) -> pd.Series:
    """진행 업무만. 업무단위/회의록/액션은 업무가 아니고, 모니터링은 관찰 항목이라 뺀다."""
    return ~_status_series(posts).isin(status_rules.EXCLUDED_FROM_TASKS)


def _task_posts(posts: pd.DataFrame) -> pd.DataFrame:
    return posts[_task_mask(posts)]


def _record_posts(posts: pd.DataFrame) -> pd.DataFrame:
    return posts[~_task_mask(posts)]


def _open_posts(posts: pd.DataFrame) -> pd.DataFrame:
    """진행상황 목록용. 완료와 집계 제외 상태를 뺀 '지금 열려 있는 업무'."""
    return posts[_status_series(posts).map(status_rules.is_open)]


def _end_sort_key(posts: pd.DataFrame) -> pd.Series:
    """기한 없는 글이 오름차순에서 맨 앞을 차지하는 것을 막는다."""
    end_dt = _string_series(posts.get("end_dt")).str.strip().str.replace("-", "", regex=False)
    return end_dt.replace("", status_rules.NO_DUE_SORT_KEY)


def _sort_for_progress(posts: pd.DataFrame) -> pd.DataFrame:
    """진행상황 보고용 정렬: 열린 업무 먼저, 기한 임박순, 최신순."""
    if posts.empty:
        return posts
    ordered = posts.copy()
    ordered["_open_rank"] = _status_series(ordered).map(lambda value: 0 if status_rules.is_open(value) else 1)
    ordered["_end_sort"] = _end_sort_key(ordered)
    ordered = ordered.sort_values(
        ["_open_rank", "_end_sort", "post_date", "post_id"],
        ascending=[True, True, False, False],
    )
    return ordered.drop(columns=["_open_rank", "_end_sort"])


def _task_summary(posts: pd.DataFrame) -> dict[str, Any]:
    """업무와 기록을 나눠 센다. 기록은 감추지 않고 한 줄로 따로 밝힌다."""
    tasks = _task_posts(posts)
    records = _record_posts(posts)
    return {
        "overdue_task_count": int(len(_overdue_posts(tasks))),
        "overdue_monitoring_count": int(len(_overdue_posts(records))),
        "post_count": int(len(posts)),
        "task_count": int(len(tasks)),
        "record_count": int(len(records)),
        "status_counts": _value_counts(tasks.get("task_status")),
        "record_counts": _value_counts(records.get("task_status")),
    }


def _overdue_posts(posts: pd.DataFrame) -> pd.DataFrame:
    today = request_today()
    end_dt = _string_series(posts["end_dt"]).str.replace("-", "", regex=False)
    status = _string_series(posts["task_status"]).str.strip()
    valid = pd.to_datetime(end_dt, format="%Y%m%d", errors="coerce").notna()
    mask = valid & end_dt.str.match(r"^\d{8}$", na=False) & (end_dt < today) & status.map(status_rules.tracks_due)
    overdue = posts[mask].copy()
    if overdue.empty:
        return overdue
    overdue["_end_sort"] = _end_sort_key(overdue)
    return overdue.sort_values(["_end_sort", "post_id"], ascending=[True, True]).drop(columns=["_end_sort"])


def _risk_posts(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts
    status = _status_series(posts)
    risk_status = status.map(status_rules.is_open) & status.map(status_rules.risk_rank).lt(status_rules.UNRANKED)
    overdue_index = set(_overdue_posts(posts).index)
    overdue = posts.index.to_series().isin(overdue_index)
    risks = posts[risk_status | overdue].copy()
    risks["_risk_rank"] = _status_series(risks).map(status_rules.risk_rank)
    risks["_end_sort"] = _end_sort_key(risks)
    return risks.sort_values(["_risk_rank", "_end_sort", "post_date"], ascending=[True, True, False])


def _priority_action_posts(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts
    status = _status_series(posts)
    priority = posts[status.map(status_rules.is_priority)].copy()
    if priority.empty:
        return priority
    priority["_priority_rank"] = _status_series(priority).map(status_rules.risk_rank)
    priority["_end_sort"] = _end_sort_key(priority)
    return priority.sort_values(["_priority_rank", "_end_sort", "post_date"], ascending=[True, True, False])


def _discover_person_names(posts: pd.DataFrame, basis: str) -> list[str]:
    """기준 컬럼에 등장하는 사람 이름 목록. worker는 쉼표 다중값이라 펼친다."""
    names: list[str] = []
    seen: set[str] = set()
    for value in _person_series(posts, basis):
        for name in str(value).split(","):
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names


def _effective_person_series(posts: pd.DataFrame) -> pd.Series:
    """업무 담당자. worker가 비어 있으면 작성자를 담당자로 본다."""
    worker = _string_series(posts.get("worker")).str.strip()
    author = _string_series(posts.get("author_name")).str.strip()
    return worker.where(worker != "", author)


def _person_series(posts: pd.DataFrame, basis: str) -> pd.Series:
    basis = _normalize_basis(basis)
    if basis == BASIS_AUTHOR:
        return _string_series(posts.get("author_name")).str.strip()
    if basis == BASIS_WORKER:
        return _string_series(posts.get("worker")).str.strip()
    return _effective_person_series(posts)


def _person_mask(posts: pd.DataFrame, name: str, basis: str) -> pd.Series:
    return _person_series(posts, basis).str.contains(name, case=False, na=False, regex=False)


def _member_status_record(worker: str, posts: pd.DataFrame) -> dict[str, Any]:
    if posts.empty:
        return {
            "worker": worker,
            "post_count": 0,
            "task_count": 0,
            "record_count": 0,
            "active_count": 0,
            "status_counts": {},
            "record_counts": {},
            "overdue_count": 0,
            "projects": [],
            "risk_posts": [],
        }
    tasks = _task_posts(posts)
    active_status = _status_series(tasks).map(status_rules.is_open)
    by_project = []
    for project_id, project_posts in posts.groupby("project_id", sort=False):
        summary = _task_summary(project_posts)
        by_project.append({
            "project_id": str(project_id),
            "project_name": ALLOWED_PROJECTS.get(str(project_id), ""),
            "project_url": _project_url(str(project_id)),
            "overdue_count": int(len(_overdue_posts(project_posts))),
            **summary,
        })
    return {
        "worker": worker,
        "active_count": int(active_status.sum()),
        "overdue_count": int(len(_overdue_posts(posts))),
        "projects": by_project,
        "risk_posts": _post_records(_risk_posts(posts), limit=8),
        **_task_summary(posts),
    }


def _normalize_basis(basis: str) -> str:
    value = (basis or "").strip().lower()
    if value in {"author", "author_name", "작성자", "작성자 기준"}:
        return BASIS_AUTHOR
    if value in {"worker", "담당자", "담당", "담당자 기준"}:
        return BASIS_WORKER
    return BASIS_AUTO


def _basis_label(basis: str) -> str:
    basis = _normalize_basis(basis)
    if basis == BASIS_AUTHOR:
        return "작성자"
    if basis == BASIS_WORKER:
        return "담당자"
    return "담당자"


def _basis_note(basis: str) -> str:
    basis = _normalize_basis(basis)
    if basis == BASIS_AUTO:
        return "담당자가 비어 있는 글은 작성자를 담당자로 봅니다."
    if basis == BASIS_AUTHOR:
        return "글을 쓴 사람 기준입니다. 담당자 지정과는 다를 수 있습니다."
    return "Flow에 지정된 담당자만 셉니다. 담당자 미지정 글은 빠집니다."


def _post_records(posts: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    columns = [
        "project_id",
        "project_name",
        "post_id",
        "parent_post_id",
        "depth",
        "title",
        "post_date",
        "author_name",
        "content_text",
        "task_status",
        "progress",
        "worker",
        "start_dt",
        "end_dt",
        "remark_cnt",
        "child_cnt",
        "post_url",
    ]
    return [_clean_record(row) for row in posts.reindex(columns=columns).head(limit).to_dict("records")]


def _comment_records(comments: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    columns = ["comment_id", "post_id", "author_name", "written_at", "content_text", "is_system", "sys_code"]
    return [_clean_record(row) for row in comments.reindex(columns=columns).head(limit).to_dict("records")]


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = ""
        elif hasattr(value, "item"):
            cleaned[key] = value.item()
        else:
            cleaned[key] = value
    return cleaned


def _parse_status_change(sys_code: str, content_text: str) -> tuple[str, str] | None:
    parts = sys_code.split("^^")
    if len(parts) >= 3 and parts[0].startswith("S45"):
        return parts[1].split("@$%", 1)[0].strip("' "), parts[2].split("@$%", 1)[0].strip("' ")
    if "→" in content_text and "상태" in content_text:
        before, rest = content_text.split("→", 1)
        after = rest.split(",", 1)[0]
        return before.strip("' "), after.strip("' ")
    return None


def _string_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("")


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype("boolean").fillna(False)


def _normalize_worker_query(worker: str) -> str:
    text = (worker or "").strip()
    for suffix in ["실장님", "팀장님", "대표님", "매니저님", "실장", "팀장", "대표", "매니저", "님"]:
        text = text.replace(suffix, "")
    return text.strip()


def _known_worker_names() -> set[str]:
    try:
        posts = _load_posts()
    except Exception:
        return set()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
    names: set[str] = set()
    for value in _string_series(allowed["worker"]):
        for name in re.findall(r"[가-힣]{2,4}", value):
            normalized = _normalize_worker_query(name)
            if normalized:
                names.add(normalized)
    return names


def known_person_names() -> set[str]:
    """허용 프로젝트에 실제로 등장하는 사람 이름(담당자 + 작성자 + 리더 팀)."""
    names = _known_worker_names() | set(LEADER_TEAM_MEMBERS)
    try:
        posts = _load_posts()
    except Exception:
        return names
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
    for value in _string_series(allowed.get("author_name")):
        for name in re.findall(r"[가-힣]{2,4}", value):
            normalized = _normalize_worker_query(name)
            if normalized:
                names.add(normalized)
    return names


def is_known_person(name: str) -> bool:
    """LLM이 '승인검증' 같은 말을 사람 이름으로 뽑아내는 것을 걸러낸다."""
    normalized = _normalize_worker_query(name or "")
    return bool(normalized) and normalized in known_person_names()


def snap_worker_name(worker: str) -> str:
    """LLM이 '조민준'을 '조민jun'처럼 망가뜨려도 실제 이름으로 되돌린다.

    한글 조각만 남긴 뒤 알려진 담당자/팀원 이름과 맞춰본다. 못 찾으면 원본을 그대로 둔다.
    """
    text = _normalize_worker_query(worker or "")
    if not text:
        return ""

    known = _known_worker_names() | set(LEADER_TEAM_MEMBERS)
    if text in known:
        return text

    hangul = "".join(re.findall(r"[가-힣]+", text))
    if len(hangul) < 2:
        return text

    exact = [name for name in known if name == hangul]
    if exact:
        return exact[0]
    prefixed = sorted((name for name in known if name.startswith(hangul)), key=len)
    if prefixed:
        logger.info("담당자 이름 보정: %r -> %r", worker, prefixed[0])
        return prefixed[0]
    contained = sorted((name for name in known if hangul in name), key=len)
    if contained:
        logger.info("담당자 이름 보정: %r -> %r", worker, contained[0])
        return contained[0]
    return text


def _expand_topic_keywords(keyword: str) -> list[str]:
    keyword = keyword.strip()
    if any(term in keyword for term in ["마케팅", "실적", "성과"]):
        return [
            "마케팅",
            "실적",
            "성과",
            "광고",
            "검색광고",
            "플레이스",
            "파워링크",
            "체험단",
            "리뷰",
            "인스타",
            "인스타그램",
            "메타",
            "Threads",
            "쓰레드",
            "바이럴",
            "유입",
            "노출",
            "클릭",
            "KPI",
        ]
    return [keyword]


def _format_dt(value: Any) -> str:
    if pd.isna(value):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def _schema(name: str, description: str, properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required or [],
            },
        },
    }


_register()
