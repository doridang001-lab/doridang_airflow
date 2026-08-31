"""Flow parquet lookup tools for Doridang bot."""

from __future__ import annotations

import inspect
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from modules.transform.doridang_bot import conversation
from modules.transform.utility.paths import FLOW_COMMENT_PARQUET, FLOW_POST_PARQUET, FLOW_PROJECT_PARQUET

logger = logging.getLogger(__name__)

ALLOWED_PROJECTS = {
    "2926716": "[브랜드 전략기획부] 직영점 성장전략(온라인 유입)",
    "2926717": "[브랜드 전략기획부] 브랜드 바이럴",
    "2926713": "[브랜드 전략기획부] 회사 현황판 구축",
}
LEADER_TEAM_MEMBERS = ["조민준", "황유경", "차보령"]

TOOL_FUNCTIONS: dict[str, Callable[..., Any]] = {}
_CACHE_TTL_SEC = 60
_PARQUET_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}


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

    status_counts = _value_counts(posts.get("task_status"))
    author_counts = _value_counts(posts.get("author_name"))
    worker_counts = _value_counts(posts.get("worker"))
    overdue = _overdue_posts(posts)
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "project_url": _project_url(project_id),
        "post_count": int(len(posts)),
        "status_counts": status_counts,
        "author_counts": author_counts,
        "worker_counts": worker_counts,
        "overdue_count": int(len(overdue)),
        "overdue_posts": _post_records(overdue, limit=20),
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
        _string_series(posts["post_id"]).eq(root_id)
        | _string_series(posts["parent_post_id"]).eq(root_id)
    ]
    thread_comments = comments[_string_series(comments["post_id"]).isin(_string_series(thread["post_id"]))]
    thread = thread.sort_values(["depth", "post_date", "post_id"], ascending=[True, True, True])
    thread_comments = thread_comments.sort_values(["written_at", "comment_id"], ascending=[True, True])
    return {
        "project_id": project_id,
        "project_name": ALLOWED_PROJECTS[project_id],
        "root_post_id": root_id,
        "posts": _post_records(thread, limit=100),
        "comments": _comment_records(thread_comments, limit=200),
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


def get_worker_status(worker: str, basis: str = "worker") -> dict[str, Any]:
    worker = _normalize_worker_query(worker)
    if not worker:
        return {"message": "담당자 이름이 비어 있습니다", "posts": []}

    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)]
    basis = _normalize_basis(basis)
    person_column = _basis_column(basis)
    if basis == "author":
        matches = allowed[_team_member_mask(allowed, worker)].copy()
        basis_label = "작성자/참여자"
    else:
        worker_text = _string_series(allowed[person_column])
        matches = allowed[worker_text.str.contains(worker, case=False, na=False, regex=False)].copy()
        basis_label = _basis_label(basis)
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
            "post_count": int(len(project_posts)),
            "status_counts": _value_counts(project_posts.get("task_status")),
            "overdue_count": int(len(_overdue_posts(project_posts))),
            "posts": _post_records(
                project_posts.sort_values(["end_dt", "post_date", "post_id"], ascending=[True, False, False]),
                limit=20,
            ),
        })

    return {
        "worker": worker,
        "basis": basis,
        "basis_label": basis_label,
        "post_count": int(len(matches)),
        "status_counts": _value_counts(matches.get("task_status")),
        "overdue_count": int(len(_overdue_posts(matches))),
        "projects": by_project,
        "posts": _post_records(
            matches.sort_values(["end_dt", "post_date", "post_id"], ascending=[True, False, False]),
            limit=30,
        ),
    }


def get_team_status(basis: str = "author") -> dict[str, Any]:
    basis = _normalize_basis(basis)
    person_column = _basis_column(basis)
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    empty_label = "작성자 미지정" if basis == "author" else "담당자 미지정"
    allowed["_person_bucket"] = _string_series(allowed[person_column]).str.strip().replace("", empty_label)

    if basis == "author":
        ordered = [
            _member_status_record(
                name,
                allowed[_team_member_mask(allowed, name)],
            )
            for name in LEADER_TEAM_MEMBERS
        ]
        risk_source = allowed[
            pd.concat([_team_member_mask(allowed, name) for name in LEADER_TEAM_MEMBERS], axis=1).any(axis=1)
        ]
    else:
        members = []
        for person, person_posts in allowed.groupby("_person_bucket", sort=False):
            names = [name.strip() for name in person.split(",") if name.strip()] if "," in person else [person]
            for name in names:
                if name == empty_label:
                    member_posts = allowed[allowed["_person_bucket"].eq(empty_label)]
                else:
                    member_posts = allowed[_string_series(allowed[person_column]).str.contains(name, case=False, na=False, regex=False)]
                if member_posts.empty:
                    continue
                members.append(_member_status_record(name, member_posts))
        deduped = {member["worker"]: member for member in members}
        ordered = sorted(
            deduped.values(),
            key=lambda item: (int(item.get("overdue_count", 0)), int(item.get("active_count", 0)), int(item.get("post_count", 0))),
            reverse=True,
        )
        risk_source = allowed
    return {
        "basis": basis,
        "basis_label": "작성자/참여자" if basis == "author" else _basis_label(basis),
        "post_count": int(len(allowed)),
        "status_counts": _value_counts(allowed.get("task_status")),
        "member_count": len(ordered),
        "team_members": LEADER_TEAM_MEMBERS if basis == "author" else [],
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


def filter_posts(status: str | None = None, due: str | None = None, project_id: str | None = None) -> dict[str, Any]:
    posts = _load_posts()
    allowed = posts[posts["project_id"].isin(ALLOWED_PROJECTS)].copy()
    if project_id:
        denied = _deny_if_not_allowed(project_id)
        if denied:
            return denied
        allowed = allowed[allowed["project_id"].eq(str(project_id))]

    status = (status or "").strip()
    due = (due or "").strip()
    matches = allowed
    if status:
        status_series = _string_series(matches["task_status"]).str.strip()
        matches = matches[status_series.eq(status)]
    if due == "none":
        due_series = _string_series(matches["end_dt"]).str.strip()
        matches = matches[due_series.eq("")]
    elif due == "overdue":
        matches = _overdue_posts(matches)
    elif re.fullmatch(r"\d{8}", due):
        due_series = _string_series(matches["end_dt"]).str.replace("-", "", regex=False).str.strip()
        matches = matches[due_series.eq(due)]

    matches = matches.sort_values(["post_date", "post_id"], ascending=[False, False])
    return {
        "project_id": str(project_id or ""),
        "project_name": ALLOWED_PROJECTS.get(str(project_id or ""), "전체 허용 프로젝트"),
        "project_url": _project_url(str(project_id or "")),
        "status_filter": status,
        "due_filter": due,
        "post_count": int(len(matches)),
        "status_counts": _value_counts(matches.get("task_status")) if not matches.empty else {},
        "posts": _post_records(matches, limit=50),
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
            "post_count": int(len(project_posts)),
            "status_counts": _value_counts(project_posts.get("task_status")),
            "posts": _post_records(
                project_posts.sort_values(["post_date", "post_id"], ascending=[False, False]),
                limit=20,
            ),
        })

    return {
        "keyword": keyword,
        "keywords": keywords,
        "post_count": int(len(matches)),
        "status_counts": _value_counts(matches.get("task_status")),
        "projects": by_project,
        "posts": _post_records(matches.sort_values(["post_date", "post_id"], ascending=[False, False]), limit=40),
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
            "basis": {"type": "string", "description": "worker 또는 author"},
        }, ["worker"]),
        _schema("get_team_status", "허용된 3개 프로젝트 전체에서 팀원별 업무 진행상황을 집계합니다.", {
            "basis": {"type": "string", "description": "worker 또는 author. 기본 author"},
        }),
        _schema("get_risk_status", "허용된 3개 프로젝트에서 위험 업무 또는 피드백/결제중 업무를 조회합니다.", {
            "project_id": {"type": "string", "description": "선택 project_id"},
            "priority_only": {"type": "boolean", "description": "true면 피드백/결제중만 조회"},
        }),
        _schema("filter_posts", "허용된 3개 프로젝트에서 상태와 기한 조건으로 업무를 필터링합니다.", {
            "status": {"type": "string", "description": "예: 보류, 대기, 진행, 완료, 피드백, 결제중"},
            "due": {"type": "string", "description": "none 또는 overdue"},
            "project_id": {"type": "string", "description": "선택 project_id"},
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


def _filter_arguments(func: Callable[..., Any], arguments: dict[str, Any]) -> dict[str, Any]:
    signature = inspect.signature(func)
    allowed = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}
    }
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
    now = time.time()
    cached = _PARQUET_CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL_SEC:
        return cached[1].copy()
    df = pd.read_parquet(path)
    _PARQUET_CACHE[key] = (now, df)
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


def _overdue_posts(posts: pd.DataFrame) -> pd.DataFrame:
    today = datetime.now().strftime("%Y%m%d")
    end_dt = _string_series(posts["end_dt"]).str.replace("-", "", regex=False)
    status = _string_series(posts["task_status"]).str.strip()
    mask = end_dt.str.match(r"^\d{8}$", na=False) & (end_dt < today) & ~status.eq("완료")
    return posts[mask].sort_values(["end_dt", "post_id"], ascending=[True, True])


def _risk_posts(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts
    status = _string_series(posts["task_status"]).str.strip()
    risk_status = status.isin(["결제중", "피드백", "보류", "대기"])
    overdue_index = set(_overdue_posts(posts).index)
    overdue = posts.index.to_series().isin(overdue_index)
    risks = posts[risk_status | overdue].copy()
    risks["_risk_rank"] = status.map({"결제중": 0, "피드백": 1, "보류": 2, "대기": 3}).fillna(4)
    risks["_end_sort"] = _string_series(risks["end_dt"]).replace("", "99999999")
    return risks.sort_values(["_risk_rank", "_end_sort", "post_date"], ascending=[True, True, False])


def _priority_action_posts(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts
    status = _string_series(posts["task_status"]).str.strip()
    priority = posts[status.isin(["결제중", "피드백"])].copy()
    if priority.empty:
        return priority
    priority["_priority_rank"] = status.map({"결제중": 0, "피드백": 1}).fillna(2)
    priority["_end_sort"] = _string_series(priority["end_dt"]).replace("", "99999999")
    return priority.sort_values(["_priority_rank", "_end_sort", "post_date"], ascending=[True, True, False])


def _team_member_mask(posts: pd.DataFrame, name: str) -> pd.Series:
    author = _string_series(posts.get("author_name")).str.contains(name, case=False, na=False, regex=False)
    worker = _string_series(posts.get("worker")).str.contains(name, case=False, na=False, regex=False)
    return author | worker


def _member_status_record(worker: str, posts: pd.DataFrame) -> dict[str, Any]:
    if posts.empty:
        return {
            "worker": worker,
            "post_count": 0,
            "active_count": 0,
            "status_counts": {},
            "overdue_count": 0,
            "projects": [],
            "risk_posts": [],
        }
    active_status = _string_series(posts["task_status"]).str.strip().isin(["진행", "대기", "보류", "피드백", "결제중"])
    by_project = []
    for project_id, project_posts in posts.groupby("project_id", sort=False):
        by_project.append({
            "project_id": str(project_id),
            "project_name": ALLOWED_PROJECTS.get(str(project_id), ""),
            "project_url": _project_url(str(project_id)),
            "post_count": int(len(project_posts)),
            "status_counts": _value_counts(project_posts.get("task_status")),
            "overdue_count": int(len(_overdue_posts(project_posts))),
        })
    return {
        "worker": worker,
        "post_count": int(len(posts)),
        "active_count": int(active_status.sum()),
        "status_counts": _value_counts(posts.get("task_status")),
        "overdue_count": int(len(_overdue_posts(posts))),
        "projects": by_project,
        "risk_posts": _post_records(_risk_posts(posts), limit=8),
    }


def _normalize_basis(basis: str) -> str:
    basis = (basis or "author").strip().lower()
    if basis in {"author", "author_name", "작성자"}:
        return "author"
    return "worker"


def _basis_column(basis: str) -> str:
    return "author_name" if _normalize_basis(basis) == "author" else "worker"


def _basis_label(basis: str) -> str:
    return "작성자" if _normalize_basis(basis) == "author" else "담당자"


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
