"""
Flow store project collection pipeline.

Collects Flow projects, posts, nested task posts, and remarks into parquet
masters partitioned by project for mart consumption.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote, urlencode, urlparse

import pandas as pd
import pendulum
import requests
from bs4 import BeautifulSoup

from modules.transform.utility.paths import (
    FLOW_ATTACHMENT_FILES_DIR,
    FLOW_ATTACHMENT_PARQUET,
    FLOW_COMMENT_PARQUET,
    FLOW_LEGACY_COMMENT_PARQUET,
    FLOW_LEGACY_POST_PARQUET,
    FLOW_LEGACY_PROJECT_PARQUET,
    FLOW_POST_PARQUET,
    FLOW_PROJECT_PARQUET,
    FLOW_STATE_JSON,
)

logger = logging.getLogger(__name__)

MAX_DEPTH = 3
REQUEST_TIMEOUT = 30
REQUEST_RETRY_COUNT = 5
REQUEST_INTERVAL_SECONDS = float(os.getenv("FLOW_REQUEST_INTERVAL_SECONDS", "0.6"))
DETAIL_MAX_POSTS = int(os.getenv("FLOW_DETAIL_MAX_POSTS", "50"))
DETAIL_MAX_CALLS = int(os.getenv("FLOW_DETAIL_MAX_CALLS", "0"))
DETAIL_PROGRESS_EVERY = int(os.getenv("FLOW_DETAIL_PROGRESS_EVERY", "50"))
FLOW_API_BASE = "https://api.flow.team"
_LAST_REQUEST_AT = 0.0

_TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}


def _env_or_default(name: str, default: str) -> str:
    return (os.getenv(name) or default).strip()


FLOW_PROJECTS_API_URL = _env_or_default("FLOW_PROJECTS_API_URL", f"{FLOW_API_BASE}/user/projects")
FLOW_POSTS_API_URL_TEMPLATE = _env_or_default(
    "FLOW_POSTS_API_URL_TEMPLATE",
    f"{FLOW_API_BASE}/user/posts/projects/{{project_id}}",
)
FLOW_POST_DETAIL_API_URL_TEMPLATE = _env_or_default(
    "FLOW_POST_DETAIL_API_URL_TEMPLATE",
    f"{FLOW_API_BASE}/user/posts/{{post_id}}",
)
FLOW_REMARKS_API_URL_TEMPLATE = _env_or_default(
    "FLOW_REMARKS_API_URL_TEMPLATE",
    f"{FLOW_API_BASE}/user/comments/{{post_id}}",
)
FLOW_REPLIES_API_URL_TEMPLATE = _env_or_default(
    "FLOW_REPLIES_API_URL_TEMPLATE",
    f"{FLOW_API_BASE}/user/comments/{{post_id}}/replies/{{comment_id}}",
)
FLOW_REMARKS_API_METHOD = _env_or_default("FLOW_REMARKS_API_METHOD", "GET").upper()
FLOW_REMARKS_API_BODY_TEMPLATE = os.getenv("FLOW_REMARKS_API_BODY_TEMPLATE", "").strip()
FLOW_REMARKS_API_BODY_FORMAT = _env_or_default("FLOW_REMARKS_API_BODY_FORMAT", "json")
FLOW_REMARKS_API_HEADERS_JSON = os.getenv("FLOW_REMARKS_API_HEADERS_JSON", "").strip()
FLOW_COMMENT_GAP_FAIL_ON_MISMATCH = os.getenv("FLOW_COMMENT_GAP_FAIL_ON_MISMATCH", "")
FLOW_AUTH_HEADER_NAME = _env_or_default("FLOW_AUTH_HEADER_NAME", "x-flow-api-key")
FLOW_AUTH_HEADER_PREFIX = os.getenv("FLOW_AUTH_HEADER_PREFIX", "").strip()
FLOW_API_KEY = os.getenv("FLOW_API_KEY", "").strip()
FLOW_ATTACHMENT_PROJECT_IDS_ENV = "FLOW_ATTACHMENT_PROJECT_IDS"
FLOW_REPAIR_PROJECT_IDS_ENV = "FLOW_REPAIR_PROJECT_IDS"
FLOW_REPAIR_POST_IDS_ENV = "FLOW_REPAIR_POST_IDS"

_PROJECT_COLS = [
    "project_id",
    "project_name",
    "project_url",
    "is_store",
    "region",
    "store_name",
    "status_tag",
    "collected_at",
]

_POST_COLS = [
    "post_id",
    "project_id",
    "project_name",
    "store_name",
    "parent_post_id",
    "depth",
    "title",
    "post_date",
    "registered_at",
    "edited_at",
    "author_name",
    "author_id",
    "content_text",
    "post_type",
    "task_nm",
    "task_status",
    "progress",
    "worker",
    "start_dt",
    "end_dt",
    "remark_cnt",
    "child_cnt",
    "post_url",
    "image_cnt",
    "attach_cnt",
    "content_hash",
    "collected_at",
]

_COMMENT_COLS = [
    "comment_id",
    "post_id",
    "project_id",
    "author_name",
    "author_id",
    "written_at",
    "content_text",
    "mention_names",
    "is_system",
    "sys_code",
    "reply_cnt",
    "parent_comment_id",
    "root_comment_id",
    "comment_depth",
    "comment_order",
    "collected_at",
]

_ATTACHMENT_COLS = [
    "attachment_id",
    "post_id",
    "project_id",
    "attachment_type",
    "file_name",
    "file_size",
    "extension",
    "download_url",
    "thumbnail_url",
    "local_path",
    "downloaded",
    "download_error",
    "content_hash",
    "width",
    "height",
    "registered_at",
    "author_name",
    "collected_at",
]

_NON_STORE_MARKERS = [
    "TF_",
    "업무 ",
    "업무_",
    "브랜드",
    "전략기획",
    "현황판",
    "프로세스",
    "마케팅팀",
    "알림 BOT",
]


def _now_iso() -> str:
    return pendulum.now("Asia/Seoul").to_iso8601_string()


def _require_api_config() -> None:
    missing = [
        name
        for name, value in [
            ("FLOW_API_KEY", FLOW_API_KEY),
        ]
        if not value
    ]
    if missing:
        raise RuntimeError(f"Flow API 설정 누락: {', '.join(missing)}")


def _headers(extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    _require_api_config()
    auth_value = FLOW_API_KEY
    if FLOW_AUTH_HEADER_PREFIX:
        auth_value = f"{FLOW_AUTH_HEADER_PREFIX} {FLOW_API_KEY}"
    headers = {
        FLOW_AUTH_HEADER_NAME: auth_value,
        "Accept": "application/json",
        "Accept-Language": "ko-KR",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    if extra_headers:
        headers.update({str(key): str(value) for key, value in extra_headers.items()})
    return headers


def _request_json(
    url: str,
    method: str = "GET",
    body: Any = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Any:
    global _LAST_REQUEST_AT

    method = (method or "GET").upper()
    for attempt in range(REQUEST_RETRY_COUNT):
        elapsed = time.monotonic() - _LAST_REQUEST_AT
        if elapsed < REQUEST_INTERVAL_SECONDS:
            time.sleep(REQUEST_INTERVAL_SECONDS - elapsed)

        headers = _headers(extra_headers)
        if method == "POST":
            resp = requests.post(url, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
        else:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        _LAST_REQUEST_AT = time.monotonic()

        if resp.status_code == 429 and attempt < REQUEST_RETRY_COUNT - 1:
            wait_seconds = max(5.0, REQUEST_INTERVAL_SECONDS * (attempt + 2) * 10)
            logger.warning("Flow API 429 rate limit: %.1f초 대기 후 재시도", wait_seconds)
            time.sleep(wait_seconds)
            continue

        if not resp.ok:
            logger.warning("Flow API %s: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()
        return resp.json()

    raise RuntimeError("Flow API 요청 재시도 한도 초과")


def _unwrap_response_data(payload: Any) -> Any:
    if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        payload = payload[0]
    if not isinstance(payload, dict) or "response" not in payload:
        return payload

    response = payload.get("response") or {}
    success = response.get("success")
    if success is False:
        code = response.get("code", "")
        message = response.get("message", "")
        raise RuntimeError(f"Flow API 실패: {code} {message}".strip())
    return response.get("data")


def _format_api_url(template: str, **values: Any) -> str:
    safe_values = {key: "" if value is None else value for key, value in values.items()}
    return template.format(**safe_values)


def _append_query(url: str, params: Dict[str, Any]) -> str:
    clean_params = {key: value for key, value in params.items() if value not in (None, "")}
    if not clean_params:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode(clean_params)}"


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _dag_conf(context: Dict[str, Any]) -> Dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    return conf if isinstance(conf, dict) else {}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _as_text(value).strip().lower() in _TRUE_VALUES


def _int_from_conf(conf: Dict[str, Any], key: str, default: int) -> int:
    value = conf.get(key)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Flow DAG conf 정수값 오류: {key}={value!r}") from exc


def _detail_limits_from_context(context: Dict[str, Any]) -> Tuple[int, int]:
    conf = _dag_conf(context)
    full_backfill = _as_bool(conf.get("full_backfill")) or _as_bool(conf.get("flow_full_backfill"))
    if full_backfill:
        return 0, 0
    return (
        _int_from_conf(conf, "flow_detail_max_posts", DETAIL_MAX_POSTS),
        _int_from_conf(conf, "flow_detail_max_calls", DETAIL_MAX_CALLS),
    )


def _comment_gap_fail_on_mismatch(context: Dict[str, Any]) -> bool:
    conf = _dag_conf(context)
    if "flow_comment_gap_fail_on_mismatch" in conf:
        return _as_bool(conf.get("flow_comment_gap_fail_on_mismatch"))
    return _as_bool(FLOW_COMMENT_GAP_FAIL_ON_MISMATCH)


def _flow_remarks_api_config(context: Optional[Dict[str, Any]]) -> Dict[str, str]:
    conf = _dag_conf(context or {})
    def pick(conf_key: str, variable_name: str, default: str) -> str:
        return _as_text(conf.get(conf_key)) or _airflow_variable(variable_name) or default

    return {
        "url_template": pick("flow_remarks_api_url_template", "FLOW_REMARKS_API_URL_TEMPLATE", FLOW_REMARKS_API_URL_TEMPLATE),
        "method": (pick("flow_remarks_api_method", "FLOW_REMARKS_API_METHOD", FLOW_REMARKS_API_METHOD) or "GET").upper(),
        "body_template": pick("flow_remarks_api_body_template", "FLOW_REMARKS_API_BODY_TEMPLATE", FLOW_REMARKS_API_BODY_TEMPLATE),
        "body_format": pick("flow_remarks_api_body_format", "FLOW_REMARKS_API_BODY_FORMAT", FLOW_REMARKS_API_BODY_FORMAT) or "json",
        "headers_json": pick("flow_remarks_api_headers_json", "FLOW_REMARKS_API_HEADERS_JSON", FLOW_REMARKS_API_HEADERS_JSON),
        "replies_url_template": pick("flow_replies_api_url_template", "FLOW_REPLIES_API_URL_TEMPLATE", FLOW_REPLIES_API_URL_TEMPLATE),
    }


def _parse_project_ids(value: Any) -> set[str]:
    if value in (None, ""):
        return set()
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = re.split(r"[,;\s]+", _as_text(value))
    return {_as_text(item).strip() for item in raw_values if _as_text(item).strip()}


def _airflow_variable(name: str) -> str:
    try:
        from airflow.models import Variable

        return _as_text(Variable.get(name, default_var=""))
    except Exception:
        return ""


def _attachment_project_ids_from_context(context: Dict[str, Any]) -> set[str]:
    conf = _dag_conf(context)
    project_ids = set()
    project_ids.update(_parse_project_ids(os.getenv(FLOW_ATTACHMENT_PROJECT_IDS_ENV, "")))
    project_ids.update(_parse_project_ids(_airflow_variable(FLOW_ATTACHMENT_PROJECT_IDS_ENV)))
    project_ids.update(_parse_project_ids(conf.get("flow_attachment_project_ids")))
    project_ids.update(_parse_project_ids(conf.get("attachment_project_ids")))
    return project_ids


def _repair_project_ids_from_context(context: Dict[str, Any]) -> set[str]:
    conf = _dag_conf(context)
    project_ids = set()
    project_ids.update(_parse_project_ids(os.getenv(FLOW_REPAIR_PROJECT_IDS_ENV, "")))
    project_ids.update(_parse_project_ids(_airflow_variable(FLOW_REPAIR_PROJECT_IDS_ENV)))
    project_ids.update(_parse_project_ids(conf.get("flow_repair_project_ids")))
    project_ids.update(_parse_project_ids(conf.get("repair_project_ids")))
    return project_ids


def _repair_post_ids_from_context(context: Dict[str, Any]) -> set[str]:
    conf = _dag_conf(context)
    post_ids = set()
    post_ids.update(_parse_project_ids(os.getenv(FLOW_REPAIR_POST_IDS_ENV, "")))
    post_ids.update(_parse_project_ids(_airflow_variable(FLOW_REPAIR_POST_IDS_ENV)))
    post_ids.update(_parse_project_ids(conf.get("flow_repair_post_ids")))
    post_ids.update(_parse_project_ids(conf.get("repair_post_ids")))
    return post_ids


def _matches_repair_post(project_id: Any, post_id: Any, repair_post_ids: set[str]) -> bool:
    pid = _as_text(project_id)
    post = _as_text(post_id)
    return post in repair_post_ids or f"{pid}:{post}" in repair_post_ids


def _first(data: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return _as_text(value)
    return ""


def _clean_nbsp(value: str) -> str:
    return _as_text(value).replace("\xa0", " ").strip()


def _clean_html_text(value: str) -> str:
    soup = BeautifulSoup(_as_text(value), "html.parser")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _extract_mentions(value: str) -> str:
    soup = BeautifulSoup(_as_text(value), "html.parser")
    names: List[str] = []
    for anchor in soup.find_all("a"):
        onclick = _as_text(anchor.get("onClick") or anchor.get("onclick"))
        if "fn_profile" not in onclick and not anchor.get("profile-data"):
            continue
        name = _clean_html_text(anchor.get_text())
        if name and name not in names:
            names.append(name)
    return ",".join(names)


def _date_from_yyyymmddhhmmss(value: str) -> str:
    text = _as_text(value)
    if len(text) < 8 or not text[:8].isdigit():
        return ""
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _sha256_text(value: str) -> str:
    return hashlib.sha256(_as_text(value).encode("utf-8")).hexdigest()


def _get_task_column_value(task: Dict[str, Any], column_type: str, key: str) -> str:
    for col in task.get("TASK_COLUMN_REC") or []:
        if col.get("COLUMN_TYPE") != column_type:
            continue
        records = col.get("COLUMN_DATA_REC") or []
        if records:
            return _as_text(records[0].get(key))
    return ""


_TASK_STATUS_CODE_LABELS = {
    "0": "요청",
    "1": "진행",
    "2": "완료",
    "3": "보류",
}


def _task_status_from_code(value: Any) -> str:
    code = _as_text(value).strip()
    return _TASK_STATUS_CODE_LABELS.get(code, code)


def _get_task_column_status(task: Dict[str, Any]) -> str:
    label = (
        _get_task_column_value(task, "STATUS", "OPTION_NAME").strip()
        or _get_task_column_value(task, "STTS", "OPTION_NAME").strip()
    )
    if label:
        return label
    code = (
        _get_task_column_value(task, "STTS", "CUSTOM_COLUMN_DATA").strip()
        or _get_task_column_value(task, "STTS", "OPTION_CATEGORY").strip()
        or _as_text(task.get("STTS")).strip()
    )
    return _task_status_from_code(code) if code else ""


def _first_nested_task_value(task: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = task.get(key)
        if isinstance(value, dict):
            nested = _first(value, "OPTION_NAME", "optionName", "name", "label", "value")
            if nested:
                return nested.strip()
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    nested = _first(item, "OPTION_NAME", "optionName", "name", "label", "value")
                    if nested:
                        return nested.strip()
                elif _as_text(item).strip():
                    return _as_text(item).strip()
        elif _as_text(value).strip():
            return _as_text(value).strip()
    return ""


def _extract_task_status(task: Dict[str, Any]) -> str:
    return (
        _get_task_column_status(task)
        or _first_nested_task_value(
            task,
            "TASK_STATUS",
            "taskStatus",
            "STATUS",
            "status",
            "STTS",
            "stts",
            "STATUS_NM",
            "statusName",
            "PROGRESS_STATUS",
            "progressStatus",
            "WORK_STATUS",
            "workStatus",
        )
    )


def _extract_task_progress(task: Dict[str, Any]) -> str:
    return _first_nested_task_value(
        task,
        "PROGRESS",
        "progress",
        "PROGRESS_RATE",
        "progressRate",
        "PERCENT",
        "percent",
    )


def _extract_task_start_dt(task: Dict[str, Any]) -> str:
    return _first_nested_task_value(task, "START_DT", "startDt", "startDate", "START_DATE")


def _extract_task_end_dt(task: Dict[str, Any]) -> str:
    return _first_nested_task_value(task, "END_DT", "endDt", "endDate", "END_DATE", "DUE_DT", "dueDate")


def _extract_worker(task: Dict[str, Any]) -> str:
    names: List[str] = []
    for worker in task.get("WORKER_REC") or []:
        name = _first(worker, "USER_NM", "RGSR_NM", "WORKER_NM", "FLNM")
        if name and name not in names:
            names.append(name)
    return ",".join(names)


def _current_task_meta(data: Dict[str, Any], fallback: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if fallback:
        return fallback
    tasks = data.get("tasks") or []
    if tasks and isinstance(tasks[0], dict):
        return tasks[0]
    return {}


def _iter_child_tasks(data: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for collection_name in ("tasks", "subTasks"):
        for task in data.get(collection_name) or []:
            if isinstance(task, dict) and task.get("COLABO_COMMT_SRNO"):
                yield task


_TRAILING_PAREN_RE = re.compile(r"\s*\([^()]*\)\s*$")


def _strip_trailing_parens(value: str) -> str:
    """끝에 연속으로 붙은 괄호 접미사(예: "평택비전점(양수)", "시흥장현점(양수)(68호점)")를 전부 제거한다."""
    text = value or ""
    while True:
        stripped = _TRAILING_PAREN_RE.sub("", text)
        if stripped == text:
            return text.strip()
        text = stripped


def _infer_project_parts(project_name: str) -> Tuple[bool, str, str, str]:
    name = _clean_nbsp(project_name)
    status_tag = ""
    work_name = name
    for prefix in ("폐업_", "양도_", "TF_"):
        if work_name.startswith(prefix):
            status_tag = prefix[:-1]
            work_name = work_name[len(prefix):]
            break

    if "_" in work_name:
        region, store_name = work_name.split("_", 1)
    elif " - " in work_name:
        region, store_name = work_name.split(" - ", 1)
    else:
        region, store_name = "", work_name

    is_non_store = (
        status_tag == "TF"
        or name.startswith("[")
        or name.startswith("(")
        or any(marker in name for marker in _NON_STORE_MARKERS)
    )
    store_like = bool(store_name) and (store_name.endswith("점") or bool(region))
    is_store = bool(store_like and not is_non_store)
    return is_store, region.strip(), _strip_trailing_parens(store_name.strip()), status_tag


def _normalize_project(project: Dict[str, Any], collected_at: str) -> Dict[str, Any]:
    project_id = _first(project, "projectId", "project_id")
    project_name = _first(project, "title", "project_name", "projectTitle")
    is_store, region, store_name, status_tag = _infer_project_parts(project_name)
    return {
        "project_id": project_id,
        "project_name": project_name,
        "project_url": _first(project, "projectUrl", "project_url"),
        "is_store": is_store,
        "region": region,
        "store_name": store_name,
        "status_tag": status_tag,
        "collected_at": collected_at,
    }


def _infer_post_type(title: str, task_nm: str) -> str:
    if "방문일지" in title:
        return "방문일지"
    if "매장 정보" in title or title == "매장정보":
        return "매장정보"
    if task_nm:
        return "업무"
    return "기타"


def _normalize_post(
    data: Dict[str, Any],
    project_meta: Dict[str, Any],
    parent_post_id: str,
    depth: int,
    task_meta: Optional[Dict[str, Any]],
    collected_at: str,
) -> Dict[str, Any]:
    task = {**data, **_current_task_meta(data, task_meta)}
    content_text = _as_text(data.get("outContent"))
    title = _first(data, "title", "TASK_NM") or _first(task, "TASK_NM")
    registered_at = _first(data, "registeredDateTime", "RGSN_DTTM")
    edited_at = _first(data, "editedDateTime", "EDTR_DTTM", "registeredDateTime")
    task_nm = _first(task, "TASK_NM")
    child_cnt = _first(data, "subTaskCount") or _first(task, "SUB_TASK_CNT")
    if not child_cnt:
        child_cnt = _as_text(len(data.get("subTasks") or []))

    return {
        "post_id": _first(data, "postId", "COLABO_COMMT_SRNO") or _first(task, "COLABO_COMMT_SRNO"),
        "project_id": _first(data, "projectId", "COLABO_SRNO") or project_meta.get("project_id", ""),
        "project_name": project_meta.get("project_name", ""),
        "store_name": project_meta.get("store_name", ""),
        "parent_post_id": parent_post_id,
        "depth": depth,
        "title": title,
        "post_date": _date_from_yyyymmddhhmmss(registered_at),
        "registered_at": registered_at,
        "edited_at": edited_at,
        "author_name": _first(data, "registerName", "RGSR_NM"),
        "author_id": _first(data, "registerId", "RGSR_ID"),
        "content_text": content_text,
        "post_type": _infer_post_type(title, task_nm),
        "task_nm": task_nm,
        "task_status": _extract_task_status(task),
        "progress": _extract_task_progress(task),
        "worker": _extract_worker(task),
        "start_dt": _extract_task_start_dt(task),
        "end_dt": _extract_task_end_dt(task),
        "remark_cnt": _first(data, "remarkCount") or _first(task, "REMARK_CNT") or _as_text(len(data.get("remarks") or [])),
        "child_cnt": child_cnt,
        "post_url": _first(data, "connectUrl"),
        "image_cnt": len(data.get("imageAttachments") or []),
        "attach_cnt": len(data.get("attachments") or []),
        "content_hash": _sha256_text(content_text),
        "collected_at": collected_at,
    }


_COMMENT_REPLY_LIST_KEYS = (
    "replyRemarks",
    "replies",
    "subRemarks",
    "REPLY_REMARK_REC",
    "SUB_REMARK_REC",
    "REMARK_REPLY_REC",
)
_COMMENT_PARENT_ID_KEYS = (
    "PARENT_COLABO_REMARK_SRNO",
    "PARENT_REMARK_SRNO",
    "UPPER_COLABO_REMARK_SRNO",
    "UPPER_REMARK_SRNO",
    "parentRemarkId",
    "parentCommentId",
    "parent_id",
)
_COMMENT_ROOT_ID_KEYS = (
    "ROOT_COLABO_REMARK_SRNO",
    "ROOT_REMARK_SRNO",
    "rootRemarkId",
    "rootCommentId",
    "root_id",
)
_COMMENT_ORDER_KEYS = (
    "SORT_ORDR",
    "SORT_ORDER",
    "REMARK_ORDR",
    "ORDER_NO",
    "orderNo",
    "sortOrder",
)
_COMMENT_DEPTH_KEYS = (
    "DEPTH",
    "REMARK_DEPTH",
    "commentDepth",
    "depth",
)


def _normalize_comment(
    comment: Dict[str, Any],
    fallback_post_id: str,
    collected_at: str,
    parent_comment_id: str = "",
    root_comment_id: str = "",
    comment_depth: int = 0,
    comment_order: int = 0,
) -> Dict[str, Any]:
    content = _as_text(comment.get("CNTN") or comment.get("REMARK_CNTN") or comment.get("contents"))
    comment_id = _first(comment, "COLABO_REMARK_SRNO", "commentId", "replyId")
    detected_parent_id = _first(comment, *_COMMENT_PARENT_ID_KEYS) or parent_comment_id
    detected_root_id = _first(comment, *_COMMENT_ROOT_ID_KEYS) or root_comment_id or comment_id
    detected_depth = _first(comment, *_COMMENT_DEPTH_KEYS)
    detected_order = _first(comment, *_COMMENT_ORDER_KEYS)
    return {
        "comment_id": comment_id,
        "post_id": _first(comment, "COLABO_COMMT_SRNO", "postId") or fallback_post_id,
        "project_id": _first(comment, "COLABO_SRNO", "projectId"),
        "author_name": _first(comment, "RGSR_NM", "registerName"),
        "author_id": _first(comment, "RGSR_ID", "registerId"),
        "written_at": _first(comment, "RGSN_DTTM", "registeredDateTime"),
        "content_text": _clean_html_text(content),
        "mention_names": _extract_mentions(content),
        "is_system": _first(comment, "SYSTEM_REMARK_YN") == "Y" or bool(_first(comment, "systemCode")),
        "sys_code": _first(comment, "SYS_CODE", "systemCode"),
        "reply_cnt": _first(comment, "REPLY_CNT", "replyCount"),
        "parent_comment_id": detected_parent_id,
        "root_comment_id": detected_root_id,
        "comment_depth": detected_depth or _as_text(comment_depth),
        "comment_order": detected_order or _as_text(comment_order),
        "collected_at": collected_at,
    }


def _iter_comment_tree(
    comments: Iterable[Dict[str, Any]],
    fallback_post_id: str,
    collected_at: str,
    parent_comment_id: str = "",
    root_comment_id: str = "",
    depth: int = 0,
) -> Iterable[Dict[str, Any]]:
    for index, comment in enumerate(comments):
        row = _normalize_comment(
            comment,
            fallback_post_id=fallback_post_id,
            collected_at=collected_at,
            parent_comment_id=parent_comment_id,
            root_comment_id=root_comment_id,
            comment_depth=depth,
            comment_order=index,
        )
        yield row

        comment_id = row["comment_id"]
        child_root_id = row["root_comment_id"] or comment_id
        child_comments: List[Dict[str, Any]] = []
        for key in _COMMENT_REPLY_LIST_KEYS:
            value = comment.get(key)
            if isinstance(value, list):
                child_comments.extend(item for item in value if isinstance(item, dict))
        if child_comments:
            yield from _iter_comment_tree(
                child_comments,
                fallback_post_id=fallback_post_id,
                collected_at=collected_at,
                parent_comment_id=comment_id,
                root_comment_id=child_root_id,
                depth=depth + 1,
            )


def _int_or_zero(value: Any) -> int:
    try:
        return int(_as_text(value).strip() or "0")
    except ValueError:
        return 0


def _merge_remarks(primary: Iterable[Dict[str, Any]], extra: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for comment in list(primary or []) + list(extra or []):
        if not isinstance(comment, dict):
            continue
        comment_id = _first(comment, "COLABO_REMARK_SRNO", "commentId", "replyId")
        key = comment_id or json.dumps(comment, ensure_ascii=False, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        merged.append(comment)
    return merged


def _extract_extra_remarks_payload(payload: Any) -> List[Dict[str, Any]]:
    data = _unwrap_response_data(payload)
    if isinstance(data, dict):
        common_head = data.get("COMMON_HEAD")
        if isinstance(common_head, dict) and common_head.get("ERROR"):
            code = _as_text(common_head.get("CODE"))
            message = _as_text(common_head.get("MESSAGE"))
            raise RuntimeError(f"Flow 댓글 API 오류: code={code} message={message}")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if not isinstance(data, dict):
        return []
    for key in (
        "remarks",
        "comments",
        "replyRemarks",
        "replies",
        "subRemarks",
        "REMARK_REC",
        "REPLY_REMARK_REC",
        "SUB_REMARK_REC",
        "REMARK_REPLY_REC",
        "items",
        "list",
        "records",
    ):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _format_template(value: str, **kwargs: str) -> str:
    return value.format(**{key: _as_text(item) for key, item in kwargs.items()})


def _remarks_api_headers(headers_json: str) -> Dict[str, str]:
    if not headers_json:
        return {}
    try:
        parsed = json.loads(headers_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Flow 댓글 API 헤더 JSON 형식 오류: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Flow 댓글 API 헤더 JSON은 object여야 합니다.")
    return {str(key): str(value) for key, value in parsed.items()}


def _remarks_api_body(
    url: str,
    body_template: str,
    body_format: str,
    project_id: str,
    post_id: str,
    comment_id: str,
) -> Tuple[Any, Dict[str, str]]:
    if body_template:
        body_text = _format_template(
            body_template,
            project_id=project_id,
            post_id=post_id,
            comment_id=comment_id,
        )
    elif urlparse(url).path.endswith(".jct"):
        body_text = json.dumps(
            {
                "COLABO_SRNO": project_id,
                "COLABO_COMMT_SRNO": post_id,
                "COLABO_REMARK_SRNO": comment_id,
                "PG_NO": "1",
                "PG_PER_CNT": "100",
            },
            ensure_ascii=False,
        )
        body_format = "flow_jct"
    else:
        return None, {}

    fmt = (body_format or "json").lower()
    if fmt in {"flow_jct", "jct"}:
        return {"_JSON_": quote(body_text)}, {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    if fmt == "form_json":
        return {"_JSON_": body_text}, {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    if fmt == "form":
        try:
            parsed = json.loads(body_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Flow 댓글 API form body JSON 형식 오류: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("Flow 댓글 API form body는 object JSON이어야 합니다.")
        return parsed, {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    return body_text.encode("utf-8"), {"Content-Type": "application/json; charset=UTF-8"}


def _fetch_extra_remarks(
    project_id: str,
    post_id: str,
    comment_id: str = "",
    context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    config = _flow_remarks_api_config(context)
    url_template = config["replies_url_template"] if comment_id else config["url_template"]
    if not url_template:
        return []
    url = _format_api_url(
        url_template,
        project_id=project_id,
        post_id=post_id,
        comment_id=comment_id,
    )
    body, body_headers = _remarks_api_body(
        url,
        config["body_template"],
        config["body_format"],
        project_id,
        post_id,
        comment_id,
    )
    headers = _remarks_api_headers(config["headers_json"])
    headers.update(body_headers)
    payload = _request_json(url, method=config["method"], body=body, extra_headers=headers)
    return _extract_extra_remarks_payload(payload)


def _append_missing_reply_remarks(parent: Dict[str, Any], replies: Iterable[Dict[str, Any]]) -> None:
    comment_id = _first(parent, "COLABO_REMARK_SRNO", "commentId")
    existing = set()
    for key in _COMMENT_REPLY_LIST_KEYS:
        value = parent.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    existing.add(_first(item, "COLABO_REMARK_SRNO") or json.dumps(item, ensure_ascii=False, sort_keys=True))

    target_key = "replyRemarks"
    parent.setdefault(target_key, [])
    for reply in replies:
        if not isinstance(reply, dict):
            continue
        row = copy.deepcopy(reply)
        row.setdefault("COLABO_COMMT_SRNO", _first(parent, "COLABO_COMMT_SRNO"))
        row.setdefault("postId", _first(parent, "postId"))
        row.setdefault("COLABO_SRNO", _first(parent, "COLABO_SRNO"))
        row.setdefault("projectId", _first(parent, "projectId"))
        row.setdefault("PARENT_COLABO_REMARK_SRNO", comment_id)
        row.setdefault("parentCommentId", comment_id)
        row.setdefault("ROOT_COLABO_REMARK_SRNO", comment_id)
        key = _first(row, "COLABO_REMARK_SRNO", "commentId", "replyId") or json.dumps(row, ensure_ascii=False, sort_keys=True)
        if key in existing:
            continue
        existing.add(key)
        parent[target_key].append(row)


def _hydrate_missing_reply_remarks(detail: Dict[str, Any], project_id: str, post_id: str, context: Dict[str, Any]) -> None:
    if not _flow_remarks_api_config(context)["replies_url_template"]:
        return
    for comment in detail.get("remarks") or []:
        expected_text = _first(comment, "REPLY_CNT", "replyCount")
        expected_replies = _int_or_zero(expected_text)
        actual_replies = _direct_reply_count(comment)
        should_probe_replies = not expected_text and bool(_first(comment, "commentId"))
        if not should_probe_replies and expected_replies <= actual_replies:
            continue
        comment_id = _first(comment, "COLABO_REMARK_SRNO", "commentId")
        if not comment_id:
            continue
        replies = _fetch_extra_remarks(project_id, post_id, comment_id=comment_id, context=context)
        if replies:
            _append_missing_reply_remarks(comment, replies)


def _direct_reply_count(comment: Dict[str, Any]) -> int:
    total = 0
    for key in _COMMENT_REPLY_LIST_KEYS:
        value = comment.get(key)
        if isinstance(value, list):
            total += sum(1 for item in value if isinstance(item, dict))
    return total


def _comment_gap(
    detail: Dict[str, Any],
    post_row: Dict[str, Any],
    comment_rows: List[Dict[str, Any]],
) -> Dict[str, Any] | None:
    expected_total = _int_or_zero(detail.get("remarkCount"))
    actual_total = len(comment_rows)
    reply_gaps: List[Dict[str, Any]] = []
    for comment in detail.get("remarks") or []:
        expected_replies = _int_or_zero(_first(comment, "REPLY_CNT", "replyCount"))
        actual_replies = _direct_reply_count(comment)
        if expected_replies > actual_replies:
            reply_gaps.append(
                {
                    "comment_id": _first(comment, "COLABO_REMARK_SRNO", "commentId"),
                    "expected_replies": expected_replies,
                    "actual_replies": actual_replies,
                }
            )

    if expected_total <= actual_total and not reply_gaps:
        return None
    return {
        "project_id": post_row.get("project_id"),
        "post_id": post_row.get("post_id"),
        "title": post_row.get("title"),
        "expected_comments": expected_total,
        "actual_comments": actual_total,
        "reply_gaps": reply_gaps,
    }


def _safe_filename(value: Any, fallback: str) -> str:
    name = Path(unquote(_as_text(value))).name.strip() or fallback
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:180] or fallback


def _attachment_id(raw: Dict[str, Any], post_id: str, index: int) -> str:
    return _first(raw, "ATCH_SRNO", "ATCH_ID", "FILE_ID", "DOC_ID") or f"{post_id}:{index}"


def _attachment_file_name(raw: Dict[str, Any], attachment_id: str, attachment_type: str) -> str:
    name = _first(raw, "FILE_NAME", "ORCP_FILE_NM", "fileName")
    if name:
        return _safe_filename(name, f"{attachment_id}.bin")
    parsed_name = Path(urlparse(_first(raw, "ATCH_URL", "downloadUrl")).path).name
    if parsed_name:
        return _safe_filename(parsed_name, f"{attachment_id}.bin")
    extension = _first(raw, "EXTENSION").strip(".")
    suffix = f".{extension}" if extension else (".png" if attachment_type == "image" else ".bin")
    return _safe_filename(f"{attachment_id}{suffix}", f"{attachment_id}{suffix}")


def _download_attachment_file(url: str, path: Path) -> str:
    if not url:
        raise RuntimeError("첨부 다운로드 URL이 비어 있습니다.")
    if path.exists() and path.stat().st_size > 0:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        resp = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            logger.warning("Flow 첨부 다운로드 %s: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()
        tmp.write_bytes(resp.content)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_attachment(
    raw: Dict[str, Any],
    post_id: str,
    project_id: str,
    attachment_type: str,
    index: int,
    collected_at: str,
) -> Dict[str, Any]:
    attachment_id = _attachment_id(raw, post_id, index)
    file_name = _attachment_file_name(raw, attachment_id, attachment_type)
    return {
        "attachment_id": attachment_id,
        "post_id": post_id,
        "project_id": project_id,
        "attachment_type": attachment_type,
        "file_name": file_name,
        "file_size": _first(raw, "FILE_SIZE", "fileSize"),
        "extension": _first(raw, "EXTENSION") or Path(file_name).suffix.lstrip("."),
        "download_url": _first(raw, "ATCH_URL", "downloadUrl"),
        "thumbnail_url": _first(raw, "THUM_IMG_PATH", "thumbnailUrl"),
        "local_path": "",
        "downloaded": False,
        "download_error": "",
        "content_hash": "",
        "width": _first(raw, "WIDTH", "width"),
        "height": _first(raw, "HEIGHT", "height"),
        "registered_at": _first(raw, "RGSN_DTTM", "registeredDateTime"),
        "author_name": _first(raw, "RGSR_NM", "registerName"),
        "collected_at": collected_at,
    }


def _download_post_attachments(detail: Dict[str, Any], post_row: Dict[str, Any], collected_at: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    project_id = _as_text(post_row["project_id"])
    post_id = _as_text(post_row["post_id"])
    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []
    raw_items: List[Tuple[str, Dict[str, Any]]] = []
    raw_items.extend(("file", item) for item in detail.get("attachments") or [])
    raw_items.extend(("image", item) for item in detail.get("imageAttachments") or [])

    for index, (attachment_type, raw) in enumerate(raw_items, start=1):
        row = _normalize_attachment(raw, post_id, project_id, attachment_type, index, collected_at)
        file_path = (
            FLOW_ATTACHMENT_FILES_DIR
            / f"project_id={_safe_project_partition(project_id)}"
            / f"post_id={_safe_project_partition(post_id)}"
            / _safe_filename(row["file_name"], f"{row['attachment_id']}.bin")
        )
        row["local_path"] = str(file_path)
        try:
            row["content_hash"] = _download_attachment_file(row["download_url"], file_path)
            row["downloaded"] = True
        except Exception as exc:
            row["download_error"] = _as_text(exc)[:300]
            failures.append(
                {
                    "project_id": project_id,
                    "post_id": post_id,
                    "attachment_id": row["attachment_id"],
                    "error": row["download_error"],
                }
            )
            logger.warning(
                "Flow 첨부 다운로드 실패: project_id=%s post_id=%s attachment_id=%s error=%s",
                project_id,
                post_id,
                row["attachment_id"],
                row["download_error"],
            )
        rows.append(row)
    return rows, failures


def _load_state() -> Dict[str, Dict[str, Any]]:
    if not FLOW_STATE_JSON.exists():
        return {}
    try:
        return json.loads(FLOW_STATE_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Flow state 읽기 실패: %s", exc)
        return {}


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _has_parquet_data(path: Path) -> bool:
    if path.is_file():
        return path.suffix == ".parquet"
    if not path.is_dir():
        return False
    return any(path.rglob("*.parquet"))


def _read_parquet_dataset(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)

    frames: List[pd.DataFrame] = []
    for file_path in sorted(path.rglob("*.parquet")):
        df = pd.read_parquet(file_path)
        try:
            relative_parts = file_path.relative_to(path).parts[:-1]
        except ValueError:
            relative_parts = ()
        for part in relative_parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key and key not in df.columns:
                df[key] = value
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    columns = list(dict.fromkeys(column for frame in frames for column in frame.columns))
    return pd.concat([frame.reindex(columns=columns) for frame in frames], ignore_index=True)


def _read_parquet_or_empty(path: Path, columns: List[str]) -> pd.DataFrame:
    if not _has_parquet_data(path):
        return pd.DataFrame(columns=columns)
    return _read_parquet_dataset(path).reindex(columns=columns)


def _existing_post_project_ids() -> set[str]:
    if not _has_parquet_data(FLOW_POST_PARQUET):
        return set()
    try:
        df = _read_parquet_dataset(FLOW_POST_PARQUET)
    except Exception as exc:
        logger.warning("Flow post project_id 목록 읽기 실패, 누락 파티션 복구 판정 생략: %s", exc)
        return set()
    if df.empty or "project_id" not in df.columns:
        return set()
    return set(df["project_id"].dropna().astype(str))


def _existing_post_keys() -> set[str]:
    if not _has_parquet_data(FLOW_POST_PARQUET):
        return set()
    try:
        df = _read_parquet_dataset(FLOW_POST_PARQUET)
    except Exception as exc:
        logger.warning("Flow post_id 목록 읽기 실패, 누락 게시글 복구 판정 생략: %s", exc)
        return set()
    if df.empty or not {"project_id", "post_id"}.issubset(df.columns):
        return set()
    return {
        f"{row['project_id']}:{row['post_id']}"
        for row in df[["project_id", "post_id"]].dropna().astype(str).to_dict("records")
        if row.get("project_id") and row.get("post_id")
    }


def _read_flow_parquet_or_legacy(primary_path: Path, legacy_path: Path, columns: List[str]) -> pd.DataFrame:
    if _has_parquet_data(primary_path):
        return _read_parquet_or_empty(primary_path, columns)
    if _has_parquet_data(legacy_path):
        return _read_parquet_or_empty(legacy_path, columns)
    return pd.DataFrame(columns=columns)


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _safe_project_partition(value: Any) -> str:
    project_id = _as_text(value).strip()
    if not project_id:
        return "__missing"
    return re.sub(r"[^0-9A-Za-z_.-]", "_", project_id)


def _write_project_partitions(
    df: pd.DataFrame,
    base_path: Path,
    columns: List[str],
    project_ids: Iterable[Any],
) -> None:
    base_path.mkdir(parents=True, exist_ok=True)
    if "project_id" not in df.columns:
        raise RuntimeError("Flow parquet 파티션 저장에는 project_id 컬럼이 필요합니다.")

    normalized = df.reindex(columns=columns).copy()
    normalized["project_id"] = normalized["project_id"].astype(str)
    for raw_project_id in sorted({_as_text(project_id) for project_id in project_ids if _as_text(project_id)}):
        partition_name = f"project_id={_safe_project_partition(raw_project_id)}"
        partition_dir = base_path / partition_name
        tmp_dir = base_path / f".tmp_{partition_name}_{os.getpid()}_{time.time_ns()}"
        group = normalized[normalized["project_id"].eq(raw_project_id)]

        try:
            if partition_dir.exists():
                shutil.rmtree(partition_dir)
            if group.empty:
                continue
            tmp_dir.mkdir(parents=True, exist_ok=False)
            group.drop(columns=["project_id"]).to_parquet(
                tmp_dir / "part.parquet",
                index=False,
                engine="pyarrow",
            )
            os.replace(tmp_dir, partition_dir)
        finally:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)


def _upsert_by_key(old_df: pd.DataFrame, new_df: pd.DataFrame, key: str) -> pd.DataFrame:
    if new_df.empty:
        return old_df.copy()
    if old_df.empty:
        return new_df.copy()
    kept = old_df[~old_df[key].astype(str).isin(set(new_df[key].astype(str)))]
    return pd.concat([kept, new_df], ignore_index=True)


def _fetch_post_detail(project_id: str, post_id: str) -> Dict[str, Any]:
    url = _format_api_url(
        FLOW_POST_DETAIL_API_URL_TEMPLATE,
        project_id=project_id,
        post_id=post_id,
    )
    data = _unwrap_response_data(_request_json(url))
    if not isinstance(data, dict):
        raise RuntimeError(f"Flow 상세 응답 형식 오류: project_id={project_id} post_id={post_id}")
    return data


def extract_project_list(**context) -> List[Dict[str, Any]]:
    collected_at = _now_iso()
    data = _unwrap_response_data(_request_json(FLOW_PROJECTS_API_URL))
    projects = data.get("projects") if isinstance(data, dict) else None
    if not isinstance(projects, list):
        raise RuntimeError("Flow 프로젝트 목록 응답에 projects[]가 없습니다.")
    rows = [_normalize_project(project, collected_at) for project in projects]
    logger.info("Flow 프로젝트 목록 수집 완료: %s건", len(rows))
    return rows


def extract_post_list(projects: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for project in projects:
        project_id = _as_text(project.get("project_id"))
        project_rows: List[Dict[str, Any]] = []
        cursor = ""
        seen_cursors = set()
        while True:
            url = _format_api_url(
                FLOW_POSTS_API_URL_TEMPLATE,
                project_id=project_id,
                cursor=cursor,
            )
            url = _append_query(url, {"cursor": cursor})
            data = _unwrap_response_data(_request_json(url))
            if not isinstance(data, dict):
                raise RuntimeError(f"Flow 게시글 목록 응답 형식 오류: project_id={project_id}")
            posts = data.get("posts") or []
            for post in posts:
                post_row = dict(post)
                post_row["project_id"] = _first(post, "projectId", "project_id") or project_id
                post_row["project_name"] = project.get("project_name", "")
                post_row["project_url"] = project.get("project_url", "")
                post_row["store_name"] = project.get("store_name", "")
                post_row["is_store"] = project.get("is_store", False)
                post_row["region"] = project.get("region", "")
                post_row["status_tag"] = project.get("status_tag", "")
                rows.append(post_row)
                project_rows.append(post_row)

            has_next = data.get("hasNext") in (True, "true", "True", "Y", "1")
            next_cursor = _as_text(data.get("lastCursor"))
            if not has_next:
                break
            if not next_cursor or next_cursor == "-1" or next_cursor in seen_cursors:
                logger.warning("Flow 게시글 커서 중단: project_id=%s cursor=%s", project_id, next_cursor)
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        type_counts: Dict[str, int] = {}
        for row in project_rows:
            raw_type = (
                _first(row, "postType", "post_type", "type", "COLABO_COMMT_TP_CD", "TASK_TP_CD")
                or ("task" if _first(row, "TASK_NM", "taskName") else "post")
            )
            type_counts[_as_text(raw_type) or "(blank)"] = type_counts.get(_as_text(raw_type) or "(blank)", 0) + 1
        logger.info("Flow 프로젝트 게시글 목록: project_id=%s posts=%s raw_types=%s", project_id, len(project_rows), type_counts)
    logger.info("Flow 게시글 목록 수집 완료: %s건", len(rows))
    return rows


def detect_changed_posts(post_list: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    state = _load_state()
    attachment_project_ids = _attachment_project_ids_from_context(context)
    repair_project_ids = _repair_project_ids_from_context(context)
    repair_post_ids = _repair_post_ids_from_context(context)
    existing_project_ids = _existing_post_project_ids()
    existing_post_keys = _existing_post_keys()
    changed: List[Dict[str, Any]] = []
    missing_parquet_count = 0
    repair_project_count = 0
    for post in post_list:
        project_id = _first(post, "projectId", "project_id")
        post_id = _first(post, "postId", "post_id")
        if not project_id or not post_id:
            continue
        updated_at = _first(post, "editedDateTime", "updated_at", "updatedDateTime", "registeredDateTime", "created_at")
        key = f"{project_id}:{post_id}"
        prev = state.get(key) or {}
        needs_attachment_check = (
            _as_text(project_id) in attachment_project_ids
            and not _as_text(prev.get("attachment_checked_at"))
        )
        needs_missing_partition_repair = (
            _as_text(project_id) in repair_project_ids
            and _as_text(project_id) not in existing_project_ids
        )
        needs_project_repair = _as_text(project_id) in repair_project_ids
        needs_missing_parquet_post = (
            bool(existing_post_keys)
            and key not in existing_post_keys
            and (not repair_project_ids or _as_text(project_id) in repair_project_ids)
        )
        needs_post_repair = _matches_repair_post(project_id, post_id, repair_post_ids)
        if needs_project_repair:
            repair_project_count += 1
        if needs_missing_parquet_post:
            missing_parquet_count += 1
        if (
            _as_text(prev.get("updated_at")) == updated_at
            and not needs_attachment_check
            and not needs_missing_partition_repair
            and not needs_project_repair
            and not needs_missing_parquet_post
            and not needs_post_repair
        ):
            continue
        row = dict(post)
        row["project_id"] = project_id
        row["post_id"] = post_id
        row["updated_at"] = updated_at
        row["attachment_check_only"] = (
            needs_attachment_check
            and _as_text(prev.get("updated_at")) == updated_at
            and not needs_missing_partition_repair
            and not needs_project_repair
            and not needs_missing_parquet_post
            and not needs_post_repair
        )
        changed.append(row)
    logger.info(
        "Flow 신규/변경 게시글 판정 완료: %s/%s건 repair_projects=%s repair_posts=%s repair_project_rows=%s missing_parquet_posts=%s",
        len(changed),
        len(post_list),
        ",".join(sorted(repair_project_ids)) or "(none)",
        ",".join(sorted(repair_post_ids)) or "(none)",
        repair_project_count,
        missing_parquet_count,
    )
    return changed


def collect_post_details(changed_posts: List[Dict[str, Any]], **context) -> Dict[str, Any]:
    collected_at = _now_iso()
    queue: List[Dict[str, Any]] = []
    projects_by_id: Dict[str, Dict[str, Any]] = {}
    posts: List[Dict[str, Any]] = []
    comments: List[Dict[str, Any]] = []
    attachments: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []
    attachment_failures: List[Dict[str, str]] = []
    comment_gaps: List[Dict[str, Any]] = []
    state_updates: Dict[str, str] = {}
    attachment_checked_post_ids = set()
    visited = set()
    processed_count = 0
    call_limit_reached = False
    detail_max_posts, detail_max_calls = _detail_limits_from_context(context)
    attachment_project_ids = _attachment_project_ids_from_context(context)

    logger.info(
        "Flow 상세 수집 제한: max_posts=%s max_calls=%s changed_posts=%s attachment_projects=%s",
        "unlimited" if detail_max_posts <= 0 else detail_max_posts,
        "unlimited" if detail_max_calls <= 0 else detail_max_calls,
        len(changed_posts),
        ",".join(sorted(attachment_project_ids)) or "(none)",
    )

    if detail_max_posts > 0 and len(changed_posts) > detail_max_posts:
        logger.info(
            "Flow 상세 수집 1회 처리 제한 적용: %s/%s건",
            detail_max_posts,
            len(changed_posts),
        )
        changed_posts = changed_posts[:detail_max_posts]

    for post in changed_posts:
        project_id = _as_text(post.get("project_id"))
        post_id = _as_text(post.get("post_id"))
        project_meta = {
            "project_id": project_id,
            "project_name": _as_text(post.get("project_name")),
            "project_url": _as_text(post.get("project_url")),
            "is_store": bool(post.get("is_store")),
            "region": _as_text(post.get("region")),
            "store_name": _as_text(post.get("store_name")),
            "status_tag": _as_text(post.get("status_tag")),
        }
        projects_by_id[project_id] = project_meta
        queue.append(
            {
                "project_id": project_id,
                "post_id": post_id,
                "parent_post_id": "",
                "depth": 0,
                "project_meta": project_meta,
                "task_meta": None,
                "state_updated_at": _as_text(post.get("updated_at")),
            }
        )

    while queue:
        if detail_max_calls > 0 and processed_count >= detail_max_calls:
            call_limit_reached = True
            logger.info(
                "Flow 상세 수집 전체 호출 제한 도달: processed=%s remaining_queue=%s",
                processed_count,
                len(queue),
            )
            break

        item = queue.pop(0)
        project_id = item["project_id"]
        post_id = item["post_id"]
        key = f"{project_id}:{post_id}"
        if not post_id or key in visited:
            continue
        visited.add(key)
        processed_count += 1

        if DETAIL_PROGRESS_EVERY > 0 and (
            processed_count == 1 or processed_count % DETAIL_PROGRESS_EVERY == 0
        ):
            logger.info(
                "Flow 상세 수집 진행: processed=%s queue=%s posts=%s comments=%s failures=%s",
                processed_count,
                len(queue),
                len(posts),
                len(comments),
                len(failures),
            )

        try:
            detail = _fetch_post_detail(project_id, post_id)
        except Exception as exc:
            logger.warning("Flow 게시글 상세 수집 실패: project_id=%s post_id=%s error=%s", project_id, post_id, exc)
            failures.append({"project_id": project_id, "post_id": post_id, "error": _as_text(exc)[:300]})
            continue

        try:
            extra_remarks = _fetch_extra_remarks(project_id, post_id, context=context)
        except Exception as exc:
            logger.warning("Flow 댓글 추가 수집 실패: project_id=%s post_id=%s error=%s", project_id, post_id, exc)
            extra_remarks = []
        if extra_remarks:
            detail["remarks"] = _merge_remarks(detail.get("remarks") or [], extra_remarks)
        try:
            _hydrate_missing_reply_remarks(detail, project_id, post_id, context)
        except Exception as exc:
            logger.warning("Flow 대댓글 추가 수집 실패: project_id=%s post_id=%s error=%s", project_id, post_id, exc)

        project_meta = item["project_meta"]
        post_row = _normalize_post(
            data=detail,
            project_meta=project_meta,
            parent_post_id=item["parent_post_id"],
            depth=item["depth"],
            task_meta=item["task_meta"],
            collected_at=collected_at,
        )
        posts.append(post_row)
        state_updates[post_row["post_id"]] = (
            item.get("state_updated_at")
            or post_row["edited_at"]
            or post_row["registered_at"]
        )
        comment_rows = list(
            _iter_comment_tree(
                detail.get("remarks") or [],
                fallback_post_id=post_id,
                collected_at=collected_at,
            )
        )
        gap = _comment_gap(detail, post_row, comment_rows)
        if gap:
            comment_gaps.append(gap)
            logger.warning(
                "Flow 댓글 수집 불완전: project_id=%s post_id=%s expected=%s actual=%s reply_gaps=%s",
                gap["project_id"],
                gap["post_id"],
                gap["expected_comments"],
                gap["actual_comments"],
                gap["reply_gaps"],
            )
        comments.extend(comment_rows)
        if project_id in attachment_project_ids:
            attachment_rows, download_failures = _download_post_attachments(detail, post_row, collected_at)
            attachments.extend(attachment_rows)
            attachment_failures.extend(download_failures)
            if not download_failures:
                attachment_checked_post_ids.add(post_row["post_id"])

        if item["depth"] >= MAX_DEPTH:
            continue
        for child_task in _iter_child_tasks(detail):
            child_post_id = _as_text(child_task.get("COLABO_COMMT_SRNO"))
            child_key = f"{project_id}:{child_post_id}"
            if not child_post_id or child_key in visited:
                continue
            queue.append(
                {
                    "project_id": project_id,
                    "post_id": child_post_id,
                    "parent_post_id": post_id,
                    "depth": item["depth"] + 1,
                    "project_meta": project_meta,
                    "task_meta": child_task,
                    "state_updated_at": "",
                }
            )

    logger.info(
        "Flow 상세 수집 완료: posts=%s comments=%s attachments=%s failures=%s attachment_failures=%s comment_gaps=%s",
        len(posts),
        len(comments),
        len(attachments),
        len(failures),
        len(attachment_failures),
        len(comment_gaps),
    )
    return {
        "projects": list(projects_by_id.values()),
        "posts": posts,
        "comments": comments,
        "attachments": attachments,
        "failures": failures,
        "attachment_failures": attachment_failures,
        "comment_gaps": comment_gaps,
        "collected_post_ids": [post["post_id"] for post in posts],
        "state_updates": state_updates,
        "attachment_checked_post_ids": sorted(attachment_checked_post_ids),
        "attachment_project_ids": sorted(attachment_project_ids),
        "complete": not call_limit_reached,
        "remaining_queue": len(queue),
        "collected_at": collected_at,
    }


def save_flow_parquet(details: Dict[str, Any], **context) -> str:
    comment_gaps = details.get("comment_gaps") or []
    if comment_gaps and _comment_gap_fail_on_mismatch(context):
        sample = comment_gaps[:5]
        raise RuntimeError(
            "Flow 댓글 수집 불완전으로 저장을 중단합니다: "
            f"count={len(comment_gaps)} sample={json.dumps(sample, ensure_ascii=False)}"
        )
    if comment_gaps:
        logger.warning(
            "Flow 댓글 수집 불완전 항목을 보존하고 저장을 진행합니다: count=%s sample=%s",
            len(comment_gaps),
            json.dumps(comment_gaps[:5], ensure_ascii=False),
        )

    projects = pd.DataFrame(details.get("projects") or [])
    posts = pd.DataFrame(details.get("posts") or [])
    comments = pd.DataFrame(details.get("comments") or [])
    attachments = pd.DataFrame(details.get("attachments") or [])

    if posts.empty:
        raise RuntimeError("저장할 Flow 게시글 상세가 없습니다.")

    projects = projects.reindex(columns=_PROJECT_COLS)
    posts = posts.reindex(columns=_POST_COLS)
    comments = comments.reindex(columns=_COMMENT_COLS)
    attachments = attachments.reindex(columns=_ATTACHMENT_COLS)

    post_primary_had_data = _has_parquet_data(FLOW_POST_PARQUET)
    comment_primary_had_data = _has_parquet_data(FLOW_COMMENT_PARQUET)
    attachment_primary_had_data = _has_parquet_data(FLOW_ATTACHMENT_PARQUET)
    old_projects = _read_flow_parquet_or_legacy(
        FLOW_PROJECT_PARQUET,
        FLOW_LEGACY_PROJECT_PARQUET,
        _PROJECT_COLS,
    )
    old_posts = _read_flow_parquet_or_legacy(
        FLOW_POST_PARQUET,
        FLOW_LEGACY_POST_PARQUET,
        _POST_COLS,
    )
    old_comments = _read_flow_parquet_or_legacy(
        FLOW_COMMENT_PARQUET,
        FLOW_LEGACY_COMMENT_PARQUET,
        _COMMENT_COLS,
    )
    old_attachments = _read_parquet_or_empty(FLOW_ATTACHMENT_PARQUET, _ATTACHMENT_COLS)

    merged_projects = _upsert_by_key(old_projects, projects, "project_id").reindex(columns=_PROJECT_COLS)
    merged_posts = _upsert_by_key(old_posts, posts, "post_id").reindex(columns=_POST_COLS)

    collected_post_ids = set(posts["post_id"].astype(str))
    if old_comments.empty:
        merged_comments = comments.copy()
    else:
        kept_comments = old_comments[~old_comments["post_id"].astype(str).isin(collected_post_ids)]
        merged_comments = pd.concat([kept_comments, comments], ignore_index=True)
    merged_comments = merged_comments.reindex(columns=_COMMENT_COLS)

    if old_attachments.empty:
        merged_attachments = attachments.copy()
    else:
        kept_attachments = old_attachments[~old_attachments["post_id"].astype(str).isin(collected_post_ids)]
        merged_attachments = pd.concat([kept_attachments, attachments], ignore_index=True)
    merged_attachments = merged_attachments.reindex(columns=_ATTACHMENT_COLS)

    _write_parquet_atomic(merged_projects, FLOW_PROJECT_PARQUET)

    affected_project_ids = set(posts["project_id"].dropna().astype(str))
    affected_post_project_ids = set(affected_project_ids)
    affected_comment_project_ids = set(affected_project_ids)
    configured_attachment_project_ids = set(_as_text(project_id) for project_id in details.get("attachment_project_ids") or [])
    affected_attachment_project_ids = affected_project_ids & configured_attachment_project_ids
    if not post_primary_had_data:
        affected_post_project_ids.update(merged_posts["project_id"].dropna().astype(str))
    if not comment_primary_had_data:
        affected_comment_project_ids.update(merged_comments["project_id"].dropna().astype(str))
    if not attachment_primary_had_data:
        affected_attachment_project_ids.update(
            set(merged_attachments["project_id"].dropna().astype(str)) & configured_attachment_project_ids
        )

    _write_project_partitions(merged_posts, FLOW_POST_PARQUET, _POST_COLS, affected_post_project_ids)
    _write_project_partitions(
        merged_comments,
        FLOW_COMMENT_PARQUET,
        _COMMENT_COLS,
        affected_comment_project_ids,
    )
    _write_project_partitions(
        merged_attachments,
        FLOW_ATTACHMENT_PARQUET,
        _ATTACHMENT_COLS,
        affected_attachment_project_ids,
    )

    if details.get("complete", True):
        state = _load_state()
        state_updates = details.get("state_updates") or {}
        attachment_checked_post_ids = set(_as_text(post_id) for post_id in details.get("attachment_checked_post_ids") or [])
        for row in posts.to_dict("records"):
            key = f"{row['project_id']}:{row['post_id']}"
            previous = state.get(key) or {}
            state[key] = {
                "project_id": row["project_id"],
                "post_id": row["post_id"],
                "updated_at": state_updates.get(row["post_id"]) or row["edited_at"] or row["registered_at"],
                "content_hash": row["content_hash"],
                "last_collected_at": row["collected_at"],
                "attachment_checked_at": previous.get("attachment_checked_at", ""),
            }
            if _as_text(row["post_id"]) in attachment_checked_post_ids:
                state[key]["attachment_checked_at"] = row["collected_at"]
        _write_json_atomic(FLOW_STATE_JSON, state)
    else:
        logger.warning(
            "Flow 상세 수집이 호출 제한으로 중단되어 state 업데이트를 생략합니다: remaining_queue=%s",
            details.get("remaining_queue"),
        )

    message = (
        f"Flow parquet 저장 완료: projects={len(merged_projects)} "
        f"posts={len(merged_posts)} comments={len(merged_comments)} "
        f"attachments={len(merged_attachments)} comment_gaps={len(comment_gaps)}"
    )
    logger.info(message)
    return message
