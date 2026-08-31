"""Flow 방문일지 Power BI 단일 테이블 생성."""

from __future__ import annotations

import json
import re
import hashlib
from typing import Any

import pandas as pd

from modules.transform.pipelines.strategy.SMP_flow_store_collect import _write_parquet_atomic
from modules.transform.pipelines.strategy import flow_visit_prompts as prompts
from modules.transform.utility.paths import FLOW_VISIT_VIZ_PARQUET


VIZ_OUTPUT_COLUMNS = [
    "store_name",
    "visit_date",
    "visit_ym",
    "visit_purpose",
    "author_name",
    "post_url",
    "store_status_summary",
    "owner_status",
    "key_concerns",
    "concern_text",
    "handling_points",
    "handover_summary",
    "next_visit_action",
    "severity",
    "is_recurring",
    "issue_recurrence_cnt",
    "issue_first_date",
    "issue_last_date",
    "category",
    "issue_label",
    "followup_summary",
    "status",
    "issue_problem_detail",
    "issue_action_detail",
    "issue_evidence_text",
    "analysis_evidence",
    "followup_cnt",
    "followup_responder_list",
    "followup_reply",
    "followup_state",
    "followup_next",
    "latest_followup_at",
    "owner_summary",
    "sv_summary",
    "owner_voice_raw",
    "sv_action_raw",
    "raw_text",
    "has_issue",
    "concern_is_problem",
    "project_id",
    "store_key",
    "store_rel_key",
    "visit_rel_key",
    "post_id",
    "issue_seq",
    "issue_key",
    "issue_rel_key",
]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _store_key(store_name: Any) -> str:
    value = re.sub(r"\s+", "", _as_text(store_name))
    value = re.sub(r"\([^)]*\)$", "", value)
    return value


def _rel_key(*parts: Any) -> str | None:
    values = [_as_text(part).strip() for part in parts]
    if any(not value for value in values):
        return None
    return "|".join(values)


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False)


def _list_values(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            # '·'는 "주문·수익", "신규 메뉴·판매 채널" 같은 한국어 복합어에도 쓰인다.
            # bullet 분리 기호로 쓰면 성향 문장이 깨지므로 줄바꿈/명시 bullet만 분리한다.
            value = re.split(r"\n+|(?:^|\s)[•]\s+|(?:^|\s)-\s+", value)
    if not isinstance(value, list):
        value = [value]
    rows = []
    seen = set()
    for item in value:
        text = re.sub(r"\s+", " ", _as_text(item)).strip(" .,-")
        text = re.sub(r"^(?:[-•*]\s*)+", "", text).strip()
        text = re.sub(r"^[^:：\n]{0,40}내용\s*[:：]\s*", "", text).strip()
        if not text:
            continue
        key = re.sub(r"\s+", "", text)
        if key in seen:
            continue
        seen.add(key)
        rows.append(text)
    return rows


def _bullet_list(value: Any) -> str:
    rows = _list_values(value)
    return "\n".join(f"- {row}" for row in rows)


def _display_issue_label(value: Any) -> str | None:
    text = _as_text(value).strip()
    if re.sub(r"\s+", "", text) == "방문일지주요내용":
        return None
    return text or None


_NO_ISSUE_RESULT_RE = re.compile(
    r"(?:요청\s*사항|특이\s*사항|불편\s*사항|건의\s*사항|용기\s*불량|이상\s*현상).{0,12}"
    r"(?:없음|없습니다|없으심|무|ALL|특이사항\s*없|문제\s*없|이상\s*없)",
    re.IGNORECASE,
)


def _is_empty_content_issue(issue: dict[str, Any] | None) -> bool:
    if not issue:
        return False
    parts = [
        issue.get("owner_voice"),
        issue.get("owner_voice_raw"),
        issue.get("sv_action"),
        issue.get("raw_text"),
        issue.get("issue_label"),
    ]
    texts = [_as_text(part).strip() for part in parts]
    if not any(texts):
        return True
    if all(prompts.is_empty_content(text) for text in texts if text):
        return True
    return bool(_NO_ISSUE_RESULT_RE.search(" ".join(texts)))


def _display_text(value: Any, fallback: str) -> str:
    text = _as_text(value).strip()
    return text if text else fallback


def _compact_text(value: Any, limit: int = 95) -> str:
    text = re.sub(r"\s+", " ", _as_text(value)).strip(" .,-")
    text = re.split(r"-{3,}|\n", text, maxsplit=1)[0].strip()
    if len(text) <= limit:
        return text
    cut = text[:limit].rstrip()
    return cut + "..."


def _first_list_text(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            parts = [part.strip() for part in re.split(r"\s*[·/]\s*", value) if part.strip()]
            return parts[0] if parts else value.strip()
    if isinstance(value, list):
        return next((_as_text(item).strip() for item in value if _as_text(item).strip()), "")
    return _as_text(value).strip()


def _issue_display_name(issue: dict[str, Any] | None) -> str:
    issue_label = _as_text((issue or {}).get("issue_label")).strip()
    if re.sub(r"\s+", "", issue_label) == "방문일지주요내용":
        issue_label = ""
    issue_key = _as_text((issue or {}).get("issue_key")).strip()
    return issue_label or issue_key or "방문일지 주요 내용"


def _issue_problem_detail(issue: dict[str, Any] | None) -> str:
    """이슈 내용 한 줄. "문제 요약" 칸의 정본이다.

    이슈명/조치/상태는 각자 컬럼이 있으므로 다시 이어붙이지 않는다.
    점주 발언이 없으면 담당자 조치, 그것도 없으면 원문 요지를 쓴다.
    """
    if not issue:
        return ""
    return (
        _compact_text(issue.get("owner_voice") or issue.get("owner_voice_raw"), 120)
        or _compact_text(issue.get("sv_action") or issue.get("sv_action_raw"), 120)
        or _compact_text(issue.get("raw_text"), 120)
    )


PROBLEM_STATUS = {"미해결", "진행중"}


def _evidence_lookup(profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """profile.analysis_evidence를 issue_rel_key로 색인한다.

    화두(concern)와 이슈 행의 연결은 위치가 아니라 이 키로 맺는다.
    """
    rows = profile.get("analysis_evidence")
    if isinstance(rows, str):
        try:
            rows = json.loads(rows)
        except Exception:
            rows = []
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        rel_key = _as_text(row.get("issue_rel_key")).strip()
        if rel_key:
            lookup[rel_key] = row
        # 같은 화두로 병합된 형제 이슈가 화두를 물려받도록 issue_key로도 걸어둔다.
        issue_key = _as_text(row.get("issue_key")).strip()
        if issue_key and issue_key != "기타":
            lookup.setdefault("key:" + issue_key, row)
    return lookup


def _issue_action_detail(
    issue: dict[str, Any] | None,
    profile: dict[str, Any],
    concern_row: dict[str, Any] | None = None,
) -> str:
    if not issue:
        return _first_list_text(profile.get("handling_points"))
    # 근거가 있는 값(LLM이 쓴 next_action 또는 매핑된 액션문)만 쓴다.
    # "OO 처리 상태 확인" 같은 라벨 반복 문구는 만들지 않는다.
    action = _as_text((concern_row or {}).get("action_hint")).strip()
    if not action:
        return ""
    status = _as_text(issue.get("status")).strip()
    if status in {"해결", "안내완료"}:
        return f"{action} 후 점주가 이해했는지 재확인"
    return action


def _issue_evidence_text(issue: dict[str, Any] | None) -> str:
    if not issue:
        return ""
    return _compact_text(
        issue.get("evidence")
        or issue.get("owner_voice")
        or issue.get("owner_voice_raw")
        or issue.get("raw_text"),
        220,
    )


def _followup_parts(
    followups: list[dict[str, Any]],
    issue: dict[str, Any] | None,
    profile: dict[str, Any],
    concern_row: dict[str, Any] | None = None,
    is_problem: bool = True,
) -> dict[str, str]:
    """후속 처리 상황을 본사답변 / 상태 / 남은확인 세 조각으로 나눈다.

    규약: 현재 문제·고민이 아닌 행은 세 값과 요약이 모두 공란이다.
    """
    if issue is None or not is_problem:
        return {"reply": "", "state": "", "next": ""}
    status = _as_text(issue.get("status")).strip()
    reply = _compact_text(followups[0].get("reply_text"), 150) if followups else ""
    state = (_as_text(followups[0].get("resolution_status")) if followups else "") or status
    # 없다는 사실을 글자로 채우지 않는다. 빈 값은 빈 칸으로 둔다.
    return {"reply": reply, "state": state, "next": _issue_action_detail(issue, profile, concern_row)}


def _issue_group_key(issue: dict[str, Any] | None) -> str:
    if not issue:
        return ""
    seg_id = _as_text(issue.get("seg_id"))
    if seg_id and "#coverage-" not in seg_id:
        return f"seg:{seg_id}"
    raw_text = re.sub(r"\s+", "", _as_text(issue.get("raw_text")))
    if raw_text:
        return "raw:" + hashlib.sha1(raw_text.encode("utf-8")).hexdigest()[:12]
    return ""


def _related_issue_labels(post: dict[str, Any], current_issue: dict[str, Any] | None) -> list[str]:
    group_key = _issue_group_key(current_issue)
    if not group_key:
        return []
    labels = []
    seen = set()
    for issue in post.get("issues") or []:
        if _issue_group_key(issue) != group_key:
            continue
        label = _issue_display_name(issue)
        key = re.sub(r"\s+", "", label)
        if not label or key in seen:
            continue
        seen.add(key)
        labels.append(label)
    return labels


def _profile_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {_as_text(row.get("project_id")): row for row in payload.get("profiles") or []}


def _profile_snapshot_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _as_text(row.get("visit_rel_key")): row
        for row in payload.get("profile_snapshots") or []
        if _as_text(row.get("visit_rel_key"))
    }


def _recurring_lookup(payload: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for profile in payload.get("profiles") or []:
        project_id = _as_text(profile.get("project_id"))
        for item in profile.get("recurring_issues") or []:
            result[(project_id, _as_text(item.get("issue_key")))] = item
    return result


def _followups_for_issue(post: dict[str, Any], issue_key: Any) -> list[dict[str, Any]]:
    key = _as_text(issue_key)
    rows = []
    for followup in post.get("hq_followups") or []:
        if followup.get("is_noise"):
            continue
        if not key or _as_text(followup.get("linked_issue_key")) == key:
            rows.append(followup)
    return rows


def _base_row(post: dict[str, Any], profile: dict[str, Any], generated_at: str, prompt_version: str) -> dict[str, Any]:
    visit_date = pd.to_datetime(post.get("visit_date"), errors="coerce")
    project_id = _as_text(post.get("project_id"))
    store_key = _store_key(post.get("store_name"))
    post_id = _as_text(post.get("post_id"))
    return {
        "project_id": project_id,
        "store_name": post.get("store_name"),
        "store_key": store_key,
        "store_rel_key": _rel_key(project_id, store_key),
        "visit_rel_key": _rel_key(project_id, post_id),
        "visit_date": visit_date,
        "visit_ym": visit_date.strftime("%Y-%m") if pd.notna(visit_date) else None,
        "post_id": post_id,
        "post_url": post.get("post_url"),
        "visit_purpose": post.get("visit_purpose"),
        "author_name": post.get("author_name"),
        "store_status_summary": _bullet_list(post.get("store_status_summary")),
        "owner_status": profile.get("owner_status"),
        "key_concerns": _bullet_list(profile.get("key_concerns")),
        "handling_points": _bullet_list(profile.get("handling_points")),
        "handover_summary": profile.get("handover_summary"),
        "next_visit_action": profile.get("next_visit_action"),
        "analysis_evidence": _json_dumps(profile.get("analysis_evidence")),
        "profile_period_start": profile.get("period_start"),
        "profile_period_end": profile.get("period_end"),
        "profile_as_of_date": profile.get("profile_as_of_date"),
        "llm_model": post.get("llm_model"),
        "visit_date_source": post.get("visit_date_source"),
        "prompt_version": prompt_version,
        "generated_at": generated_at,
    }


def build_visit_viz_table(payload: dict[str, Any], generated_at: str, prompt_version: str) -> pd.DataFrame:
    profiles = _profile_map(payload)
    snapshots = _profile_snapshot_map(payload)
    recurring = _recurring_lookup(payload)
    rows: list[dict[str, Any]] = []
    for post in payload.get("posts") or []:
        project_id = _as_text(post.get("project_id"))
        visit_rel_key = _rel_key(project_id, _as_text(post.get("post_id")))
        profile = snapshots.get(_as_text(visit_rel_key)) or profiles.get(project_id, {})
        evidence_lookup = _evidence_lookup(profile)
        post_issues = [issue for issue in post.get("issues") or [] if not _is_empty_content_issue(issue)]
        issues = post_issues or [None]
        for seq, issue in enumerate(issues, 1):
            row = _base_row(post, profile, generated_at, prompt_version)
            concern_row: dict[str, Any] = {}
            if issue is None:
                followups = _followups_for_issue(post, None)
                row.update({
                    "has_issue": False,
                    "seg_id": None,
                    "issue_seq": seq,
                    "category": None,
                    "issue_key": None,
                    "issue_rel_key": None,
                    "issue_label": None,
                    "owner_summary": None,
                    "sv_summary": None,
                    "owner_voice_raw": None,
                    "sv_action_raw": None,
                    "raw_text": None,
                    "opinion_source": None,
                    "is_request": False,
                    "severity": None,
                    "status": None,
                    "is_recurring": False,
                    "issue_recurrence_cnt": 0,
                    "issue_first_date": pd.NaT,
                    "issue_last_date": pd.NaT,
                    "concern_text": None,
                    "concern_is_problem": False,
                })
            else:
                recur = recurring.get((project_id, _as_text(issue.get("issue_key"))), {})
                followups = _followups_for_issue(post, issue.get("issue_key"))
                issue_seq = issue.get("issue_seq") or seq
                row.update({
                    "has_issue": True,
                    "seg_id": issue.get("seg_id"),
                    "issue_seq": issue_seq,
                    "category": issue.get("category"),
                    "issue_key": issue.get("issue_key"),
                    "issue_rel_key": _rel_key(project_id, row.get("post_id"), issue_seq),
                    "issue_label": _display_issue_label(issue.get("issue_label")),
                    "owner_summary": _as_text(issue.get("owner_voice")).strip(),
                    "sv_summary": _as_text(issue.get("sv_action")).strip(),
                    "owner_voice_raw": issue.get("owner_voice_raw"),
                    "sv_action_raw": issue.get("sv_action_raw"),
                    "raw_text": issue.get("raw_text"),
                    "opinion_source": issue.get("opinion_source"),
                    "is_request": bool(issue.get("is_request")),
                    "severity": issue.get("severity"),
                    "status": issue.get("status"),
                    "is_recurring": bool(recur),
                    "issue_recurrence_cnt": int(recur.get("cnt") or 0),
                    "issue_first_date": pd.to_datetime(recur.get("first_date"), errors="coerce"),
                    "issue_last_date": pd.to_datetime(recur.get("last_date"), errors="coerce"),
                    "llm_model": issue.get("llm_model") or post.get("llm_model"),
                })
                concern_row = (
                    evidence_lookup.get(_as_text(row.get("issue_rel_key")))
                    or evidence_lookup.get("key:" + _as_text(issue.get("issue_key")))
                    or {}
                )
                # status로 되돌아가지 않는다. status 기본값이 "미해결"이라
                # 폴백을 두면 전 행이 문제·고민이 되어 부분집합이 다시 무의미해진다.
                # 화두로 안 잡힌 이슈(내용 없음 등)는 문제·고민도 아니다.
                row.update({
                    "concern_text": _as_text(concern_row.get("concern")) or None,
                    "concern_is_problem": bool(concern_row.get("is_problem")),
                })
            row.update({
                "followup_cnt": len(followups),
                "followup_responder_list": " · ".join(
                    sorted({_as_text(f.get("responder")) for f in followups if f.get("responder")})
                ),
                **{
                    f"followup_{name}": value
                    for name, value in _followup_parts(
                        followups, issue, profile, concern_row, bool(row.get("concern_is_problem"))
                    ).items()
                },
                "latest_followup_at": pd.to_datetime(
                    max((_as_text(f.get("written_at")) for f in followups if f.get("written_at")), default=None),
                    errors="coerce",
                ),
            })
            issue_problem_detail = _issue_problem_detail(issue)
            row.update({
                # 하위호환: 대시보드가 "followup_summary IS NOT NULL"로 문제·고민을 거른다.
                # 그 규약이 성립하려면 문제·고민 행에만 값이 있어야 한다.
                # (issue_problem_detail은 화두 행에도 항상 채워진다)
                "followup_summary": issue_problem_detail if row.get("concern_is_problem") else None,
                "issue_problem_detail": issue_problem_detail,
                "issue_action_detail": _issue_action_detail(issue, profile, concern_row),
                "issue_evidence_text": _issue_evidence_text(issue),
            })
            rows.append(row)
    df = pd.DataFrame(rows)
    bool_cols = ["has_issue", "is_recurring", "concern_is_problem"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)
    return df.reindex(columns=VIZ_OUTPUT_COLUMNS)


def _normalize_output_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reindex(columns=VIZ_OUTPUT_COLUMNS).copy()
    if "followup_summary" in df.columns:
        df["followup_summary"] = df["followup_summary"].where(
            df["followup_summary"].fillna("").astype(str).str.strip().ne(""),
            None,
        )
    return df


def _merge_existing_project_rows(df: pd.DataFrame, project_ids: list[str] | None) -> pd.DataFrame:
    df = _normalize_output_df(df)
    if not project_ids or not FLOW_VISIT_VIZ_PARQUET.exists():
        return df
    try:
        existing = _normalize_output_df(pd.read_parquet(FLOW_VISIT_VIZ_PARQUET))
    except Exception:
        return df
    if existing.empty or "project_id" not in existing.columns:
        return df
    keep = existing[~existing["project_id"].astype(str).isin(set(project_ids))].copy()
    if keep.empty:
        return df
    return _normalize_output_df(pd.concat([keep, df], ignore_index=True))


def save_visit_viz_table(
    payload: dict[str, Any],
    generated_at: str,
    prompt_version: str,
    merge_project_ids: list[str] | None = None,
) -> str:
    df = build_visit_viz_table(payload, generated_at, prompt_version)
    df = _merge_existing_project_rows(df, merge_project_ids)
    _write_parquet_atomic(df, FLOW_VISIT_VIZ_PARQUET)
    return f"Flow 방문일지 시각화 테이블 저장 완료: rows={len(df)} path={FLOW_VISIT_VIZ_PARQUET}"
