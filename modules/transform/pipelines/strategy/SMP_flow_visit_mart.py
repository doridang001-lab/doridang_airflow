"""Flow 방문일지 데이터마트 생성 파이프라인."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.pipelines.strategy.SMP_flow_store_collect import (
    _strip_trailing_parens,
    _write_parquet_atomic,
    _write_project_partitions,
)
from modules.transform.utility.paths import (
    FLOW_COMMENT_PARQUET,
    FLOW_LEGACY_COMMENT_PARQUET,
    FLOW_LEGACY_POST_PARQUET,
    FLOW_LEGACY_PROJECT_PARQUET,
    FLOW_POST_PARQUET,
    FLOW_PROJECT_PARQUET,
    FLOW_STORE_PROFILE_PARQUET,
    FLOW_VISIT_CORPUS_JSONL,
    FLOW_VISIT_FOLLOWUP_PARQUET,
    FLOW_VISIT_ISSUE_PARQUET,
    FLOW_VISIT_LLM_CACHE,
    FLOW_VISIT_LOG_PARQUET,
    FLOW_VISIT_PROFILE_CACHE,
    FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET,
    FLOW_VISIT_SUBTASK_PARQUET,
    FLOW_VISIT_TODO_PARQUET,
    FLOW_VISIT_VIZ_PARQUET,
    MART_DB,
)
from modules.transform.pipelines.strategy import flow_visit_prompts as prompts
from modules.transform.pipelines.strategy import flow_visit_segmenter

logger = logging.getLogger(__name__)

PROMPT_VERSION = prompts.PROMPT_VERSION
ISSUE_PROMPT_VERSION = prompts.ISSUE_PROMPT_VERSION
SCHEMA_VERSION = "flow_visit_profile_todo_v3"
TAXONOMY_PATH = Path(__file__).parent / "flow_visit_taxonomy.json"
TARGET_PROJECT_IDS = os.getenv("FLOW_VISIT_PROJECT_IDS", "2466857,2742104")
FORCE_REBUILD = (os.getenv("FLOW_VISIT_FORCE_REBUILD", "") or "").strip().lower() in {"1", "true", "y", "yes"}
MAX_FALLBACK_RATIO = float(os.getenv("FLOW_VISIT_MAX_FALLBACK_RATIO", "0.3"))
LLM_MAX_SEGMENTS = int(os.getenv("FLOW_VISIT_LLM_MAX_SEGMENTS", "12"))
# 요약은 분류와 상한을 나눈다. 분류 상한(12)에 묶이면 대부분의 세그먼트가
# 요약 없이 원문 절단으로 남는다. 요약 결과는 캐시되므로 첫 실행만 느리다.
SUMMARY_MAX_SEGMENTS = int(os.getenv("FLOW_VISIT_SUMMARY_MAX_SEGMENTS", "400"))


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default)) or str(default)))
    except ValueError:
        logger.warning("%s 값이 정수가 아니어서 기본값 %s를 사용합니다.", name, default)
        return default


FLOW_VISIT_CACHE_FLUSH_INTERVAL = _env_int("FLOW_VISIT_CACHE_FLUSH_INTERVAL", 5)
PROFILE_LLM_PROVIDER = (os.getenv("FLOW_VISIT_PROFILE_PROVIDER", "off") or "off").strip().lower()
PROFILE_OPENAI_MODEL = os.getenv("FLOW_VISIT_PROFILE_OPENAI_MODEL", "gpt-5-mini")
PROFILE_LOCAL_MAX_PROMPT_CHARS = _env_int("FLOW_VISIT_PROFILE_LOCAL_MAX_PROMPT_CHARS", 6000)

CATEGORY_ORDER = ["정책", "매출/광고/수익", "물류/사입", "기타"]
VALID_CATEGORIES = set(CATEGORY_ORDER)
VALID_SENTIMENT = {"긍정", "중립", "부정", "불만"}
VALID_SEVERITY = {"높음", "보통", "낮음"}
VALID_STATUS = {"미해결", "진행중", "해결", "안내완료"}
VALID_SOURCE = {"점주직접", "담당자판단"}
VALID_TODO_STATUS = {"대기", "진행", "완료", "보류"}
# 화두(key_concerns) 중 "현재 문제·고민"으로 볼 상태.
CONCERN_PROBLEM_STATUS = {"미해결", "진행중"}
CONCERN_LIMIT = 10
FLOW_TASK_STATUS_CODE_LABELS = {
    "0": "요청",
    "1": "진행",
    "2": "완료",
    "3": "보류",
}
ISSUE_DISPLAY_LABELS = {
    "계육_순살품질": "순살 품질",
    "계육_뼈닭내장": "뼈닭 내장 제거",
    "김치_품질편차": "김치 품질 편차",
    "묵은지_숙성경도": "묵은지 숙성도",
    "우거지_질김": "우거지 질김",
    "소스_용량표기": "소스 용량 표기",
    "소스_당도": "소스 당도",
    "대창_외관": "대창 외관/규격",
    "파김치_소포장": "파김치 소포장",
    "용기_불량": "용기 불량",
    "용기_규격도입": "용기 규격",
    "발주_마켓봄전환": "마켓봄 발주 전환",
    "발주_마감시한": "발주 마감시한",
    "사입_전용상품준수": "전용 상품 준수",
    "부자재_1인봉투": "1인 봉투",
    "유니폼_수령": "유니폼 수령",
    "리뷰이벤트_사리품목": "리뷰 이벤트 사리",
    "매출_홀부진": "홀 매출 부진",
    "매출_배달정체": "배달 매출 정체",
    "수익_감소체감": "순수익 감소 체감",
    "고정비_부담": "고정비 부담",
    "상권_악화": "상권 악화",
    "매장이전_양도양수": "매장이전/양도양수",
    "운영_홀배달동시한계": "홀/배달 동시 운영 한계",
    "광고_우가클단가": "우가클 단가",
    "광고_즉시할인": "즉시할인 광고",
    "광고_쿠팡노출률": "쿠팡 광고 노출",
    "광고_한그릇하나만": "한그릇/하나만 운영",
    "배달대행_배정지연": "배달대행 배정 지연",
    "플랫폼_배달팁설정": "배달팁 설정",
    "메뉴_1인메뉴확대": "1인 메뉴 확대",
    "메뉴_신메뉴도입": "신메뉴 도입",
    "메뉴_홀등록요청": "홀 메뉴 등록",
    "메뉴_중량과다": "메뉴 중량 과다",
    "토더_설정공지": "토더 설정/공지",
    "POS_전환": "POS 전환",
    "인테리어_내부디자인": "매장 내부 디자인",
    "인테리어_간판외부": "간판/외부 디자인",
    "판촉물_현수막배너": "판촉물/배너",
    "리뷰_누락응대": "리뷰/누락 응대",
    "정책_가격인상": "가격 인상 정책",
    "정책_지원금종료": "지원금 종료",
    "메뉴판_설명물": "메뉴판 설명물",
    "밑반찬_자율화": "밑반찬 자율화",
    "교육_현장지원": "현장 교육 지원",
    "기타": "방문일지 주요 내용",
}

_LOG_COLS = [
    "project_id", "store_name", "store_key", "store_rel_key", "visit_rel_key",
    "post_id", "post_url", "visit_date", "visit_date_source",
    "registered_date", "visit_purpose", "author_name", "content_clean", "topic_table_json",
    "task_status", "progress", "task_nm", "worker", "start_dt", "end_dt",
    "store_status_summary", "issue_cnt", "followup_cnt",
    "image_cnt", "attach_cnt", "content_hash", "llm_model", "prompt_version", "generated_at",
]
_ISSUE_COLS = [
    "project_id", "store_name", "store_key", "store_rel_key", "visit_rel_key", "issue_rel_key",
    "post_id", "visit_date", "seg_id", "source_kind", "issue_seq",
    "category", "issue_key", "issue_label", "owner_voice", "sv_action", "owner_voice_raw",
    "sv_action_raw", "raw_text", "opinion_source", "is_request", "severity", "status",
    "evidence", "evidence_ok", "llm_model", "is_fallback", "grounding_flag", "is_concern",
    "next_action",
]
_FOLLOWUP_COLS = [
    "project_id", "post_id", "visit_date", "comment_id", "responder", "reply_text",
    "linked_issue_key", "resolution_status", "is_noise", "written_at",
]
_SUBTASK_COLS = [
    "project_id", "store_name", "store_key", "store_rel_key", "visit_rel_key",
    "parent_post_id", "direct_parent_post_id", "direct_parent_title",
    "subtask_post_id", "subtask_url", "subtask_depth", "subtask_path_titles", "visit_date",
    "registered_date", "registered_time", "author_name", "title",
    "task_status", "start_dt", "end_dt", "task_nm", "worker", "content_text",
    "comment_group", "comment_author", "comment_text", "comment_history_json", "generated_at",
]
_PROFILE_COLS = [
    "project_id", "store_name", "store_key", "store_rel_key", "last_visit_date", "visit_cnt", "owner_status",
    "store_status_summary", "key_concerns_json", "handling_points_json", "open_issues_json",
    "recurring_issues_json", "category_counts_json", "handover_summary",
    "next_visit_action", "analysis_evidence_json",
    "llm_model", "prompt_version", "schema_version", "generated_at",
]
_PROFILE_SNAPSHOT_COLS = [
    "project_id", "store_name", "store_key", "store_rel_key", "visit_rel_key",
    "post_id", "visit_date", "visit_seq_in_month", "period_start", "period_end",
    "profile_as_of_date", "visit_cnt_as_of", "owner_status",
    "store_status_summary", "key_concerns_json", "handling_points_json",
    "open_issues_json", "recurring_issues_json", "category_counts_json", "handover_summary",
    "next_visit_action", "analysis_evidence_json",
    "llm_model", "prompt_version", "schema_version", "generated_at",
]
_TODO_COLS = [
    "todo_id", "project_id", "store_name", "store_key", "post_id", "visit_date",
    "issue_key", "issue_seq", "todo_seq", "store_rel_key", "visit_rel_key", "issue_rel_key",
    "todo_list", "todo_due_date", "todo_owner", "todo_status",
    "analysis_evidence", "llm_model", "prompt_version", "schema_version", "generated_at",
]
_DATE_PATTERNS = [
    r"(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일",
    r"(\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일",
    r"\b(\d{2})\.\s*(\d{1,2})\.\s*(\d{1,2})",
    r"\b(20\d{2})[-.](\d{1,2})[-.](\d{1,2})",
]
_MD_PATTERN = r"(?<![\d.])(\d{1,2})\s*[/월]\s*(\d{1,2})\s*일?"


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_value(v) for v in value]
    return value


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    obj = df.astype(object).where(pd.notna(df), None)
    return [_json_value(row) for row in obj.to_dict("records")]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _store_key(store_name: Any) -> str:
    value = re.sub(r"\s+", "", _as_text(store_name))
    return _strip_trailing_parens(value)


def _rel_key(*parts: Any) -> str | None:
    values = [_as_text(part).strip() for part in parts]
    if not values or any(not value for value in values):
        return None
    return "|".join(values)


def _relation_keys(
    project_id: Any,
    store_name: Any,
    post_id: Any = None,
    issue_seq: Any = None,
) -> dict[str, str | None]:
    store_key = _store_key(store_name)
    return {
        "store_key": store_key,
        "store_rel_key": _rel_key(project_id, store_key),
        "visit_rel_key": _rel_key(project_id, post_id),
        "issue_rel_key": _rel_key(project_id, post_id, issue_seq),
    }


def _read_parquet_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"Flow parquet 경로가 없습니다: {path}")
    df = _read_parquet_dataset(path)
    if df.empty:
        raise RuntimeError(f"Flow parquet 데이터가 비어 있습니다: {path}")
    return df


def _read_parquet_dataset(path: Path) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)

    frames: list[pd.DataFrame] = []
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


def _read_parquet_first(paths: list[Path], required: bool = True) -> pd.DataFrame:
    checked = []
    for path in paths:
        checked.append(str(path))
        if not path.exists():
            continue
        df = _read_parquet_dataset(path)
        if not df.empty:
            logger.info("Flow parquet 읽기: %s rows=%s", path, len(df))
            return df
    if required:
        raise RuntimeError("Flow parquet 경로가 없거나 비어 있습니다: " + " | ".join(checked))
    return pd.DataFrame()


def _load_taxonomy() -> dict[str, Any]:
    return json.loads(TAXONOMY_PATH.read_text(encoding="utf-8"))


def _issue_maps() -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    issues = _load_taxonomy().get("issues") or []
    return {str(issue["key"]): issue for issue in issues}, issues


def _sha1_text(value: Any) -> str:
    return hashlib.sha1(_as_text(value).encode("utf-8")).hexdigest()


def _taxonomy_hash() -> str:
    return hashlib.sha1(TAXONOMY_PATH.read_bytes()).hexdigest()[:8]


def _is_llm_failure(result: Any) -> bool:
    return not isinstance(result, dict) or "parse_error" in result or "raw_response" in result


def _normalize_key_text(value: Any) -> str:
    return re.sub(r"[\s_:\-]+", "", _as_text(value))


def _resolve_issue_key(value: Any, evidence_text: str = "") -> str:
    issue_by_key, issues = _issue_maps()
    key = _as_text(value).strip() or "기타"
    key = re.sub(r"^\d+\s*[.)]\s*", "", key).strip()
    key = re.sub(r'^issue_key\s*=\s*["\']?|["\']$', "", key).strip()
    if key in issue_by_key:
        return key
    if ":" in key:
        key = key.split(":")[-1].strip()
        if key in issue_by_key:
            return key
    normalized = _normalize_key_text(key)
    for issue_key in issue_by_key:
        if normalized and normalized == _normalize_key_text(issue_key):
            return issue_key
    for issue in issues:
        issue_key = issue.get("key")
        if issue_key == "기타":
            continue
        if normalized and normalized in _normalize_key_text(issue_key):
            return issue_key
        for alias in issue.get("aliases") or []:
            alias_norm = _normalize_key_text(alias)
            if alias_norm and (alias_norm == normalized or alias in evidence_text):
                return issue_key
    return "기타"


def _status_from_text(text: str, default: str = "미해결") -> str:
    value = _as_text(text)
    if any(token in value for token in ["완료", "진행 예정", "진행예정", "피드백 예정", "확인 예정"]):
        return "진행중" if "예정" in value else "해결"
    if any(token in value for token in ["어려움", "어렵", "불가", "현행", "안내 필요", "안내 부탁"]):
        return "안내완료"
    return default


def _extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\d+(?:[,.]\d+)*\s*(?:원|만원|%|개월|년|월|일)?", _as_text(text)))


def _grounding_flag(value: str, evidence: str) -> bool:
    numbers = {re.sub(r"\D", "", token) for token in _extract_numbers(value)}
    evidence_numbers = {re.sub(r"\D", "", token) for token in _extract_numbers(evidence)}
    numbers = {num for num in numbers if num}
    return bool(numbers and not numbers <= evidence_numbers)


def _comment_hash(comments: list[dict[str, Any]]) -> str:
    text = "\n".join(
        f"{row.get('comment_id')}|{row.get('author_name')}|{row.get('content_text')}"
        for row in comments
    )
    return _sha1_text(text)[:12]


def _join_json_list(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return value
    if isinstance(value, list):
        return " · ".join(_as_text(item) for item in value if _as_text(item))
    return _as_text(value)


def _clean_display_text(value: Any, limit: int = 140) -> str:
    text = re.sub(r"\s+", " ", _as_text(value)).strip(" .,-")
    text = re.sub(r"^(ㄴ|[-•*])\s*", "", text).strip()
    # "5. ", "1. 매장현황 - " 같은 목차 번호/제목 접두는 표시 문구가 아니다.
    text = prompts.strip_heading_prefix(text)
    return text[:limit].strip()


def _compact_key(value: Any) -> str:
    return re.sub(r"[\s:：·,\-_/]+", "", _as_text(value)).lower()


def _dedupe_keep_order(values: list[str], limit: int | None = None) -> list[str]:
    result = []
    seen = set()
    for value in values:
        text = _clean_display_text(value, 220)
        if not text:
            continue
        key = re.sub(r"\s+", "", text)
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
        if limit and len(result) >= limit:
            break
    return result


_BROKEN_STORE_TRAIT_RE = re.compile(
    r"^(?:신규\s*메뉴|판매\s*채널\s*확대에는\s*비교적\s*적극적인\s*편|광고는\s*실제\s*주문|수익\s*근거를\s*확인한\s*뒤\s*판단하는\s*편)$"
)


def _clean_store_trait_text(value: Any) -> str:
    text = _clean_display_text(value, 90).lstrip("-• ").strip()
    if not text:
        return ""
    if re.search(r"(반복\s*성향|주의\s*화두|기존\s*대응)\s*:", text):
        return ""
    if re.fullmatch(r".{1,24}\s*\d+\s*회", text) and not re.search(r"(편|경향|성향|중요|민감|판단)", text):
        return ""
    if _BROKEN_STORE_TRAIT_RE.match(text):
        return ""
    # LLM이 한 문장을 줄바꿈으로 쪼개 "신규 메뉴" 같은 파편을 만들면 성향으로 쓰지 않는다.
    if len(re.sub(r"[^가-힣A-Za-z0-9]", "", text)) <= 5 and not text.endswith("편"):
        return ""
    return text


def _store_trait_axis(value: str) -> str:
    text = _as_text(value)
    if any(token in text for token in ["광고", "우가클", "주문 증가", "전환", "효과"]):
        return "ad"
    if any(token in text for token in ["수익", "순이익", "남는 금액", "수수료", "고정비"]):
        return "profit"
    if any(token in text for token in ["품질", "원재료", "계육", "묵은지", "우거지", "CS", "클레임"]):
        return "quality"
    if any(token in text for token in ["신규 메뉴", "신메뉴", "키워드", "판매 확대", "판매 채널"]):
        return "menu"
    if any(token in text for token in ["안내받은 내용", "확인 후 실행", "숙지", "실행하려는"]):
        return "execution"
    if any(token in text for token in ["부담", "우려", "걱정", "불만", "처리 결과"]):
        return "reaction"
    return "other"


def _canonical_store_trait(axis: str, value: str) -> str:
    if axis == "profit":
        return "수익성과 실제 남는 금액을 중요하게 확인하는 편"
    if axis == "ad":
        return "광고는 실제 주문·수익 효과를 확인한 뒤 판단하는 편"
    if axis == "quality":
        return "원재료 품질 편차와 처리 결과를 구체적으로 확인하는 편"
    if axis == "menu":
        return "신규 메뉴와 판매 확대에는 비교적 적극적인 편"
    if axis == "execution":
        return "안내받은 내용은 확인 후 실행하려는 편"
    if axis == "reaction":
        return "문제 발생 시 부담과 우려를 구체적으로 표현하는 편"
    return value


def _compact_store_trait_texts(rows: list[str], limit: int = 3) -> list[str]:
    cleaned = _dedupe_keep_order([_clean_store_trait_text(item) for item in rows], 8)
    axis_priority = {
        "profit": 0,
        "ad": 1,
        "quality": 2,
        "menu": 3,
        "execution": 4,
        "reaction": 5,
        "other": 9,
    }
    by_axis: dict[str, str] = {}
    for item in cleaned:
        axis = _store_trait_axis(item)
        if axis == "other" and re.search(r"(간담회|공지|교육|일정|요청사항|매장현황|마켓봄|토더)", item):
            continue
        by_axis.setdefault(axis, _canonical_store_trait(axis, item))
    return [
        text
        for _, text in sorted(by_axis.items(), key=lambda row: axis_priority.get(row[0], 99))
    ][:limit]


def _summary_sentence_from_issue(issue: dict[str, Any]) -> str:
    key = _as_text(issue.get("issue_key"))
    text = _issue_text(issue)
    if key in {"수익_감소체감", "매출_배달정체", "매출_홀부진", "고정비_부담"}:
        if any(token in text for token in ["순이익", "수익", "남는", "부담", "20%", "광고비", "수수료"]):
            return "실제 남는 금액과 수익성 근거를 확인한 뒤 판단하는 편"
        return ""
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        if any(token in text for token in ["효율", "전환", "의미", "효과", "주문", "수익", "광고비", "우가클", "줄이고", "거부"]):
            return "광고는 비용 대비 실제 주문·수익 근거를 확인한 뒤 판단하는 편"
        return ""
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        if any(token in text for token in ["품질", "냄새", "질기", "잔털", "내장", "불량", "개선", "CS", "클레임"]):
            return "원재료 품질과 CS 처리 기준을 중요하게 보는 편"
        return ""
    if key in {"메뉴_1인메뉴확대", "메뉴_신메뉴도입", "메뉴_홀등록요청"}:
        if any(token in text for token in ["희망", "관심", "키워드", "출시", "추가", "확대", "맛보고", "판매"]):
            return "신규 메뉴와 판매 확대에는 비교적 적극적인 편"
        return ""
    if key == "토더_설정공지":
        return ""
    if any(token in text for token in ["부담", "걱정", "답답", "불만", "섭섭"]):
        return "문제 발생 시 부담과 우려를 구체적으로 표현하는 편"
    if any(token in text for token in ["숙지", "진행하신다고", "이해", "확인"]):
        return "안내받은 내용은 확인 후 실행하려는 편"
    return ""


GENERIC_ISSUE_LABELS = {"방문일지 주요 내용", "방문일지주요내용"}


def _is_generic_issue(issue: dict[str, Any]) -> bool:
    key = _as_text(issue.get("issue_key"))
    label = re.sub(r"\s+", "", _as_text(issue.get("issue_label")))
    return key == "기타" or label in GENERIC_ISSUE_LABELS


def _is_generic_label(value: Any) -> bool:
    return re.sub(r"\s+", "", _as_text(value)) in GENERIC_ISSUE_LABELS


def _issue_display_label(issue_key: Any, fallback: Any = "") -> str:
    key = _as_text(issue_key)
    mapped = ISSUE_DISPLAY_LABELS.get(key)
    if mapped and not _is_generic_label(mapped):
        return mapped
    fallback_text = _clean_display_text(fallback, 24)
    if fallback_text and not _is_generic_label(fallback_text) and not re.search(r"(현재 상태도|일자 계|일자 전)$", fallback_text):
        return fallback_text
    if mapped:
        # 기타의 자리표시자. 구체적인 라벨이 없을 때만 여기로 온다.
        return mapped
    if key == "기타":
        return ""
    return (key.replace("_", " ") if key else "방문일지 주요 내용")[:24]


def _issue_text(issue: dict[str, Any]) -> str:
    return " ".join(
        _as_text(issue.get(field))
        for field in ["owner_voice", "owner_voice_raw", "raw_text", "sv_action", "sv_action_raw", "issue_label"]
    )


def _issue_priority(issue: dict[str, Any], latest_visit_date: str, recurring_counts: dict[str, int]) -> int:
    key = _as_text(issue.get("issue_key"))
    score = 0
    if _as_text(issue.get("visit_date")) == latest_visit_date:
        score += 100
    if issue.get("status") in {"미해결", "진행중"}:
        score += 35
    if recurring_counts.get(key, 0) >= 2:
        score += 25
    if issue.get("severity") == "높음":
        score += 20
    if issue.get("is_request"):
        score += 15
    if issue.get("opinion_source") == "점주직접":
        score += 10
    return score


def _concern_text(issue: dict[str, Any]) -> str:
    key = _as_text(issue.get("issue_key"))
    text = _issue_text(issue)
    label = _issue_display_label(key, issue.get("issue_label"))
    if key == "매출_배달정체":
        if "1400" in text or "1500" in text:
            return "배달 매출 1,400~1,500만원 정체"
        if "1200" in text or "1300" in text:
            return "배달 매출 1,200~1,300만원 정체"
        return "배달 매출 정체"
    if key == "광고_즉시할인":
        if any(token in text for token in ["빼면", "빠지", "의미없"]):
            return "즉시할인 의존·효율 불신"
        return "즉시할인 광고 효율"
    if key == "광고_우가클단가":
        return "우가클 단가·전환율"
    if key == "광고_쿠팡노출률":
        return "쿠팡 광고 노출 확인"
    if key == "광고_한그릇하나만":
        return "한그릇/하나만 운영 의존"
    if key == "상권_악화":
        return "재개발·공사 상권 악화"
    if key == "매장이전_양도양수":
        return "매장이전 검토"
    if key == "수익_감소체감":
        if "300" in text or "400" in text:
            return "순수익 월 300~400만원 체감"
        return "순수익 감소 체감"
    if key == "묵은지_숙성경도":
        return "묵은지·우거지 품질"
    if key == "우거지_질김":
        return "묵은지·우거지 품질"
    if key == "메뉴_1인메뉴확대":
        if "쿠팡" in text:
            return "쿠팡 1인 메뉴 등록"
        return "1인 메뉴 확대"
    if key == "토더_설정공지":
        return "토더 공지 확인과 설정 교육 필요"
    if key == "정책_가격인상":
        return "가격 인상 정책 안내에 대한 수용 여부"
    if key == "정책_지원금종료":
        return "순살 지원금 종료 안내 확인"
    if key == "계육_순살품질":
        return "순살 품질"
    if key == "계육_뼈닭내장":
        return "뼈닭 내장 제거"
    if key == "인테리어_내부디자인":
        return "매장 내부 디자인 보완"
    if key == "매출_홀부진":
        return "홀 매출 부진·고정비 부담"
    if key == "고정비_부담":
        return "월세·관리비 고정비 부담"
    if key == "메뉴_홀등록요청":
        return "홀 메뉴 등록"
    if key == "판촉물_현수막배너":
        return "판촉물·배너 지원"
    if key == "메뉴_신메뉴도입":
        if _PROBLEM_SIGNAL_RE.search(text):
            if any(token in text for token in ["불조절", "물", "짠", "화구", "조리"]):
                return "신메뉴 조리·판매 우려"
            if any(token in text for token in ["판매", "개선", "품질", "불편", "부담"]):
                return "신메뉴 판매 개선 필요"
        return "신메뉴 도입 관심"
    if key == "사입_전용상품준수":
        return "전용 상품 사용 기준 확인"
    summary = _clean_display_text(issue.get("owner_voice") or issue.get("raw_text"), 80)
    if _is_generic_issue(issue):
        # 기타 이슈는 원문을 그대로 자르면 화두가 아니라 문단 조각이 된다.
        # LLM이 붙인 issue_label을 우선 쓰고, 없을 때만 첫 절을 40자로 줄인다.
        llm_label = prompts.normalize_summary(issue.get("issue_label"), 24)
        if llm_label and not _is_generic_label(llm_label):
            return llm_label
        return _first_clause(summary, 40)
    return f"{label}: {summary}" if summary else label


def _first_clause(text: Any, limit: int = 40) -> str:
    """문단에서 첫 절만 남긴다. 한국어 방문일지는 마침표가 거의 없어 종결어미로도 끊는다."""
    value = _clean_display_text(text, 400)
    if not value:
        return ""
    parts = re.split(
        r"(?<=[.!?。])\s+|(?<=하심)\s+|(?<=합니다)\s+|(?<=했음)\s+|(?<=중임)\s+|(?<=운영\s중)\s+|\s+-\s+",
        value,
        maxsplit=1,
    )
    head = _clean_display_text(parts[0], limit + 20)
    if len(head) <= limit:
        return head
    cut = head[:limit].rstrip()
    space = cut.rfind(" ")
    if space >= limit // 2:
        cut = cut[:space].rstrip()
    return cut.rstrip(" ,·-")


def _handling_text(issue: dict[str, Any], allow_generic: bool = True) -> str:
    """다음 확인 액션문. allow_generic=False면 매핑에 없는 이슈는 공란으로 둔다.

    이슈 단위 표시에는 "OO 처리 상태 확인" 같은 라벨 반복 문구가 노이즈라
    근거가 있는 매핑/LLM 결과만 쓴다. 방문 단위(next_visit_action)는
    무엇부터 볼지 지시가 비는 것보다 나으므로 기본값을 유지한다.
    """
    key = _as_text(issue.get("issue_key"))
    if key == "매출_배달정체":
        return "선택기간 매출 흐름 먼저 공유"
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return "광고비 대비 주문·객단가 자료 준비"
    if key in {"상권_악화", "매장이전_양도양수"}:
        return "이전 상담 기준 자료 준비"
    if key == "수익_감소체감":
        text = _issue_text(issue)
        if any(token in text for token in ["순이익", "도리당만 분리", "20%"]):
            return "도리당 단독 순이익 산출 가능 여부 확인 및 광고세팅 동일 조건의 수익 변화 자료 준비"
        return "수수료·광고비 제외 순수익 기준 설명"
    if key in {"묵은지_숙성경도", "우거지_질김"}:
        return "묵은지·우거지 개선 가능 여부 확인"
    if key in {"계육_순살품질", "계육_뼈닭내장"}:
        return "계육 품질 개선 가능 여부 확인"
    if key == "메뉴_1인메뉴확대":
        return "쿠팡 1인 메뉴 등록 진행 상태 확인"
    if key == "토더_설정공지":
        return "토더 공지 확인 방법 현장 재시연"
    if key in {"정책_가격인상", "정책_지원금종료"}:
        return "정책 시행일·금액·영향 재안내"
    if _is_generic_issue(issue) or not allow_generic:
        return ""
    label = _issue_display_label(key, issue.get("issue_label"))
    return f"{label} 처리 상태 확인"


def _request_text(issue: dict[str, Any]) -> str:
    key = _as_text(issue.get("issue_key"))
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 개선 가능 여부 확인 요청"
    if key == "메뉴_1인메뉴확대":
        return "쿠팡 1인 메뉴 등록 진행 여부 확인 요청" if "쿠팡" in _issue_text(issue) else "1인 메뉴 확대 진행 여부 확인 요청"
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 효과 확인 요청"
    if key in {"인테리어_내부디자인", "인테리어_간판외부"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 보완 진행 여부 확인 요청"
    if key in {"판촉물_현수막배너", "메뉴_홀등록요청", "유니폼_수령"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 지원 여부 확인 요청"
    if _is_generic_issue(issue):
        return "후속 확인 요청"
    label = _issue_display_label(key, issue.get("issue_label"))
    return f"{label} 확인 요청"


def _todo_text(issue: dict[str, Any]) -> str:
    request = _request_text(issue)
    key = _as_text(issue.get("issue_key"))
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 이슈를 관련 부서에 전달하고 개선 가능 여부 확인"
    if key == "메뉴_1인메뉴확대":
        return "쿠팡 1인 메뉴 등록 현황 확인 후 점주에게 회신" if "쿠팡" in _issue_text(issue) else "1인 메뉴 확대 가능 여부 확인 후 점주에게 회신"
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return f"{_issue_display_label(key, issue.get('issue_label'))} 전후 주문 실적 확인 후 공유"
    return request.replace(" 요청", " 후 점주에게 회신")


def _problem_text(issue: dict[str, Any]) -> str:
    key = _as_text(issue.get("issue_key"))
    if key in {"수익_감소체감", "매출_배달정체", "매출_홀부진", "고정비_부담"}:
        return "수익성 부담\n수수료·광고비·고정비를 제외한 실제 수익성에 부담을 느끼는 상태"
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return "광고 효율 불확실\n광고비가 실제 주문 증가와 수익 개선으로 연결되는지 확신하지 못함"
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        return "상품 품질 우려\n원재료 품질 편차 또는 이전 대비 저하를 우려하는 상태"
    if key in {"상권_악화", "매장이전_양도양수"}:
        return "상권 변화 부담\n상권 악화나 이전 가능성 때문에 향후 운영 방향을 고민하는 상태"
    if key == "메뉴_1인메뉴확대":
        return "신규 판매 방식 진행 불확실\n요청한 메뉴·채널 확대가 실제 진행되는지 확인이 필요한 상태"
    label = _issue_display_label(key, issue.get("issue_label"))
    summary = _clean_display_text(issue.get("owner_voice") or issue.get("raw_text"), 90)
    return f"{label}\n{summary}" if summary else label


def _problem_detail_text(issue: dict[str, Any]) -> str:
    """화두의 상세 = 점주 요지 한 줄.

    이슈명·담당자 조치·상태는 issue_label / sv_summary / status 컬럼에 이미 따로 있다.
    여기에 "문제: / 점주 의견: / 담당자 조치: / 상태:"로 다시 이어붙이면
    같은 값이 중복되고 표에 라벨 문자열이 그대로 노출된다.
    """
    owner_voice = _clean_display_text(issue.get("owner_voice") or issue.get("owner_voice_raw"), 120)
    if owner_voice:
        return owner_voice
    return _clean_display_text(issue.get("raw_text"), 120)


def _owner_state_sentence(issues: list[dict[str, Any]], latest_issues: list[dict[str, Any]]) -> str:
    source = latest_issues or issues
    if not source:
        return "최근 방문에서 점주 상태를 판단할 구체 근거가 부족함"
    keys = {_as_text(issue.get("issue_key")) for issue in source}
    haystack = " ".join(_issue_text(issue) for issue in source)
    if keys & {"수익_감소체감", "고정비_부담"}:
        return "실제 남는 금액에 부담을 느끼며 수익성 개선 가능성을 확인하려는 상태"
    if keys & {"매출_배달정체", "매출_홀부진"}:
        return "매출 회복 가능성은 지켜보지만 현재 흐름에는 답답함을 느끼는 상태"
    if keys & {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return "광고 집행 효과를 확신하지 못해 실제 주문·수익 근거를 확인하려는 상태"
    if keys & {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        return "원재료 품질에 우려가 있어 본사 확인과 개선 가능성을 기다리는 상태"
    if keys & {"메뉴_1인메뉴확대", "메뉴_신메뉴도입", "메뉴_홀등록요청"}:
        return "신규 메뉴나 판매 채널 확대에는 관심이 있고 진행 상황을 확인하려는 상태"
    if any(token in haystack for token in ["고맙", "흔쾌", "이해", "따르"]):
        return "본사 제안에는 수용적인 편이나 후속 안내가 이어져야 하는 상태"
    if any(token in haystack for token in ["답답", "스트레스", "걱정", "힘들", "부담", "섭섭"]):
        return "최근 방문 이슈에 부담과 우려를 표현해 후속 확인을 기다리는 상태"
    return "최근 방문 이슈를 기준으로 추가 확인이 필요한 상태"


def _store_trait_texts(issues: list[dict[str, Any]], recurring_counts: dict[str, int]) -> list[str]:
    traits: list[str] = []
    key_text = {key: " ".join(_issue_text(issue) for issue in issues if _as_text(issue.get("issue_key")) == key) for key in recurring_counts}
    key_visit_counts = {
        key: len({_as_text(issue.get("visit_date")) for issue in issues if _as_text(issue.get("issue_key")) == key and _as_text(issue.get("visit_date"))})
        for key in recurring_counts
    }
    quality_keys = {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}
    profit_keys = {"수익_감소체감", "매출_배달정체", "매출_홀부진", "고정비_부담"}
    ad_keys = {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}
    menu_keys = {"메뉴_1인메뉴확대", "메뉴_신메뉴도입", "메뉴_홀등록요청"}
    recurring_key_set = {key for key, count in key_visit_counts.items() if count >= 2}
    text_by_group = {
        "profit": " ".join(key_text.get(key, "") for key in profit_keys),
        "quality": " ".join(key_text.get(key, "") for key in quality_keys),
        "ad": " ".join(key_text.get(key, "") for key in ad_keys),
        "menu": " ".join(key_text.get(key, "") for key in menu_keys),
    }
    if recurring_key_set & profit_keys and any(token in text_by_group["profit"] for token in ["순이익", "수익", "남는", "부담", "20%", "광고비", "수수료"]):
        traits.append("수익성과 실제 남는 금액을 중요하게 확인하는 편")
    if recurring_key_set & quality_keys and any(token in text_by_group["quality"] for token in ["품질", "냄새", "질기", "잔털", "내장", "불량", "개선", "클레임"]):
        traits.append("원재료 품질 편차와 처리 결과를 구체적으로 확인하는 편")
    if recurring_key_set & ad_keys and any(token in text_by_group["ad"] for token in ["효율", "전환", "의미", "효과", "주문", "수익", "광고비", "우가클", "줄이고", "거부"]):
        traits.append("광고는 실제 주문·수익 효과를 확인한 뒤 판단하는 편")
    if recurring_key_set & menu_keys and any(token in text_by_group["menu"] for token in ["희망", "관심", "키워드", "출시", "추가", "확대", "맛보고", "판매"]):
        traits.append("신규 메뉴와 판매 확대에는 비교적 적극적인 편")
    for key, text in key_text.items():
        if key in quality_keys and any(token in text for token in ["중요", "메인", "퀄리티", "좋아야"]):
            traits.append("원재료 품질 기준을 명확하게 중요시하는 편")
        if key in ad_keys and any(token in text for token in ["효율", "전환", "의미", "효과"]):
            traits.append("광고는 실제 주문·수익 효과를 확인한 뒤 판단하는 편")
    if len(traits) < 3:
        recurring_issue_keys = recurring_key_set or {
            key for key, count in recurring_counts.items()
            if count >= 2 and key not in {"기타"}
        }
        traits.extend(
            _summary_sentence_from_issue(issue)
            for issue in issues
            if _as_text(issue.get("issue_key")) in recurring_issue_keys and not _is_generic_issue(issue)
        )
    return _compact_store_trait_texts(traits, 3) or ["누적 히스토리에서 반복 특성을 더 확인해야 함"]


# _status_from_text의 기본값이 "미해결"이라 status만으로는 판별력이 없다.
# (중립적인 매장현황 서술까지 전부 미해결로 떨어져 화두 == 문제·고민이 된다)
# 그래서 상태 위에 문제 신호를 AND로 얹는다.
# "문제·고민" = 점주가 곤란해하는 것. 요청·희망·관심과 본사 내부 과제는 제외한다.
# 그래서 요청 계열 어휘(요청/건의/문의/희망/좋겠/필요)는 신호에 넣지 않는다.
_PROBLEM_SIGNAL_RE = re.compile(
    r"(부담|불만|걱정|답답|힘들|어렵|우려|아쉽|섭섭|스트레스|불편|곤란|"
    r"정체|하락|감소|부진|빠지|줄어|미흡|누락|지연|오류|불량|클레임|컴플레인|"
    # 품질 불만. 원재료 이슈가 이쪽 표현으로만 들어오는 경우가 많다.
    r"품질|이취|냄새|단단|딱딱|질기|질겨|상하|변질|파손|깨지|과다|길어|늦어|밀리)"
)
_POSITIVE_ONLY_RE = re.compile(
    r"(상승|증가|만족|양호|원활|잘\s*지켜|잘\s*운영|문제\s*없|이상\s*없|특이사항\s*없|"
    r"성실|호응|긍정|개선\s*됨|해결\s*됨)"
)


_NO_ISSUE_RESULT_RE = re.compile(
    r"(?:요청\s*사항|특이\s*사항|불편\s*사항|건의\s*사항|용기\s*불량|이상\s*현상).{0,12}"
    r"(?:없음|없습니다|없으심|무|ALL|특이사항\s*없|문제\s*없|이상\s*없)",
    re.IGNORECASE,
)


def _is_empty_content_issue(issue: dict[str, Any]) -> bool:
    """내용이 사실상 비어 있는 이슈인지.

    '점주님 요청사항-없음' 같은 기타 행뿐 아니라,
    '용기 불량: 특이사항 없음'처럼 점검 결과가 없음인 분류 행도 화두에서 제외한다.
    """
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
    combined = " ".join(texts)
    if _NO_ISSUE_RESULT_RE.search(combined):
        return True
    return False


def _is_problem_issue(issue: dict[str, Any]) -> bool:
    """화두 중 '현재 문제·고민'인지. 화두 자체는 이 값과 무관하게 남는다.

    상태가 먼저다. 해결/안내완료면 어떤 신호가 있어도 고민이 아니다.
    그 다음은 요약 LLM의 판단, LLM이 안 돌았으면 신호 규칙.
    """
    if _as_text(issue.get("status")) not in CONCERN_PROBLEM_STATUS:
        return False
    if isinstance(issue.get("is_concern"), bool):
        return issue["is_concern"]
    text = _issue_text(issue)
    # is_request만으로는 고민이 아니다. 요청·희망은 화두이되 고민은 아니다.
    if issue.get("severity") == "높음":
        return True
    if not _PROBLEM_SIGNAL_RE.search(text):
        return False
    # 부정 신호가 있어도 긍정 서술뿐이면 고민이 아니다.
    if _POSITIVE_ONLY_RE.search(text) and not _PROBLEM_SIGNAL_RE.search(
        _POSITIVE_ONLY_RE.sub(" ", text)
    ):
        return False
    return True


def _evidence_rows(
    issues: list[dict[str, Any]],
    limit: int = CONCERN_LIMIT,
    project_id: Any = None,
) -> list[dict[str, Any]]:
    """화두 1건 = 1행. key_concerns가 이 행들에서 파생되므로 concern 기준으로 먼저 병합한다."""
    rows: list[dict[str, Any]] = []
    by_concern: dict[str, dict[str, Any]] = {}
    for issue in sorted(issues, key=lambda row: _as_text(row.get("visit_date")), reverse=True):
        if _is_empty_content_issue(issue):
            # "요청사항-없음" 류는 화두가 아니다. 화두가 아니면 문제·고민도 아니므로
            # key_concerns / handling_points 양쪽에서 함께 빠진다.
            continue
        concern = _clean_display_text(_concern_text(issue), 90)
        if not concern or prompts.is_empty_content(concern):
            continue
        evidence = _clean_display_text(issue.get("evidence") or issue.get("owner_voice") or issue.get("raw_text"), 180)
        problem_detail = _problem_detail_text(issue)
        if not evidence and not problem_detail:
            continue
        key = _compact_key(concern)
        merged = by_concern.get(key)
        if merged is not None:
            # 같은 문구로 접히는 이슈는 버리지 않고 근거만 합친다. 행을 늘리면 1:1 대응이 깨진다.
            merged["is_problem"] = merged["is_problem"] or _is_problem_issue(issue)
            if evidence and evidence not in merged["evidence"]:
                merged["evidence"] = _clean_display_text(f"{merged['evidence']} / {evidence}", 360)
            if problem_detail and problem_detail not in merged["problem_detail"]:
                merged["problem_detail"] = _clean_display_text(f"{merged['problem_detail']} / {problem_detail}", 440)
            continue
        if len(rows) >= limit:
            continue
        row = {
            "concern": concern,
            "issue_key": _as_text(issue.get("issue_key")),
            "issue_label": _issue_display_label(issue.get("issue_key"), issue.get("issue_label")),
            "issue_rel_key": _rel_key(project_id, issue.get("post_id"), issue.get("issue_seq")),
            "visit_date": issue.get("visit_date"),
            "post_id": _as_text(issue.get("post_id")),
            "problem_detail": problem_detail,
            "owner_voice": _clean_display_text(issue.get("owner_voice") or issue.get("owner_voice_raw"), 180),
            "sv_action": _clean_display_text(issue.get("sv_action") or issue.get("sv_action_raw"), 180),
            "status": _as_text(issue.get("status")),
            "evidence": evidence,
            "is_problem": _is_problem_issue(issue),
            # 다음 방문 액션문. 화두 문구와 달리 "무엇을 할지"이며 viz의 남은확인 자리에 쓴다.
            # LLM이 근거를 보고 쓴 값 우선. 없으면 매핑된 액션문, 그것도 없으면 공란.
            "action_hint": (
                _clean_display_text(issue.get("next_action"), prompts.NEXT_ACTION_MAX_CHARS)
                or _handling_text(issue, allow_generic=False)
            ),
        }
        by_concern[key] = row
        rows.append(row)
    return rows


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False)


def _list_texts(value: Any, limit: int, text_limit: int = 120) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = re.split(r"\n+|[·•]\s*|(?:^|\s)-\s+", value)
    if not isinstance(value, list):
        return []
    rows = []
    for item in value:
        text = _clean_display_text(item, text_limit)
        if text:
            rows.append(text)
    return _dedupe_keep_order(rows, limit)


def _profile_list_texts(value: Any, fallback: list[str], *, limit: int = 5, text_limit: int = 120) -> list[str]:
    rows = _list_texts(value, limit, text_limit)
    rows = [_clean_store_trait_text(row) for row in rows]
    fallback_rows = [_clean_store_trait_text(row) for row in fallback]
    return _dedupe_keep_order(rows, limit) or _dedupe_keep_order(fallback_rows, limit)


def _clean_owner_status(value: Any, fallback: str) -> str:
    text = _clean_display_text(value, 180)
    text = re.sub(r"^\s*\d+(?:\.\d+)?\s*/\s*10\s*[·.\-:]*\s*", "", text)
    text = re.sub(r"\b\d+(?:\.\d+)?\s*점\b\s*[·.\-:]*\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip(" ·.-")
    if not text:
        return fallback
    if re.search(r"\d+(?:\.\d+)?\s*/\s*10|\b\d+(?:\.\d+)?\s*점\b", text):
        return fallback
    return text


def _build_next_visit_action(todos: list[dict[str, Any]], handling_points: list[str]) -> str:
    todo_actions = [_as_text(row.get("todo_list")) for row in todos if _as_text(row.get("todo_list"))]
    if todo_actions:
        return " → ".join(todo_actions[:2])
    compact = [re.split(r"\n", item, maxsplit=1)[0] for item in handling_points if item]
    return " → ".join(compact[:2]) if compact else "최근 문제 근거와 점주 반응 확인한다"


def _clean_handover_item(value: Any, forbidden_texts: set[str] | None = None) -> str:
    text = _clean_display_text(value, 80)
    if not text:
        return ""
    text = re.sub(r"^\s*(?:[-*•·]|\d+[.)])\s*", "", text)
    text = re.sub(r"^(?:반복\s*성향|주의\s*화두|기존\s*대응|방문\s*전\s*준비|이전\s*안내[·/ ]*조치)\s*[:：]\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip(" .·-/")
    if not text:
        return ""
    if re.search(r"(반복\s*성향|주의\s*화두|기존\s*대응)\s*:", text):
        return ""
    normalized = re.sub(r"\s+", "", text)
    for forbidden in forbidden_texts or set():
        if normalized == re.sub(r"\s+", "", forbidden):
            return ""
    return text


def _handover_texts(value: Any, forbidden_texts: set[str] | None = None, *, limit: int = 3) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = re.split(r"\n+|[·•]\s*|(?:^|\s)-\s+|(?:\s→\s)", value)
    if not isinstance(value, list):
        value = [value]
    rows = [_clean_handover_item(item, forbidden_texts) for item in value]
    return _dedupe_keep_order([row for row in rows if row], limit)


def _handover_bullet_summary(items: list[str]) -> str:
    rows = _dedupe_keep_order([_clean_handover_item(item) for item in items], 3)
    if not rows:
        rows = ["최근 후속사항과 운영 확인 필요 항목 재점검"]
    return "\n".join(f"- {row}" for row in rows[:3])


def _handover_issue_candidate(issue: dict[str, Any], action_texts: list[str]) -> tuple[str, str]:
    key = _as_text(issue.get("issue_key"))
    label = _issue_display_label(key, issue.get("issue_label"))
    text = _issue_text(issue)
    action_text = _clean_display_text(issue.get("sv_action") or issue.get("sv_action_raw"), 70)
    if key in {"수익_감소체감", "매출_배달정체", "매출_홀부진", "고정비_부담"}:
        if any(token in text for token in ["광고", "순이익", "수익", "남는", "수수료"]):
            return ("자료", "광고 동일 조건의 순이익 변동 자료 준비")
        return ("자료", "수수료·광고비 제외 순이익 자료 준비")
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return ("자료", "광고 전후 주문·순이익 비교 자료 준비")
    if key == "토더_설정공지":
        return ("운영", "토더 공지 확인 여부 재점검")
    if key in {"발주_마켓봄전환", "발주_마감시한"}:
        return ("운영", "발주 마감 준수 여부 재확인")
    if key in {"사입_전용상품준수", "용기_불량", "부자재_1인봉투"}:
        return ("운영", f"{label} 운영 기준 재확인")
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장"}:
        return ("후속", f"{label} 개선 가능 여부 확인")
    if key in {"메뉴_1인메뉴확대", "메뉴_신메뉴도입", "메뉴_홀등록요청"}:
        return ("후속", "신메뉴·키워드 확대 진행 상황 확인")
    if action_text:
        return ("후속", action_text)
    if action_texts:
        return ("후속", action_texts[0])
    return ("운영", f"{label} 후속 필요 여부 재확인")


def _handover_axis_for_text(value: str) -> str:
    text = _as_text(value)
    if any(token in text for token in ["자료", "데이터", "비교", "순이익", "수익", "실적", "주문"]):
        return "자료"
    if any(token in text for token in ["토더", "발주", "공지", "설정", "운영", "마감", "사입"]):
        return "운영"
    return "후속"


def _build_handover_summary(
    store_traits: list[str],
    problem_issues: list[dict[str, Any]],
    action_texts: list[str],
) -> str:
    forbidden = set(store_traits)
    axis_order = {"후속": 0, "자료": 1, "운영": 2}
    by_axis: dict[str, str] = {}
    for issue in problem_issues:
        axis, item = _handover_issue_candidate(issue, action_texts)
        cleaned = _clean_handover_item(item, forbidden)
        if cleaned and axis not in by_axis:
            by_axis[axis] = cleaned
    if len(by_axis) < 3:
        for item in action_texts:
            cleaned = _clean_handover_item(item, forbidden)
            if cleaned and cleaned not in by_axis.values():
                by_axis.setdefault(_handover_axis_for_text(cleaned), cleaned)
    rows = [item for axis, item in sorted(by_axis.items(), key=lambda row: axis_order.get(row[0], 99))]
    return _handover_bullet_summary(rows)


def _parse_todo_due_date(text: str, visit_date: Any) -> str | None:
    base = pd.to_datetime(visit_date, errors="coerce")
    if pd.isna(base):
        return None
    value = _as_text(text)
    match = re.search(r"(20\d{2})[-./년\s]+(\d{1,2})[-./월\s]+(\d{1,2})", value)
    if match:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    match = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일?\s*까지", value)
    if match:
        year = int(base.year)
        month = int(match.group(1))
        day = int(match.group(2))
        try:
            return dt.date(year, month, day).isoformat()
        except ValueError:
            return None
    weekdays = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}
    match = re.search(r"(이번\s*주|다음\s*주)?\s*([월화수목금토일])요일?\s*까지", value)
    if match:
        target = weekdays[match.group(2)]
        current = int(base.weekday())
        delta = (target - current) % 7
        if "다음" in _as_text(match.group(1)):
            delta += 7 if delta == 0 else 7
        elif delta == 0:
            delta = 0
        return (base.date() + dt.timedelta(days=delta)).isoformat()
    return None


def _todo_owner(issue: dict[str, Any]) -> str:
    key = _as_text(issue.get("issue_key"))
    if key in {"광고_우가클단가", "광고_즉시할인", "광고_쿠팡노출률", "광고_한그릇하나만"}:
        return "마케팅 담당"
    if key in {"묵은지_숙성경도", "우거지_질김", "계육_순살품질", "계육_뼈닭내장", "대창_외관", "파김치_소포장"}:
        return "상품 담당"
    if key in {"발주_마켓봄전환", "발주_마감시한", "부자재_1인봉투", "용기_규격도입", "용기_불량"}:
        return "물류 담당"
    if key in {"메뉴_1인메뉴확대", "메뉴_신메뉴도입", "메뉴_홀등록요청"}:
        return "SV 담당"
    return "담당자 미정"


def _todo_status(issue: dict[str, Any]) -> str:
    status = _as_text(issue.get("status"))
    if status == "진행중":
        return "진행"
    if status in {"해결", "안내완료"}:
        return "완료"
    return "대기"


def _is_actionable_request(issue: dict[str, Any]) -> bool:
    if not bool(issue.get("is_request")):
        return False
    if issue.get("status") in {"해결", "안내완료"}:
        return False
    text = _issue_text(issue)
    key = _as_text(issue.get("issue_key"))
    if key in {"매출_배달정체", "매출_홀부진", "수익_감소체감", "고정비_부담"}:
        return bool(re.search(r"(자료|실적|매출|수익).{0,20}(요청|문의|확인해|공유해|보내)", text))
    if re.search(r"(요청|문의|건의|전달|등록\s*요청|확인\s*요청|개선\s*요청|해\s*주세요|해주세요|희망|좋겠)", text):
        return True
    if "확인" in text and any(token in text for token in ["부탁", "요청", "회신"]):
        return True
    return False


def _build_post_handover_summary(store_name: str, issues: list[dict[str, Any]]) -> str:
    if not issues:
        return "누적 히스토리에서 반복 특성을 더 확인해야 함"
    visit_dates = [_as_text(issue.get("visit_date")) for issue in issues if _as_text(issue.get("visit_date"))]
    if not visit_dates:
        return "누적 히스토리에서 반복 특성을 더 확인해야 함"
    counts: dict[str, int] = {}
    for issue in issues:
        key = _as_text(issue.get("issue_key"))
        counts[key] = counts.get(key, 0) + 1
    return "\n".join(_store_trait_texts(issues, counts))


def _build_profile_copy(
    store_name: str,
    posts: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    recurring_counts: dict[str, int],
) -> dict[str, Any]:
    latest_visit_date = _as_text(posts[0].get("visit_date")) if posts else ""
    latest_issues = [issue for issue in issues if _as_text(issue.get("visit_date")) == latest_visit_date]
    current_or_open_issues = [
        issue for issue in issues
        if _as_text(issue.get("visit_date")) == latest_visit_date
        or (
            _as_text(issue.get("visit_date")) != latest_visit_date
            and issue.get("status") in {"미해결", "진행중"}
        )
    ]
    ranked = sorted(
        current_or_open_issues or issues,
        key=lambda issue: (_issue_priority(issue, latest_visit_date, recurring_counts), _as_text(issue.get("visit_date"))),
        reverse=True,
    )
    latest_ranked = sorted(
        latest_issues,
        key=lambda issue: (_issue_priority(issue, latest_visit_date, recurring_counts), _as_text(issue.get("visit_date"))),
        reverse=True,
    )
    concern_ranked = latest_issues or ranked
    project_id = _as_text(posts[0].get("project_id")) if posts else ""
    # analysis_evidence가 화두의 정본이다. key_concerns / handling_points는 여기서 파생시켜
    # 1:1 대응과 부분집합 관계를 자료구조로 보장한다.
    evidence = _evidence_rows(concern_ranked or issues, CONCERN_LIMIT, project_id)
    key_concerns = [row["concern"] for row in evidence]
    handling_points = [row["concern"] for row in evidence if row["is_problem"]]
    store_traits = _store_trait_texts(issues, recurring_counts)
    owner_status = _owner_state_sentence(issues, latest_issues or ranked)
    problem_issues = [issue for issue in (concern_ranked or issues) if _is_problem_issue(issue)]
    # 방문 단위 액션도 근거가 있는 값만 쓴다.
    # LLM이 쓴 next_action -> 매핑된 액션문 -> 제외.
    # "OO 처리 상태 확인"은 이슈명을 되풀이할 뿐이라 방문 요약에서도 뺀다.
    action_texts = _dedupe_keep_order(
        [
            _clean_display_text(issue.get("next_action"), prompts.NEXT_ACTION_MAX_CHARS)
            or _handling_text(issue, allow_generic=False)
            for issue in problem_issues
        ],
        3,
    )
    handover_summary = _build_handover_summary(store_traits, problem_issues, action_texts)
    next_visit_action = _build_next_visit_action([], action_texts)
    return {
        "owner_status": owner_status,
        "store_status_summary": "\n".join(store_traits),
        "key_concerns": key_concerns,
        "handling_points": handling_points,
        "handover_summary": handover_summary,
        "next_visit_action": next_visit_action,
        "action_hints": action_texts,
        "analysis_evidence": evidence,
    }


def _history_issue_row(issue: dict[str, Any]) -> dict[str, Any]:
    owner_voice = _clean_display_text(issue.get("owner_voice") or issue.get("owner_voice_raw"), 160)
    evidence = _clean_display_text(issue.get("evidence") or issue.get("raw_text"), 120)
    problem_detail = _problem_detail_text(issue)
    return {
        "visit_date": issue.get("visit_date"),
        "post_id": _as_text(issue.get("post_id")),
        "issue_seq": issue.get("issue_seq"),
        "issue_key": issue.get("issue_key"),
        "issue_label": issue.get("issue_label"),
        "category": issue.get("category"),
        "owner_voice": owner_voice,
        "sv_action": _clean_display_text(issue.get("sv_action"), 160),
        "is_request": bool(issue.get("is_request")),
        "status": issue.get("status"),
        "is_problem": _is_problem_issue(issue),
        "problem_detail": problem_detail,
        "evidence": evidence,
    }


def _build_store_history_digest(
    store_name: str,
    posts: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    recurring_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    sorted_posts = sorted(posts, key=lambda row: row.get("visit_date") or "", reverse=True)
    latest_post = sorted_posts[0] if sorted_posts else {}
    recent_dates = [
        _as_text(post.get("visit_date"))
        for post in sorted_posts[:3]
        if _as_text(post.get("visit_date"))
    ]
    latest_issues = [
        _history_issue_row(issue)
        for issue in issues
        if _as_text(issue.get("visit_date")) == _as_text(latest_post.get("visit_date"))
    ][:12]
    recent_issues = [
        _history_issue_row(issue)
        for issue in issues
        if _as_text(issue.get("visit_date")) in set(recent_dates)
    ][:24]
    unresolved = [
        _history_issue_row(issue)
        for issue in issues
        if issue.get("status") in {"미해결", "진행중"}
    ][:24]
    requests = [
        _history_issue_row(issue)
        for issue in issues
        if bool(issue.get("is_request")) and issue.get("status") not in {"해결", "안내완료"}
    ][:16]
    visit_summaries = []
    for post in sorted_posts[:8]:
        visit_issues = [
            _history_issue_row(issue)
            for issue in post.get("issues") or []
        ][:10]
        visit_summaries.append({
            "visit_date": post.get("visit_date"),
            "post_id": _as_text(post.get("post_id")),
            "visit_purpose": _clean_display_text(post.get("visit_purpose"), 80),
            "issues": visit_issues,
        })
    return {
        "store_name": store_name,
        "project_id": _as_text(latest_post.get("project_id")),
        "last_visit_date": latest_post.get("visit_date"),
        "visit_count": len(sorted_posts),
        "latest_visit": visit_summaries[0] if visit_summaries else {},
        "recent_visit_dates": recent_dates,
        "recent_issues": recent_issues,
        "unresolved_issues": unresolved,
        "explicit_requests": requests,
        "recurring_patterns": recurring_rows[:10],
        "visit_history": visit_summaries,
    }


def _compact_local_profile_digest(history_digest: dict[str, Any]) -> dict[str, Any]:
    def item_id(prefix: str, idx: int) -> str:
        return f"{prefix}{idx}"

    def issue_candidate(prefix: str, row: dict[str, Any], idx: int, text_limit: int = 50) -> dict[str, Any]:
        candidate = {
            "id": item_id(prefix, idx),
            "visit_date": row.get("visit_date"),
            "post_id": _as_text(row.get("post_id")),
            "issue_seq": row.get("issue_seq"),
            "issue_key": row.get("issue_key"),
            "label": _clean_display_text(row.get("issue_label"), 40),
            "request": bool(row.get("is_request")),
            "status": row.get("status"),
            "evidence": _clean_display_text(row.get("owner_voice") or row.get("evidence"), text_limit),
        }
        if row.get("category"):
            candidate["category"] = row.get("category")
        problem_detail = _clean_display_text(row.get("problem_detail"), text_limit)
        if bool(row.get("is_problem")) or problem_detail:
            candidate["problem"] = bool(row.get("is_problem"))
            candidate["problem_detail"] = problem_detail
        return candidate

    latest = [
        issue_candidate("L", row, idx)
        for idx, row in enumerate((history_digest.get("latest_visit") or {}).get("issues") or [], 1)
    ][:6]
    unresolved = [
        issue_candidate("U", row, idx)
        for idx, row in enumerate(history_digest.get("unresolved_issues") or [], 1)
    ][:6]
    requests = [
        issue_candidate("Q", row, idx)
        for idx, row in enumerate(history_digest.get("explicit_requests") or [], 1)
    ][:5]
    recurring = []
    for idx, row in enumerate(history_digest.get("recurring_patterns") or [], 1):
        recurring.append({
            "id": item_id("R", idx),
            "issue_key": row.get("issue_key"),
            "label": _clean_display_text(row.get("issue_label"), 40),
            "cnt": row.get("cnt"),
            "dates": row.get("dates") or [],
        })
        if len(recurring) >= 6:
            break
    return {
        "store_name": history_digest.get("store_name"),
        "project_id": history_digest.get("project_id"),
        "last_visit_date": history_digest.get("last_visit_date"),
        "visit_count": history_digest.get("visit_count"),
        "local_candidates": {
            "latest": latest,
            "recurring": recurring,
            "unresolved": unresolved,
            "requests": requests,
        },
    }


def _history_hash(history_digest: dict[str, Any]) -> str:
    stable = json.dumps(history_digest, ensure_ascii=False, sort_keys=True, default=str)
    return _sha1_text(stable)


def _load_profile_cache() -> dict[str, Any]:
    if not FLOW_VISIT_PROFILE_CACHE.exists():
        return {}
    try:
        return json.loads(FLOW_VISIT_PROFILE_CACHE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Flow 방문일지 프로필 캐시 읽기 실패, 새로 생성합니다: %s", exc)
        return {}


def _save_profile_cache(cache: dict[str, Any]) -> None:
    FLOW_VISIT_PROFILE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = FLOW_VISIT_PROFILE_CACHE.with_suffix(FLOW_VISIT_PROFILE_CACHE.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, FLOW_VISIT_PROFILE_CACHE)


def _profile_cache_key(project_id: Any, store_name: Any, history_digest: dict[str, Any], provider: str, model: str) -> str:
    return "|".join([
        _as_text(project_id),
        _store_key(store_name),
        _history_hash(history_digest),
        PROMPT_VERSION,
        SCHEMA_VERSION,
        provider,
        model,
    ])


def _query_openai_profile_json(prompt: str, system_prompt: str) -> dict[str, Any]:
    try:
        from openai import OpenAI
    except Exception as exc:
        raise RuntimeError("openai 패키지가 설치되어 있지 않습니다.") from exc
    client = OpenAI()
    response = client.chat.completions.create(
        model=PROFILE_OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def _query_history_profile_json(history_digest: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    provider = PROFILE_LLM_PROVIDER
    if provider in {"", "off", "false", "0", "none"}:
        return None, None
    if provider == "local":
        from modules.transform.utility.qwen_client import get_ollama_client_with_candidates, query_qwen_json

        local_digest = _compact_local_profile_digest(history_digest)
        prompt, system_prompt = prompts.build_profile_prompt(local_digest)
        if len(prompt) > PROFILE_LOCAL_MAX_PROMPT_CHARS:
            logger.warning(
                "Flow 방문일지 local 프로필 프롬프트가 제한을 초과했습니다: chars=%s limit=%s",
                len(prompt),
                PROFILE_LOCAL_MAX_PROMPT_CHARS,
            )
        client, model_candidates = get_ollama_client_with_candidates()
        model_candidates = _gpt_oss_profile_candidates(model_candidates)
        preferred_models = _preferred_gpt_oss_profile_models(model_candidates)
        result = query_qwen_json(
            prompt,
            system_prompt=system_prompt,
            preferred_models=preferred_models,
            client=client,
            model_candidates=model_candidates,
            options_override={"temperature": 0, "top_p": 0.2, "num_predict": 1800, "num_ctx": 12000},
        )
        return result, f"local_gpt_oss_profile:{preferred_models[0] if preferred_models else model_candidates[0]}"
    if provider == "openai":
        prompt, system_prompt = prompts.build_profile_prompt(history_digest)
        return _query_openai_profile_json(prompt, system_prompt), PROFILE_OPENAI_MODEL
    logger.warning("지원하지 않는 FLOW_VISIT_PROFILE_PROVIDER=%s, 규칙 기반 프로필로 대체합니다.", provider)
    return None, None


def _gpt_oss_profile_candidates(model_candidates: list[str]) -> list[str]:
    candidates = [model for model in model_candidates if "gpt-oss" in _as_text(model)]
    if not candidates:
        raise RuntimeError(f"Flow 방문일지 profile local LLM에 사용할 gpt-oss 모델이 없습니다: {model_candidates}")
    return candidates


def _preferred_gpt_oss_profile_models(model_candidates: list[str]) -> list[str]:
    configured = (os.getenv("FLOW_VISIT_PROFILE_LLM_MODELS", "") or "").strip()
    if configured:
        requested = [model.strip() for model in configured.split(",") if model.strip()]
        blocked = [model for model in requested if "gpt-oss" not in model]
        if blocked:
            logger.warning("Flow profile local LLM에서 qwen/비 gpt-oss 모델 요청을 무시합니다: %s", blocked)
        preferred = [model for model in requested if "gpt-oss" in model and model in set(model_candidates)]
        if preferred:
            return preferred
    return [model for model in prompts.GPT_OSS_MODELS if model in set(model_candidates)] or model_candidates


def _local_profile_cache_model() -> str:
    configured = (os.getenv("FLOW_VISIT_PROFILE_LLM_MODELS", "") or "").strip()
    if configured:
        requested = [model.strip() for model in configured.split(",") if model.strip()]
        gpt_requested = [model for model in requested if "gpt-oss" in model]
        if gpt_requested:
            return ",".join(gpt_requested)
    return ",".join(prompts.GPT_OSS_MODELS)


def _merge_existing_project_rows(path: Path, new_df: pd.DataFrame, columns: list[str], project_ids: list[str]) -> pd.DataFrame:
    new_df = new_df.reindex(columns=columns)
    if not project_ids or not path.exists():
        return new_df
    try:
        existing = pd.read_parquet(path).reindex(columns=columns)
    except Exception as exc:
        logger.warning("기존 단일 파일 마트 읽기 실패, 신규 데이터만 저장합니다: %s | %s", path, exc)
        return new_df
    if existing.empty or "project_id" not in existing.columns:
        return new_df
    keep = existing[~existing["project_id"].astype(str).isin(set(project_ids))].copy()
    if keep.empty:
        return new_df
    return pd.concat([keep, new_df], ignore_index=True).reindex(columns=columns)


def _normalize_evidence_rows(value: Any, fallback: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return fallback
    rows = []
    seen_concerns: set[str] = set()
    fallback_by_key = {_as_text(row.get("issue_key")): row for row in fallback if _as_text(row.get("issue_key"))}
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        evidence = _clean_display_text(item.get("evidence"), 180)
        fallback_row = fallback_by_key.get(_as_text(item.get("issue_key"))) or (fallback[idx] if idx < len(fallback) else {})
        if not evidence:
            evidence = _clean_display_text(fallback_row.get("evidence"), 180)
        concern = _clean_display_text(item.get("concern"), 90)
        if not concern or "_" in concern:
            concern = _clean_display_text(fallback_row.get("concern"), 90) or concern
        issue_key = _as_text(item.get("issue_key")) or _as_text(fallback_row.get("issue_key"))
        issue_label = _clean_display_text(item.get("issue_label"), 40) or _clean_display_text(fallback_row.get("issue_label"), 40)
        problem_detail = _clean_display_text(item.get("problem_detail"), 220)
        if len(problem_detail) < 40:
            problem_detail = _clean_display_text(fallback_row.get("problem_detail"), 220) or problem_detail
        owner_voice = _clean_display_text(item.get("owner_voice"), 180) or _clean_display_text(fallback_row.get("owner_voice"), 180)
        sv_action = _clean_display_text(item.get("sv_action"), 180) or _clean_display_text(fallback_row.get("sv_action"), 180)
        status = _as_text(item.get("status")) or _as_text(fallback_row.get("status"))
        if not concern or not (evidence or problem_detail):
            continue
        concern_key = _compact_key(concern)
        if concern_key in seen_concerns:
            # concern 1건 = 행 1건. 중복 행을 남기면 key_concerns와의 1:1 대응이 깨진다.
            continue
        seen_concerns.add(concern_key)
        item_is_problem = item.get("is_problem")
        if isinstance(item_is_problem, bool):
            is_problem = item_is_problem
        else:
            is_problem = bool(fallback_row.get("is_problem"))
        rows.append({
            "concern": concern,
            "issue_key": issue_key,
            "issue_label": issue_label,
            "issue_rel_key": _as_text(item.get("issue_rel_key")) or fallback_row.get("issue_rel_key") or None,
            "visit_date": _as_text(item.get("visit_date")) or fallback_row.get("visit_date") or None,
            "post_id": _as_text(item.get("post_id")) or _as_text(fallback_row.get("post_id")),
            "problem_detail": problem_detail,
            "owner_voice": owner_voice,
            "sv_action": sv_action,
            "status": status,
            "evidence": evidence,
            "is_problem": is_problem,
            "action_hint": _as_text(item.get("action_hint")) or _as_text(fallback_row.get("action_hint")),
        })
        if len(rows) >= CONCERN_LIMIT:
            break
    return rows or fallback


def _normalize_history_profile_result(result: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    owner_status = _clean_owner_status(
        result.get("owner_status") or result.get("owner_sentiment"),
        fallback["owner_status"],
    )
    traits = _compact_store_trait_texts(
        _profile_list_texts(result.get("store_status_summary"), fallback["store_status_summary"].splitlines(), limit=5),
        3,
    )
    key_concerns = _profile_list_texts(
        result.get("key_concerns"), fallback["key_concerns"], limit=CONCERN_LIMIT, text_limit=90
    )
    # handling_points는 key_concerns의 부분집합이므로 겹치는 문구를 배제하지 않는다.
    handling_points = _profile_list_texts(
        result.get("handling_points"), fallback["handling_points"], limit=CONCERN_LIMIT, text_limit=90
    )
    next_actions = _list_texts(result.get("next_visit_action"), 2, 80)
    raw_key_concerns = _list_texts(result.get("key_concerns"), CONCERN_LIMIT, 90)
    raw_handling_points = _list_texts(result.get("handling_points"), CONCERN_LIMIT, 90)
    handover_forbidden = (
        set(traits)
        | set(key_concerns)
        | set(handling_points)
        | set(raw_key_concerns)
        | set(raw_handling_points)
        | set(fallback.get("key_concerns") or [])
        | set(fallback.get("handling_points") or [])
    )
    handover_items = _handover_texts(result.get("handover_summary"), handover_forbidden, limit=3)
    fallback_handover_items = _handover_texts(fallback.get("handover_summary"), handover_forbidden, limit=3)
    normalized = dict(fallback)
    normalized.update({
        "owner_status": owner_status,
        "store_status_summary": "\n".join(traits) if traits else fallback["store_status_summary"],
        "key_concerns": key_concerns or fallback["key_concerns"],
        "handling_points": handling_points,
        "handover_summary": _handover_bullet_summary(handover_items or fallback_handover_items),
        "next_visit_action": " → ".join(next_actions) if next_actions else fallback["next_visit_action"],
        "analysis_evidence": _normalize_evidence_rows(result.get("analysis_evidence"), fallback["analysis_evidence"]),
        "todos": result.get("todos") if isinstance(result.get("todos"), list) else [],
    })
    return _enforce_concern_invariants(normalized)


def _enforce_concern_invariants(profile: dict[str, Any]) -> dict[str, Any]:
    """analysis_evidence를 정본으로 삼아 화두와 문제·고민의 관계를 강제한다.

    보장하는 것:
    - key_concerns == 각 analysis_evidence 행의 concern (1:1)
    - handling_points는 key_concerns의 부분집합
    위치(index) 기반 정렬은 중복 문구가 접히는 순간 어긋나므로 쓰지 않는다.
    """
    rows = [
        row for row in (profile.get("analysis_evidence") or [])
        if isinstance(row, dict) and _clean_display_text(row.get("concern"), 90)
    ]
    if not rows:
        return profile
    for row in rows:
        row["concern"] = _clean_display_text(row.get("concern"), 90)
    declared = {_compact_key(item) for item in (profile.get("handling_points") or []) if _compact_key(item)}
    for row in rows:
        row["is_problem"] = (
            bool(row.get("is_problem"))
            or _compact_key(row["concern"]) in declared
        )
    profile["analysis_evidence"] = rows
    profile["key_concerns"] = [row["concern"] for row in rows]
    profile["handling_points"] = [row["concern"] for row in rows if row["is_problem"]]
    return profile


def _build_profile_copy_from_history(
    store_name: str,
    project_id: Any,
    posts: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    recurring_rows: list[dict[str, Any]],
    recurring_counts: dict[str, int],
    profile_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = _build_profile_copy(store_name, posts, issues, recurring_counts)
    history_digest = _build_store_history_digest(store_name, posts, issues, recurring_rows)
    provider = PROFILE_LLM_PROVIDER
    model = PROFILE_OPENAI_MODEL if provider == "openai" else _local_profile_cache_model()
    cache_key = _profile_cache_key(project_id, store_name, history_digest, provider, model)
    cache = profile_cache if profile_cache is not None else _load_profile_cache()
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        profile = _normalize_history_profile_result(cached.get("result") or cached, fallback)
        if provider == "local":
            profile["analysis_evidence"] = fallback["analysis_evidence"]
            profile["todos"] = []
        _enforce_concern_invariants(profile)
        profile["llm_model"] = cached.get("llm_model") or model
        profile["_profile_cache_key"] = cache_key
        return profile
    try:
        result, llm_model = _query_history_profile_json(history_digest)
    except Exception as exc:
        logger.warning("Flow 방문일지 히스토리 프로필 LLM 실패, 규칙 기반으로 대체합니다: %s", exc)
        result, llm_model = None, None
    if not isinstance(result, dict) or result.get("parse_error"):
        fallback["llm_model"] = "rule_history_fallback"
        fallback["_profile_cache_key"] = cache_key
        return fallback
    profile = _normalize_history_profile_result(result, fallback)
    if provider == "local":
        profile["analysis_evidence"] = fallback["analysis_evidence"]
        profile["todos"] = []
    _enforce_concern_invariants(profile)
    profile["llm_model"] = llm_model or model
    profile["_profile_cache_key"] = cache_key
    cache[cache_key] = {
        "project_id": _as_text(project_id),
        "store_key": _store_key(store_name),
        "prompt_version": PROMPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "provider": provider,
        "llm_model": profile["llm_model"],
        "history_hash": _history_hash(history_digest),
        "result": result,
        "cached_at": _generated_at(),
    }
    if profile_cache is None:
        _save_profile_cache(cache)
    return profile


def _profile_stats(posts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int], list[dict[str, Any]], list[str], dict[str, int]]:
    issues = [issue for post in posts for issue in post.get("issues") or []]
    counts = {category: 0 for category in CATEGORY_ORDER}
    recurring: dict[str, dict[str, Any]] = {}
    for issue in issues:
        category = issue.get("category") if issue.get("category") in VALID_CATEGORIES else "기타"
        counts[category] = counts.get(category, 0) + 1
        key = issue.get("issue_key") or "기타"
        item = recurring.setdefault(key, {
            "issue_key": key,
            "issue_label": issue.get("issue_label") or key,
            "row_cnt": 0,
            "dates": [],
        })
        item["row_cnt"] += 1
        item["dates"].append(issue.get("visit_date") or "")
        issue["issue_label"] = _issue_display_label(issue.get("issue_key"), issue.get("issue_label"))
    recurring_rows = []
    for item in recurring.values():
        dates = sorted({date for date in item["dates"] if date})
        visit_cnt = len(dates)
        if visit_cnt >= 2:
            recurring_rows.append({
                "issue_key": item["issue_key"],
                "issue_label": _issue_display_label(item["issue_key"], item["issue_label"]),
                "cnt": visit_cnt,
                "first_date": dates[0] if dates else None,
                "last_date": dates[-1] if dates else None,
                "dates": dates,
            })
    recurring_rows = sorted(recurring_rows, key=lambda row: row["cnt"], reverse=True)
    open_issues = [
        f"{issue.get('issue_key')}: {issue.get('issue_label')}"
        for issue in issues
        if issue.get("status") in {"미해결", "진행중"}
    ][:10]
    recurring_counts = {
        key: len({date for date in item["dates"] if date})
        for key, item in recurring.items()
    }
    return issues, counts, recurring_rows, open_issues, recurring_counts


def _period_start_for_visit(current_post: dict[str, Any], previous_post: dict[str, Any] | None) -> str | None:
    visit_date = pd.to_datetime(current_post.get("visit_date"), errors="coerce")
    if pd.isna(visit_date):
        return None
    month_start = visit_date.replace(day=1).date()
    if previous_post is None:
        return month_start.isoformat()
    prev_date = pd.to_datetime(previous_post.get("visit_date"), errors="coerce")
    if pd.isna(prev_date) or prev_date.strftime("%Y-%m") != visit_date.strftime("%Y-%m"):
        return month_start.isoformat()
    start = prev_date.date() + dt.timedelta(days=1)
    if start > visit_date.date():
        start = visit_date.date()
    return start.isoformat()


def _build_profile_snapshot(
    project_id: Any,
    store_name: Any,
    current_post: dict[str, Any],
    history_posts: list[dict[str, Any]],
    previous_post: dict[str, Any] | None,
    visit_seq_in_month: int,
    profile_cache: dict[str, Any] | None,
) -> dict[str, Any]:
    issues, counts, recurring_rows, open_issues, recurring_counts = _profile_stats(history_posts)
    profile_copy = _build_profile_copy_from_history(
        _as_text(store_name),
        project_id,
        sorted(history_posts, key=lambda row: row.get("visit_date") or "", reverse=True),
        issues,
        recurring_rows,
        recurring_counts,
        profile_cache,
    )
    rels = _relation_keys(project_id, store_name, current_post.get("post_id"))
    return {
        "project_id": _as_text(project_id),
        "store_name": store_name,
        "store_key": rels["store_key"],
        "store_rel_key": rels["store_rel_key"],
        "visit_rel_key": rels["visit_rel_key"],
        "post_id": _as_text(current_post.get("post_id")),
        "visit_date": current_post.get("visit_date"),
        "visit_seq_in_month": visit_seq_in_month,
        "period_start": _period_start_for_visit(current_post, previous_post),
        "period_end": current_post.get("visit_date"),
        "profile_as_of_date": current_post.get("visit_date"),
        "visit_cnt_as_of": len(history_posts),
        "owner_status": profile_copy["owner_status"],
        "store_status_summary": profile_copy["store_status_summary"],
        "key_concerns": profile_copy["key_concerns"],
        "handling_points": profile_copy["handling_points"],
        "open_issues": open_issues,
        "recurring_issues": recurring_rows,
        "category_counts": counts,
        "handover_summary": profile_copy["handover_summary"],
        "next_visit_action": profile_copy["next_visit_action"],
        "analysis_evidence": profile_copy["analysis_evidence"],
        "llm_model": profile_copy.get("llm_model") or current_post.get("llm_model") or "unknown",
    }


def _dag_conf(context: dict[str, Any] | None) -> dict[str, Any]:
    if not context:
        return {}
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    return conf if isinstance(conf, dict) else {}


def _split_conf_values(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = re.split(r"[,;\n\r\t]+", _as_text(value))
    return [text for item in raw_values if (text := _as_text(item).strip())]


def _iter_flow_visit_target_rows(value: Any) -> list[dict[str, Any]]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RuntimeError("flow_visit_targets는 JSON 배열 또는 객체여야 합니다.") from exc
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list):
        raise RuntimeError("flow_visit_targets는 JSON 배열 또는 객체여야 합니다.")
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(value, start=1):
        if not isinstance(row, dict):
            raise RuntimeError(f"flow_visit_targets[{idx}]는 객체여야 합니다.")
        rows.append(row)
    return rows


def _normalize_target_store_name(value: Any) -> str:
    text = re.sub(r"\s+", "", _as_text(value))
    text = _strip_trailing_parens(text)
    if text.startswith("도리당"):
        text = text[len("도리당"):]
    return text


def _flow_visit_target_request(context: dict[str, Any] | None) -> dict[str, Any]:
    conf = _dag_conf(context)
    project_ids: set[str] = set()
    store_names: set[str] = set()

    source_targets = context.get("flow_visit_targets") if context else None
    source_project_ids = context.get("flow_visit_project_ids") if context else None
    source_store_names = context.get("flow_visit_store_names") if context else None
    # 명시적인 실행 대상은 DAG의 기본 목록을 대체한다.
    if any(conf.get(key) for key in ("flow_visit_targets", "flow_visit_project_ids", "flow_visit_store_names")):
        source_targets = conf.get("flow_visit_targets")
        source_project_ids = conf.get("flow_visit_project_ids")
        source_store_names = conf.get("flow_visit_store_names")
    if not source_targets:
        source_targets = conf.get("flow_visit_targets")
    if not source_project_ids:
        source_project_ids = conf.get("flow_visit_project_ids")
    if not source_store_names:
        source_store_names = conf.get("flow_visit_store_names")

    store_name_by_id: dict[str, str] = {}
    for row in _iter_flow_visit_target_rows(source_targets):
        row_project_ids = _split_conf_values(row.get("project_id") or row.get("projectId"))
        row_store_names = _split_conf_values(row.get("store_name") or row.get("storeName") or row.get("name"))
        project_ids.update(row_project_ids)
        store_names.update(row_store_names)
        if len(row_project_ids) == 1 and len(row_store_names) == 1:
            store_name_by_id[next(iter(row_project_ids))] = next(iter(row_store_names))

    project_ids.update(_split_conf_values(source_project_ids))
    store_names.update(_split_conf_values(source_store_names))

    labels = sorted(project_ids | store_names)
    return {
        "project_ids": project_ids,
        "store_names": store_names,
        "explicit": bool(project_ids or store_names),
        "labels": labels,
        "store_name_by_id": store_name_by_id,
    }


def _project_ids_for_store_names(project_df: pd.DataFrame, store_names: set[str]) -> set[str]:
    if not store_names or project_df.empty or "project_id" not in project_df.columns:
        return set()

    target_names = {_normalize_target_store_name(name) for name in store_names}
    matched: set[str] = set()
    candidate_columns = [column for column in ("store_name", "project_name") if column in project_df.columns]
    for _, row in project_df.iterrows():
        for column in candidate_columns:
            normalized = _normalize_target_store_name(row.get(column))
            if normalized and normalized in target_names:
                project_id = _as_text(row.get("project_id")).strip()
                if project_id:
                    matched.add(project_id)
    return matched


def _target_project_ids(project_df: pd.DataFrame, context: dict[str, Any] | None = None) -> set[str]:
    request = _flow_visit_target_request(context)
    if request["explicit"]:
        if request["project_ids"]:
            return set(request["project_ids"])
        return _project_ids_for_store_names(project_df, request["store_names"])

    raw = (os.getenv("FLOW_VISIT_PROJECT_IDS", TARGET_PROJECT_IDS) or "").strip()
    if raw:
        return {part.strip() for part in raw.split(",") if part.strip()}
    if "is_store" not in project_df.columns:
        return set(project_df["project_id"].dropna().astype(str))
    return set(project_df[project_df["is_store"].fillna(False)]["project_id"].dropna().astype(str))


def _fallback_visit_payload_from_existing_mart(target_ids: set[str]) -> dict[str, Any] | None:
    if not FLOW_VISIT_LOG_PARQUET.exists():
        return None
    try:
        log_df = pd.read_parquet(FLOW_VISIT_LOG_PARQUET)
    except Exception as exc:
        logger.warning("기존 방문일지 로그 fallback 읽기 실패: %s", exc)
        return None
    if log_df.empty or "project_id" not in log_df.columns:
        return None
    log_df = log_df[log_df["project_id"].astype(str).isin(target_ids)].copy()
    if log_df.empty:
        return None
    if "content_text" not in log_df.columns:
        log_df["content_text"] = ""
    if "content_clean" in log_df.columns:
        log_df["content_text"] = log_df["content_text"].where(
            log_df["content_text"].fillna("").astype(str).str.strip().ne(""),
            log_df["content_clean"],
        )
    if "title" not in log_df.columns:
        log_df["title"] = ""
    log_df["title"] = log_df["title"].where(
        log_df["title"].fillna("").astype(str).str.strip().ne(""),
        log_df.apply(
            lambda row: f"{row.get('visit_date') or ''} {row.get('store_name') or ''} 방문일지".strip(),
            axis=1,
        ),
    )
    if "post_date" not in log_df.columns:
        log_df["post_date"] = log_df.get("registered_date")
    if "registered_at" not in log_df.columns:
        log_df["registered_at"] = log_df.get("registered_date")
    if "project_name" not in log_df.columns:
        log_df["project_name"] = log_df.get("store_name")

    comments = pd.DataFrame()
    if FLOW_VISIT_FOLLOWUP_PARQUET.exists():
        try:
            comments = pd.read_parquet(FLOW_VISIT_FOLLOWUP_PARQUET)
        except Exception as exc:
            logger.warning("기존 방문일지 followup fallback 읽기 실패: %s", exc)
            comments = pd.DataFrame()
    if not comments.empty:
        comments = comments[comments["post_id"].astype(str).isin(log_df["post_id"].astype(str))].copy()
        comments["author_name"] = comments.get("responder", "")
        comments["content_text"] = comments.get("reply_text", "")
        comments["is_system"] = False
    logger.warning(
        "Flow 원천 방문일지 0건으로 기존 마트 로그 fallback 사용: posts=%s comments=%s",
        len(log_df),
        len(comments),
    )
    return {"posts": _records(log_df), "comments": _records(comments)}


def extract_visit_logs(**context) -> dict[str, Any]:
    analytics_flow_dir = MART_DB.parent / "analytics" / "flow"
    post = _read_parquet_first([
        FLOW_POST_PARQUET,
        analytics_flow_dir / "flow_post",
        MART_DB / "flow" / "flow_post",
        FLOW_LEGACY_POST_PARQUET,
        analytics_flow_dir / "flow_post.parquet",
        MART_DB / "flow" / "flow_post.parquet",
    ])
    comments = _read_parquet_first([
        FLOW_COMMENT_PARQUET,
        analytics_flow_dir / "flow_comment",
        MART_DB / "flow" / "flow_comment",
        FLOW_LEGACY_COMMENT_PARQUET,
        analytics_flow_dir / "flow_comment.parquet",
        MART_DB / "flow" / "flow_comment.parquet",
    ], required=False)
    projects = _read_parquet_first([
        FLOW_PROJECT_PARQUET,
        analytics_flow_dir / "flow_project" / "flow_project.parquet",
        analytics_flow_dir / "flow_project",
        MART_DB / "flow" / "flow_project" / "flow_project.parquet",
        FLOW_LEGACY_PROJECT_PARQUET,
        analytics_flow_dir / "flow_project.parquet",
        MART_DB / "flow" / "flow_project.parquet",
    ], required=False)

    if "project_id" not in post.columns:
        raise RuntimeError("FLOW_POST_PARQUET은 base dir 전체를 읽어 project_id 파티션 컬럼이 복원되어야 합니다.")
    project_source = projects if not projects.empty else post[["project_id"]].drop_duplicates()
    target_request = _flow_visit_target_request(context)
    target_ids = _target_project_ids(project_source, context=context)
    target_labels = ", ".join(target_request["labels"])
    available_project_ids = set(post["project_id"].dropna().astype(str))
    missing_project_ids = target_ids - available_project_ids
    if missing_project_ids and target_request["explicit"]:
        raise RuntimeError(
            "Flow 방문일지 대상 프로젝트 원천 누락: "
            f"{', '.join(sorted(missing_project_ids))}. "
            f"요청 대상={target_labels or sorted(target_ids)}. "
            "먼저 Strategy_FlowStore_01_Collect_Dags 수집 결과에 해당 프로젝트가 있는지 확인하세요."
        )
    post = post[post["project_id"].astype(str).isin(target_ids)].copy()
    if post.empty and target_request["explicit"]:
        raise RuntimeError(
            "Flow 방문일지 대상 프로젝트 데이터 없음: "
            f"{target_labels or sorted(target_ids)}. "
            "먼저 Strategy_FlowStore_01_Collect_Dags 수집 결과에 해당 프로젝트가 있는지 확인하세요."
        )
    if not comments.empty and "project_id" in comments.columns:
        comments = comments[comments["project_id"].astype(str).isin(target_ids)].copy()

    title_norm = post["title"].fillna("").astype(str).map(lambda value: re.sub(r"\s+", "", value))
    body = post["content_text"].fillna("").astype(str)
    visit_mask = title_norm.str.contains("방문일지", regex=False, na=False) | body.str.contains(r"방문\s*일자", regex=True, na=False)
    visit_posts = post[visit_mask].copy()
    missing_visit_project_ids = target_ids - set(visit_posts["project_id"].dropna().astype(str))
    if missing_visit_project_ids:
        # 오픈 준비 중인 신규 매장은 raw 게시글은 있어도 방문일지가 아직 0건일 수 있다.
        # 대상 목록에 이런 매장이 섞이는 건 정상 운영이므로 실패가 아니라 경고로 남기고 진행한다.
        store_name_by_id = target_request.get("store_name_by_id") or {}
        missing_labels = [
            f"{store_name_by_id.get(project_id) or '?'}({project_id})"
            for project_id in sorted(missing_visit_project_ids)
        ]
        logger.warning(
            "Flow 방문일지 0건 매장 제외하고 진행: %s / 방문일지 있는 매장=%s/%s",
            ", ".join(missing_labels),
            len(target_ids) - len(missing_visit_project_ids),
            len(target_ids),
        )
    if visit_posts.empty:
        fallback = _fallback_visit_payload_from_existing_mart(target_ids)
        if fallback is not None:
            return fallback
        if target_request["explicit"]:
            raise RuntimeError(
                "Flow 방문일지 게시글이 0건입니다: "
                f"{target_labels or sorted(target_ids)}. "
                "대상 프로젝트에 방문일지 게시글이 수집됐는지 확인하세요."
            )
        raise RuntimeError("Flow 방문일지 게시글이 0건입니다.")
    visit_post_ids = set(visit_posts["post_id"].astype(str))
    subtask_posts = pd.DataFrame()
    if "parent_post_id" in post.columns:
        post_work = post.copy()
        post_work["_post_id_text"] = post_work["post_id"].fillna("").astype(str)
        post_work["_parent_post_id_text"] = post_work["parent_post_id"].fillna("").astype(str)
        post_title_by_id = {
            _as_text(row.get("post_id")): _as_text(row.get("title") or row.get("task_nm"))
            for row in _records(post_work)
        }
        subtask_meta: dict[str, dict[str, Any]] = {}
        subtask_indexes: list[Any] = []
        queue: list[tuple[str, str, int, list[str]]] = [
            (visit_post_id, visit_post_id, 0, []) for visit_post_id in sorted(visit_post_ids)
        ]
        seen_subtasks: set[str] = set()
        while queue:
            direct_parent_id, visit_parent_id, parent_depth, parent_path = queue.pop(0)
            children = post_work[post_work["_parent_post_id_text"].eq(direct_parent_id)]
            for index, child in children.iterrows():
                child_id = _as_text(child.get("post_id"))
                if not child_id or child_id in seen_subtasks or child_id in visit_post_ids:
                    continue
                seen_subtasks.add(child_id)
                child_title = _as_text(child.get("title") or child.get("task_nm"))
                child_path = parent_path + ([child_title] if child_title else [])
                subtask_indexes.append(index)
                subtask_meta[child_id] = {
                    "visit_parent_post_id": visit_parent_id,
                    "direct_parent_post_id": direct_parent_id,
                    "direct_parent_title": post_title_by_id.get(direct_parent_id, ""),
                    "subtask_depth": parent_depth + 1,
                    "subtask_path_titles": " > ".join(child_path),
                }
                queue.append((child_id, visit_parent_id, parent_depth + 1, child_path))
        if subtask_indexes:
            subtask_posts = post_work.loc[subtask_indexes].drop(
                columns=["_post_id_text", "_parent_post_id_text"],
                errors="ignore",
            ).copy()
            for col in [
                "visit_parent_post_id",
                "direct_parent_post_id",
                "direct_parent_title",
                "subtask_depth",
                "subtask_path_titles",
            ]:
                subtask_posts[col] = subtask_posts["post_id"].astype(str).map(
                    lambda post_id, key=col: subtask_meta.get(post_id, {}).get(key, "")
                )
    subtask_post_ids = set(subtask_posts["post_id"].astype(str)) if not subtask_posts.empty else set()
    if not comments.empty and "post_id" in comments.columns:
        comments = comments[comments["post_id"].astype(str).isin(visit_post_ids | subtask_post_ids)].copy()

    logger.info(
        "Flow 방문일지 추출 완료: posts=%s subtasks=%s comments=%s",
        len(visit_posts),
        len(subtask_posts),
        len(comments),
    )
    return {"posts": _records(visit_posts), "subtasks": _records(subtask_posts), "comments": _records(comments)}


def _clean_content(text: Any) -> str:
    cleaned = _as_text(text)
    cleaned = cleaned.replace("\xa0", " ").replace("\u200b", " ")
    cleaned = re.sub(r"https://docs\.google\.com/\S+", "", cleaned)
    cleaned = re.sub(r"[=~_\-]{4,}", "\n", cleaned)
    cleaned = re.sub(
        r"주제\s*[1-9]\s*(전달내용|내용)\s*(가맹점의견|점주의견)?\s*(답변사항|담당자\s*최종\s*의견|담당자\s*의견)?\s*(?=주제|\Z)",
        "",
        cleaned,
    )
    cleaned = re.sub(r"[ \t]{3,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _date_from_match(match: re.Match[str], default_year: int | None = None) -> str | None:
    groups = match.groups()
    try:
        if len(groups) == 3:
            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
            if year < 100:
                year += 2000
        else:
            if default_year is None:
                return None
            year, month, day = default_year, int(groups[0]), int(groups[1])
        return dt.date(year, month, day).isoformat()
    except ValueError:
        return None


def _parse_post_date(value: Any) -> str | None:
    text = _as_text(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    try:
        if len(digits) >= 14:
            return dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S").date().isoformat()
        if len(digits) == 8:
            return dt.datetime.strptime(digits, "%Y%m%d").date().isoformat()
    except ValueError:
        pass
    try:
        return pd.to_datetime(text).date().isoformat()
    except Exception:
        return None


def _parse_post_time(value: Any) -> str | None:
    text = _as_text(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    try:
        if len(digits) >= 14:
            return dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S").strftime("%H:%M")
    except ValueError:
        pass
    try:
        return pd.to_datetime(text).strftime("%H:%M")
    except Exception:
        return None


def _find_date(text: str, default_year: int | None = None) -> tuple[str | None, str | None]:
    for pattern in _DATE_PATTERNS:
        match = re.search(pattern, text)
        if match:
            parsed = _date_from_match(match)
            if parsed:
                return parsed, "full"
    match = re.search(_MD_PATTERN, text)
    if match:
        parsed = _date_from_match(match, default_year=default_year)
        if parsed:
            return parsed, "md"
    return None, None


def _parse_visit_date(title: Any, content: str, post_date: Any) -> tuple[str, str]:
    fallback = _parse_post_date(post_date)
    default_year = int(fallback[:4]) if fallback else dt.date.today().year
    parsed, kind = _find_date(_as_text(title), default_year)
    if parsed:
        return parsed, "title_md" if kind == "md" else "title"
    body_match = re.search(r"방문\s*일자\s*[:：]?\s*(.{0,25})", content)
    if body_match:
        parsed, _ = _find_date(body_match.group(1), default_year)
        if parsed:
            return parsed, "body"
    if fallback:
        return fallback, "post_date"
    raise RuntimeError(f"방문일자를 파싱할 수 없습니다: {_as_text(title)[:80]}")


def _related_context_for_post(
    post: dict[str, Any],
    subtasks: list[dict[str, Any]],
    comments: list[dict[str, Any]],
) -> str:
    post_id = _as_text(post.get("post_id"))
    rows = []
    for subtask in subtasks:
        if _as_text(subtask.get("visit_parent_post_id") or subtask.get("parent_post_id")) != post_id:
            continue
        title = _clean_display_text(subtask.get("title") or subtask.get("task_nm"), 80)
        body = _clean_display_text(_clean_content(subtask.get("content_text")), 260)
        comment_text = _clean_display_text(
            " ".join(_as_text(row.get("content_text")) for row in _raw_comments_for_post(comments, subtask.get("post_id"))),
            220,
        )
        parts = [part for part in [title, body, comment_text] if part]
        if parts:
            rows.append("- " + " / ".join(parts))
    return "\n".join(rows)


def _parse_topic_table(content: str) -> tuple[str | None, list[dict[str, str]] | None]:
    purpose = None
    purpose_match = re.search(r"방문\s*목적\s*(.*?)(?=주제\s*1|\Z)", content, re.S)
    if purpose_match:
        purpose = re.sub(r"\s+", " ", purpose_match.group(1)).strip() or None

    rows: list[dict[str, str]] = []
    for match in re.finditer(r"주제\s*([1-8])(.*?)(?=주제\s*[1-8]|\Z)", content, re.S):
        block = match.group(2).strip()
        split = re.match(
            r"^(.*?)(?:전달\s*내용|내용)(.*?)(?:가맹점\s*의견|점주\s*의견)(.*?)(?:답변\s*사항|담당자\s*최종\s*의견|담당자\s*의견)(.*)$",
            block,
            re.S,
        )
        if split:
            topic, message, owner, answer = split.group(1), split.group(2), split.group(3), split.group(4)
        else:
            split = re.match(r"^(.*?)(?:전달\s*내용|내용)(.*?)(?:가맹점\s*의견|점주\s*의견)(.*)$", block, re.S)
            if split:
                topic, message, owner = split.group(1), split.group(2), split.group(3)
                answer = ""
            else:
                topic, message, owner, answer = block, "", "", ""
        row = {
            "topic_no": match.group(1),
            "topic": re.sub(r"\s+", " ", topic).strip(),
            "message": re.sub(r"\s+", " ", message).strip(),
            "owner_opinion": re.sub(r"\s+", " ", owner).strip(),
            "answer_note": re.sub(r"\s+", " ", answer).strip(),
        }
        if row["message"] or row["owner_opinion"] or row["answer_note"]:
            rows.append(row)
    return purpose, rows or None


def parse_visit_meta(payload: dict[str, Any], **context) -> dict[str, Any]:
    posts = []
    subtasks = payload.get("subtasks") or []
    comments = payload.get("comments") or []
    for post in payload.get("posts") or []:
        content_clean = _clean_content(post.get("content_text"))
        related_context = _related_context_for_post(post, subtasks, comments)
        if related_context:
            content_clean = f"{content_clean}\n\n[하위 글/업무]\n{related_context}".strip()
        visit_date, source = _parse_visit_date(post.get("title"), content_clean, post.get("post_date"))
        purpose, topic_table = _parse_topic_table(content_clean)
        enriched = dict(post)
        enriched.update({
            "visit_date": visit_date,
            "visit_date_source": source,
            "registered_date": _parse_post_date(post.get("registered_at") or post.get("post_date")),
            "visit_purpose": purpose,
            "content_clean": content_clean,
            "topic_table": topic_table,
        })
        posts.append(enriched)
    payload = dict(payload)
    payload["posts"] = posts
    logger.info("Flow 방문일지 메타 파싱 완료: %s건", len(posts))
    return payload


def _load_cache() -> dict[str, Any]:
    if not FLOW_VISIT_LLM_CACHE.exists():
        return {}
    try:
        return json.loads(FLOW_VISIT_LLM_CACHE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Flow 방문일지 LLM 캐시 읽기 실패, 새로 생성합니다: %s", exc)
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    FLOW_VISIT_LLM_CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = FLOW_VISIT_LLM_CACHE.with_suffix(FLOW_VISIT_LLM_CACHE.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, FLOW_VISIT_LLM_CACHE)


def _comments_for_post(comments: list[dict[str, Any]], post_id: Any) -> list[dict[str, Any]]:
    pid = _as_text(post_id)
    result = []
    for comment in comments:
        if _as_text(comment.get("post_id")) != pid:
            continue
        if bool(comment.get("is_system")):
            continue
        text = _as_text(comment.get("content_text")).strip()
        if not text:
            continue
        result.append(comment)
    return result


def _raw_comments_for_post(comments: list[dict[str, Any]], post_id: Any) -> list[dict[str, Any]]:
    pid = _as_text(post_id)
    rows = []
    for comment in comments:
        if _as_text(comment.get("post_id")) != pid:
            continue
        text = _as_text(comment.get("content_text")).strip()
        if not text:
            continue
        rows.append(comment)
    return sorted(rows, key=lambda row: _as_text(row.get("written_at")))


def _joined_comment_authors(comments: list[dict[str, Any]]) -> str:
    authors = []
    for comment in comments:
        author = _as_text(comment.get("author_name")).strip()
        if author and author not in authors:
            authors.append(author)
    return ",".join(authors)


def _comment_group(comment: dict[str, Any]) -> str:
    text = _as_text(comment.get("content_text"))
    sys_code = _as_text(comment.get("sys_code"))
    groups = []
    if bool(comment.get("is_system")):
        if "S45" in sys_code or "상태를 변경" in text:
            groups.append("상태변경")
        if "S47" in sys_code or "시작일" in text:
            groups.append("시작일변경")
        if "S48" in sys_code or "마감일" in text:
            groups.append("마감일변경")
        if "S38" in sys_code or "제목을 변경" in text:
            groups.append("제목변경")
        if not groups:
            groups.append("시스템댓글")
    else:
        groups.append("일반댓글")
    try:
        reply_cnt = int(_as_text(comment.get("reply_cnt")) or "0")
    except ValueError:
        reply_cnt = 0
    if reply_cnt > 0:
        groups.append("대댓글있음")
    return "/".join(groups)


def _joined_comment_groups(comments: list[dict[str, Any]]) -> str:
    groups = []
    for comment in comments:
        group = _comment_group(comment)
        if group == "일반댓글" and any("일반댓글" in existing.split("/") for existing in groups):
            continue
        if group and group not in groups:
            groups.append(group)
    return ",".join(groups)


def _latest_status_from_comments(comments: list[dict[str, Any]]) -> str | None:
    status = None
    for comment in sorted(comments, key=lambda row: _as_text(row.get("written_at"))):
        text = _as_text(comment.get("content_text"))
        sys_code = _as_text(comment.get("sys_code"))
        if not bool(comment.get("is_system")) and "상태를 변경" not in text and "S45" not in sys_code:
            continue
        match = re.search(r"'([^']+)'\s*→\s*'([^']+)'\s*,?\s*상태를 변경", text)
        if match:
            status = match.group(2)
            continue
        for segment in sys_code.split("@$%"):
            parts = segment.split("^^")
            if len(parts) >= 3 and parts[0] == "S45":
                status = FLOW_TASK_STATUS_CODE_LABELS.get(parts[2], parts[2])
    return status


def _task_status_from_subtask(subtask: dict[str, Any], comments: list[dict[str, Any]]) -> str | None:
    comment_status = _latest_status_from_comments(comments)
    if comment_status:
        return comment_status
    status = _as_text(subtask.get("task_status")).strip()
    if status:
        return status
    code = _as_text(subtask.get("STTS") or subtask.get("stts")).strip()
    if code:
        return FLOW_TASK_STATUS_CODE_LABELS.get(code, code)
    return None


def _joined_comment_text(comments: list[dict[str, Any]]) -> str:
    return "\n".join(
        _as_text(comment.get("content_text")).strip()
        for comment in comments
        if _as_text(comment.get("content_text")).strip()
    )


def _comment_sort_key(comment: dict[str, Any]) -> tuple[str, int, str]:
    try:
        order = int(_as_text(comment.get("comment_order")) or "0")
    except ValueError:
        order = 0
    return (
        _as_text(comment.get("written_at")),
        order,
        _as_text(comment.get("comment_id")),
    )


def _comment_history_json(comments: list[dict[str, Any]]) -> str:
    if not comments:
        return json.dumps({"groups": [], "system_events": []}, ensure_ascii=False)

    comments = sorted(comments, key=_comment_sort_key)
    by_id = {_as_text(comment.get("comment_id")): comment for comment in comments}
    roots: dict[str, dict[str, Any]] = {}
    systems: list[dict[str, Any]] = []

    for comment in comments:
        text = _as_text(comment.get("content_text")).strip()
        if not text:
            continue
        item = {
            "comment_id": _as_text(comment.get("comment_id")),
            "parent_comment_id": _as_text(comment.get("parent_comment_id")),
            "author_name": _as_text(comment.get("author_name")),
            "written_at": _as_text(comment.get("written_at")),
            "content_text": text,
            "comment_group": _comment_group(comment),
        }
        if bool(comment.get("is_system")):
            systems.append(item)
            continue

        root_id = _as_text(comment.get("root_comment_id")) or item["comment_id"]
        parent_id = item["parent_comment_id"]
        if parent_id and parent_id in by_id:
            root_id = _as_text(by_id[parent_id].get("root_comment_id")) or parent_id
        root_comment = by_id.get(root_id, comment)
        group_author = _as_text(root_comment.get("author_name")) or item["author_name"]
        if not group_author:
            group_author = "(작성자 없음)"
        group = roots.setdefault(group_author, {"group_author": group_author, "comments": []})
        group["comments"].append(item)

    return json.dumps(
        {"groups": list(roots.values()), "system_events": systems},
        ensure_ascii=False,
    )


def _usable_comments(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        comment
        for comment in comments
        if not flow_visit_segmenter.is_noise_comment(comment.get("content_text"))
    ]


def _segment_has_issue_signal(segment: dict[str, Any], issue_candidates: list[dict[str, Any]]) -> bool:
    text = "\n".join([
        _as_text(segment.get("topic")),
        _as_text(segment.get("raw_text")),
        _as_text(segment.get("owner_voice_raw")),
        _as_text(segment.get("sv_action_raw")),
    ])
    generic_headings = {
        "점주님요청사항",
        "점주요청사항",
        "담당자의견",
        "점주의견",
        "점주님의견",
    }
    raw_compact = re.sub(r"\s+", "", _as_text(segment.get("raw_text")))
    topic_compact = re.sub(r"\s+", "", _as_text(segment.get("topic")))
    raw_stripped = re.sub(r"^\d+[.)]?", "", raw_compact)
    if raw_stripped in generic_headings or topic_compact in generic_headings:
        return False
    if _has_policy_boundary(text):
        return True
    for issue in issue_candidates:
        if issue.get("key") == "기타":
            continue
        for alias in issue.get("aliases") or []:
            if alias and alias in text:
                return True
    return any(
        token in text
        for token in ["요청", "문의", "건의", "희망", "필요", "개선", "변경", "완료", "진행", "부담", "답답"]
    )


def _evidence_ok(evidence: str, content: str) -> bool:
    normalized_evidence = re.sub(r"\s+", "", evidence or "")
    normalized_content = re.sub(r"\s+", "", content or "")
    return bool(normalized_evidence and normalized_evidence in normalized_content)


_SALES_AD_PROFIT_KEYS = {
    "매출_홀부진",
    "매출_배달정체",
    "수익_감소체감",
    "고정비_부담",
    "상권_악화",
    "매장이전_양도양수",
    "운영_홀배달동시한계",
    "광고_우가클단가",
    "광고_즉시할인",
    "광고_쿠팡노출률",
    "광고_한그릇하나만",
    "배달대행_배정지연",
    "플랫폼_배달팁설정",
}
_LOGISTICS_PURCHASE_PREFIXES = (
    "계육_",
    "김치_",
    "묵은지_",
    "우거지_",
    "소스_",
    "대창_",
    "파김치_",
    "용기_",
    "발주_",
    "사입_",
    "부자재_",
    "유니폼_",
    "리뷰이벤트_",
)
_POLICY_SUBJECT_RE = re.compile(
    r"(기준|조건|예외|권한|책임|비용|보상|지원|지원금|의무|절차|승인|허용|가능\s*여부|"
    r"적용|기간|종료|중단|약정서|동의|서명|계약|위반|사입\s*의심|지정\s*상품|대체품|"
    r"가격\s*인상|가격\s*변경|판매\s*채널|판매\s*가능|홀\s*판매|참여\s*조건|대상\s*선정)"
)
_POLICY_SUPPORT_RE = re.compile(
    r"(지원\s*(대상|가맹점|금액|조건|비율|범위|리스트)|"
    r"(물류|사입|광고|할인)\s*지원|"
    r"(보상|반품|교환|회수|패널티)\s*(기준|조건)|"
    r"(계약|약정서|동의|서명|승인)\s*(필요|여부|절차)|"
    r"(양도양수|사입\s*의심|계약\s*위반|예외\s*승인))"
)
_POLICY_DECISION_RE = re.compile(
    r"(정하|정하겠|정하였|정했|변경|바꾸|개정|수립|결정|검토|판단|승인|허용|불가|가능한지|"
    r"필요|요구|확대|적용|산출|선정|공지|설문|약정|동의|서명|분담|부담)"
)
_EXECUTION_ONLY_RE = re.compile(
    r"(등록\s*처리|설정\s*처리|확인\s*완료|안내\s*완료|전달\s*완료|업로드|입력|일정\s*조율|"
    r"자료\s*요청|정보\s*요청|담당자\s*연결|기존\s*기준|기준에\s*따라)"
)
_LOGISTICS_EXECUTION_RE = re.compile(
    r"(발주|입고|배송|재고|품절|불량|오배송|반품|교환|공급업체|물량|식자재|원부자재|포장재|계육|소스|용기|"
    r"(?:상품|제품|물품)\s*(?:을|를)?\s*회수)"
)
_SALES_AD_PROFIT_RE = re.compile(
    r"(매출|매출현황|판매현황|판매\s*현황|주문현황|주문\s*현황|주문량|판매량|고객\s*유입|광고|배달\s*플랫폼|"
    r"쿠폰|프로모션|노출|클릭|전환율|원가율|마진|수익률|손익|순이익|순수익)"
)


def _normalize_category(value: Any) -> str:
    category = _as_text(value).strip()
    return category if category in VALID_CATEGORIES else ""


def _has_policy_boundary(text: str) -> bool:
    value = re.sub(r"\s+", " ", _as_text(text)).strip()
    if not value:
        return False
    if _EXECUTION_ONLY_RE.search(value) and not re.search(r"(예외|선례|새로|변경|확대|약정|동의|서명|전체|모든)", value):
        return False
    support = bool(_POLICY_SUPPORT_RE.search(value))
    if support:
        return True
    # 다른 문장의 계약 현황과 육아 부담 등을 묶어 정책 결정으로 오인하지 않는다.
    clauses = re.split(r"\n+|(?<=[.!?])\s+|\*\s*|ㄴ\s*", _as_text(text))
    if any(_POLICY_SUBJECT_RE.search(clause) and _POLICY_DECISION_RE.search(clause)
           for clause in clauses):
        return True
    return any(
        re.search(r"(모든|전체|전\s*가맹점|타\s*가맹점|다른\s*가맹점|선례|공통)", clause)
        and _POLICY_SUBJECT_RE.search(clause)
        for clause in clauses
    )


def _infer_category(issue_key: Any, text: Any, proposed: Any = None) -> str:
    evidence = _as_text(text)
    proposed_category = _normalize_category(proposed)
    if _has_policy_boundary(evidence):
        return "정책"
    if proposed_category == "정책":
        return "정책"
    if proposed_category in {"매출/광고/수익", "물류/사입"}:
        return proposed_category
    key = _as_text(issue_key)
    if key in _SALES_AD_PROFIT_KEYS or _SALES_AD_PROFIT_RE.search(evidence):
        return "매출/광고/수익"
    if key.startswith(_LOGISTICS_PURCHASE_PREFIXES) or _LOGISTICS_EXECUTION_RE.search(evidence):
        return "물류/사입"
    return "기타"


def _heuristic_extract(post: dict[str, Any], comments: list[dict[str, Any]]) -> dict[str, Any]:
    issue_by_key, issues = _issue_maps()
    text = f"{post.get('title') or ''}\n{post.get('content_clean') or ''}"
    found: list[dict[str, Any]] = []
    for issue in issues:
        key = issue["key"]
        if key == "기타":
            continue
        aliases = issue.get("aliases") or []
        matched = next((alias for alias in aliases if alias and alias in text), None)
        if not matched:
            continue
        evidence_text = _snippet_around(text, matched)
        found.append({
            "category": _infer_category(key, evidence_text, issue.get("category")),
            "issue_key": key,
            "issue_label": key.replace("_", " "),
            "owner_voice": matched,
            "sv_action": "",
            "opinion_source": "담당자판단",
            "is_request": any(token in text for token in ["요청", "문의", "건의", "필요"]),
            "severity": "보통",
            "status": "미해결",
            "evidence": evidence_text[:40],
        })
    if not found and len(_as_text(post.get("content_clean"))) >= 80:
        snippet = re.sub(r"\s+", " ", _as_text(post.get("content_clean"))).strip()[:40]
        found.append({
            "category": _infer_category("기타", snippet, issue_by_key["기타"].get("category")),
            "issue_key": "기타",
            "issue_label": "방문일지 주요 내용",
            "owner_voice": snippet,
            "sv_action": "",
            "opinion_source": "담당자판단",
            "is_request": False,
            "severity": "낮음",
            "status": "미해결",
            "evidence": snippet,
        })
    followups = [{
        "responder": _as_text(comment.get("author_name")),
        "reply_text": _as_text(comment.get("content_text")),
        "issue_key": None,
        "resolution_status": "안내완료",
    } for comment in comments]
    return {
        "store_status": f"{post.get('store_name') or post.get('project_name') or ''} 방문일지",
        "owner_status": "중립",
        "issues": found,
        "hq_followups": followups,
        "llm_model": "taxonomy_fallback",
    }


def _build_prompt(post: dict[str, Any], comments: list[dict[str, Any]]) -> str:
    _, issues = _issue_maps()
    issue_lines = [
        f"- {issue['category']}: {issue['key']} (aliases: {', '.join(issue.get('aliases') or [])})"
        for issue in issues
    ]
    topic_table = post.get("topic_table") or []
    hint = ""
    if topic_table:
        hint_lines = []
        for row in topic_table:
            parts = [
                f"{row.get('topic_no')}. {row.get('topic')}",
                f"전달내용: {row.get('message')}",
                f"가맹점의견: {row.get('owner_opinion')}",
            ]
            answer_note = _as_text(row.get("answer_note")).strip()
            if answer_note:
                parts.append(f"답변사항: {answer_note}")
            hint_lines.append(" / ".join(parts))
        hint = (
            "\n[구조화 힌트]\n"
            "전달내용은 본사 전달·공지·요청이고, 가맹점의견은 점주 의견·반응이며, 답변사항은 본사 후속 회신이다.\n"
            "세 역할을 섞지 말고 항목별로 분리해서 읽는다.\n"
            + "\n".join(hint_lines)
        )
    comment_text = "\n".join(f"{c.get('author_name')}: {c.get('content_text')}" for c in comments)
    return (
        f"매장: {post.get('store_name')} / 방문일자: {post.get('visit_date')} / "
        f"방문목적: {post.get('visit_purpose')} / 작성자: {post.get('author_name')}\n\n"
        "[허용 issue_key 목록]\n"
        + "\n".join(issue_lines)
        + hint
        + f"\n\n[본문]\n{post.get('content_clean')}\n\n[본사 댓글]\n{comment_text}"
    )


def _normalize_llm_result(result: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    issue_by_key, _ = _issue_maps()
    normalized = {
        "store_status": _as_text(result.get("store_status"))[:300],
        "owner_status": _clean_display_text(result.get("owner_status") or result.get("owner_sentiment"), 180) or "최근 방문에서 점주 상태를 판단할 구체 근거가 부족함",
        "issues": [],
        "hq_followups": result.get("hq_followups") if isinstance(result.get("hq_followups"), list) else [],
        "llm_model": _as_text(result.get("llm_model")) or "ollama",
    }
    seen: dict[str, dict[str, Any]] = {}
    for raw in result.get("issues") or []:
        if not isinstance(raw, dict):
            continue
        key = _as_text(raw.get("issue_key")) or "기타"
        if key not in issue_by_key:
            key = "기타"
        issue_meta = issue_by_key[key]
        evidence_text = "\n".join([
            _as_text(raw.get("issue_label")),
            _as_text(raw.get("owner_voice")),
            _as_text(raw.get("sv_action")),
            _as_text(raw.get("evidence")),
            _as_text(post.get("title")),
            _as_text(post.get("content_clean")),
        ])
        row = {
            "category": _infer_category(key, evidence_text, raw.get("category") or issue_meta.get("category")),
            "issue_key": key,
            "issue_label": _as_text(raw.get("issue_label")) or key.replace("_", " "),
            "owner_voice": _as_text(raw.get("owner_voice")),
            "sv_action": _as_text(raw.get("sv_action")),
            "opinion_source": raw.get("opinion_source") if raw.get("opinion_source") in VALID_SOURCE else "담당자판단",
            "is_request": bool(raw.get("is_request")),
            "severity": raw.get("severity") if raw.get("severity") in VALID_SEVERITY else "보통",
            "status": raw.get("status") if raw.get("status") in VALID_STATUS else "미해결",
            "evidence": _as_text(raw.get("evidence"))[:40],
        }
        if key in seen:
            seen[key]["owner_voice"] = "\n".join(filter(None, [seen[key]["owner_voice"], row["owner_voice"]]))
            seen[key]["sv_action"] = "\n".join(filter(None, [seen[key]["sv_action"], row["sv_action"]]))
        else:
            seen[key] = row
    normalized["issues"] = list(seen.values())
    if not normalized["issues"]:
        normalized = _heuristic_extract(post, [])
    return normalized


def _fallback_segment_issue(post: dict[str, Any], segment: dict[str, Any], comments: list[dict[str, Any]]) -> dict[str, Any]:
    _, issues = _issue_maps()
    text = " ".join([
        _as_text(segment.get("topic")),
        _as_text(segment.get("raw_text")),
        " ".join(_as_text(row.get("content_text")) for row in comments),
    ])
    candidates = prompts.select_issue_candidates(_as_text(post.get("store_name")), segment, issues, None, limit=8)
    rule_result = _rule_class_result(segment, candidates, [])
    key = _resolve_issue_key(rule_result.get("issue_key") if rule_result else "기타", text)
    issue_by_key, _ = _issue_maps()
    meta = issue_by_key.get(key) or issue_by_key["기타"]
    owner_voice = _first_clause(segment.get("owner_voice_raw") or segment.get("raw_text"), 80)
    evidence_text = "\n".join([
        _as_text(segment.get("topic")),
        _as_text(segment.get("raw_text")),
        _as_text(segment.get("owner_voice_raw")),
        _as_text(segment.get("sv_action_raw")),
    ])
    return {
        "seg_id": segment.get("seg_id"),
        "source_kind": segment.get("source_kind"),
        "category": _infer_category(key, evidence_text, (rule_result or {}).get("category") or meta.get("category")),
        "issue_key": key,
        "issue_label": _issue_display_label(key, segment.get("topic")),
        "owner_voice": owner_voice,
        "sv_action": _first_clause(segment.get("sv_action_raw"), 80),
        "owner_voice_raw": _as_text(segment.get("owner_voice_raw")),
        "sv_action_raw": _as_text(segment.get("sv_action_raw")),
        "raw_text": _as_text(segment.get("raw_text")),
        "opinion_source": segment.get("opinion_source") if segment.get("opinion_source") in VALID_SOURCE else "담당자판단",
        "is_request": any(token in text for token in ["요청", "문의", "건의", "희망", "필요", "개선"]),
        "severity": "보통",
        "status": _status_from_text(text),
        "evidence": _as_text(segment.get("raw_text"))[:80],
        "llm_model": "rule_fallback",
        "is_fallback": True,
        "grounding_flag": False,
    }


def _normalize_segment_issue(
    segment: dict[str, Any],
    class_result: dict[str, Any],
    summary_result: dict[str, Any],
    model_name: str,
) -> dict[str, Any]:
    issue_by_key, _ = _issue_maps()
    evidence_text = "\n".join([
        _as_text(segment.get("raw_text")),
        _as_text(segment.get("sv_action_raw")),
        _as_text(segment.get("owner_voice_raw")),
    ])
    key = _resolve_issue_key(class_result.get("issue_key"), evidence_text)
    meta = issue_by_key.get(key) or issue_by_key["기타"]
    category = _infer_category(key, evidence_text, class_result.get("category") or meta.get("category"))
    # 3단 폴백: LLM이 쓴 요약 -> (구 캐시의) 문장 번호 선택 -> 원문 첫 절.
    # 구 pick 형식 캐시 엔트리와 호환을 유지한다.
    owner_summary = prompts.normalize_summary(summary_result.get("owner_summary"))
    sv_summary = prompts.normalize_summary(summary_result.get("sv_summary"))
    if not owner_summary and summary_result.get("owner_pick") is not None:
        owner_sentences = prompts.split_sentences(segment.get("owner_voice_raw") or segment.get("raw_text"), max_items=6)
        owner_summary = prompts.assemble_summary(owner_sentences, summary_result.get("owner_pick"))
    if not sv_summary and summary_result.get("sv_pick") is not None:
        sv_sentences = prompts.split_sentences(segment.get("sv_action_raw"), max_items=6)
        sv_summary = prompts.assemble_summary(sv_sentences, summary_result.get("sv_pick"))
    if not owner_summary:
        owner_summary = _first_clause(segment.get("owner_voice_raw") or segment.get("raw_text"), 80)
    if not sv_summary:
        sv_summary = _first_clause(segment.get("sv_action_raw"), 80)
    # 문장 선택(tier 2)은 split_sentences 결과를 그대로 쓰므로 목차 번호가 남는다.
    # 세 경로 모두 같은 정리를 거치게 한다.
    owner_summary = _clean_display_text(owner_summary, 200)
    sv_summary = _clean_display_text(sv_summary, 200)
    # LLM이 안 돌아 문장 선택으로 떨어진 경우에도 "요약" 칸에 문단이 통째로 들어가면 안 된다.
    if len(owner_summary) > 80:
        owner_summary = _first_clause(owner_summary, 80)
    if len(sv_summary) > 80:
        sv_summary = _first_clause(sv_summary, 80)
    label = _issue_display_label(
        key,
        prompts.normalize_label(summary_result.get("issue_label"), _as_text(segment.get("topic")) or key.replace("_", " ")),
    )
    return {
        "seg_id": segment.get("seg_id"),
        "source_kind": segment.get("source_kind"),
        "category": category,
        "issue_key": key,
        "issue_label": label,
        "owner_voice": owner_summary,
        "sv_action": sv_summary,
        "owner_voice_raw": _as_text(segment.get("owner_voice_raw")),
        "sv_action_raw": _as_text(segment.get("sv_action_raw")),
        "raw_text": _as_text(segment.get("raw_text")),
        "opinion_source": segment.get("opinion_source") if segment.get("opinion_source") in VALID_SOURCE else "담당자판단",
        "is_request": bool(class_result.get("is_request")),
        # 요약 LLM이 판단한 "현재 문제·고민" 여부. LLM이 안 돌면 None으로 남고 규칙이 판단한다.
        "is_concern": summary_result.get("is_concern") if isinstance(summary_result.get("is_concern"), bool) else None,
        "severity": class_result.get("severity") if class_result.get("severity") in VALID_SEVERITY else "보통",
        "status": class_result.get("status") if class_result.get("status") in VALID_STATUS else "미해결",
        "evidence": _as_text(segment.get("raw_text"))[:80],
        "llm_model": model_name,
        "is_fallback": False,
        "grounding_flag": _grounding_flag(" ".join([owner_summary, sv_summary, label]), evidence_text),
    }


def _recover_class_result(
    result: Any,
    segment: dict[str, Any],
    issue_candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    raw = _as_text(result.get("raw_response"))
    if not raw:
        return None
    candidate_keys = [_as_text(issue.get("key")) for issue in issue_candidates if issue.get("key")]
    evidence = "\n".join([raw, _as_text(segment.get("raw_text"))])
    picked = next((key for key in candidate_keys if key and key in raw), None)
    if not picked:
        return None
    severity = next((value for value in VALID_SEVERITY if value in raw), "보통")
    status = next((value for value in VALID_STATUS if value in raw), _status_from_text(evidence))
    category = next((value for value in CATEGORY_ORDER if value in raw), None)
    return {
        "issue_key": picked,
        "category": _infer_category(picked, evidence, category),
        "severity": severity,
        "status": status if status in VALID_STATUS else "미해결",
        "is_request": any(token in evidence for token in ["요청", "문의", "건의", "희망", "필요", "개선"]),
        "llm_recovered": True,
    }


def _rule_class_result(
    segment: dict[str, Any],
    issue_candidates: list[dict[str, Any]],
    comments: list[dict[str, Any]],
) -> dict[str, Any] | None:
    topic_text = _as_text(segment.get("topic"))
    owner_text = _as_text(segment.get("owner_voice_raw"))
    sv_text = _as_text(segment.get("sv_action_raw"))
    raw_text = _as_text(segment.get("raw_text"))
    comment_text = "\n".join(_as_text(row.get("content_text")) for row in comments)
    text = "\n".join([topic_text, raw_text, sv_text, owner_text, comment_text])
    best: tuple[int, str] | None = None
    for issue in issue_candidates:
        key = _as_text(issue.get("key"))
        if key == "기타":
            continue
        aliases = [alias for alias in issue.get("aliases") or [] if alias]
        # 표의 공통 안내문만으로 점주의 다른 의견을 덮지 않는다.
        if owner_text and not prompts.is_empty_content(owner_text):
            if not any(alias in topic_text or alias in owner_text for alias in aliases):
                continue
        score = 0
        for alias in aliases:
            if alias in topic_text:
                score += 100 + min(len(alias), 20)
            if alias in owner_text:
                score += 60 + min(len(alias), 20)
            if alias in sv_text:
                score += 35 + min(len(alias), 20)
            if alias in raw_text:
                score += 20 + min(len(alias), 20)
            if comment_text and alias in comment_text:
                score += 8 + min(len(alias), 20)
        if score and (best is None or score > best[0]):
            best = (score, key)
    if best:
        return {
            "issue_key": best[1],
            "category": _infer_category(best[1], text),
            "severity": "높음" if any(token in text for token in ["부상", "보험", "이취", "법", "위반"]) else "보통",
            "status": _status_from_text(text),
            "is_request": any(token in text for token in ["요청", "문의", "건의", "희망", "필요", "개선"]),
            "rule_confidence": "alias_score",
        }
    return None


def _deterministic_summary(segment: dict[str, Any], issue_key: str) -> dict[str, Any]:
    owner_sentences = prompts.split_sentences(segment.get("owner_voice_raw") or segment.get("raw_text"), max_items=4)
    sv_sentences = prompts.split_sentences(segment.get("sv_action_raw"), max_items=4)
    label = _as_text(segment.get("topic")) or issue_key.replace("_", " ")
    return {
        "owner_pick": [1] if owner_sentences else [],
        "sv_pick": [1] if sv_sentences else [],
        "issue_label": label[:12],
    }


def _snippet_around(text: str, token: str, radius: int = 90) -> str:
    idx = text.find(token)
    if idx < 0:
        return text[: radius * 2].strip()
    start = max(0, idx - radius)
    end = min(len(text), idx + len(token) + radius)
    return text[start:end].strip()


def _supplement_post_issue_coverage(post: dict[str, Any], issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing = {_as_text(issue.get("issue_key")) for issue in issues if issue.get("issue_key")}
    store_name = _as_text(post.get("store_name"))
    store_hint_keys = set(prompts.STORE_ISSUE_HINTS.get(store_name, []))
    issue_by_key, taxonomy_issues = _issue_maps()
    content = _as_text(post.get("content_clean") or post.get("content_text"))
    supplemental: list[dict[str, Any]] = []
    segments = flow_visit_segmenter.segment_post(post)
    for meta in taxonomy_issues:
        key = _as_text(meta.get("key"))
        if not key or key == "기타" or key in existing:
            continue
        aliases = [alias for alias in meta.get("aliases") or [] if alias and alias in content]
        if not aliases:
            continue
        usable_aliases = [
            alias for alias in aliases
            if key in store_hint_keys or len(re.sub(r"\s+", "", alias)) >= 4
        ]
        if not usable_aliases:
            continue
        alias = sorted(usable_aliases, key=len, reverse=True)[0]
        matched_segment = next((segment for segment in segments
                                if alias in _as_text(segment.get("raw_text"))
                                and _rule_class_result(segment, [meta], [])), None)
        if matched_segment is None:
            continue
        evidence = _snippet_around(_as_text(matched_segment.get("raw_text")), alias)
        supplemental.append({
            "seg_id": f"{post.get('post_id')}#coverage-{key}",
            "source_kind": "post_coverage",
            "category": _infer_category(key, evidence, meta.get("category")),
            "issue_key": key,
            "issue_label": _issue_display_label(key),
            "owner_voice": _first_clause(matched_segment.get("owner_voice_raw") or evidence, 80),
            "sv_action": _first_clause(matched_segment.get("sv_action_raw"), 80),
            "owner_voice_raw": _as_text(matched_segment.get("owner_voice_raw")) or evidence,
            "sv_action_raw": _as_text(matched_segment.get("sv_action_raw")),
            "raw_text": evidence,
            "opinion_source": "점주직접" if re.search(r"(점주|가맹점|요청|문의|희망|답답|부담)", evidence) else "담당자판단",
            "is_request": any(token in evidence for token in ["요청", "문의", "건의", "희망", "필요", "개선"]),
            "severity": "높음" if any(token in evidence for token in ["이취", "위반", "누락"]) else "보통",
            "status": _status_from_text(evidence),
            "evidence": evidence[:80],
            "llm_model": "rule_coverage",
            "is_fallback": False,
            "grounding_flag": False,
        })
        existing.add(key)
    return supplemental


# 이슈명을 되풀이할 뿐인 빈 문구. 프롬프트로 금지해도 모델이 가끔 만든다.
_GENERIC_ACTION_RE = re.compile(r"(처리\s*상태\s*확인|진행\s*상태\s*확인|상태\s*확인)\s*$")


def _topic_tokens(*values: Any) -> set[str]:
    text = " ".join(_as_text(value) for value in values)
    return {token for token in re.findall(r"[가-힣A-Za-z0-9]{2,}", text)}


def _anchored_next_action(action: str, issue: dict[str, Any]) -> str:
    """다음 확인 문구가 이 이슈 주제에 대한 것인지 확인한다.

    한 문단이 여러 주제로 쪼개지는 경우가 많아, 요약 LLM이 옆 주제의 할 일을
    가져오는 일이 있다(토더 공지 행에 순이익 회신이 붙는 식).
    이슈 주제와 어휘가 하나도 겹치지 않으면 근거 없는 값으로 보고 버린다.
    """
    if not action or _GENERIC_ACTION_RE.search(action):
        return ""
    topic = _topic_tokens(
        issue.get("issue_label"),
        issue.get("issue_key", "").replace("_", " "),
        issue.get("owner_voice"),
        issue.get("sv_action"),
    )
    if not topic:
        return action
    # 한국어는 조사·접미가 붙어 토큰이 정확히 일치하지 않는다.
    # ("순이익" vs "순이익확인") 그래서 부분 문자열 포함으로 본다.
    action_tokens = _topic_tokens(action)
    overlap = any(a in t or t in a for a in action_tokens for t in topic)
    return action if overlap else ""


def _apply_llm_summary(
    issue: dict[str, Any],
    store_name: str,
    *,
    client: Any,
    model_candidates: list[str] | None,
    system_prompt: str,
    cache: dict[str, Any],
    state: dict[str, Any],
) -> bool:
    """이슈의 요약 필드를 로컬 gpt-oss 결과로 채운다. 적용되면 True.

    세그먼트 이슈와 coverage/fallback 이슈가 같은 캐시 키 체계를 쓴다.
    요약 프롬프트는 댓글을 보지 않으므로 캐시 키의 댓글 해시는 빈 값으로 둔다.
    """
    segment = {
        "seg_id": issue.get("seg_id"),
        "topic": issue.get("issue_label"),
        "raw_text": issue.get("raw_text"),
        "owner_voice_raw": issue.get("owner_voice_raw") or issue.get("owner_voice"),
        "sv_action_raw": issue.get("sv_action_raw") or issue.get("sv_action"),
    }
    key = _cache_key("s1", segment, [])
    result = None if FORCE_REBUILD else cache.get(key)
    if (
        result is None
        and client is not None
        and model_candidates
        and state["count"] < SUMMARY_MAX_SEGMENTS
    ):
        state["count"] += 1
        queried = _query_flow_json(
            prompts.build_summary_prompt(store_name, segment), system_prompt, client, model_candidates
        )
        if _is_llm_failure(queried):
            # 요약 실패는 분류 실패가 아니다. MAX_FALLBACK_RATIO 가드에 넣지 않는다.
            logger.warning(
                "Flow 방문일지 요약 LLM 실패, 규칙 요약으로 대체합니다: seg=%s", segment.get("seg_id")
            )
        else:
            result = queried
            cache[key] = result
            state["dirty"] = True
    if not isinstance(result, dict):
        return False
    owner = prompts.normalize_summary(result.get("owner_summary"))
    sv = prompts.normalize_summary(result.get("sv_summary"))
    if owner:
        issue["owner_voice"] = owner
    if sv:
        issue["sv_action"] = sv
    if isinstance(result.get("is_concern"), bool):
        issue["is_concern"] = result["is_concern"]
    issue["next_action"] = _anchored_next_action(
        prompts.normalize_summary(result.get("next_action"), prompts.NEXT_ACTION_MAX_CHARS), issue
    )
    label = prompts.normalize_summary(result.get("issue_label"), 24)
    if label and _is_generic_issue(issue):
        # 택소노미 라벨이 있는 이슈는 그쪽이 낫고, 기타 이슈만 LLM 라벨이 의미가 있다.
        issue["issue_label"] = label
    issue["grounding_flag"] = _grounding_flag(
        " ".join([_as_text(issue.get("owner_voice")), _as_text(issue.get("sv_action"))]),
        "\n".join([
            _as_text(issue.get("raw_text")),
            _as_text(issue.get("owner_voice_raw")),
            _as_text(issue.get("sv_action_raw")),
        ]),
    )
    return True


def _cache_key(kind: str, segment: dict[str, Any], comments: list[dict[str, Any]]) -> str:
    raw_hash = _sha1_text(
        "\n".join([
            _as_text(segment.get("raw_text")),
            _as_text(segment.get("sv_action_raw")),
            _as_text(segment.get("owner_voice_raw")),
        ])
    )[:12]
    return (
        f"{kind}|{segment.get('seg_id')}|{raw_hash}|{_comment_hash(comments)}|"
        f"{ISSUE_PROMPT_VERSION}|{flow_visit_segmenter.SEGMENTER_VERSION}|{_taxonomy_hash()}"
    )


def _query_flow_json(prompt: str, system_prompt: str, client: Any, model_candidates: list[str]) -> dict[str, Any]:
    from modules.transform.utility.qwen_client import query_qwen_json

    preferred_models = None
    configured_models = (os.getenv("FLOW_VISIT_LLM_MODELS", "") or "").strip()
    if configured_models:
        preferred_models = [model.strip() for model in configured_models.split(",") if model.strip()]
    if not model_candidates:
        raise RuntimeError("Ollama 후보 모델이 없습니다.")
    return query_qwen_json(
        prompt,
        system_prompt=system_prompt,
        preferred_models=preferred_models,
        client=client,
        model_candidates=model_candidates,
        options_override={"temperature": 0, "top_p": 0.25, "num_predict": 512},
    )


def _link_comments_to_issues(post: dict[str, Any], comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    linked = []
    issues = post.get("issues") or []
    issue_by_key, _ = _issue_maps()
    for comment in comments:
        text = _as_text(comment.get("content_text"))
        is_noise = flow_visit_segmenter.is_noise_comment(text)
        linked_key = None
        if not is_noise:
            for issue in issues:
                key = issue.get("issue_key")
                meta = issue_by_key.get(key) or {}
                haystack = " ".join([text, _as_text(issue.get("raw_text")), _as_text(issue.get("owner_voice_raw"))])
                label_tokens = re.findall(r"[가-힣A-Za-z0-9]{2,}", _as_text(issue.get("issue_label")))
                if key and (
                    any(alias and alias in haystack for alias in meta.get("aliases") or [])
                    or any(token and token in text for token in label_tokens)
                ):
                    linked_key = key
                    break
        linked.append({
            "comment_id": _as_text(comment.get("comment_id")),
            "responder": comment.get("author_name"),
            "reply_text": text,
            "linked_issue_key": linked_key,
            "resolution_status": _status_from_text(text, "안내완료" if linked_key else "미해결"),
            "written_at": comment.get("written_at"),
            "is_noise": is_noise,
        })
    return linked


def _build_store_status_summary(post: dict[str, Any], issues: list[dict[str, Any]]) -> str:
    return _build_post_handover_summary(_as_text(post.get("store_name")), issues)


def llm_extract_issues(payload: dict[str, Any], **context) -> dict[str, Any]:
    comments = payload.get("comments") or []
    posts = payload.get("posts") or []
    empty_posts = [_as_text(post.get("post_id")) for post in posts
                   if not _as_text(post.get("content_clean") or post.get("content_text")).strip()]
    if empty_posts:
        raise RuntimeError("Flow 방문일지 본문 누락, 상세 재수집 필요: " + ", ".join(empty_posts))
    cache = _load_cache()
    cache_dirty = False
    client = None
    model_candidates = None
    try:
        from modules.transform.utility.qwen_client import get_ollama_client_with_candidates

        client, model_candidates = get_ollama_client_with_candidates()
    except Exception as exc:
        logger.warning("Ollama 연결 실패, rule fallback만 사용합니다: %s", exc)

    system_prompt = (
        "당신은 프랜차이즈 본사 SV 방문일지를 구조화하는 분석가다. "
        "원문에 없는 내용은 만들지 말고 유효한 JSON 객체 하나만 출력한다."
    )
    _, taxonomy_issues = _issue_maps()
    enriched_posts = []
    fallback_count = 0
    total_count = 0
    classification_calls = 0
    summary_state = {"count": 0, "dirty": False}
    for post in posts:
        post_comments_all = _comments_for_post(comments, post.get("post_id"))
        post_comments = _usable_comments(post_comments_all)
        segments = flow_visit_segmenter.segment_post(post)
        issues = []
        model_name = prompts.GPT_OSS_MODELS[0]
        for segment in segments:
            candidates = prompts.select_issue_candidates(
                _as_text(post.get("store_name")), segment, taxonomy_issues, None
            )
            if not _segment_has_issue_signal(segment, candidates):
                continue
            total_count += 1
            class_key = _cache_key("p1", segment, post_comments)
            class_result = None if FORCE_REBUILD else cache.get(class_key)
            model_name = prompts.GPT_OSS_MODELS[0]
            if class_result is None:
                class_result = _rule_class_result(segment, candidates, [])
                if class_result is not None:
                    model_name = "rule_storefit"
            if class_result is None and client is not None and model_candidates and classification_calls < LLM_MAX_SEGMENTS:
                if class_result is None:
                    classification_calls += 1
                    class_result = _query_flow_json(
                        prompts.build_issue_prompt(_as_text(post.get("store_name")), segment, candidates, []),
                        system_prompt,
                        client,
                        model_candidates,
                    )
                    if _is_llm_failure(class_result):
                        recovered = _recover_class_result(class_result, segment, candidates)
                        if recovered:
                            class_result = recovered
                    if not _is_llm_failure(class_result):
                        cache[class_key] = class_result
                        cache_dirty = True
                        if total_count % FLOW_VISIT_CACHE_FLUSH_INTERVAL == 0:
                            _save_cache(cache)
                            cache_dirty = False

            if not _is_llm_failure(class_result):
                resolved_key = _resolve_issue_key(class_result.get("issue_key"), _as_text(segment.get("raw_text")))
                summary_result = _deterministic_summary(segment, resolved_key)
                issue = _normalize_segment_issue(segment, class_result, summary_result, model_name)
            else:
                issue = _fallback_segment_issue(post, segment, post_comments)
                fallback_count += 1
            issue.update({
                "project_id": _as_text(post.get("project_id")),
                "store_name": post.get("store_name"),
                "post_id": _as_text(post.get("post_id")),
                "visit_date": post.get("visit_date"),
            })
            issue["evidence_ok"] = _evidence_ok(_as_text(issue.get("evidence")), _as_text(post.get("content_clean")))
            issues.append(issue)

        for issue in _supplement_post_issue_coverage(post, issues):
            issue.update({
                "project_id": _as_text(post.get("project_id")),
                "store_name": post.get("store_name"),
                "post_id": _as_text(post.get("post_id")),
                "visit_date": post.get("visit_date"),
            })
            issue["evidence_ok"] = _evidence_ok(_as_text(issue.get("evidence")), _as_text(post.get("content_clean")))
            issues.append(issue)

        # 요약은 세그먼트 이슈뿐 아니라 coverage/fallback 이슈에도 필요하다.
        # 이 패스를 안 돌면 그쪽 이슈들만 원문 180자가 그대로 남는다.
        for issue in issues:
            if _apply_llm_summary(
                issue,
                _as_text(post.get("store_name")),
                client=client,
                model_candidates=model_candidates,
                system_prompt=system_prompt,
                cache=cache,
                state=summary_state,
            ):
                if "llm_summary" not in _as_text(issue.get("llm_model")):
                    # 분류는 rule, 요약은 LLM인 하이브리드임을 라벨에 남긴다.
                    issue["llm_model"] = f"{issue.get('llm_model')}+llm_summary"
        if summary_state["dirty"]:
            _save_cache(cache)
            summary_state["dirty"] = False
        followups = _link_comments_to_issues({"issues": issues}, post_comments_all)
        linked_status = {
            row.get("linked_issue_key"): row.get("resolution_status")
            for row in followups
            if row.get("linked_issue_key") and row.get("resolution_status") in VALID_STATUS
        }
        for issue in issues:
            if issue.get("issue_key") in linked_status:
                issue["status"] = linked_status[issue.get("issue_key")]
        owner_status = _owner_state_sentence(issues, issues)
        merged = dict(post)
        merged.update({
            "segments": segments,
            "store_status_summary": _build_store_status_summary(post, issues),
            "owner_status": owner_status,
            "issues": issues,
            "hq_followups": followups,
            "llm_model": "rule_fallback" if issues and all(issue.get("is_fallback") for issue in issues) else model_name,
        })
        enriched_posts.append(merged)

    if cache_dirty:
        _save_cache(cache)
    fallback_ratio = fallback_count / max(total_count, 1)
    if total_count and fallback_ratio > MAX_FALLBACK_RATIO:
        raise RuntimeError(f"Flow 방문일지 fallback 비율 초과: {fallback_count}/{total_count} ({fallback_ratio:.1%})")

    payload = dict(payload)
    payload["posts"] = enriched_posts
    logger.info(
        "Flow 방문일지 GPT-OSS 이슈 추출 완료: posts=%s segments=%s fallback=%s",
        len(enriched_posts),
        total_count,
        fallback_count,
    )
    return payload


def _build_todo_rows_for_profile(
    profile: dict[str, Any],
    posts: list[dict[str, Any]],
    generated_at: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    todo_seq = 0
    store_name = profile.get("store_name")
    project_id = _as_text(profile.get("project_id"))
    for post in sorted(posts, key=lambda row: row.get("visit_date") or "", reverse=True):
        post_id = _as_text(post.get("post_id"))
        visit_date = post.get("visit_date")
        for issue in post.get("issues") or []:
            if not _is_actionable_request(issue):
                continue
            todo_seq += 1
            issue_seq = issue.get("issue_seq")
            rels = _relation_keys(project_id, store_name, post_id, issue_seq)
            todo_list = _todo_text(issue)
            normalized_todo = re.sub(r"\s+", "", todo_list)
            todo_hash = _sha1_text("|".join([
                project_id,
                post_id,
                _as_text(issue_seq),
                normalized_todo,
            ]))[:16]
            evidence_rows = _evidence_rows([issue], 1)
            due_date = _parse_todo_due_date(_issue_text(issue), visit_date)
            status = _todo_status(issue)
            if status == "완료":
                continue
            if status not in VALID_TODO_STATUS:
                status = "대기"
            rows.append({
                "todo_id": f"todo-{todo_hash}",
                "project_id": project_id,
                "store_name": store_name,
                "store_key": rels["store_key"],
                "post_id": post_id,
                "visit_date": visit_date,
                "issue_key": issue.get("issue_key"),
                "issue_seq": issue_seq,
                "todo_seq": todo_seq,
                "store_rel_key": rels["store_rel_key"],
                "visit_rel_key": rels["visit_rel_key"],
                "issue_rel_key": rels["issue_rel_key"],
                "todo_list": todo_list,
                "todo_due_date": due_date,
                "todo_owner": _todo_owner(issue),
                "todo_status": status,
                "analysis_evidence": _json_dumps(evidence_rows),
                "llm_model": profile.get("llm_model"),
                "prompt_version": PROMPT_VERSION,
                "schema_version": SCHEMA_VERSION,
                "generated_at": generated_at,
            })
    return rows


def _valid_due_date(value: Any) -> str | None:
    text = _as_text(value).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
        return None
    return text


def _build_todo_rows_from_llm(
    profile: dict[str, Any],
    posts: list[dict[str, Any]],
    llm_todos: list[Any],
    generated_at: str | None = None,
) -> list[dict[str, Any]]:
    if not llm_todos:
        return []
    issues_by_ref: dict[tuple[str, str], dict[str, Any]] = {}
    issues_by_post_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for post in posts:
        post_id = _as_text(post.get("post_id"))
        for issue in post.get("issues") or []:
            issue_seq = _as_text(issue.get("issue_seq"))
            issue_key = _as_text(issue.get("issue_key"))
            if post_id and issue_seq:
                issues_by_ref[(post_id, issue_seq)] = issue
            if post_id and issue_key:
                issues_by_post_key.setdefault((post_id, issue_key), []).append(issue)

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    project_id = _as_text(profile.get("project_id"))
    store_name = profile.get("store_name")
    for todo_seq, raw in enumerate(llm_todos, 1):
        if not isinstance(raw, dict):
            continue
        todo_list = _clean_display_text(raw.get("todo_list"), 160)
        if not todo_list:
            continue
        post_id = _as_text(raw.get("post_id"))
        issue_seq = _as_text(raw.get("issue_seq"))
        issue_key = _as_text(raw.get("issue_key"))
        issue = issues_by_ref.get((post_id, issue_seq))
        if issue is None and post_id and issue_key:
            issue = next(iter(issues_by_post_key.get((post_id, issue_key), [])), None)
        if issue is not None:
            issue_seq = _as_text(issue.get("issue_seq"))
            issue_key = _as_text(issue.get("issue_key"))
            visit_date = issue.get("visit_date")
        else:
            visit_date = next((post.get("visit_date") for post in posts if _as_text(post.get("post_id")) == post_id), None)
        if not post_id or not issue_seq:
            continue
        rels = _relation_keys(project_id, store_name, post_id, issue_seq)
        normalized_todo = re.sub(r"\s+", "", todo_list)
        todo_hash = _sha1_text("|".join([project_id, post_id, issue_seq, normalized_todo]))[:16]
        todo_id = f"todo-{todo_hash}"
        if todo_id in seen:
            continue
        seen.add(todo_id)
        status = raw.get("todo_status") if raw.get("todo_status") in VALID_TODO_STATUS else "대기"
        if status == "완료":
            continue
        owner = _clean_display_text(raw.get("todo_owner"), 40) or "담당자 미정"
        rows.append({
            "todo_id": todo_id,
            "project_id": project_id,
            "store_name": store_name,
            "store_key": rels["store_key"],
            "post_id": post_id,
            "visit_date": visit_date,
            "issue_key": issue_key or (issue or {}).get("issue_key"),
            "issue_seq": issue_seq,
            "todo_seq": len(rows) + 1,
            "store_rel_key": rels["store_rel_key"],
            "visit_rel_key": rels["visit_rel_key"],
            "issue_rel_key": rels["issue_rel_key"],
            "todo_list": todo_list,
            "todo_due_date": _valid_due_date(raw.get("todo_due_date")),
            "todo_owner": owner,
            "todo_status": status,
            "analysis_evidence": _json_dumps(_normalize_evidence_rows(raw.get("analysis_evidence"), _evidence_rows([issue], 1) if issue else [])),
            "llm_model": profile.get("llm_model"),
            "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
        })
    return rows


def build_store_profile(payload: dict[str, Any], **context) -> dict[str, Any]:
    profiles: list[dict[str, Any]] = []
    profile_snapshots: list[dict[str, Any]] = []
    todos: list[dict[str, Any]] = []
    profile_cache = _load_profile_cache() if PROFILE_LLM_PROVIDER not in {"", "off", "false", "0", "none"} else None
    for project_id, posts in _group_posts(payload.get("posts") or []).items():
        posts = sorted(posts, key=lambda row: row.get("visit_date") or "", reverse=True)
        for post in posts:
            for seq, issue in enumerate(post.get("issues") or [], 1):
                issue["issue_seq"] = issue.get("issue_seq") or seq
                issue["visit_date"] = issue.get("visit_date") or post.get("visit_date")
                issue["post_id"] = _as_text(issue.get("post_id") or post.get("post_id"))
        issues, counts, recurring_rows, open_issues, recurring_counts = _profile_stats(posts)
        store_name = posts[0].get("store_name") or posts[0].get("project_name")
        profile_copy = _build_profile_copy_from_history(
            _as_text(store_name),
            project_id,
            posts,
            issues,
            recurring_rows,
            recurring_counts,
            profile_cache,
        )
        rels = _relation_keys(project_id, store_name)
        profile = {
            "project_id": project_id,
            "store_name": store_name,
            "store_key": rels["store_key"],
            "store_rel_key": rels["store_rel_key"],
            "last_visit_date": posts[0].get("visit_date"),
            "visit_cnt": len(posts),
            "owner_status": profile_copy["owner_status"],
            "store_status_summary": profile_copy["store_status_summary"],
            "key_concerns": profile_copy["key_concerns"],
            "handling_points": profile_copy["handling_points"],
            "open_issues": open_issues,
            "recurring_issues": sorted(recurring_rows, key=lambda row: row["cnt"], reverse=True),
            "category_counts": counts,
            "handover_summary": profile_copy["handover_summary"],
            "next_visit_action": profile_copy["next_visit_action"],
            "analysis_evidence": profile_copy["analysis_evidence"],
            "llm_model": profile_copy.get("llm_model") or posts[0].get("llm_model") or "unknown",
        }
        profile_todos = _build_todo_rows_from_llm(profile, posts, profile_copy.get("todos") or [])
        if not profile_todos:
            profile_todos = _build_todo_rows_for_profile(profile, posts)
        profile["next_visit_action"] = _build_next_visit_action(profile_todos, profile_copy.get("action_hints") or [])
        profiles.append(profile)
        todos.extend(profile_todos)
        for post in posts:
            post["store_status_summary"] = profile_copy["store_status_summary"]
            post["owner_status"] = profile_copy["owner_status"]
        posts_asc = sorted(posts, key=lambda row: (row.get("visit_date") or "", _as_text(row.get("post_id"))))
        month_seq: dict[str, int] = {}
        for idx, current_post in enumerate(posts_asc):
            ym = _as_text(current_post.get("visit_date"))[:7]
            month_seq[ym] = month_seq.get(ym, 0) + 1
            previous_post = posts_asc[idx - 1] if idx > 0 else None
            history_posts = posts_asc[: idx + 1]
            snapshot = _build_profile_snapshot(
                project_id,
                store_name,
                current_post,
                history_posts,
                previous_post,
                month_seq[ym],
                profile_cache,
            )
            profile_snapshots.append(snapshot)
            current_post["store_status_summary"] = snapshot["store_status_summary"]
            current_post["owner_status"] = snapshot["owner_status"]
    if profile_cache is not None:
        _save_profile_cache(profile_cache)
    payload = dict(payload)
    payload["profiles"] = profiles
    payload["profile_snapshots"] = profile_snapshots
    payload["todos"] = todos
    logger.info("Flow 매장 프로필 생성 완료: %s건", len(profiles))
    return payload


def _group_posts(posts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for post in posts:
        grouped.setdefault(_as_text(post.get("project_id")), []).append(post)
    return grouped


def _generated_at() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def save_visit_mart(payload: dict[str, Any], **context) -> str:
    generated_at = _generated_at()
    log_rows = []
    issue_rows = []
    for post in payload.get("posts") or []:
        issues = post.get("issues") or []
        log_rels = _relation_keys(post.get("project_id"), post.get("store_name"), post.get("post_id"))
        log_rows.append({
            "project_id": _as_text(post.get("project_id")),
            "store_name": post.get("store_name"),
            "store_key": log_rels["store_key"],
            "store_rel_key": log_rels["store_rel_key"],
            "visit_rel_key": log_rels["visit_rel_key"],
            "post_id": _as_text(post.get("post_id")),
            "post_url": post.get("post_url"),
            "visit_date": post.get("visit_date"),
            "visit_date_source": post.get("visit_date_source"),
            "registered_date": post.get("registered_date") or post.get("post_date"),
            "visit_purpose": post.get("visit_purpose"),
            "author_name": post.get("author_name"),
            "content_clean": post.get("content_clean"),
            "topic_table_json": json.dumps(post.get("topic_table") or [], ensure_ascii=False),
            "task_status": post.get("task_status"),
            "progress": post.get("progress"),
            "task_nm": post.get("task_nm"),
            "worker": post.get("worker"),
            "start_dt": post.get("start_dt"),
            "end_dt": post.get("end_dt"),
            "store_status_summary": post.get("store_status_summary"),
            "issue_cnt": len(issues),
            "followup_cnt": len(post.get("hq_followups") or []),
            "image_cnt": post.get("image_cnt"),
            "attach_cnt": post.get("attach_cnt"),
            "content_hash": post.get("content_hash"),
            "llm_model": post.get("llm_model"),
            "prompt_version": PROMPT_VERSION,
            "generated_at": generated_at,
        })
        for seq, issue in enumerate(issues, 1):
            issue_seq = issue.get("issue_seq") or seq
            issue_rels = _relation_keys(post.get("project_id"), post.get("store_name"), post.get("post_id"), issue_seq)
            row = {col: issue.get(col) for col in _ISSUE_COLS}
            row.update({
                "project_id": _as_text(post.get("project_id")),
                "store_name": post.get("store_name"),
                "store_key": issue_rels["store_key"],
                "store_rel_key": issue_rels["store_rel_key"],
                "visit_rel_key": issue_rels["visit_rel_key"],
                "issue_rel_key": issue_rels["issue_rel_key"],
                "post_id": _as_text(post.get("post_id")),
                "visit_date": post.get("visit_date"),
                "issue_seq": issue_seq,
                "evidence_ok": bool(issue.get("evidence_ok")),
            })
            issue_rows.append(row)

    followup_rows = []
    for post in payload.get("posts") or []:
        for followup in post.get("hq_followups") or []:
            followup_rows.append({
                "project_id": _as_text(post.get("project_id")),
                "post_id": _as_text(post.get("post_id")),
                "visit_date": post.get("visit_date"),
                "comment_id": _as_text(followup.get("comment_id")),
                "responder": followup.get("responder"),
                "reply_text": followup.get("reply_text"),
                "linked_issue_key": followup.get("linked_issue_key"),
                "resolution_status": followup.get("resolution_status"),
                "is_noise": bool(followup.get("is_noise")),
                "written_at": followup.get("written_at"),
            })

    post_by_id = {_as_text(post.get("post_id")): post for post in payload.get("posts") or []}
    comments = payload.get("comments") or []
    subtask_rows = []
    for subtask in payload.get("subtasks") or []:
        direct_parent_post_id = _as_text(subtask.get("direct_parent_post_id") or subtask.get("parent_post_id"))
        parent_post_id = _as_text(subtask.get("visit_parent_post_id") or subtask.get("parent_post_id"))
        parent = post_by_id.get(parent_post_id, {})
        project_id = _as_text(subtask.get("project_id") or parent.get("project_id"))
        store_name = subtask.get("store_name") or parent.get("store_name")
        rels = _relation_keys(project_id, store_name, parent_post_id)
        subtask_comments = _raw_comments_for_post(comments, subtask.get("post_id"))
        subtask_rows.append({
            "project_id": project_id,
            "store_name": store_name,
            "store_key": rels["store_key"],
            "store_rel_key": rels["store_rel_key"],
            "visit_rel_key": rels["visit_rel_key"],
            "parent_post_id": parent_post_id,
            "direct_parent_post_id": direct_parent_post_id,
            "direct_parent_title": subtask.get("direct_parent_title"),
            "subtask_post_id": _as_text(subtask.get("post_id")),
            "subtask_url": subtask.get("post_url"),
            "subtask_depth": subtask.get("subtask_depth") or (1 if direct_parent_post_id else ""),
            "subtask_path_titles": subtask.get("subtask_path_titles") or (subtask.get("title") or subtask.get("task_nm")),
            "visit_date": parent.get("visit_date"),
            "registered_date": _parse_post_date(subtask.get("registered_at") or subtask.get("post_date")),
            "registered_time": _parse_post_time(subtask.get("registered_at") or subtask.get("post_date")),
            "author_name": subtask.get("author_name"),
            "title": subtask.get("title") or subtask.get("task_nm"),
            "task_status": _task_status_from_subtask(subtask, subtask_comments),
            "start_dt": _parse_post_date(subtask.get("start_dt")) or subtask.get("start_dt"),
            "end_dt": _parse_post_date(subtask.get("end_dt")) or subtask.get("end_dt"),
            "task_nm": subtask.get("task_nm") or subtask.get("title"),
            "worker": subtask.get("worker"),
            "content_text": subtask.get("content_text"),
            "comment_group": _joined_comment_groups(subtask_comments),
            "comment_author": _joined_comment_authors(subtask_comments),
            "comment_text": _joined_comment_text(subtask_comments),
            "comment_history_json": _comment_history_json(subtask_comments),
            "generated_at": generated_at,
        })

    profile_rows = []
    for profile in payload.get("profiles") or []:
        profile_rels = _relation_keys(profile.get("project_id"), profile.get("store_name"))
        profile_rows.append({
            "project_id": _as_text(profile.get("project_id")),
            "store_name": profile.get("store_name"),
            "store_key": profile.get("store_key") or profile_rels["store_key"],
            "store_rel_key": profile.get("store_rel_key") or profile_rels["store_rel_key"],
            "last_visit_date": profile.get("last_visit_date"),
            "visit_cnt": profile.get("visit_cnt"),
            "owner_status": profile.get("owner_status"),
            "store_status_summary": profile.get("store_status_summary"),
            "key_concerns_json": json.dumps(profile.get("key_concerns") or [], ensure_ascii=False),
            "handling_points_json": json.dumps(profile.get("handling_points") or [], ensure_ascii=False),
            "open_issues_json": json.dumps(profile.get("open_issues") or [], ensure_ascii=False),
            "recurring_issues_json": json.dumps(profile.get("recurring_issues") or [], ensure_ascii=False),
            "category_counts_json": json.dumps(profile.get("category_counts") or {}, ensure_ascii=False),
            "handover_summary": profile.get("handover_summary"),
            "next_visit_action": profile.get("next_visit_action"),
            "analysis_evidence_json": json.dumps(profile.get("analysis_evidence") or [], ensure_ascii=False),
            "llm_model": profile.get("llm_model"),
            "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
        })

    profile_snapshot_rows = []
    for snapshot in payload.get("profile_snapshots") or []:
        snapshot_rels = _relation_keys(snapshot.get("project_id"), snapshot.get("store_name"), snapshot.get("post_id"))
        profile_snapshot_rows.append({
            "project_id": _as_text(snapshot.get("project_id")),
            "store_name": snapshot.get("store_name"),
            "store_key": snapshot.get("store_key") or snapshot_rels["store_key"],
            "store_rel_key": snapshot.get("store_rel_key") or snapshot_rels["store_rel_key"],
            "visit_rel_key": snapshot.get("visit_rel_key") or snapshot_rels["visit_rel_key"],
            "post_id": _as_text(snapshot.get("post_id")),
            "visit_date": snapshot.get("visit_date"),
            "visit_seq_in_month": snapshot.get("visit_seq_in_month"),
            "period_start": snapshot.get("period_start"),
            "period_end": snapshot.get("period_end"),
            "profile_as_of_date": snapshot.get("profile_as_of_date"),
            "visit_cnt_as_of": snapshot.get("visit_cnt_as_of"),
            "owner_status": snapshot.get("owner_status"),
            "store_status_summary": snapshot.get("store_status_summary"),
            "key_concerns_json": json.dumps(snapshot.get("key_concerns") or [], ensure_ascii=False),
            "handling_points_json": json.dumps(snapshot.get("handling_points") or [], ensure_ascii=False),
            "open_issues_json": json.dumps(snapshot.get("open_issues") or [], ensure_ascii=False),
            "recurring_issues_json": json.dumps(snapshot.get("recurring_issues") or [], ensure_ascii=False),
            "category_counts_json": json.dumps(snapshot.get("category_counts") or {}, ensure_ascii=False),
            "handover_summary": snapshot.get("handover_summary"),
            "next_visit_action": snapshot.get("next_visit_action"),
            "analysis_evidence_json": json.dumps(snapshot.get("analysis_evidence") or [], ensure_ascii=False),
            "llm_model": snapshot.get("llm_model"),
            "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
        })

    todo_rows = []
    for todo in payload.get("todos") or []:
        row = {col: todo.get(col) for col in _TODO_COLS}
        row["generated_at"] = row.get("generated_at") or generated_at
        row["prompt_version"] = row.get("prompt_version") or PROMPT_VERSION
        row["schema_version"] = row.get("schema_version") or SCHEMA_VERSION
        if row.get("todo_status") not in VALID_TODO_STATUS:
            row["todo_status"] = "대기"
        todo_rows.append(row)

    project_ids = sorted({_as_text(row["project_id"]) for row in log_rows if _as_text(row["project_id"])})
    log_df = pd.DataFrame(log_rows).reindex(columns=_LOG_COLS)
    issue_df = pd.DataFrame(issue_rows).reindex(columns=_ISSUE_COLS)
    followup_df = pd.DataFrame(followup_rows).reindex(columns=_FOLLOWUP_COLS)
    subtask_df = pd.DataFrame(subtask_rows).reindex(columns=_SUBTASK_COLS)
    profile_df = _merge_existing_project_rows(
        FLOW_STORE_PROFILE_PARQUET,
        pd.DataFrame(profile_rows),
        _PROFILE_COLS,
        project_ids,
    )
    profile_snapshot_df = pd.DataFrame(profile_snapshot_rows).reindex(columns=_PROFILE_SNAPSHOT_COLS)
    todo_df = pd.DataFrame(todo_rows).reindex(columns=_TODO_COLS)
    _write_project_partitions(log_df, FLOW_VISIT_LOG_PARQUET, _LOG_COLS, project_ids)
    _write_project_partitions(issue_df, FLOW_VISIT_ISSUE_PARQUET, _ISSUE_COLS, project_ids)
    _write_project_partitions(followup_df, FLOW_VISIT_FOLLOWUP_PARQUET, _FOLLOWUP_COLS, project_ids)
    _write_project_partitions(subtask_df, FLOW_VISIT_SUBTASK_PARQUET, _SUBTASK_COLS, project_ids)
    _write_project_partitions(profile_snapshot_df, FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET, _PROFILE_SNAPSHOT_COLS, project_ids)
    _write_project_partitions(todo_df, FLOW_VISIT_TODO_PARQUET, _TODO_COLS, project_ids)
    _write_parquet_atomic(profile_df, FLOW_STORE_PROFILE_PARQUET)
    message = (
        f"Flow 방문일지 마트 저장 완료: visit={len(log_df)} issue={len(issue_df)} "
        f"followup={len(followup_df)} subtask={len(subtask_df)} profile={len(profile_df)} "
        f"profile_snapshot={len(profile_snapshot_df)} todo={len(todo_df)}"
    )
    logger.info(message)
    return message


def build_visit_viz_table(payload: dict[str, Any], **context) -> str:
    from modules.transform.pipelines.strategy.flow_visit_viz import save_visit_viz_table

    project_ids = sorted({
        _as_text(row.get("project_id"))
        for row in (payload.get("posts") or []) + (payload.get("profiles") or [])
        if _as_text(row.get("project_id"))
    })
    message = save_visit_viz_table(payload, _generated_at(), PROMPT_VERSION, merge_project_ids=project_ids)
    logger.info(message)
    return message


def export_llm_corpus(payload: dict[str, Any], **context) -> str:
    FLOW_VISIT_CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    tmp = FLOW_VISIT_CORPUS_JSONL.with_suffix(FLOW_VISIT_CORPUS_JSONL.suffix + ".tmp")
    rows = []
    for post in payload.get("posts") or []:
        clean_issues = [
            issue for issue in post.get("issues") or []
            if not issue.get("is_fallback") and not issue.get("grounding_flag")
            and (_as_text(issue.get("owner_voice")) or _as_text(issue.get("sv_action")))
        ]
        if clean_issues:
            issue_text = "\n".join(
                f"- {issue.get('issue_label')}: 점주 {issue.get('owner_voice')} / 본사 {issue.get('sv_action')}"
                for issue in clean_issues
            )
            rows.append({
                "doc_id": f"visit-{post.get('post_id')}",
                "doc_type": "visit",
                "store_name": post.get("store_name"),
                "project_id": _as_text(post.get("project_id")),
                "visit_date": post.get("visit_date"),
                "text": f"## 매장현황\n{post.get('store_status_summary')}\n\n## 이슈\n{issue_text}",
                "metadata": {
                    "post_id": post.get("post_id"),
                    "issue_keys": [issue.get("issue_key") for issue in clean_issues],
                    "prompt_version": PROMPT_VERSION,
                },
            })
        for issue in clean_issues:
            rows.append({
                "doc_id": f"issue-{issue.get('seg_id')}",
                "doc_type": "issue",
                "store_name": post.get("store_name"),
                "project_id": _as_text(post.get("project_id")),
                "visit_date": post.get("visit_date"),
                "text": f"{issue.get('issue_label')}\n점주 의견: {issue.get('owner_voice')}\n담당자 조치: {issue.get('sv_action')}",
                "metadata": {
                    "post_id": post.get("post_id"),
                    "seg_id": issue.get("seg_id"),
                    "issue_key": issue.get("issue_key"),
                    "category": issue.get("category"),
                    "raw_text": issue.get("raw_text"),
                    "prompt_version": PROMPT_VERSION,
                },
            })
    for profile in payload.get("profiles") or []:
        text = "\n".join(
            part for part in [
                profile.get("owner_status"),
                profile.get("store_status_summary"),
                _join_json_list(profile.get("key_concerns")),
                _join_json_list(profile.get("handling_points")),
                profile.get("next_visit_action"),
                profile.get("handover_summary"),
            ]
            if _as_text(part)
        )
        if len(text.strip()) >= 30:
            rows.append({
                "doc_id": f"profile-{profile.get('project_id')}",
                "doc_type": "profile",
                "store_name": profile.get("store_name"),
                "project_id": _as_text(profile.get("project_id")),
                "visit_date": profile.get("last_visit_date"),
                "text": text,
                "metadata": {
                    "recurring_issues": profile.get("recurring_issues") or [],
                    "category_counts": profile.get("category_counts") or {},
                    "prompt_version": PROMPT_VERSION,
                },
            })
    target_ids = {_as_text(row.get("project_id"))
                  for row in (payload.get("posts") or []) + (payload.get("profiles") or [])}
    if FLOW_VISIT_CORPUS_JSONL.exists():
        kept = []
        with FLOW_VISIT_CORPUS_JSONL.open(encoding="utf-8") as existing:
            for line in existing:
                if line.strip():
                    row = json.loads(line)
                    if _as_text(row.get("project_id")) not in target_ids:
                        kept.append(row)
        rows = kept + rows
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.replace(tmp, FLOW_VISIT_CORPUS_JSONL)
    message = f"Flow 방문일지 RAG JSONL 저장 완료: {len(rows)} lines"
    logger.info(message)
    return message


def eval_quality(**context) -> str:
    from modules.transform.pipelines.strategy.flow_visit_quality import evaluate_visit_quality

    _, summary = evaluate_visit_quality()
    if summary["empty_post_count"] or summary["invalid_category_count"]:
        raise RuntimeError(
            f"Flow 방문일지 품질 오류: 상세 재수집 필요={summary['empty_post_ids']} "
            f"분류 공백/허용값 오류={summary['invalid_category_count']}"
        )
    message = (
        "Flow 방문일지 품질검사 완료: "
        f"cases={summary['case_cnt']} issue={summary['issue_cnt']} "
        f"avg_recall={summary['avg_recall']:.1%} "
        f"perfect={summary['perfect_case_cnt']}/{summary['case_cnt']} "
        f"missing={summary['missing_total']} forbidden={summary['forbidden_total']} "
        f"fallback={summary['fallback_count']}"
    )
    logger.info(message)
    return message
