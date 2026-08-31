"""Flow 방문일지 원문 세그먼트 분리."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from modules.transform.utility.paths import FLOW_VISIT_LOG_PARQUET

logger = logging.getLogger(__name__)

SEGMENTER_VERSION = "seg_v2_storefit"

_TOPIC_RE = re.compile(r"주제\s*(\d{1,2})(.*?)(?=주제\s*\d{1,2}|\Z)", re.S)
_MESSAGE_RE = re.compile(r"(전달\s*내용|내용)")
_OWNER_RE = re.compile(r"(가맹점\s*의견|점주\s*의견|점주님\s*의견)")
_TAIL_RE = re.compile(r"(\[내용\s*기입\]|\[담당자\s*최종\s*의견\]|담당자\s*의견|ALL\b)", re.S)
_HEADER_RE = re.compile(
    r"(?m)^\s*(담당자\s*의견|점주님\s*요청사항|점주\s*개별\s*의견|건의사항|특이사항|매장\s*현황|"
    r"본사정책\s*안내|개선\s*지적\s*사항)\s*$"
)
_NUMBER_RE = re.compile(r"(?m)^\s*(\d+)[.)]\s*([^\n]{1,80})")
_NAME_ONLY_RE = re.compile(r"^[가-힣]{2,4}(?:\s+[가-힣]{2,4}){0,4}$")
_PHOTO_WORDS = ("사진", "참고자료", "첨부", "자료")
_OWNER_LINE_RE = re.compile(
    r"(점주|점주님|가맹점|하심|하셨|다고\s*함|라고\s*함|내용\s*전달|전달\s*주셨|"
    r"말씀|답변|수긍|이해|고맙|희망|요청|문의|걱정|답답|스트레스|부담|섭섭)"
)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _overlaps(claimed: list[tuple[int, int]], start: int, end: int) -> bool:
    return any(start < c_end and end > c_start for c_start, c_end in claimed)


def _claim(claimed: list[tuple[int, int]], start: int, end: int) -> bool:
    if start >= end or _overlaps(claimed, start, end):
        return False
    claimed.append((start, end))
    claimed.sort()
    return True


def is_noise_comment(text: Any) -> bool:
    value = _compact(_as_text(text))
    if not value:
        return True
    if _NAME_ONLY_RE.match(value):
        return True
    if len(value) < 30 and any(word in value for word in _PHOTO_WORDS):
        return True
    return False


def _split_topic_block(block: str) -> tuple[str, str, str]:
    block = block.strip()
    msg_match = _MESSAGE_RE.search(block)
    owner_match = _OWNER_RE.search(block)
    if msg_match and owner_match and msg_match.start() < owner_match.start():
        topic = block[:msg_match.start()]
        message = block[msg_match.end():owner_match.start()]
        owner = block[owner_match.end():]
    elif msg_match:
        topic = block[:msg_match.start()]
        message = ""
        owner = block[msg_match.end():]
    else:
        topic = block
        message = ""
        owner = ""

    tail = _TAIL_RE.search(owner)
    if tail:
        owner = owner[:tail.start()]
    return _compact(topic), _compact(message), _compact(owner)


def _opinion_source(topic: str, message: str, owner: str, raw_text: str) -> str:
    if owner or re.search(r"가맹점\s*의견|점주\s*의견|점주님\s*요청사항", raw_text):
        return "점주직접"
    return "담당자판단"


def _split_free_block(raw: str) -> tuple[str, str]:
    owner_lines: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _OWNER_LINE_RE.search(stripped):
            owner_lines.append(stripped)
    owner = _compact(" ".join(owner_lines))
    return _compact(raw), owner


def _segment_row(
    post_id: str,
    content: str,
    start: int,
    end: int,
    source_kind: str,
    topic: str | None = None,
    sv_action_raw: str = "",
    owner_voice_raw: str = "",
) -> dict[str, Any]:
    raw = content[start:end].strip()
    seg_start = content.find(raw, start, end) if raw else start
    if seg_start < 0:
        seg_start = start
    seg_end = seg_start + len(raw)
    return {
        "seg_id": f"{post_id}#{seg_start}-{seg_end}",
        "post_id": post_id,
        "start": seg_start,
        "end": seg_end,
        "source_kind": source_kind,
        "topic": topic or None,
        "sv_action_raw": sv_action_raw,
        "owner_voice_raw": owner_voice_raw,
        "raw_text": raw,
        "opinion_source": _opinion_source(topic or "", sv_action_raw, owner_voice_raw, raw),
    }


def segment_post(post: dict[str, Any]) -> list[dict[str, Any]]:
    content = _as_text(post.get("content_clean") or post.get("content_text"))
    content = re.sub(r"(?<![\n\d])\s+(\d+[.]\s*[가-힣A-Za-z])", r"\n\1", content)
    post_id = _as_text(post.get("post_id"))
    claimed: list[tuple[int, int]] = []
    segments: list[dict[str, Any]] = []

    for match in _TOPIC_RE.finditer(content):
        start, end = match.span()
        block = match.group(2)
        topic, message, owner = _split_topic_block(block)
        raw = content[start:end]
        if not _compact(raw):
            continue
        if _claim(claimed, start, end):
            segments.append(_segment_row(post_id, content, start, end, "topic_table", topic, message, owner))

    header_matches = list(_HEADER_RE.finditer(content))
    for idx, match in enumerate(header_matches):
        start = match.start()
        end = header_matches[idx + 1].start() if idx + 1 < len(header_matches) else len(content)
        if _claim(claimed, start, end):
            raw = content[start:end]
            sv, owner = _split_free_block(raw)
            segments.append(_segment_row(post_id, content, start, end, "header_block", _compact(match.group(1)), sv, owner))

    number_matches = list(_NUMBER_RE.finditer(content))
    for idx, match in enumerate(number_matches):
        start = match.start()
        end = number_matches[idx + 1].start() if idx + 1 < len(number_matches) else len(content)
        if _claim(claimed, start, end):
            raw = content[start:end]
            sv, owner = _split_free_block(raw)
            segments.append(_segment_row(post_id, content, start, end, "numbered", _compact(match.group(2)), sv, owner))

    if not segments and _compact(content):
        for para in re.finditer(r"\S(?:.*?)(?=\n\s*\n|\Z)", content, re.S):
            start, end = para.span()
            if _claim(claimed, start, end):
                sv, owner = _split_free_block(para.group(0))
                segments.append(_segment_row(post_id, content, start, end, "residual", None, sv, owner))

    segments.sort(key=lambda row: (row["start"], row["end"]))
    _assert_invariants(content, segments)
    return segments


def _assert_invariants(content: str, segments: list[dict[str, Any]]) -> None:
    intervals = [(int(row["start"]), int(row["end"])) for row in segments]
    for idx, (start, end) in enumerate(intervals):
        raw = _as_text(segments[idx].get("raw_text"))
        assert raw and content[start:end] == raw
        for other_start, other_end in intervals[idx + 1:]:
            assert not (start < other_end and end > other_start)


def selftest() -> dict[str, Any]:
    if not FLOW_VISIT_LOG_PARQUET.exists():
        raise RuntimeError(f"방문일지 마트가 없습니다: {FLOW_VISIT_LOG_PARQUET}")
    log_df = pd.read_parquet(FLOW_VISIT_LOG_PARQUET)
    posts = log_df.to_dict("records")
    seg_rows = [seg for post in posts for seg in segment_post(post)]
    sv_rate = sum(bool(row.get("sv_action_raw")) for row in seg_rows) / max(len(seg_rows), 1)
    owner_rate = sum(bool(row.get("owner_voice_raw")) for row in seg_rows) / max(len(seg_rows), 1)
    assert is_noise_comment("김대진 김덕기")
    assert is_noise_comment("매장 방문 사진 & 참고자료")
    result = {
        "posts": len(posts),
        "segments": len(seg_rows),
        "sv_action_rate": round(sv_rate, 4),
        "owner_voice_rate": round(owner_rate, 4),
        "version": SEGMENTER_VERSION,
    }
    logger.info("Flow 방문일지 세그먼터 selftest: %s", result)
    return result
