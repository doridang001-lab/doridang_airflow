"""HTTP server for Doridang Flow assistant."""

from __future__ import annotations

import json
import logging
import mimetypes
import re
import threading
import time
import uuid
import queue
from collections import OrderedDict
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from modules.transform.doridang_bot import conversation, dialogue, integrity, llm_backend, router, tools
from modules.transform.utility import flow_task_status as status_rules

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8788
STATIC_DIR = Path(__file__).resolve().parent / "static"
ANSWER_TIMEOUT_SECONDS = 60


# 봇이 "방금 무엇을 보여줬는지"를 몰라 후속 질문에 같은 리포트를 다시 주던 문제.
# 구버전 클라이언트를 위한 메모리 캐시. 새 클라이언트는 마지막 완료 턴의 맥락을 함께 보낸다.
SESSION_MEMORY_MAX = 50
_SESSION_MEMORY: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_SESSION_LOCK = threading.Lock()

# 직전 답변을 가리키는 표현. 조건이 붙은 질문은 정상 라우팅이 더 정확하므로 여기서 걸러낸다.
REFERENCE_MARKERS = (
    "그 ", "그중", "그 중", "그건", "그거", "그게", "저건", "방금", "위에", "아까",
    "첫번째", "첫 번째", "두번째", "두 번째", "세번째", "세 번째", "마지막",
    "자세히", "상세", "무슨 내용", "어떤 내용", "뭐야", "뭔데", "어떤 거", "어떤거",
)
_ORDINALS = (("첫", 1), ("두", 2), ("세", 3), ("네", 4), ("다섯", 5))
_COUNT_REFERENCE_RE = re.compile(r"(\d+)\s*(?:건|개)")
# 되묻는 표현. 새 조회 요청("보여줘","찾아줘")과 구분한다.
ASK_MARKERS = ("뭐야", "뭔데", "뭐", "어떤", "무슨", "자세히", "상세", "설명", "알려줘", "원인", "이유", "왜", "그 ", "그거", "그건")


def _remember_turn(session_id: str, payload: dict[str, Any]) -> None:
    if not session_id:
        return
    with _SESSION_LOCK:
        _SESSION_MEMORY[session_id] = payload
        _SESSION_MEMORY.move_to_end(session_id)
        while len(_SESSION_MEMORY) > SESSION_MEMORY_MAX:
            _SESSION_MEMORY.popitem(last=False)


def _recall_turn(session_id: str) -> dict[str, Any] | None:
    if not session_id:
        return None
    with _SESSION_LOCK:
        payload = _SESSION_MEMORY.get(session_id)
        if payload is not None:
            _SESSION_MEMORY.move_to_end(session_id)
        return payload


def _refers_to_last(question: str, last: dict[str, Any] | None) -> bool:
    """직전 답변을 가리키는 질문인가. 애매하면 False — 정상 라우팅이 안전하다."""
    if not last or not last.get("posts"):
        return False
    text = question or ""
    if tools.detect_project_id(text):
        return False                                   # 새 프로젝트를 지목
    name = tools.detect_worker_name(text)
    if name and tools.is_known_person(name):
        return False                                   # 새 사람을 지목

    # "진행2건은 뭐야?" — 건수가 방금 보여준 수와 맞고 되묻는 표현이면 직전 답변 이야기다.
    # "보류 3건 보여줘" 처럼 수가 안 맞거나 되묻지 않으면 새 조회로 본다.
    counted = _COUNT_REFERENCE_RE.search(text)
    if counted and int(counted.group(1)) == len(last.get("posts") or []) and _is_asking_about(text):
        return True

    if router.status_due_filters(text):
        return False                                   # "그 중 기한 지난 건" 은 filter_posts가 낫다
    return any(marker in text for marker in REFERENCE_MARKERS)


def _is_asking_about(question: str) -> bool:
    return any(marker in (question or "") for marker in ASK_MARKERS)


def _requested_ordinal(question: str) -> int | None:
    text = (question or "").replace(" ", "")
    matched = re.search(r"(\d+)\s*(번째|번쨰)", question or "")
    if matched:
        return int(matched.group(1))
    for word, index in _ORDINALS:
        if f"{word}번째" in text:
            return index
    return None


def _safe_json(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


class DoridangBotHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path
        if route == "/health":
            self._send_bytes(b"ok", content_type="text/plain; charset=utf-8")
            return
        if route == "/":
            self._send_static_file(STATIC_DIR / "index.html")
            return
        if route.startswith("/static/"):
            rel = unquote(route.removeprefix("/static/"))
            self._send_static_file(STATIC_DIR / rel)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/chat":
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        try:
            payload = self._read_json()
            question = str(payload.get("message") or payload.get("question") or "").strip()
            user = str(payload.get("user") or "사용자").strip()
            history = _normalize_history(payload.get("history"))
            session_id = str(payload.get("session_id") or "").strip()[:128]
            if not question:
                self._send_json({"error": "message is required"}, status=HTTPStatus.BAD_REQUEST)
                return
            self._stream_chat(user=user, question=question, history=history, session_id=session_id,
                              context=payload.get("context"), message_id=str(payload.get("message_id") or uuid.uuid4())[:128])
        except Exception as exc:
            logger.exception("채팅 처리 실패: %s", exc)
            if not self.wfile.closed:
                self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _stream_chat(self, *, user: str, question: str, history=None, session_id="", context=None, message_id="") -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self._send_cors_headers()
        self.end_headers()
        started = time.monotonic()
        history = history or []
        # 브라우저의 마지막 완료 턴이 우선이다. 중단된 요청의 서버 메모리는 이어받지 않는다.
        previous = context if isinstance(context, dict) else (_recall_turn(session_id) or {}).get("context", {})
        try:
            self._send_sse({"type": "status", "text": "업무와 최근 기록을 확인하고 있어요.", "message_id": message_id})
            deadline = started + ANSWER_TIMEOUT_SECONDS
            updates = queue.Queue()
            cancelled = threading.Event()

            def build_answer():
                try:
                    with tools.request_snapshot():
                        current, evidence, direct = dialogue.gather(question, history, previous)
                        note = _data_as_of_note() if evidence else ""
                        prepared = integrity.fact_bank(question, current, evidence)
                    updates.put(("prepared", (current, evidence, note, prepared)))
                    if cancelled.is_set():
                        return
                    list_answer, ids = dialogue.requested_list_answer(question, evidence)
                    if direct or list_answer:
                        result = integrity.VerifiedAnswer(direct or list_answer,
                            {"outcome": "direct", "attempts": 0}, ids)
                    else:
                        result = integrity.generate(question, current, evidence, deadline=deadline, prepared=prepared)
                    updates.put(("result", result))
                except Exception:
                    logger.exception("근거 조회/검증 실패")
                    updates.put(("failed", None))

            threading.Thread(target=build_answer, daemon=True).start()
            current, evidence, note, prepared = {}, [], "", None
            result = None
            while time.monotonic() < deadline:
                try:
                    kind, value = updates.get(timeout=min(1, max(.001, deadline - time.monotonic())))
                except queue.Empty:
                    self._send_sse({"type": "status", "text": "답변의 건수와 근거를 검증하고 있어요."})
                    continue
                if kind == "prepared":
                    current, evidence, note, prepared = value
                elif kind == "result":
                    result = value
                    break
                elif kind == "failed":
                    break
            cancelled.set()
            if result is None:
                if prepared:
                    result = integrity.safe_answer(*prepared, reason="timeout_or_unavailable")
                else:
                    result = integrity.VerifiedAnswer("업무 기록을 시간 안에 확인하지 못했습니다. 잠시 후 다시 질문해 주세요.",
                                                       {"outcome": "unavailable", "attempts": 0}, [])
            answer = result.text
            current, sources = dialogue.final_metadata(answer, current, evidence, listed_ids=result.post_ids)
            # 링크는 조회 결과에서만 작성한다. 모델이 만든 URL은 완료 본문에서 제거한다.
            answer = re.sub(r"\[([^\]]+)\]\(https?://[^)]+\)", r"\1", answer)
            answer = re.sub(r"https?://\S+", "", answer)
            if sources:
                answer += "\n\n근거: " + " · ".join(f"[Flow {i + 1}]({s['url']})" for i, s in enumerate(sources))
            if evidence:
                answer += "\n\n" + note
            # 구버전도 검증이 끝난 최종 본문만 한 번 받는다.
            self._send_sse({"type": "delta", "text": answer})
            self._send_sse({"type": "answer", "text": answer, "sources": sources,
                            "validation": result.validation,
                            "context": current, "message_id": message_id})
            # 원천 데이터/OneDrive 로그는 변경하지 않는다. 로컬 로그만 기록한다.
            conversation.append_turn(user, question, answer,
                                     [{"name": e["name"], "arguments": e["arguments"]} for e in evidence])
            _remember_turn(session_id, {"context": current})
            self._send_raw_sse("[DONE]")
            logger.info("대화 완료: 조회=%d 검증=%s 시도=%d 차단=%d 전체=%.2fs", len(evidence),
                        result.validation["outcome"], result.validation.get("attempts", 0),
                        result.validation.get("blocked_drafts", 0), time.monotonic() - started)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            logger.info("클라이언트가 응답 수신을 중단했습니다.")
        except Exception:
            logger.exception("대화 생성 실패")
            try:
                self._send_sse({"type": "error", "message": "답변을 완료하지 못했습니다. 다시 시도해 주세요."})
                self._send_raw_sse("[DONE]")
            except OSError:
                pass
        finally:
            if "cancelled" in locals():
                cancelled.set()
            self.close_connection = True

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        body = self.rfile.read(length)
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        self._send_bytes(_safe_json(payload), content_type="application/json; charset=utf-8", status=status)

    def _send_bytes(self, payload: bytes, *, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _send_static_file(self, path: Path) -> None:
        try:
            resolved = path.resolve()
            resolved.relative_to(STATIC_DIR.resolve())
        except ValueError:
            self.send_error(HTTPStatus.FORBIDDEN, "forbidden")
            return
        if not resolved.exists() or not resolved.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        if content_type.startswith("text/") or resolved.suffix in {".js", ".css"}:
            content_type = f"{content_type}; charset=utf-8"
        self._send_bytes(resolved.read_bytes(), content_type=content_type)

    def _send_sse(self, payload: Any) -> None:
        self._send_raw_sse(json.dumps(payload, ensure_ascii=False))

    def _send_raw_sse(self, payload: str) -> None:
        self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
        self.wfile.flush()

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

# 기존 정형 포맷의 호환 검사에서 사용하는 보조 함수. 정상 대화 경로는 dialogue를 사용한다.
INTERPRETATION_MARKERS = ("결론", "리스크", "제안", "왜 ", "이유", "원인", "분석", "브리핑", "평가", "요약해")
SUMMARY_SEPARATOR = "\n\n---\n\n"


def _wants_interpretation(question: str) -> bool:
    text = question or ""
    return any(marker in text for marker in INTERPRETATION_MARKERS)


DETAIL_MARKERS = ("자세히", "상세", "전체", "전부", "목록", "다 보여", "모두 보여", "원문")
CAUSE_MARKERS = ("원인", "이유", "왜", "막힌 이유", "지연 사유", "늦어진")


def _wants_detail(question: str) -> bool:
    text = question or ""
    return any(marker in text for marker in DETAIL_MARKERS)


def _wants_cause(question: str) -> bool:
    text = question or ""
    return any(marker in text for marker in CAUSE_MARKERS)


def _data_as_of_note() -> str:
    """답변 대상 데이터가 언제 수집된 것인지 밝힌다.

    Flow는 하루 6회 수집되므로, 하루 넘게 갱신이 없으면 수집이 멈춘 것이다.
    리더가 "왜 어제 올린 게 안 보이지"를 스스로 알 수 있어야 한다.
    """
    freshness = tools.data_freshness()
    latest = freshness.get("latest")
    if not latest:
        return ""
    if not freshness.get("stale"):
        return f"\n\n_데이터 기준: {latest} 수집분 (Flow 수집은 하루 6회)_"
    age = freshness.get("age_days")
    age_text = f"{age}일 지났습니다" if age else "갱신이 멈춰 있습니다"
    return (
        f"\n\n_⚠ 이 답변의 데이터는 {latest} 수집분으로 {age_text}. "
        "Flow는 하루 6회 수집되지만 이 프로젝트들은 갱신이 멈춰 있어 최신 내용이 빠져 있을 수 있습니다._"
    )


def _format_last_result_detail(question: str, last: dict[str, Any]) -> str:
    """직전에 보여준 건들을 본문·댓글·하위 업무까지 붙여 다시 설명한다.

    같은 줄을 반복하지 않으려면 새 정보가 있어야 한다. 본문이 있는 글은 243건 중 79건뿐이라
    댓글과 하위 업무를 함께 보여준다. 전부 수집된 데이터라 지어내는 부분은 없다.
    """
    posts = list(last.get("posts") or [])
    if _wants_cause(question):
        return _format_last_result_causes(question, last, posts)

    ordinal = _requested_ordinal(question)
    if ordinal and 1 <= ordinal <= len(posts):
        posts = [posts[ordinal - 1]]
        heading = f"직전 답변의 {ordinal}번째 건"
    else:
        heading = f"직전 답변에서 보여드린 {len(posts)}건"

    lines = [f"## {heading}", ""]
    for index, post in enumerate(posts[:5], start=1):
        lines.append(f"{index}. {_post_line(post, with_people=True).lstrip('- ')}")
        lines.extend(_format_post_detail(post))
        lines.append("")
    if len(posts) > 5:
        lines.append(f"- 외 {len(posts) - 5}건")

    lines.append(f"_직전 질문 「{last.get('question', '')}」 의 결과를 자세히 본 것입니다._")
    return "\n".join(lines)


def _format_last_result_causes(question: str, last: dict[str, Any], posts: list[dict[str, Any]]) -> str:
    """직전 결과에서 지연/기한/원인 질문에 답한다. 근거 없는 원인은 단정하지 않는다."""
    target_posts = _cause_target_posts(question, posts)
    lines = ["## 원인 확인", ""]
    if not target_posts:
        lines.extend([
            "직전 답변 안에서 원인을 볼 만한 진행 중 또는 기한 경과 업무를 찾지 못했습니다.",
            "",
            f"_직전 질문 「{last.get('question', '')}」 기준입니다._",
        ])
        return "\n".join(lines)

    for index, post in enumerate(target_posts[:5], start=1):
        lines.append(f"{index}. {_post_line(post, with_people=True).lstrip('- ')}")
        signals = _cause_signals_for_post(post)
        if signals:
            lines.append("   - 확인된 근거: " + " / ".join(signals[:3]))
            lines.append(f"   - 추정되는 병목: {_infer_bottleneck(signals)}")
        else:
            lines.append("   - 확인된 근거: 본문·댓글에 원인이 직접 적혀 있지 않습니다.")
            lines.append("   - 추정되는 병목: 근거 부족으로 단정할 수 없습니다.")
        lines.append(f"   - 확인 질문: {_followup_question_for_post(post, signals)}")

    if len(target_posts) > 5:
        lines.append(f"- 외 {len(target_posts) - 5}건은 목록 요청 시 이어서 보여드리겠습니다.")
    lines.extend([
        "",
        "※ 위 원인은 Flow 본문·댓글·하위업무에서 읽히는 근거 기반 추정입니다. 명시 근거가 없는 부분은 확인 질문으로 남겼습니다.",
        f"_직전 질문 「{last.get('question', '')}」 기준입니다._",
    ])
    return "\n".join(lines)


def _cause_target_posts(question: str, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    text = question or ""
    candidates = posts
    if "기한" in text or "지연" in text or "늦" in text:
        overdue = [post for post in candidates if _is_overdue_open_post(post)]
        if overdue:
            return overdue
    open_posts = [post for post in candidates if status_rules.is_open(post.get("task_status"))]
    return open_posts or candidates


def _is_overdue_open_post(post: dict[str, Any]) -> bool:
    end_dt = str(post.get("end_dt") or "").replace("-", "").strip()
    return bool(re.fullmatch(r"\d{8}", end_dt)) and end_dt < datetime.now().strftime("%Y%m%d") and status_rules.is_open(post.get("task_status"))


def _cause_signals_for_post(post: dict[str, Any]) -> list[str]:
    signals: list[str] = []
    for line in _content_excerpt(post, limit=420).splitlines():
        picked = _signal_from_text(line)
        if picked:
            signals.append(picked)

    post_id = str(post.get("post_id") or "").strip()
    if post_id:
        thread = tools.get_post_thread(post_id)
        if isinstance(thread, dict) and thread.get("allowed") is not False:
            for child in thread.get("posts") or []:
                if str(child.get("post_id")) == post_id:
                    continue
                status = child.get("task_status") or "상태 없음"
                title = child.get("title") or "(제목 없음)"
                if status_rules.is_open(status):
                    signals.append(f"하위업무 [{status}] {title}")
            remarks = [
                item for item in (thread.get("comments") or [])
                if not item.get("is_system") and str(item.get("content_text") or "").strip()
            ]
            for comment in remarks[-4:][::-1]:
                picked = _signal_from_text(str(comment.get("content_text") or ""))
                if picked:
                    author = comment.get("author_name") or "작성자"
                    signals.append(f"{author} 댓글: {picked}")

    deduped: list[str] = []
    for signal in signals:
        compact = " ".join(str(signal).split())[:150]
        if compact and compact not in deduped:
            deduped.append(compact)
    return deduped


def _signal_from_text(text: str) -> str:
    compact = " ".join(str(text or "").split())
    if not compact:
        return ""
    keywords = (
        "확인", "컨펌", "문의", "답변", "연장", "미완료", "예정", "요청", "논의", "검토",
        "수정", "등록", "보정", "제안", "대기", "완료", "진행", "반영",
    )
    if any(keyword in compact for keyword in keywords):
        return compact[:150]
    return ""


def _infer_bottleneck(signals: list[str]) -> str:
    joined = " ".join(signals)
    if any(word in joined for word in ["컨펌", "확인", "승인", "답변"]):
        return "확인 또는 의사결정 대기 가능성이 큽니다."
    if any(word in joined for word in ["문의", "업체", "고객센터", "연장", "미완료"]):
        return "외부 답변이나 업체 진행 일정에 묶였을 가능성이 있습니다."
    if any(word in joined for word in ["수정", "보정", "등록", "반영", "제작"]):
        return "제작·수정·등록 작업이 남아 진행이 멈춘 것으로 보입니다."
    if any(word in joined for word in ["논의", "검토", "제안"]):
        return "방향 검토 또는 실행안 확정이 필요한 상태로 보입니다."
    return "근거는 있으나 병목 유형은 추가 확인이 필요합니다."


def _followup_question_for_post(post: dict[str, Any], signals: list[str]) -> str:
    owner = str(post.get("worker") or post.get("author_name") or "담당자").strip()
    title = post.get("title") or "이 건"
    if not signals:
        return f"{owner}에게 '{title}'의 현재 막힌 지점과 새 완료 예정일을 확인하세요."
    return f"{owner}에게 남은 의사결정, 외부 답변 대기 여부, 새 완료 예정일을 확인하세요."


def _format_post_detail(post: dict[str, Any]) -> list[str]:
    """본문 발췌 + 최근 댓글 + 하위 업무. 없으면 그 줄은 생략한다."""
    lines: list[str] = []
    progress = str(post.get("progress") or "").strip()
    if progress and progress not in {"0", "nan"}:
        lines.append(f"   - 진행률 {progress}%")

    excerpt = _content_excerpt(post, limit=300)
    if excerpt:
        for row in excerpt.splitlines()[:5]:
            lines.append(f"   > {row}")

    post_id = str(post.get("post_id") or "").strip()
    if not post_id:
        return lines
    thread = tools.get_post_thread(post_id)
    if not isinstance(thread, dict) or thread.get("allowed") is False:
        return lines

    children = [
        item for item in (thread.get("posts") or [])
        if str(item.get("post_id")) != post_id
    ]
    if children:
        lines.append(f"   - 하위 업무 {len(children)}건: " + ", ".join(
            f"[{child.get('task_status') or '상태 없음'}] {child.get('title') or '(제목 없음)'}"
            for child in children[:4]
        ))

    remarks = [
        item for item in (thread.get("comments") or [])
        if not item.get("is_system") and str(item.get("content_text") or "").strip()
    ]
    if remarks:
        lines.append(f"   - 댓글 {len(remarks)}건 (최근순)")
        for comment in remarks[-3:][::-1]:
            text = " ".join(str(comment.get("content_text") or "").split())[:160]
            when = str(comment.get("written_at") or "")[:8]
            lines.append(f"     · {comment.get('author_name') or '작성자'} ({when}) {text}")
    return lines


def _format_route_answer(tool_name: str, result: Any, *, question: str = "") -> str:
    formatter = {
        "get_team_status": _format_team_status,
        "get_worker_status": _format_worker_status,
        "get_risk_status": _format_risk_status,
        "filter_posts": _format_filtered_posts,
        "find_posts": _format_post_lookup,
        "get_topic_status": _format_topic_status,
        "get_project_status": _format_project_status,
    }.get(tool_name)
    if formatter is None:
        return json.dumps(result, ensure_ascii=False)
    return formatter(result, detailed=_wants_detail(question)) + _data_as_of_note()


def _tool_name_and_args(call: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    function = call.get("function") or {}
    name = str(function.get("name") or "")
    arguments = function.get("arguments") or {}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            arguments = {}
    return name, arguments


def _normalize_history(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in value[-12:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        text = str(item.get("text") or item.get("content") or "").strip()
        if role in {"user", "assistant"} and text:
            normalized.append({"role": role, "text": text[:2000]})
    return normalized


def _compact_evidence(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    compact = dict(payload)
    posts = compact.get("posts")
    if isinstance(posts, list):
        compact["posts"] = [_compact_post(post) for post in posts[:8]]
        compact["posts_truncated"] = max(0, len(posts) - 8)
    projects = compact.get("projects")
    if isinstance(projects, list):
        compact["projects"] = [_compact_project(project) for project in projects]
    return compact


def _memory_posts_for_result(result: Any) -> list[dict[str, Any]]:
    """후속질문에서 다시 볼 대표 목록. 팀 현황은 우선 확인 건을 기억한다."""
    if not isinstance(result, dict):
        return []
    for key in ("posts", "priority_posts", "risk_posts", "overdue_posts"):
        posts = result.get(key)
        if isinstance(posts, list) and posts:
            return posts
    return []


def _compact_project(project: Any) -> Any:
    if not isinstance(project, dict):
        return project
    compact = {
        key: project.get(key)
        for key in ["project_id", "project_name", "post_count", "status_counts", "overdue_count"]
        if key in project
    }
    posts = project.get("posts")
    if isinstance(posts, list):
        compact["sample_titles"] = [
            {
                "title": post.get("title") or "",
                "task_status": post.get("task_status") or "",
                "end_dt": post.get("end_dt") or "",
            }
            for post in posts[:3]
            if isinstance(post, dict)
        ]
        compact["posts_truncated"] = max(0, len(posts) - 3)
    return compact


def _compact_post(post: Any) -> Any:
    if not isinstance(post, dict):
        return post
    content = str(post.get("content_text") or "").replace("\n", " ").strip()
    return {
        "project_id": post.get("project_id") or "",
        "project_name": post.get("project_name") or "",
        "post_id": post.get("post_id") or "",
        "title": post.get("title") or "",
        "task_status": post.get("task_status") or "",
        "worker": post.get("worker") or "",
        "start_dt": post.get("start_dt") or "",
        "end_dt": post.get("end_dt") or "",
        "post_date": post.get("post_date") or "",
        "remark_cnt": post.get("remark_cnt") or "",
        "child_cnt": post.get("child_cnt") or "",
        "content_excerpt": content[:220],
        "post_url": post.get("post_url") or "",
    }


def _record_line(result: dict[str, Any]) -> str | None:
    """업무단위/회의록/액션은 묶음·기록이고 모니터링은 관찰 항목이다.

    진행 업무 집계에서 빼되 감추지는 않는다.
    """
    count = int(result.get("record_count") or 0)
    if not count:
        return None
    counts = _format_counts(result.get("record_counts") or {})
    return f"- 진행 업무 외: {count}건 ({counts}) — 진행 업무 집계에서 제외했습니다"


def _due_text(post: dict[str, Any]) -> str:
    end_dt = str(post.get("end_dt") or "")
    if not end_dt:
        return " / 기한 없음"
    overdue = end_dt.replace("-", "") < datetime.now().strftime("%Y%m%d")
    is_open = status_rules.is_open(post.get("task_status"))
    return f" / 기한 {end_dt}{' ⚠지남' if overdue and is_open else ''}"


def _post_line(post: dict[str, Any], *, with_people: bool = False) -> str:
    status = post.get("task_status") or "상태 없음"
    title = post.get("title") or "(제목 없음)"
    people = f" / {_format_people(post)}" if with_people else ""
    return (
        f"- **[{status}]** {title}{people} / {_format_project_label(post)}"
        f"{_due_text(post)}{_format_source_link(post)}"
    )


def _format_project_status(status: dict[str, Any], *, detailed: bool = False) -> str:
    project_id = status.get("project_id") or ""
    project_name = _short_project_name(status.get("project_name") or tools.ALLOWED_PROJECTS.get(str(project_id), "프로젝트"))
    if status.get("post_count") == 0:
        return f"## 프로젝트 {project_name}\n\n수집된 게시글이 없습니다.\n\n{_format_flow_evidence(status)}"

    lines = [
        f"## 프로젝트 {project_name}",
        "",
        f"- 업무: {status.get('task_count', 0)}건 (진행 중 {status.get('open_count', 0)}건)",
        f"- 상태: {_format_counts(status.get('status_counts') or {})}",
        f"- 기한 경과: {status.get('overdue_count', 0)}건",
    ]
    if detailed:
        lines.append(f"- 작성자: {_format_counts(status.get('author_counts') or {})}")
    record_line = _record_line(status)
    if record_line:
        lines.append(record_line)

    open_posts = status.get("posts") or []
    lines.extend(["", "## 지금 볼 업무", ""])
    if open_posts:
        limit = 8 if detailed else 4
        lines.extend(_post_line(post, with_people=True) for post in open_posts[:limit])
        if not detailed and len(open_posts) > limit:
            lines.append(f"- 외 {len(open_posts) - limit}건은 '전체 목록'이라고 물으면 보여드리겠습니다.")
    else:
        lines.append("- 진행 중인 업무가 없습니다.")

    overdue_posts = status.get("overdue_posts") or []
    if overdue_posts:
        lines.extend(["", "## 먼저 확인할 기한 경과", ""])
        limit = 8 if detailed else 4
        lines.extend(_post_line(post, with_people=True) for post in overdue_posts[:limit])

        lines.extend([
            "",
            "## 다음 액션",
            "- 기한 경과 건은 오늘 완료 가능 여부와 새 완료일을 확인합니다.",
            "- 피드백 건이 있으면 필요한 의사결정 또는 보완 내용을 먼저 확인합니다.",
        ])

    lines.extend(["", _format_flow_evidence(status)])
    return "\n".join(lines)


def _format_team_status(result: dict[str, Any], *, detailed: bool = False) -> str:
    members = result.get("members") or []
    priority_posts = result.get("priority_posts") or []
    active_total = sum(int(member.get("active_count") or 0) for member in members)
    overdue_total = sum(int(member.get("overdue_count") or 0) for member in members)
    total_tasks = sum(int(member.get("task_count") or 0) for member in members)

    most_loaded = sorted(
        members,
        key=lambda item: (int(item.get("overdue_count") or 0), int(item.get("active_count") or 0)),
        reverse=True,
    )
    lead = most_loaded[0] if most_loaded else {}
    lead_text = (
        f"{lead.get('worker')}님 쪽에 열린 업무 {lead.get('active_count', 0)}건, "
        f"기한 경과 {lead.get('overdue_count', 0)}건이 몰려 있습니다."
        if lead else "팀원별 집계 대상이 없습니다."
    )
    lines: list[str] = [
        "## 결론",
        "",
        f"- 전체 업무 **{result.get('task_count', 0)}건** 중 열린 업무가 **{active_total}건**, 기한 경과가 **{overdue_total}건**입니다.",
        f"- 바로 확인할 피드백/보완은 **{len(priority_posts)}건**입니다.",
        f"- {lead_text}",
    ]
    if detailed:
        lines.insert(3, f"- 팀원별 합산은 **{total_tasks}건**입니다({result.get('basis_label') or tools.basis_label()} 기준). 공동 담당 글은 팀원별로 중복 집계됩니다.")
    record_line = _record_line(result)
    if record_line:
        lines.append(record_line)

    priority_posts = result.get("priority_posts") or []
    if priority_posts:
        lines.extend(["", "## 바로 물어볼 건", ""])
        limit = 8 if detailed else 3
        lines.extend(_post_line(post, with_people=True) for post in priority_posts[:limit])
        lines.append("")

    lines.extend([
        "## 팀원별 한눈에 보기",
        "",
        f"- 분류 기준: **{result.get('basis_label') or tools.basis_label()}** — {result.get('basis_note') or tools.basis_note()}",
    ])
    for member in members[:12]:
        counts = member.get("status_counts") or {}
        done = int(counts.get("완료") or 0)
        projects = ", ".join(
            f"{_format_project_label(project)} {project.get('task_count', 0)}건"
            for project in (member.get("projects") or [])[:2]
        ) or "-"
        record_count = int(member.get("record_count") or 0)
        record_text = f" / 기록 {record_count}건" if record_count else ""
        lines.append(
            f"- **{member.get('worker')}**: 업무 {member.get('task_count', 0)}건 / "
            f"진행 중 {member.get('active_count', 0)}건 / 완료 {done}건 / "
            f"기한 경과 {member.get('overdue_count', 0)}건{record_text}"
        )
        if detailed:
            lines.append(f"  - 주요 프로젝트: {projects}")

    lines.extend([
        "",
        "## 다음에 바로 물어볼 질문",
        f"- \"{lead.get('worker') or '담당자'} 기한 지난 건 뭐야?\"",
        "- \"피드백/보완 3건 원인이 뭐야?\"",
        "- \"차보령 대리는?\"처럼 이름만 물어도 개인 현황으로 이어서 보겠습니다.",
    ])
    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_risk_status(result: dict[str, Any], *, detailed: bool = False) -> str:
    project_name = _short_project_name(result.get("project_name") or "전체 허용 프로젝트")
    posts = result.get("posts") or []
    priority_only = bool(result.get("priority_only"))
    title = "피드백/보완 확인" if priority_only else "위험 업무"
    heading = title if not result.get("project_id") else f"{project_name} {title}"
    lines = [
        f"## {heading}",
        "",
        f"- 확인 대상: **{result.get('post_count', 0)}건**",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
    ]
    if not posts:
        empty_message = "현재 기준으로 피드백/보완 업무가 없습니다." if priority_only else "현재 기준으로 기한 경과/보류/피드백/대기 위험 업무가 없습니다."
        lines.extend(["", empty_message])
    else:
        lines.extend(["", "## 확인 목록", ""])
        limit = 15 if detailed else 5
        for post in posts[:limit]:
            if priority_only:
                lines.extend(_format_priority_post_card(post))
                continue
            status = post.get("task_status") or "상태 없음"
            title = post.get("title") or "(제목 없음)"
            project = _format_project_label(post)
            end_dt = post.get("end_dt") or ""
            due_text = f" / 기한 {end_dt}" if end_dt else ""
            lines.append(f"- **[{status}]** {title} / {_format_people(post)} / {project}{due_text}{_format_source_link(post)}")
        lines.extend(["", "## 다음 액션"])
        if priority_only:
            lines.extend([
                "- 피드백 건은 필요한 의사결정 또는 보완 내용을 확인합니다.",
                "- 보완 건은 무엇을 보완해야 하는지와 담당자를 먼저 확인합니다.",
            ])
        else:
            lines.extend([
                "- 기한이 지난 업무만 완료 가능 여부와 새 완료일을 확인합니다.",
                "- 피드백/보완은 필요한 의사결정 또는 승인자를 확인합니다.",
                "- 보류/대기는 별도 요청이 있을 때만 실행 대기 사유를 정리합니다.",
            ])

    project_id = result.get("project_id") or ""
    args = []
    if project_id:
        args.append(f"project_id={project_id}")
    if priority_only:
        args.append("priority_only=True")
    args_text = ", ".join(args)
    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_post_lookup(result: dict[str, Any], *, detailed: bool = False) -> str:
    keyword = result.get("keyword") or "검색어"
    posts = result.get("posts") or []
    if not posts:
        return f"`{keyword}` 관련 게시글을 허용된 3개 프로젝트에서 찾지 못했습니다.\n\n{_format_flow_evidence(result)}"

    lines = [
        "## 게시글 확인",
        "",
        f"- 검색어: **{keyword}**",
        f"- 확인된 게시글: **{result.get('count', len(posts))}건**",
    ]

    limit = 5 if detailed else 3
    for post in posts[:limit]:
        status = post.get("task_status") or "상태 없음"
        title = post.get("title") or "(제목 없음)"
        end_dt = post.get("end_dt") or ""
        due_text = f" / 기한 {end_dt}" if end_dt else ""
        lines.extend([
            "",
            f"### {title}",
            f"- 상태: **{status}**",
            f"- {_format_people(post)}",
            f"- {_format_project_label(post)}{due_text}{_format_source_link(post)}",
        ])
        excerpt = _content_excerpt(post, limit=520)
        if excerpt:
            if detailed:
                lines.extend(["", "본문 일부:", excerpt])
    if not detailed and len(posts) > limit:
        lines.append(f"- 외 {len(posts) - limit}건은 '자세히'라고 물으면 보여드리겠습니다.")

    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_filtered_posts(result: dict[str, Any], *, detailed: bool = False) -> str:
    project_name = _short_project_name(result.get("project_name") or "전체 허용 프로젝트")
    status_filter = result.get("status_filter") or ""
    due_filter = result.get("due_filter") or ""
    labels = []
    if status_filter:
        labels.append(f"상태 {status_filter}")
    if due_filter == "none":
        labels.append("기한 없음")
    elif due_filter == "overdue":
        labels.append("기한 경과")
    elif re.fullmatch(r"\d{8}", str(due_filter)):
        labels.append(f"기한 {due_filter}")
    label_text = " · ".join(labels) or "필터"
    # 회의록/업무단위/액션/모니터링은 진행 업무가 아니므로 "업무"라고 부르지 않는다
    noun = " 업무" if status_rules.is_task(status_filter) else ""
    heading = f"{label_text}{noun}" if not result.get("project_id") else f"{project_name} {label_text}{noun}"

    posts = result.get("posts") or []
    # 기록만 조회한 경우 업무 집계가 비어 있으므로 기록 쪽 수치를 보여준다
    is_record_query = not status_rules.is_task(status_filter)
    counts = result.get("record_counts") if is_record_query else result.get("status_counts")
    total = result.get("record_count") if is_record_query else result.get("task_count", result.get("post_count", 0))
    lines = [
        f"## {heading}",
        "",
        f"- 확인 대상: **{total or 0}건**",
        f"- 상태: {_format_counts(counts or {})}",
    ]
    if not is_record_query:
        record_line = _record_line(result)
        if record_line:
            lines.append(record_line)
    if not posts:
        lines.extend(["", "해당 조건의 업무가 없습니다."])
    else:
        lines.extend(["", "## 확인 목록", ""])
        # 다른 목록과 같은 형식을 쓴다. 기한 경과에는 표시가 붙는다
        limit = 15 if detailed else 5
        lines.extend(_post_line(post, with_people=True) for post in posts[:limit])
        if len(posts) > limit:
            prompt = "전체 목록" if not detailed else "이어서"
            lines.append(f"- 외 {len(posts) - limit}건은 '{prompt}'이라고 물으면 보여드리겠습니다.")

    args = []
    if status_filter:
        args.append(f"status={status_filter}")
    if due_filter:
        args.append(f"due={due_filter}")
    if result.get("project_id"):
        args.append(f"project_id={result.get('project_id')}")
    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_worker_status(result: dict[str, Any], *, detailed: bool = False) -> str:
    worker = result.get("worker") or "작성자"
    basis_label = result.get("basis_label") or tools.basis_label()
    if not result.get("post_count"):
        return (
            f"{worker} {basis_label} 기준 게시글은 허용된 3개 프로젝트의 수집 데이터에서 찾지 못했습니다.\n\n"
            f"{_format_flow_evidence(result)}"
        )

    open_count = int(result.get("open_count") or 0)
    overdue_count = int(result.get("overdue_count") or 0)
    lines = [
        f"## {worker} 진행상황",
        "",
        f"- 분류 기준: **{basis_label}** — {result.get('basis_note') or tools.basis_note()}",
        f"- 결론: 업무 **{result.get('task_count', 0)}건** 중 열린 업무가 **{open_count}건**, 기한 경과가 **{overdue_count}건**입니다.",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
    ]
    record_line = _record_line(result)
    if record_line:
        lines.append(record_line)

    lines.extend(["", "## 프로젝트별"])
    project_limit = None if detailed else 3
    for project in (result.get("projects", [])[:project_limit]):
        lines.append(
            f"- {_format_project_label(project)} ({project.get('project_id')}): "
            f"업무 {project.get('task_count', 0)}건, 상태 {_format_counts(project.get('status_counts') or {})}, "
            f"기한 경과 {project.get('overdue_count', 0)}건"
        )

    posts = result.get("posts") or []
    lines.extend(["", "## 지금 볼 업무"])
    if posts:
        limit = 8 if detailed else 5
        lines.extend(_post_line(post) for post in posts[:limit])
        if not detailed and len(posts) > limit:
            lines.append(f"- 외 {len(posts) - limit}건은 '전체 목록'이라고 물으면 보여드리겠습니다.")
    else:
        lines.append("- 진행 중인 업무가 없습니다.")

    if posts:
        first = posts[0].get("title") or "첫 번째 건"
        lines.extend([
            "",
            "## 다음에 바로 물어볼 질문",
            f"- \"{first} 왜 지났어?\"",
            f"- \"{worker} 기한 지난 건만 보여줘\"",
        ])

    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_topic_status(result: dict[str, Any], *, detailed: bool = False) -> str:
    keyword = result.get("keyword") or "주제"
    if not result.get("post_count"):
        return (
            f"{keyword} 관련 게시글은 허용된 3개 프로젝트의 수집 데이터에서 찾지 못했습니다.\n\n"
            f"{_format_flow_evidence(result)}"
        )

    lines = [
        f"{keyword} 관련 Flow 진행상황 요약입니다.",
        "",
        f"- 관련 업무: {result.get('task_count', 0)}건",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
    ]
    record_line = _record_line(result)
    if record_line:
        lines.append(record_line)

    lines.extend(["", "프로젝트별:"])
    for project in result.get("projects", []):
        lines.append(
            f"- {project.get('project_name')} ({project.get('project_id')}): "
            f"업무 {project.get('task_count', 0)}건, 상태 {_format_counts(project.get('status_counts') or {})}"
        )

    posts = result.get("posts") or []
    status_counts = result.get("status_counts") or {}
    in_progress_count = int(status_counts.get("진행") or 0)
    completed_count = int(status_counts.get("완료") or 0)
    pending_count = sum(int(status_counts.get(status) or 0) for status in ["대기", "보류", "피드백"])

    lines.extend(["", "결론:"])
    if in_progress_count:
        lines.append(f"- 현재 진행 중인 마케팅/성과 관련 업무가 {in_progress_count}건 있어, 아직 완료 단계라기보다 실행·점검 중입니다.")
    if completed_count:
        lines.append(f"- 완료된 관련 업무는 {completed_count}건입니다.")
    if pending_count:
        lines.append(f"- 대기/보류/피드백 상태가 {pending_count}건 있어 후속 의사결정이나 실행 대기가 남아 있습니다.")
    lines.append("- 정량 실적(노출, 클릭, 전환, 매출 등)은 Flow 게시글에 명시된 값만 근거로 판단할 수 있습니다. 누락된 수치는 임의로 추정하지 않았습니다.")

    if posts:
        lines.extend(["", "주요 근거 게시글:"])
        limit = 10 if detailed else 5
        for post in posts[:limit]:
            status = post.get("task_status") or "상태 없음"
            title = post.get("title") or "(제목 없음)"
            project_id = post.get("project_id") or ""
            content = str(post.get("content_text") or "").replace("\n", " ").strip()
            excerpt = content[:120] + ("..." if len(content) > 120 else "")
            lines.append(f"- [{status}] {title} ({project_id})")
            if excerpt and detailed:
                lines.append(f"  {excerpt}")
        if not detailed and len(posts) > limit:
            lines.append(f"- 외 {len(posts) - limit}건은 '자세히'라고 물으면 이어서 보겠습니다.")

    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_counts(counts: dict[str, Any]) -> str:
    if not counts:
        return "없음"
    return ", ".join(f"{key} {value}" for key, value in counts.items())


def _format_people(post: dict[str, Any]) -> str:
    author = str(post.get("author_name") or "").strip()
    worker = str(post.get("worker") or "").strip()
    parts = [f"작성자 {author or '미지정'}"]
    if worker and worker != author:
        parts.append(f"참여자 {worker}")
    return " / ".join(parts)


def _format_priority_post_card(post: dict[str, Any]) -> list[str]:
    title = post.get("title") or "(제목 없음)"
    status = post.get("task_status") or "상태 없음"
    start_dt = _format_date_value(post.get("start_dt"))
    end_dt = _format_date_value(post.get("end_dt"))
    remaining = _remaining_days_label(post.get("end_dt"))
    progress = _progress_label(post)
    summary = _three_line_summary(post)
    lines = [
        f"### {title}",
        f"- 상태: **{status}**",
        f"- {_format_people(post)}",
        f"- {_format_project_label(post)}",
        f"- 시작일: {start_dt}",
        f"- 마감일: {end_dt}",
        f"- 남은 기간: {remaining}",
        f"- 진행률: {progress}",
    ]
    if summary:
        lines.extend(["", "내용 3줄 요약"])
        lines.extend(f"{index}. {line}" for index, line in enumerate(summary, start=1))
    source = _format_source_link(post).removeprefix(" / ")
    if source:
        lines.extend(["", source])
    lines.append("")
    return lines


def _format_project_label(item: dict[str, Any]) -> str:
    name = _short_project_name(str(item.get("project_name") or "").strip())
    project_id = str(item.get("project_id") or "").strip()
    return f"프로젝트 {name or project_id or '미지정'}"


def _format_source_link(post: dict[str, Any]) -> str:
    url = str(post.get("post_url") or "").strip()
    if not url:
        return ""
    return f" / 출처 [Flow 열기]({url})"


def _format_flow_evidence(payload: dict[str, Any]) -> str:
    urls = _collect_flow_urls(payload)
    if not urls:
        urls = [
            f"https://flow.team/main.act?projectId={project_id}"
            for project_id in tools.ALLOWED_PROJECTS
        ]
    links = " ".join(f"[Flow 열기]({url})" for url in urls[:5])
    more = f" 외 {len(urls) - 5}개" if len(urls) > 5 else ""
    return f"근거: {links}{more}"


def _collect_flow_urls(value: Any) -> list[str]:
    urls: list[str] = []

    def add(url: Any) -> None:
        text = str(url or "").strip()
        if text.startswith("http") and text not in urls:
            urls.append(text)

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            add(item.get("post_url"))
            add(item.get("project_url"))
            project_id = str(item.get("project_id") or "").strip()
            if project_id and not any(f"projectId={project_id}" in url for url in urls):
                add(f"https://flow.team/main.act?projectId={project_id}")
            for nested in item.values():
                walk(nested)
        elif isinstance(item, list):
            for nested in item:
                walk(nested)

    walk(value)
    return urls


def _content_excerpt(post: dict[str, Any], *, limit: int = 500) -> str:
    text = str(post.get("content_text") or "").replace("\r", "\n").strip()
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    compact = "\n".join(lines)
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit].rstrip()}..."


def _format_date_value(value: Any) -> str:
    parsed = _parse_flow_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else "미등록"


def _remaining_days_label(value: Any) -> str:
    due = _parse_flow_date(value)
    if not due:
        return "산정불가"
    today = datetime.now().date()
    days = (due.date() - today).days
    if days > 0:
        return f"D-{days}"
    if days == 0:
        return "D-Day"
    return f"D+{abs(days)}"


def _progress_label(post: dict[str, Any]) -> str:
    progress = post.get("progress")
    progress_text = str(progress or "").strip()
    start = _parse_flow_date(post.get("start_dt"))
    end = _parse_flow_date(post.get("end_dt"))
    if progress_text in {"0", "0.0"} and not start and not end:
        progress_text = ""
    if progress_text and progress_text.lower() != "nan":
        return progress_text if progress_text.endswith("%") else f"{progress_text}%"
    if not start or not end:
        return "산정불가"
    total = max((end.date() - start.date()).days, 1)
    elapsed = (datetime.now().date() - start.date()).days
    percent = max(0, min(100, round(elapsed / total * 100)))
    return f"{percent}% 기간 소진"


def _parse_flow_date(value: Any) -> datetime | None:
    text = str(value or "").strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", text):
        return None
    try:
        return datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def _three_line_summary(post: dict[str, Any]) -> list[str]:
    text = str(post.get("content_text") or "").replace("\xa0", " ").strip()
    if not text:
        return []
    raw_lines = [line.strip(" -\t") for line in re.split(r"[\r\n]+", text) if line.strip(" -\t")]
    candidates: list[str] = []
    for line in raw_lines:
        normalized = re.sub(r"\s+", " ", line).strip()
        normalized = re.sub(r"^\d+[\.)]\s*", "", normalized)
        if len(normalized) < 8:
            continue
        if normalized in candidates:
            continue
        candidates.append(normalized)
        if len(candidates) >= 3:
            break
    if len(candidates) < 3:
        sentences = re.split(r"(?<=[.!?。]|입니다|했습니다|합니다|됩니다|였습니다)\s*", re.sub(r"\s+", " ", text))
        for sentence in sentences:
            normalized = sentence.strip(" -\t")
            if len(normalized) < 12 or normalized in candidates:
                continue
            candidates.append(normalized)
            if len(candidates) >= 3:
                break
    return [line[:160] + ("..." if len(line) > 160 else "") for line in candidates[:3]]


def _short_project_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    prefix = "[브랜드 전략기획부] "
    if text.startswith(prefix):
        text = text[len(prefix):]
    return text


def run_server(*, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    server = ThreadingHTTPServer((host, port), DoridangBotHandler)
    logger.info("Starting Doridang bot server on http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Doridang bot server interrupted.")
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
