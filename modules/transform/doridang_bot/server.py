"""HTTP server for Doridang Flow assistant."""

from __future__ import annotations

import json
import logging
import mimetypes
import re
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from modules.transform.doridang_bot import conversation, llm_backend, tools
from modules.transform.doridang_bot.prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8788
STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_TOOL_LOOPS = 5


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
            if not question:
                self._send_json({"error": "message is required"}, status=HTTPStatus.BAD_REQUEST)
                return
            self._stream_chat(user=user, question=question, history=history)
        except Exception as exc:
            logger.exception("채팅 처리 실패: %s", exc)
            if not self.wfile.closed:
                self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _stream_chat(self, *, user: str, question: str, history: list[dict[str, str]] | None = None) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self._send_cors_headers()
        self.end_headers()

        messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        project_hint = tools.detect_project_id(question)
        if project_hint:
            messages.append({
                "role": "system",
                "content": f"이 질문에서 감지된 project_id는 {project_hint} ({tools.ALLOWED_PROJECTS[project_hint]})입니다. 해당 프로젝트 인자로 tool을 호출하세요.",
            })
        if history:
            messages.append({
                "role": "system",
                "content": "현재 웹 세션의 최근 대화입니다. 사용자가 짧게 이어서 질문하면 이 맥락을 우선 참고하세요.",
            })
            for item in history[-10:]:
                role = "assistant" if item.get("role") == "assistant" else "user"
                content = str(item.get("text") or "").strip()
                if content and content != question:
                    messages.append({"role": role, "content": content[:2000]})
        messages.append({"role": "user", "content": question})
        answer_parts: list[str] = []
        evidence: list[dict[str, Any]] = []

        try:
            if _is_light_chat(question):
                answer = "안녕하세요. Flow 프로젝트 현황, 담당자 진행상황, 마케팅 실적 같은 내용을 물어보시면 수집 데이터를 기준으로 답변하겠습니다."
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            if tools.detect_team_status_intent(question):
                basis = _basis_for_question(question, default="author")
                arguments: dict[str, Any] = {"basis": basis}
                result = tools.get_team_status(basis=basis)
                answer = _format_team_status(result)
                evidence.append({"name": "get_team_status", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "get_team_status", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            if tools.detect_risk_intent(question):
                filters = _status_due_filters(question)
                if filters:
                    if project_hint:
                        filters["project_id"] = project_hint
                    result = tools.filter_posts(**filters)
                    answer = _format_filtered_posts(result)
                    evidence.append({"name": "filter_posts", "arguments": filters})
                    self._send_sse({"type": "tool", "name": "filter_posts", "arguments": filters})
                    self._send_sse({"type": "delta", "text": answer})
                    conversation.append_turn(user, question, answer, evidence)
                    self._send_raw_sse("[DONE]")
                    self.close_connection = True
                    return

                priority_only = _is_priority_status_question(question)
                arguments = {"priority_only": priority_only}
                if project_hint:
                    arguments["project_id"] = project_hint
                result = tools.get_risk_status(project_hint, priority_only=priority_only)
                answer = _format_risk_status(result)
                evidence.append({"name": "get_risk_status", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "get_risk_status", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            worker_hint = tools.detect_worker_name(question)
            if worker_hint:
                basis = _basis_for_question(question, default="author")
                arguments = {"worker": worker_hint, "basis": basis}
                result = tools.get_worker_status(worker_hint, basis=basis)
                answer = _format_worker_status(result)
                evidence.append({"name": "get_worker_status", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "get_worker_status", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            post_lookup = tools.find_posts(question)
            if _should_answer_post_lookup(question, post_lookup):
                arguments = {"keyword": question}
                answer = _format_post_lookup(post_lookup)
                evidence.append({"name": "find_posts", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "find_posts", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            topic_hint = tools.detect_topic_keyword(question)
            if topic_hint:
                arguments = {"keyword": topic_hint}
                result = tools.get_topic_status(topic_hint)
                answer = _format_topic_status(result)
                evidence.append({"name": "get_topic_status", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "get_topic_status", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            if project_hint:
                status = tools.get_project_status(project_hint)
                arguments = {"project_id": project_hint}
                answer = _format_project_status(status)
                evidence.append({"name": "get_project_status", "arguments": arguments})
                self._send_sse({"type": "tool", "name": "get_project_status", "arguments": arguments})
                self._send_sse({"type": "delta", "text": answer})
                conversation.append_turn(user, question, answer, evidence)
                self._send_raw_sse("[DONE]")
                self.close_connection = True
                return

            for _ in range(MAX_TOOL_LOOPS):
                tool_calls: list[dict[str, Any]] = []
                for event in llm_backend.chat_stream(messages, tools=tools.tool_schemas()):
                    if event.get("type") == "delta":
                        answer_parts.append(str(event.get("text") or ""))
                        self._send_sse(event)
                    elif event.get("type") == "tool_calls":
                        tool_calls.extend(event.get("calls") or [])
                    else:
                        self._send_sse(event)

                if not tool_calls:
                    break

                assistant_message = {"role": "assistant", "content": "".join(answer_parts), "tool_calls": tool_calls}
                messages.append(assistant_message)
                for call in tool_calls:
                    name, arguments = _tool_name_and_args(call)
                    self._send_sse({"type": "tool", "name": name, "arguments": arguments})
                    result = tools.execute_tool(name, arguments)
                    evidence.append({"name": name, "arguments": arguments})
                    messages.append({
                        "role": "tool",
                        "content": json.dumps(result, ensure_ascii=False),
                        "tool_name": name,
                    })
                answer_parts.clear()
            else:
                self._send_sse({"type": "delta", "text": "\n\nTool 조회 반복 한도를 초과해 답변을 중단했습니다."})

            answer = "".join(answer_parts).strip()
            if answer:
                conversation.append_turn(user, question, answer, evidence)
            self._send_raw_sse("[DONE]")
            self.close_connection = True
        except Exception as exc:
            logger.exception("SSE 스트리밍 실패: %s", exc)
            self._send_sse({"type": "error", "message": str(exc)})
            self._send_raw_sse("[DONE]")
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

    def _stream_grounded_gpt_answer(self, *, question: str, evidence_name: str, evidence_payload: Any) -> str:
        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 도리당봇입니다. 반드시 한국어로 답합니다. "
                    "아래 제공된 업무 현황 데이터만 사용해서 리더 보고용으로 답하세요. "
                    "근거에 없는 수치, 원인, 일정, 결과는 추측하지 말고 '근거 없음' 또는 '확인 필요'라고 말하세요. "
                    "답변은 결론부터 쓰고, 진행상황/피드백/리스크/다음 액션을 질문 의도에 맞게 정리하세요. "
                    "JSON 템플릿, 코드, 일반적인 워크플로 설명을 출력하지 말고 최종 답변만 작성하세요. "
                    "마지막 줄에는 반드시 Flow 원문 또는 프로젝트 URL을 '근거: [Flow 열기](URL)' 형식으로 적으세요."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"사용자 질문:\n{question}\n\n"
                    f"사용 가능한 근거 tool: {evidence_name}\n"
                    f"업무 현황 데이터:\n{json.dumps(_compact_evidence(evidence_payload), ensure_ascii=False)}"
                ),
            },
        ]
        answer_parts: list[str] = []
        for event in llm_backend.chat_stream(messages, tools=None):
            if event.get("type") == "delta":
                text = str(event.get("text") or "")
                answer_parts.append(text)
                self._send_sse({"type": "delta", "text": text})
            else:
                self._send_sse(event)
        return "".join(answer_parts).strip()


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


def _is_light_chat(question: str) -> bool:
    normalized = (question or "").strip().replace(" ", "")
    return normalized in {"안녕", "안녕하세요", "하이", "ㅎㅇ"}


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


def _basis_for_question(question: str, *, default: str = "author") -> str:
    if tools.detect_author_basis(question):
        return "author"
    if "담당자" in (question or "") or "담당 기준" in (question or ""):
        return "worker"
    return default


def _is_priority_status_question(question: str) -> bool:
    text = question or ""
    normalized = text.replace(" ", "")
    if normalized in {"상태", "상태확인", "피드백건확인용", "상태피드백건확인용"}:
        return True
    return any(keyword in text for keyword in ["피드백", "결제중", "결재중", "결제"])


def _should_answer_post_lookup(question: str, result: dict[str, Any]) -> bool:
    text = (question or "").strip()
    if len(text) < 4:
        return False
    posts = result.get("posts") or []
    if not posts:
        return False
    if len(posts) == 1:
        return True
    normalized = text.replace(" ", "")
    return any(normalized == str(post.get("title") or "").replace(" ", "") for post in posts[:5])


def _status_due_filters(question: str) -> dict[str, Any] | None:
    text = question or ""
    filters: dict[str, Any] = {}
    for status in ["보류", "대기", "진행", "완료", "피드백", "결제중", "결재중", "보완", "액션"]:
        if status in text:
            filters["status"] = "결제중" if status == "결재중" else status
            break
    if "기한 없음" in text or "기한없음" in text:
        filters["due"] = "none"
    elif "기한 지난" in text or "기한 경과" in text:
        filters["due"] = "overdue"
    else:
        matched = re.search(r"\b(20\d{6})\b", text)
        if matched:
            filters["due"] = matched.group(1)
    if not filters:
        return None
    return filters


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


def _format_project_status(status: dict[str, Any]) -> str:
    project_id = status.get("project_id") or ""
    project_name = _short_project_name(status.get("project_name") or tools.ALLOWED_PROJECTS.get(str(project_id), "프로젝트"))
    if status.get("post_count") == 0:
        return f"## 프로젝트 {project_name}\n\n수집된 게시글이 없습니다.\n\n{_format_flow_evidence(status)}"

    lines = [
        f"## 프로젝트 {project_name}",
        "",
        f"- 게시글: {status.get('post_count', 0)}건",
        f"- 상태: {_format_counts(status.get('status_counts') or {})}",
        f"- 작성자: {_format_counts(status.get('author_counts') or {})}",
        f"- 기한 경과: {status.get('overdue_count', 0)}건",
    ]

    overdue_posts = status.get("overdue_posts") or []
    if overdue_posts:
        lines.extend(["", "## 기한 경과 게시글", ""])
        for post in overdue_posts[:8]:
            status_text = post.get("task_status") or "상태 없음"
            title = post.get("title") or "(제목 없음)"
            end_dt = post.get("end_dt") or ""
            due_text = f" / 기한 {end_dt}" if end_dt else ""
            lines.append(f"- **[{status_text}]** {title} / {_format_people(post)}{due_text}{_format_source_link(post)}")

        lines.extend([
            "",
            "## 다음 액션",
            "- 기한 경과 건은 오늘 완료 가능 여부와 새 완료일을 확인합니다.",
            "- 피드백 건이 있으면 필요한 의사결정 또는 보완 내용을 먼저 확인합니다.",
        ])

    lines.extend(["", _format_flow_evidence(status)])
    return "\n".join(lines)


def _format_team_status(result: dict[str, Any]) -> str:
    members = result.get("members") or []
    priority_posts = result.get("priority_posts") or []
    active_total = sum(int(member.get("active_count") or 0) for member in members)
    overdue_total = sum(int(member.get("overdue_count") or 0) for member in members)
    total_posts = sum(int(member.get("post_count") or 0) for member in members)

    lines: list[str] = [
        "## 결론",
        "",
        f"- 허용 프로젝트 전체 게시글은 **{result.get('post_count', 0)}건**입니다.",
        f"- 팀원별 작성자/참여 합산은 **{total_posts}건**입니다. 공동 참여 글은 팀원별로 중복 집계됩니다.",
        f"- 진행·대기·보류·피드백·결제중 상태는 **{active_total}건**, 기한 경과는 **{overdue_total}건**입니다.",
        f"- 피드백/결제중 즉시 확인 건은 **{len(priority_posts)}건**입니다.",
    ]

    priority_posts = result.get("priority_posts") or []
    if priority_posts:
        lines.extend(["", "## 바로 처리할 건", ""])
        for post in priority_posts[:8]:
            status = post.get("task_status") or "상태 없음"
            end_dt = post.get("end_dt") or "기한 없음"
            title = post.get("title") or "(제목 없음)"
            project = _format_project_label(post)
            due_text = f" / 기한 {end_dt}" if post.get("end_dt") else ""
            lines.append(f"- **[{status}]** {title} / {_format_people(post)} / {project}{due_text}{_format_source_link(post)}")
        lines.append("")

    lines.extend([
        "## 팀원별 현황",
        "",
        f"- 분류 기준: **{result.get('basis_label', '작성자')}**",
    ])
    for member in members[:12]:
        counts = member.get("status_counts") or {}
        active = sum(int(counts.get(status) or 0) for status in ["진행", "대기", "보류", "피드백", "결제중"])
        done = int(counts.get("완료") or 0)
        projects = ", ".join(
            f"{_format_project_label(project)} {project.get('post_count', 0)}건"
            for project in (member.get("projects") or [])[:2]
        ) or "-"
        lines.append(f"- **{member.get('worker')}**: 전체 {member.get('post_count', 0)}건 / 진행·대기·보류·피드백·결제중 {active}건 / 완료 {done}건 / 기한 경과 {member.get('overdue_count', 0)}건")
        lines.append(f"  - 주요 프로젝트: {projects}")

    lines.extend([
        "",
        "## 다음 액션",
        "- 피드백/결제중은 결제 또는 의사결정 지연을 막기 위해 먼저 확인합니다.",
        "- 기한 경과 업무는 오늘 완료 가능 여부와 새 완료일을 확인합니다.",
        "- 보류/대기 상세 목록은 이 화면에서 제외했습니다. 필요할 때 별도로 요청하면 됩니다.",
    ])
    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_risk_status(result: dict[str, Any]) -> str:
    project_name = _short_project_name(result.get("project_name") or "전체 허용 프로젝트")
    posts = result.get("posts") or []
    priority_only = bool(result.get("priority_only"))
    title = "피드백/결제중 확인" if priority_only else "위험 업무"
    heading = title if not result.get("project_id") else f"{project_name} {title}"
    lines = [
        f"## {heading}",
        "",
        f"- 확인 대상: **{result.get('post_count', 0)}건**",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
    ]
    if not posts:
        empty_message = "현재 기준으로 피드백/결제중 업무가 없습니다." if priority_only else "현재 기준으로 기한 경과/보류/피드백/대기 위험 업무가 없습니다."
        lines.extend(["", empty_message])
    else:
        lines.extend(["", "## 확인 목록", ""])
        for post in posts[:15]:
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
                "- 결제중 건은 결제 가능 여부와 승인자를 먼저 확인합니다.",
            ])
        else:
            lines.extend([
                "- 기한이 지난 업무만 완료 가능 여부와 새 완료일을 확인합니다.",
                "- 피드백/결제중은 필요한 의사결정 또는 승인자를 확인합니다.",
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


def _format_post_lookup(result: dict[str, Any]) -> str:
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

    for post in posts[:5]:
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
            lines.extend(["", "본문 일부:", excerpt])

    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_filtered_posts(result: dict[str, Any]) -> str:
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
    heading = f"{label_text} 업무" if not result.get("project_id") else f"{project_name} {label_text} 업무"

    posts = result.get("posts") or []
    lines = [
        f"## {heading}",
        "",
        f"- 확인 대상: **{result.get('post_count', 0)}건**",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
    ]
    if not posts:
        lines.extend(["", "해당 조건의 업무가 없습니다."])
    else:
        lines.extend(["", "## 확인 목록", ""])
        for post in posts[:15]:
            status = post.get("task_status") or "상태 없음"
            title = post.get("title") or "(제목 없음)"
            end_dt = post.get("end_dt") or ""
            due_text = f" / 기한 {end_dt}" if end_dt else ""
            lines.append(
                f"- **[{status}]** {title} / {_format_people(post)} / "
                f"{_format_project_label(post)}{due_text}{_format_source_link(post)}"
            )
        if len(posts) > 15:
            lines.append(f"- 외 {len(posts) - 15}건")

    args = []
    if status_filter:
        args.append(f"status={status_filter}")
    if due_filter:
        args.append(f"due={due_filter}")
    if result.get("project_id"):
        args.append(f"project_id={result.get('project_id')}")
    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_worker_status(result: dict[str, Any]) -> str:
    worker = result.get("worker") or "작성자"
    basis_label = result.get("basis_label", "작성자")
    if not result.get("post_count"):
        return (
            f"{worker} {basis_label} 기준 게시글은 허용된 3개 프로젝트의 수집 데이터에서 찾지 못했습니다.\n\n"
            f"{_format_flow_evidence(result)}"
        )

    lines = [
        f"## {worker} {basis_label} 기준 진행상황",
        "",
        f"- 분류 기준: **{basis_label}**",
        f"- 전체 게시글: {result.get('post_count', 0)}건",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
        f"- 기한 경과: {result.get('overdue_count', 0)}건",
        "",
        "프로젝트별:",
    ]
    for project in result.get("projects", []):
        lines.append(
            f"- {_format_project_label(project)} ({project.get('project_id')}): "
            f"{project.get('post_count', 0)}건, 상태 {_format_counts(project.get('status_counts') or {})}, "
            f"기한 경과 {project.get('overdue_count', 0)}건"
        )

    posts = result.get("posts") or []
    if posts:
        lines.extend(["", "주요 게시글:"])
        for post in posts[:8]:
            status = post.get("task_status") or "상태 없음"
            end_dt = post.get("end_dt") or ""
            due_text = f" / 기한 {end_dt}" if end_dt else ""
            title = post.get("title") or "(제목 없음)"
            lines.append(
                f"- **[{status}]** {title} / {_format_project_label(post)}"
                f"{due_text}{_format_source_link(post)}"
            )

    lines.extend(["", _format_flow_evidence(result)])
    return "\n".join(lines)


def _format_topic_status(result: dict[str, Any]) -> str:
    keyword = result.get("keyword") or "주제"
    if not result.get("post_count"):
        return (
            f"{keyword} 관련 게시글은 허용된 3개 프로젝트의 수집 데이터에서 찾지 못했습니다.\n\n"
            f"{_format_flow_evidence(result)}"
        )

    lines = [
        f"{keyword} 관련 Flow 진행상황 요약입니다.",
        "",
        f"- 관련 게시글: {result.get('post_count', 0)}건",
        f"- 상태: {_format_counts(result.get('status_counts') or {})}",
        "",
        "프로젝트별:",
    ]
    for project in result.get("projects", []):
        lines.append(
            f"- {project.get('project_name')} ({project.get('project_id')}): "
            f"{project.get('post_count', 0)}건, 상태 {_format_counts(project.get('status_counts') or {})}"
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
        for post in posts[:10]:
            status = post.get("task_status") or "상태 없음"
            title = post.get("title") or "(제목 없음)"
            project_id = post.get("project_id") or ""
            content = str(post.get("content_text") or "").replace("\n", " ").strip()
            excerpt = content[:120] + ("..." if len(content) > 120 else "")
            lines.append(f"- [{status}] {title} ({project_id})")
            if excerpt:
                lines.append(f"  {excerpt}")

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
