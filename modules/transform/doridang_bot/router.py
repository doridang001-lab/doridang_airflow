"""Question -> tool routing for Doridang bot.

`_stream_chat`에 흩어져 있던 룰 캐스케이드를 순수 함수로 분리한다.
분리 목적은 라우팅 정확도를 LLM 응답 생성 없이 측정하기 위함이다.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import threading
from dataclasses import dataclass, field
from typing import Any

from modules.transform.doridang_bot import llm_backend, tools
from modules.transform.doridang_bot.prompts import ROUTING_SYSTEM_PROMPT
from modules.transform.utility import flow_task_status as status_rules

logger = logging.getLogger(__name__)

LIGHT_CHAT_TOOL = "__light_chat__"
# 라우팅이 매달리면 룰 폴백으로 넘어간다. 사용자를 무한정 기다리게 두지 않는다.
ROUTE_TIMEOUT_SEC = float(os.getenv("DORIDANG_BOT_ROUTE_TIMEOUT", "30"))
ROUTE_HISTORY_TURNS = 6
LIGHT_CHAT_ANSWER = (
    "안녕하세요. Flow 프로젝트 현황, 담당자 진행상황, 마케팅 실적 같은 내용을 "
    "물어보시면 수집 데이터를 기준으로 답변하겠습니다."
)


@dataclass
class LlmRouting:
    """LLM 라우팅 결과.

    ok=False 는 호출 자체가 실패(타임아웃/오류)했다는 뜻이고, 이때만 룰 폴백으로 넘어간다.
    ok=True 인데 route 가 없으면 모델이 '범위 밖'이라고 판단한 것이고, text 가 그 답변이다.
    """

    ok: bool
    route: "Route | None" = None
    text: str = ""


@dataclass
class Route:
    tool: str
    arguments: dict[str, Any] = field(default_factory=dict)
    result: Any = None  # 라우팅 과정에서 이미 조회했다면 재조회를 피한다

    @property
    def is_light_chat(self) -> bool:
        return self.tool == LIGHT_CHAT_TOOL


def tool_name_and_args(call: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    function = call.get("function") or {}
    name = str(function.get("name") or "")
    arguments = function.get("arguments") or {}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError:
            arguments = {}
    if not isinstance(arguments, dict):
        arguments = {}
    return name, arguments


def _routing_messages(question: str, history: list[dict[str, str]] | None) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = [{"role": "system", "content": ROUTING_SYSTEM_PROMPT}]
    for item in (history or [])[-ROUTE_HISTORY_TURNS:]:
        role = "assistant" if item.get("role") == "assistant" else "user"
        text = str(item.get("text") or "").strip()
        if text and text != question:
            messages.append({"role": role, "content": text[:1200]})
    messages.append({"role": "user", "content": question})
    return messages


_TEXT_CALL_RE = re.compile(r"\b([a-z][a-z_]{3,})\s*\(([^()]*)\)")
_KWARG_RE = re.compile(r"([a-zA-Z_]+)\s*=\s*(\"[^\"]*\"|'[^']*'|[^,\s)]+)")


def _route_from_text(text: str) -> "Route | None":
    """모델이 tool을 호출하는 대신 filter_posts(status="진행") 처럼 문장으로 쓰는 경우를 건진다."""
    matched = _TEXT_CALL_RE.search(text or "")
    if not matched:
        return None
    name = matched.group(1)
    if name not in tools.TOOL_FUNCTIONS:
        return None
    arguments: dict[str, Any] = {}
    for key, raw in _KWARG_RE.findall(matched.group(2)):
        value = raw.strip()
        if value[:1] in "\"'" and value[-1:] == value[:1]:
            value = value[1:-1]
        elif value.lower() in {"true", "false"}:
            value = value.lower() == "true"
        elif value.isdigit():
            value = int(value)
        arguments[key] = value
    logger.info("LLM이 문장으로 쓴 tool 호출을 복구: %s(%s)", name, arguments)
    return Route(name, arguments)


def _sanitize_arguments(question: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """모델이 흘린 인자를 도구가 받을 수 있는 형태로 되돌린다."""
    arguments = dict(arguments or {})
    # 근거 없이 basis를 붙이면 통일한 기본 기준(auto)이 흔들린다
    if "basis" in arguments and not _basis_stated(question):
        arguments.pop("basis")
    # 한글 이름이 깨져 나오면 조회가 0건이 된다
    if arguments.get("worker"):
        snapped = tools.snap_worker_name(str(arguments["worker"]))
        if snapped:
            arguments["worker"] = snapped
    # 허용 밖 project_id는 통째로 거부되므로 차라리 조건에서 뺀다
    if arguments.get("project_id") and str(arguments["project_id"]) not in tools.ALLOWED_PROJECTS:
        logger.warning("허용되지 않은 project_id를 제거: %s", arguments["project_id"])
        arguments.pop("project_id")
    return arguments


# 조건을 붙였는데 고른 tool이 그 인자를 못 받으면 실행 계층에서 조용히 버려진다.
# "진행만 정리해줘"가 get_worker_status(status=진행)으로 가서 전체를 답하던 버그.
FILTER_ARGUMENTS = ("status", "due")
FILTER_TOOL = "filter_posts"


def _redirect_to_filter(tool: str, arguments: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    requested = {key for key in FILTER_ARGUMENTS if arguments.get(key)}
    if not requested:
        return tool, arguments

    missing = requested - tools.accepted_arguments(tool)
    if not missing:
        return tool, arguments

    keep = tools.accepted_arguments(FILTER_TOOL)
    lost = {key for key, value in arguments.items() if value not in (None, "") and key not in keep}
    if lost:
        # 전환하면 오히려 다른 조건(keyword 등)을 잃는다. 그대로 두고 경고만 남긴다.
        logger.warning("%s가 %s를 못 받지만 %s로 전환하면 %s를 잃는다", tool, sorted(missing), FILTER_TOOL, sorted(lost))
        return tool, arguments

    redirected = {key: value for key, value in arguments.items() if key in keep}
    logger.info("%s가 %s를 못 받아 %s로 전환: %s", tool, sorted(missing), FILTER_TOOL, redirected)
    return FILTER_TOOL, redirected


def _basis_stated(question: str) -> bool:
    text = question or ""
    return tools.detect_author_basis(text) or "담당자 기준" in text or "담당 기준" in text


def route_with_llm(question: str, *, history: list[dict[str, str]] | None = None) -> LlmRouting:
    """질문 -> tool 1개. 답변 문장은 만들지 않으므로 작은 모델로 짧게 끝난다."""
    messages = _routing_messages(question, history)
    outcome: queue.Queue = queue.Queue(maxsize=1)

    def _run() -> None:
        calls: list[dict[str, Any]] = []
        parts: list[str] = []
        try:
            for event in llm_backend.chat_stream(
                messages, tools=tools.tool_schemas(), purpose=llm_backend.PURPOSE_ROUTE
            ):
                if event.get("type") == "tool_calls":
                    calls.extend(event.get("calls") or [])
                elif event.get("type") == "delta":
                    parts.append(str(event.get("text") or ""))
            outcome.put((calls, "".join(parts).strip(), None))
        except Exception as exc:  # noqa: BLE001 - 라우팅 실패는 룰 폴백으로 흡수한다
            outcome.put(([], "", exc))

    threading.Thread(target=_run, name="doridang-route", daemon=True).start()
    try:
        calls, text, error = outcome.get(timeout=ROUTE_TIMEOUT_SEC)
    except queue.Empty:
        logger.warning("LLM 라우팅 %.0f초 초과 - 룰 폴백", ROUTE_TIMEOUT_SEC)
        return LlmRouting(ok=False)

    if error is not None:
        logger.warning("LLM 라우팅 실패 - 룰 폴백: %s", error)
        return LlmRouting(ok=False)

    for call in calls:
        name, arguments = tool_name_and_args(call)
        if name in tools.TOOL_FUNCTIONS:
            name, arguments = _redirect_to_filter(name, _sanitize_arguments(question, arguments))
            return LlmRouting(ok=True, route=Route(name, arguments))
        logger.warning("LLM이 등록되지 않은 tool을 골랐다: %s", name)

    if calls:
        # tool을 고르긴 했는데 전부 알 수 없는 이름이면 룰에게 맡긴다
        return LlmRouting(ok=False)

    salvaged = _route_from_text(text)
    if salvaged is not None:
        salvaged.tool, salvaged.arguments = _redirect_to_filter(
            salvaged.tool, _sanitize_arguments(question, salvaged.arguments)
        )
        return LlmRouting(ok=True, route=salvaged)
    return LlmRouting(ok=True, text=text)


@dataclass
class Resolution:
    """최종 라우팅 결과. server와 평가 스크립트가 같은 경로를 타도록 한다."""

    route: "Route | None" = None
    source: str = ""      # light | llm | rule | llm-scope
    text: str = ""        # 범위 밖 안내처럼 tool 없이 모델이 쓴 답변


def _is_plausible(route: "Route") -> bool:
    """LLM이 뽑은 인자가 말이 되는지 본다.

    "...진행상황 승인검증" 을 worker="승인검증" 으로 읽는 등, 사람이 아닌 말을
    이름으로 뽑아내면 조회가 0건이 된다. 이럴 때는 룰에게 맡기는 편이 낫다.
    """
    worker = route.arguments.get("worker")
    if worker and not tools.is_known_person(str(worker)):
        logger.warning("LLM이 뽑은 담당자 %r 는 수집 데이터에 없는 이름이다 - 룰로 넘긴다", worker)
        return False
    return True


def resolve_route(
    question: str,
    *,
    history: list[dict[str, str]] | None = None,
    project_hint: str | None = None,
    use_llm: bool = True,
) -> Resolution:
    if is_light_chat(question):
        return Resolution(Route(LIGHT_CHAT_TOOL), "light")

    if use_llm:
        routing = route_with_llm(question, history=history)
        if routing.route is not None and _is_plausible(routing.route):
            return Resolution(routing.route, "llm")
        if routing.route is not None:
            fallback = route_question(question, project_hint=project_hint)
            if fallback is not None:
                return Resolution(fallback, "rule")
            return Resolution(routing.route, "llm")
        if routing.ok:
            # 모델이 범위 밖이라 했어도 룰이 확실히 아는 질문이면 룰을 믿는다.
            # "황유경" 같은 이름 한 마디를 잡담으로 넘겨버리는 것을 막는다.
            fallback = route_question(question, project_hint=project_hint)
            fallback = _apply_history_context(fallback, history)
            if fallback is not None:
                logger.info("LLM은 범위 밖이라 했지만 룰이 라우팅함: %s", fallback.tool)
                return Resolution(fallback, "rule")
            return Resolution(None, "llm-scope", routing.text)

    route = route_question(question, project_hint=project_hint)
    route = _apply_history_context(route, history)
    return Resolution(route, "rule" if route else "")


def route_question(question: str, *, project_hint: str | None = None) -> Route | None:
    """룰 기반 라우팅. 어느 룰에도 안 걸리면 None(=LLM 경로)."""
    question = question or ""
    if project_hint is None:
        project_hint = tools.detect_project_id(question)

    if is_light_chat(question):
        return Route(LIGHT_CHAT_TOOL)

    if tools.detect_team_status_intent(question):
        basis = basis_for_question(question)
        return Route("get_team_status", {"basis": basis})

    worker_hint = tools.detect_worker_name(question)
    if worker_hint and ("진행상황" in question or "프로젝트" in question):
        basis = basis_for_question(question)
        return Route("get_worker_status", {"worker": worker_hint, "basis": basis})

    filters = status_due_filters(question)
    if project_hint and filters:
        filters["project_id"] = project_hint
        return Route("filter_posts", filters)

    if project_hint and ("진행상황" in question or "게시물" in question or "요약" in question):
        return Route("get_project_status", {"project_id": project_hint})

    topic_hint = tools.detect_topic_keyword(question)
    if topic_hint and ("실적" in question or "성과" in question or "마케팅" in question):
        return Route("get_topic_status", {"keyword": topic_hint})

    if tools.detect_risk_intent(question) and is_priority_status_question(question):
        arguments: dict[str, Any] = {"priority_only": True}
        if project_hint:
            arguments["project_id"] = project_hint
        return Route("get_risk_status", arguments)

    if filters:
        return Route("filter_posts", filters)

    if tools.detect_risk_intent(question):
        arguments: dict[str, Any] = {"priority_only": is_priority_status_question(question)}
        if project_hint:
            arguments["project_id"] = project_hint
        return Route("get_risk_status", arguments)

    if worker_hint:
        basis = basis_for_question(question)
        return Route("get_worker_status", {"worker": worker_hint, "basis": basis})

    post_lookup = tools.find_posts(question)
    if should_answer_post_lookup(question, post_lookup):
        return Route("find_posts", {"keyword": question}, result=post_lookup)

    topic_hint = tools.detect_topic_keyword(question)
    if topic_hint:
        return Route("get_topic_status", {"keyword": topic_hint})

    if project_hint:
        return Route("get_project_status", {"project_id": project_hint})

    return None


def _apply_history_context(route: Route | None, history: list[dict[str, str]] | None) -> Route | None:
    """짧은 후속질문에서 사람/프로젝트 조건을 직전 대화에서 이어받는다."""
    if route is None or route.tool != "filter_posts":
        return route
    arguments = dict(route.arguments)
    if not arguments.get("worker"):
        worker = _last_worker_from_history(history)
        if worker:
            arguments["worker"] = worker
    if not arguments.get("project_id"):
        project_id = _last_project_from_history(history)
        if project_id:
            arguments["project_id"] = project_id
    return Route(route.tool, arguments, result=route.result)


def _last_worker_from_history(history: list[dict[str, str]] | None) -> str:
    for item in reversed(history or []):
        text = str(item.get("text") or "")
        worker = tools.detect_worker_name(text)
        if worker and tools.is_known_person(worker):
            return worker
    return ""


def _last_project_from_history(history: list[dict[str, str]] | None) -> str:
    for item in reversed(history or []):
        project_id = tools.detect_project_id(str(item.get("text") or ""))
        if project_id:
            return project_id
    return ""


def is_light_chat(question: str) -> bool:
    normalized = (question or "").strip().replace(" ", "")
    return normalized in {"안녕", "안녕하세요", "하이", "ㅎㅇ"}


def basis_for_question(question: str, *, default: str | None = None) -> str:
    text = question or ""
    if tools.detect_author_basis(text):
        return "author"
    if "담당자" in text or "담당 기준" in text:
        return "worker"
    return default or tools.BASIS_AUTO


def is_priority_status_question(question: str) -> bool:
    text = question or ""
    if text.replace(" ", "") in {"상태", "상태확인", "피드백건확인용", "상태피드백건확인용"}:
        return True
    return any(keyword in text for keyword in ["피드백", "결제중", "결재중", "결제"])


def should_answer_post_lookup(question: str, result: dict[str, Any]) -> bool:
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


def status_due_filters(question: str) -> dict[str, Any] | None:
    text = question or ""
    filters: dict[str, Any] = {}
    if "미완료" in text or "완료되지 않은" in text:
        filters["status"] = "미완료"
    # 실제 task_status 라벨에서 가져온다. "결제중"은 캘린더 어휘라 flow_post에 없어 제거했다.
    for status in status_rules.DETECTION_ORDER:
        if "status" in filters:
            break
        if status == "진행" and "진행상황" in text and "진행중" not in text and "진행 중" not in text:
            continue
        if status in text:
            filters["status"] = status
            break
    if re.search(r"(?:기한|마감일?)\s*(?:이\s*)?(?:없|미등록|미설정|안\s*잡)", text):
        filters["due"] = "none"
    elif re.search(r"(?:기한|마감일?)\s*(?:이\s*)?(?:지난|지났|경과|넘긴|넘었)", text) or "지연" in text:
        filters["due"] = "overdue"
    else:
        matched = re.search(r"\b(20\d{6})\b", text)
        if matched:
            filters["due"] = matched.group(1)
    if not filters:
        return None
    return filters
