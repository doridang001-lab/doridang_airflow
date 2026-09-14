"""Streaming LLM backend for Doridang bot."""

from __future__ import annotations

import logging
import os
import threading
import json
import time
from typing import Any, Iterable

from modules.transform.utility import qwen_client

logger = logging.getLogger(__name__)

# 질문 -> tool 선택은 짧은 JSON만 뽑으면 되므로 작은 모델이 빠르고 충분하다.
# 최종 답변 생성만 큰 모델을 쓴다.
PURPOSE_ROUTE = "route"
PURPOSE_ANSWER = "answer"

# Ollama 기본 num_ctx(4k)로는 tool 결과 + 대화 이력이 조용히 잘린다.
# qwen_client는 다른 파이프라인도 공유하므로 봇 전용으로만 올린다.
DEFAULT_NUM_CTX = int(os.getenv("DORIDANG_BOT_NUM_CTX", "8192"))
# 미설정 시 유휴 5분 뒤 모델이 내려가 다음 질문이 재로딩을 기다린다.
KEEP_ALIVE = os.getenv("DORIDANG_BOT_KEEP_ALIVE", "30m")
ROUTE_NUM_PREDICT = 256
ANSWER_NUM_PREDICT = 1200

# GPU 1대에 qwen2.5:14b(10.9GB)와 gpt-oss:20b가 같이 올라가지 않는다.
# 라우팅과 요약이 서로 다른 모델을 쓰면 질문마다 전체 재적재(수십 초)가 일어난다.
# 기본은 한 모델(qwen)로 통일하고, VRAM이 넉넉한 환경에서만 gpt-oss를 요약에 쓴다.
ANSWER_MODEL = os.getenv("DORIDANG_BOT_ANSWER_MODEL", "qwen2.5:14b")

_MAX_CONCURRENT_CHATS = max(1, int(os.getenv("DORIDANG_BOT_MAX_CONCURRENT", "2")))
_CHAT_SLOTS = threading.BoundedSemaphore(_MAX_CONCURRENT_CHATS)


def structured_chat(messages: list, schema: dict, *, deadline: float) -> dict:
    """검증용 호출은 정해진 로컬 모델만 사용하고 남은 시간 내에 종료한다."""
    import ollama
    remaining = deadline - time.monotonic()
    if remaining <= 0 or not _CHAT_SLOTS.acquire(timeout=max(0, remaining)):
        raise TimeoutError("답변 검증 시간 초과")
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("답변 검증 시간 초과")
        client = ollama.Client(host=os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434"), timeout=remaining)
        try:
            response = client.chat(model=ANSWER_MODEL, messages=messages, format=schema,
                                   stream=False, keep_alive=KEEP_ALIVE,
                                   options={"num_ctx": DEFAULT_NUM_CTX, "num_predict": 800, "temperature": 0})
        finally:
            client._client.close()
        if time.monotonic() >= deadline or response.done_reason == "length":
            raise TimeoutError("답변 검증 시간 또는 출력 한도 초과")
        return json.loads(response.message.content)
    finally:
        _CHAT_SLOTS.release()


def chat_stream(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    *,
    purpose: str = PURPOSE_ANSWER,
) -> Iterable[dict[str, Any]]:
    if not _CHAT_SLOTS.acquire(blocking=False):
        yield {"type": "status", "text": "다른 요청이 처리 중입니다. 잠시 후 순서대로 답변합니다."}
        if not _CHAT_SLOTS.acquire(timeout=60):
            raise TimeoutError("요청 대기가 길어졌습니다. 잠시 후 다시 시도해 주세요.")

    try:
        yield from _chat_stream_locked(messages, tools=tools, purpose=purpose)
    finally:
        _CHAT_SLOTS.release()


def _chat_options(model_name: str, purpose: str) -> dict[str, Any]:
    options = qwen_client._chat_options_for_model(model_name)
    options.setdefault("num_ctx", DEFAULT_NUM_CTX)
    if purpose == PURPOSE_ROUTE:
        options["num_predict"] = ROUTE_NUM_PREDICT
        options["temperature"] = 0
    else:
        options["num_predict"] = ANSWER_NUM_PREDICT
        options["temperature"] = 0.2
    return options


def _chat_stream_locked(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    purpose: str = PURPOSE_ANSWER,
) -> Iterable[dict[str, Any]]:
    client, model_candidates = qwen_client.get_ollama_client_with_candidates()
    last_error: Exception | None = None
    # 라우팅은 항상 작은 모델. 답변도 기본은 같은 모델이라 재적재가 없다.
    prefer_stable = purpose == PURPOSE_ROUTE or ANSWER_MODEL != "gpt-oss"
    candidates = qwen_client._prioritize_primary_models(model_candidates, prefer_stable=prefer_stable)
    preferred = "gpt-oss:20b" if ANSWER_MODEL == "gpt-oss" else ANSWER_MODEL
    candidates.sort(key=lambda name: name != preferred)
    emitted = False
    for model_name in candidates:
        try:
            options = _chat_options(model_name, purpose)
            chat_kwargs: dict[str, Any] = {
                "model": model_name,
                "messages": messages,
                "stream": True,
                "think": False,
                "keep_alive": KEEP_ALIVE,
                "options": options,
            }
            if tools:
                chat_kwargs["tools"] = tools

            tool_calls: list[dict[str, Any]] = []
            for chunk in client.chat(**chat_kwargs):
                if _message_value(chunk, "done_reason") == "length":
                    raise RuntimeError("답변이 생성 길이 한도에 도달했습니다. 질문을 좁혀 다시 시도해 주세요.")
                message = _chunk_message(chunk)
                content = _message_value(message, "content") or ""
                if content:
                    emitted = True
                    yield {"type": "delta", "text": content}
                tool_calls.extend(_normalize_tool_calls(_message_value(message, "tool_calls")))
            if tool_calls:
                yield {"type": "tool_calls", "calls": tool_calls}
            return
        except Exception as exc:
            if emitted:
                raise  # 부분 답변 뒤 다른 모델의 답변을 이어 붙이지 않는다.
            last_error = exc
            if "gpt-oss" in model_name and qwen_client._should_mark_model_unhealthy(exc):
                qwen_client._mark_model_unhealthy(model_name)
            logger.warning("도리당봇 LLM 스트리밍 실패, 다음 후보 재시도: %s / %s", model_name, exc)

    if last_error:
        raise last_error
    raise RuntimeError("사용 가능한 LLM 모델이 없습니다")


def _chunk_message(chunk: Any) -> Any:
    if hasattr(chunk, "message"):
        return chunk.message
    if isinstance(chunk, dict):
        return chunk.get("message", {})
    return {}


def _message_value(message: Any, key: str) -> Any:
    if isinstance(message, dict):
        return message.get(key)
    return getattr(message, key, None)


def _normalize_tool_calls(raw_calls: Any) -> list[dict[str, Any]]:
    if not raw_calls:
        return []
    calls = []
    for call in raw_calls:
        function = _message_value(call, "function") or {}
        calls.append({
            "id": _message_value(call, "id") or "",
            "type": _message_value(call, "type") or "function",
            "function": {
                "name": _message_value(function, "name") or "",
                "arguments": _message_value(function, "arguments") or {},
            },
        })
    return calls
