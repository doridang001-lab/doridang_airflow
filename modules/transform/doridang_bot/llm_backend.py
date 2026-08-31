"""Streaming LLM backend for Doridang bot."""

from __future__ import annotations

import logging
import threading
from typing import Any, Iterable

from modules.transform.utility import qwen_client

logger = logging.getLogger(__name__)

_CHAT_LOCK = threading.Lock()


def chat_stream(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None) -> Iterable[dict[str, Any]]:
    if not _CHAT_LOCK.acquire(blocking=False):
        yield {"type": "status", "text": "다른 요청이 처리 중입니다. 잠시 후 순서대로 답변합니다."}
        _CHAT_LOCK.acquire()

    try:
        yield from _chat_stream_locked(messages, tools=tools)
    finally:
        _CHAT_LOCK.release()


def _chat_stream_locked(messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None) -> Iterable[dict[str, Any]]:
    client, model_candidates = qwen_client.get_ollama_client_with_candidates()
    last_error: Exception | None = None
    for model_name in qwen_client._prioritize_primary_models(model_candidates):
        try:
            options = qwen_client._chat_options_for_model(model_name)
            if "gpt-oss" in model_name:
                options["num_predict"] = max(int(options.get("num_predict", 0) or 0), 2048)
            chat_kwargs: dict[str, Any] = {
                "model": model_name,
                "messages": messages,
                "stream": True,
                "think": False,
                "options": options,
            }
            if tools:
                chat_kwargs["tools"] = tools

            tool_calls: list[dict[str, Any]] = []
            for chunk in client.chat(**chat_kwargs):
                message = _chunk_message(chunk)
                content = _message_value(message, "content") or ""
                thinking = _message_value(message, "thinking") or ""
                if content:
                    yield {"type": "delta", "text": content}
                elif thinking and "gpt-oss" not in model_name:
                    yield {"type": "delta", "text": thinking}
                tool_calls.extend(_normalize_tool_calls(_message_value(message, "tool_calls")))
            if tool_calls:
                yield {"type": "tool_calls", "calls": tool_calls}
            return
        except Exception as exc:
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
