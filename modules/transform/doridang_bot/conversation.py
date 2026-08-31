"""Conversation log helpers for Doridang bot."""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from modules.transform.utility.paths import DORIDANG_BOT_LOG_MD

_LOG_LOCK = threading.Lock()


def append_turn(user: str, question: str, answer: str, evidence: list[dict[str, Any]]) -> None:
    DORIDANG_BOT_LOG_MD.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    evidence_text = ", ".join(_format_evidence(item) for item in evidence) or "없음"
    payload = (
        f"\n## {timestamp} | {user or '사용자'}\n"
        f"**Q:** {question.strip()}\n"
        f"**A:** {answer.strip()}\n"
        f"**근거:** {evidence_text}\n"
    )
    with _LOG_LOCK:
        with DORIDANG_BOT_LOG_MD.open("a", encoding="utf-8", newline="\n") as fp:
            fp.write(payload)


def search_log(keyword: str, *, limit: int = 10) -> list[dict[str, str]]:
    keyword = (keyword or "").strip()
    if not keyword or not DORIDANG_BOT_LOG_MD.exists():
        return []

    text = DORIDANG_BOT_LOG_MD.read_text(encoding="utf-8", errors="replace")
    turns = [block.strip() for block in text.split("\n## ") if block.strip()]
    matches: list[dict[str, str]] = []
    for block in reversed(turns):
        normalized = block if block.startswith("## ") else f"## {block}"
        if keyword in normalized:
            lines = normalized.splitlines()
            matches.append({
                "title": lines[0].removeprefix("## ").strip() if lines else "",
                "excerpt": _excerpt(normalized, keyword),
            })
        if len(matches) >= limit:
            break
    return matches


def _format_evidence(item: dict[str, Any]) -> str:
    name = str(item.get("name", "tool"))
    arguments = item.get("arguments", {})
    if not isinstance(arguments, dict):
        return name
    args = ", ".join(f"{key}={value}" for key, value in arguments.items())
    return f"{name}({args})"


def _excerpt(text: str, keyword: str, *, radius: int = 120) -> str:
    index = text.find(keyword)
    if index < 0:
        return text[: radius * 2].replace("\n", " ")
    start = max(0, index - radius)
    end = min(len(text), index + len(keyword) + radius)
    return text[start:end].replace("\n", " ").strip()

