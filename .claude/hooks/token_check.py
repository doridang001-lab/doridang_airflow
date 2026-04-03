#!/usr/bin/env python3
"""
Claude Code Stop hook — context window 사용량이 80% 이상이면 경고 표시.
Stop 이벤트 stdin JSON에서 토큰 정보를 읽고, 없으면 transcript 파일 크기로 추정.
"""
import json
import sys
import os

THRESHOLD_PCT = 80
DEFAULT_MAX_TOKENS = 200000  # claude-sonnet-4-6 기본 컨텍스트


def estimate_from_transcript(path: str) -> int | None:
    """transcript JSONL 파일 크기로 토큰 수 추정 (1 token ≈ 7 bytes)."""
    if not path or not os.path.exists(path):
        return None
    return int(os.path.getsize(path) / 7)


def main():
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return

        data = json.loads(raw)

        # Method 1: Stop 이벤트 stdin에 usage 필드가 있는 경우 (미래 버전 대비)
        usage = data.get("usage", {})
        tokens_used = (
            usage.get("input_tokens")
            or usage.get("tokens_used")
            or data.get("context_tokens_used")
            or data.get("tokens_used")
        )
        context_window = usage.get("context_window", DEFAULT_MAX_TOKENS)

        # Method 2: transcript 파일 크기로 추정 (fallback)
        if not tokens_used:
            transcript_path = data.get("transcript_path", "")
            tokens_used = estimate_from_transcript(transcript_path)
            context_window = DEFAULT_MAX_TOKENS

        if tokens_used and context_window:
            pct = (tokens_used / context_window) * 100
            if pct >= THRESHOLD_PCT:
                pct_int = int(pct)
                msg = (
                    f"⚠️  컨텍스트 {pct_int}% 사용 중 — "
                    f"/clear 또는 /to-compact 로 새 세션을 시작하세요."
                )
                print(json.dumps({"systemMessage": msg}))

    except Exception:
        pass  # 훅 실패해도 Claude 실행 절대 블로킹 안 함


if __name__ == "__main__":
    main()
