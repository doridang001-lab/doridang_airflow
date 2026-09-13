"""도리당봇 라우팅 정확도 측정.

'질문 -> tool + 인자'만 비교한다. 최종 답변 생성은 하지 않으므로 실제 대화보다 훨씬 빠르다.
평가셋은 tests/fixtures/doridang_eval.jsonl 이며 log.md의 실제 질문에서 뽑았다.

  python scripts/eval_doridang_bot.py                 # 실제 동작(LLM 라우팅 + 룰 폴백)
  python scripts/eval_doridang_bot.py --mode rule     # 룰만 (baseline 재측정)
  python scripts/eval_doridang_bot.py --verbose       # 전 항목 출력
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Windows 콘솔 기본 코드페이지에서 한글이 깨진다
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

from modules.transform.doridang_bot import router  # noqa: E402

logger = logging.getLogger(__name__)

EVAL_PATH = REPO_ROOT / "tests" / "fixtures" / "doridang_eval.jsonl"


def load_cases(path: Path) -> list[dict[str, Any]]:
    cases = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            cases.append(json.loads(line))
    return cases


def evaluate(case: dict[str, Any], *, use_llm: bool) -> dict[str, Any]:
    started = time.time()
    resolution = router.resolve_route(
        case["question"],
        history=case.get("history"),
        use_llm=use_llm,
    )
    elapsed = time.time() - started

    actual_tool = resolution.route.tool if resolution.route else None
    actual_args = resolution.route.arguments if resolution.route else {}

    expect_tool = case.get("expect_tool")
    tool_ok = actual_tool == expect_tool
    # 인자는 부분집합 비교 — 기대하지 않은 추가 인자는 감점하지 않는다
    args_ok = all(actual_args.get(key) == value for key, value in (case.get("expect_args") or {}).items())

    return {
        "question": case["question"],
        "expect_tool": expect_tool,
        "actual_tool": actual_tool,
        "expect_args": case.get("expect_args") or {},
        "actual_args": actual_args,
        "source": resolution.source,
        "seconds": elapsed,
        "tool_ok": tool_ok,
        "args_ok": args_ok,
        "passed": tool_ok and args_ok,
        "requires_context": bool(case.get("requires_context")),
        "note": case.get("note", ""),
    }


def _label(tool: Any) -> str:
    return "(tool 없음)" if tool is None else str(tool)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["llm", "rule"], default="llm",
                        help="llm=실제 동작(LLM 라우팅+룰 폴백), rule=룰만")
    parser.add_argument("--verbose", action="store_true", help="전 항목 출력")
    parser.add_argument("--eval-path", type=Path, default=EVAL_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s - %(message)s")
    use_llm = args.mode == "llm"

    results = [evaluate(case, use_llm=use_llm) for case in load_cases(args.eval_path)]
    scored = [r for r in results if not r["requires_context"]]
    context_cases = [r for r in results if r["requires_context"]]

    passed = [r for r in scored if r["passed"]]
    failed = [r for r in scored if not r["passed"]]

    for result in results if args.verbose else failed:
        mark = "PASS" if result["passed"] else "FAIL"
        suffix = " [맥락필요]" if result["requires_context"] else ""
        print(f"[{mark}]{suffix} {result['question']}  ({result['source'] or '-'}, {result['seconds']:.1f}s)")
        if not result["passed"] or args.verbose:
            print(f"       기대: {_label(result['expect_tool'])} {result['expect_args'] or ''}")
            print(f"       실제: {_label(result['actual_tool'])} {result['actual_args'] or ''}")
            if result["note"]:
                print(f"       비고: {result['note']}")

    total = len(scored)
    accuracy = (len(passed) / total * 100) if total else 0.0
    tool_only = sum(1 for r in scored if r["tool_ok"])
    by_source: dict[str, int] = {}
    for result in scored:
        by_source[result["source"] or "-"] = by_source.get(result["source"] or "-", 0) + 1
    seconds = [r["seconds"] for r in scored]

    print()
    print(f"모드: {args.mode}")
    print(f"라우팅 정확도: {len(passed)}/{total} = {accuracy:.1f}%")
    if total:
        print(f"  tool만 일치: {tool_only}/{total} ({tool_only / total * 100:.1f}%)")
        print(f"  경로별: {by_source}")
        print(f"  질문당 {sum(seconds) / total:.2f}초 (최대 {max(seconds):.1f}초)")
    print(f"  맥락 필요(점수 제외): {len(context_cases)}건")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
