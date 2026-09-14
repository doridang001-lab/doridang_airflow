"""설치된 로컬 모델의 동일 다중 턴 비교. Flow/OneDrive에는 쓰지 않는다."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.transform.doridang_bot import dialogue  # noqa: E402

QUESTIONS = ["차보령 업무 어때?", "이 담당자는 뭘 못했어?", "왜?", "내가 뭘 결정해야 해?"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=["qwen2.5:14b", "gpt-oss:20b"])
    parser.add_argument("--output", default=str(ROOT / ".tmp/doridang_conversation_eval.json"))
    args = parser.parse_args()
    results = []
    for model in args.models:
        history, context = [], {}
        for question in QUESTIONS:
            started = time.monotonic()
            context, evidence, direct = dialogue.gather(question, history, context)
            answer, first, reason = direct, None, "direct"
            if not direct:
                body = json.dumps({"model": model, "messages": dialogue.answer_messages(question, history, context, evidence),
                    "stream": True, "think": False, "keep_alive": "10m",
                    "options": {"num_ctx": 8192, "num_predict": 1200, "temperature": 0.2}}, ensure_ascii=False).encode()
                request = urllib.request.Request("http://127.0.0.1:11434/api/chat", data=body,
                                                 headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=180) as response:
                    for line in response:
                        event = json.loads(line)
                        content = event.get("message", {}).get("content", "")
                        if content and first is None:
                            first = round(time.monotonic() - started, 2)
                        answer += content
                        if event.get("done"):
                            reason = event.get("done_reason")
            context, sources = dialogue.final_metadata(answer, context, evidence)
            result = {"model": model, "question": question, "first_seconds": first,
                      "total_seconds": round(time.monotonic() - started, 2), "done_reason": reason,
                      "context": context, "tools": [{"name": e["name"], "arguments": e["arguments"]} for e in evidence],
                      "answer": answer, "sources": sources}
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            history.extend([{"role": "user", "text": question}, {"role": "assistant", "text": answer}])
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return results


if __name__ == "__main__":
    main()
