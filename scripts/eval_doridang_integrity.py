"""고정된 실제 오류 표본으로 로컬 모델의 최종 답변을 반복 검증한다."""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from datetime import datetime
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.transform.doridang_bot import dialogue, integrity, tools  # noqa: E402

FLOWS = [
    ("team", [["팀원 별 프로젝트 진행상황"], ["팀원별 프로젝트 현황 알려줘"], ["팀 전체 프로젝트 진행상황"]]),
    ("correction", [["차보령 피드백1건은 뭐고 왜문제야?"], ["차보령 피드백 1건 뭐야?"], ["차보령 피드백1건 자세히"]]),
    ("person", [["차보령 피드백 현황", "조민준은?", "조민준 전체 현황"]] * 3),
    ("author", [["차보령 피드백 업무 자세히", "내부 메뉴판 _ 어르신 / 포장 댓글 누가 썼어?"]] * 3),
    ("deadline", [["조민준 전체 현황", "그중 기한 지난 것만", "기한 없는 것만"]] * 3),
    ("decision", [["차보령 피드백 업무 자세히", "왜?", "내가 뭘 결정해야 해?"]] * 3),
]


@contextmanager
def fixed_source():
    fixture = json.loads((ROOT / "tests/fixtures/doridang_truth.json").read_text(encoding="utf-8"))
    defaults = dict(parent_post_id="", depth=0, post_date="2026-08-01", author_name="", content_text="",
                    start_dt="", post_url="", task_nm="", collected_at=fixture["collected_at"])
    posts = pd.DataFrame([{**defaults, **row} for row in fixture["posts"]])
    comments = pd.DataFrame(fixture["comments"])
    projects = pd.DataFrame([dict(project_id=pid, project_name=name) for pid, name in tools.ALLOWED_PROJECTS.items()])
    class FixedDate(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 9, 12, tzinfo=tz)
    with ExitStack() as stack:
        for module, name, value in [(tools, "_load_posts", lambda: posts.copy()),
                                     (tools, "_load_comments", lambda: comments.copy()),
                                     (tools, "_load_projects", lambda: projects.copy()),
                                     (tools, "datetime", FixedDate), (dialogue, "datetime", FixedDate)]:
            stack.enter_context(patch.object(module, name, value))
        yield


def check(case, question, context, answer):
    failures = []
    def require(condition, label):
        if not condition:
            failures.append(label)
    if case == "team":
        require(all(w in answer for w in tools.LEADER_TEAM_MEMBERS), "팀원 누락")
        require("회사 현황판 구축" in answer and "직영점 성장전략" in answer, "프로젝트 누락")
        require("미완료 2건 중 기한 경과 1건" in answer, "업무/모니터링 집계 혼동")
    if case == "correction":
        require("1건이 아니라 2건" in answer, "잘못된 건수 미정정")
        require("성과추적test 결과" in answer and "기한 미등록" in answer, "피드백 누락/기한 추정")
    if question == "조민준은?":
        require(context.get("status") == "피드백" and "피드백 업무는 0건" in answer, "주제 유지 실패")
    if question == "조민준 전체 현황":
        require("status" not in context and "미완료 2건 중 기한 경과 1건" in answer, "전체 초기화/집계 오류")
        require("현장 요청사항" in answer and "기한 미등록" in answer, "기한 없는 업무 누락")
    if "누가" in question or case == "correction":
        require("작성자는 **오나영**" in answer, "댓글 작성자 혼동")
    if question == "그중 기한 지난 것만":
        require("현장 요청사항" not in answer and "김덕기 과장 교육" in answer, "기한 필터 혼동")
    if question == "기한 없는 것만":
        require("현장 요청사항" in answer and "김덕기 과장 교육" not in answer, "기한 미등록 필터 혼동")
    if question in {"왜?", "내가 뭘 결정해야 해?"}:
        require("Flow 등록 상태는" not in answer and "피드백 업무는" not in answer, "후속 질문에 현황 반복")
    require("역량 부족" not in answer and "모두 기한 경과" not in answer, "근거 없는 단정")
    return failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", default=str(ROOT / ".tmp/doridang_integrity_eval.json"))
    args = parser.parse_args()
    output = Path(args.output).resolve()
    if "onedrive" in str(output).lower():
        parser.error("평가 결과는 로컬 경로에 저장하세요.")
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    with fixed_source():
        for case, variants in FLOWS:
            for repeat in range(args.repetitions):
                context, history = {}, []
                for q in variants[repeat % len(variants)]:
                    started = time.monotonic()
                    context, evidence, direct = dialogue.gather(q, history, context)
                    if direct:
                        result = integrity.VerifiedAnswer(direct, {"outcome": "direct"}, [])
                    else:
                        result = integrity.generate(q, context, evidence, deadline=started+60)
                    context, sources = dialogue.final_metadata(result.text, context, evidence, listed_ids=result.post_ids)
                    row = dict(case=case, repeat=repeat+1, question=q, context=context, answer=result.text,
                               validation=result.validation, seconds=round(time.monotonic()-started, 2),
                               failures=check(case, q, context, result.text), sources=sources)
                    if history and result.text == history[-1]["text"]:
                        row["failures"].append("질문이 달라졌는데 동일 답변 반복")
                    rows.append(row)
                    history.extend([dict(role="user", text=q), dict(role="assistant", text=result.text)])
                    summary = dict(turns=len(rows), failures=sum(bool(r["failures"]) for r in rows),
                                   fallback=sum(r["validation"]["outcome"] == "fallback" for r in rows),
                                   blocked_drafts=sum(r["validation"].get("blocked_drafts", 0) for r in rows),
                                   mean_seconds=round(sum(r["seconds"] for r in rows)/len(rows),2),
                                   max_seconds=max(r["seconds"] for r in rows))
                    output.write_text(json.dumps(dict(summary=summary, rows=rows), ensure_ascii=False, indent=2), encoding="utf-8")
                    print(json.dumps({k: row[k] for k in ("case", "repeat", "question", "validation", "seconds", "failures")}, ensure_ascii=False), flush=True)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    if summary["failures"]:
        raise SystemExit(1)
    return summary


if __name__ == "__main__":
    main()
