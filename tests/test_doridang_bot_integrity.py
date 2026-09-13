"""실제 오류 사례, 적대적 초안, 제한 시간과 스냅샷의 독립 회귀 검증."""
import json
import threading
import time
import urllib.request
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.doridang_bot import dialogue, integrity, llm_backend, server, tools


@pytest.fixture
def truth(monkeypatch):
    fixture = json.loads((Path(__file__).parent / "fixtures/doridang_truth.json").read_text(encoding="utf-8"))
    defaults = dict(parent_post_id="", depth=0, post_date="2026-08-01", author_name="", content_text="",
                    start_dt="", post_url="", task_nm="", collected_at=fixture["collected_at"])
    posts = pd.DataFrame([{**defaults, **row} for row in fixture["posts"]])
    projects = pd.DataFrame([dict(project_id=pid, project_name=name) for pid, name in tools.ALLOWED_PROJECTS.items()])
    comments = pd.DataFrame(fixture["comments"])
    monkeypatch.setattr(tools, "_load_posts", lambda: posts.copy())
    monkeypatch.setattr(tools, "_load_comments", lambda: comments.copy())
    monkeypatch.setattr(tools, "_load_projects", lambda: projects.copy())
    monkeypatch.setattr(tools, "request_snapshot", nullcontext)

    class FixedDate(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 9, 12, tzinfo=tz)

    monkeypatch.setattr(tools, "datetime", FixedDate)
    monkeypatch.setattr(dialogue, "datetime", FixedDate)
    return fixture


def test_person_team_project_deadline_scope(truth):
    person = tools.get_worker_status("조민준")
    assert person["open_count"] == 2
    assert person["overdue_count"] == 2  # 기존 모니터링 포함 운영 지표 유지
    assert person["overdue_task_count"] == 1
    assert person["overdue_monitoring_count"] == 1
    team = tools.get_team_status()
    member = next(m for m in team["members"] if m["worker"] == "조민준")
    for field in ["overdue_task_count", "overdue_monitoring_count"]:
        assert member[field] == person[field]
        assert sum(p[field] for p in member["projects"]) == person[field]
    project = tools.get_project_status("2926713")
    assert project["overdue_task_count"] == project["overdue_monitoring_count"] == 1


def test_user_chain_corrects_count_author_and_person_topic(truth):
    ctx, ev, _ = dialogue.gather("팀원 별 프로젝트 진행상황", [], {})
    facts, required, ids = integrity.fact_bank("팀원 별 프로젝트 진행상황", ctx, ev)
    answer = integrity.safe_answer(facts, required, ids).text
    assert "회사 현황판 구축" in answer and "진행 2건" in answer
    ctx, ev, _ = dialogue.gather("차보령 피드백1건은 뭐고 왜문제야?", [], ctx)
    facts, required, ids = integrity.fact_bank("차보령 피드백1건은 뭐고 왜문제야?", ctx, ev)
    answer = integrity.safe_answer(facts, required, ids).text
    assert "1건이 아니라 2건" in answer
    assert "작성자는 **오나영**" in answer
    assert "성과추적test 결과**: Flow 등록 상태는 **피드백**" in answer
    assert "기한 미등록" in answer
    comment = next(v["source"] for k, v in facts.items() if k.startswith("comment:"))
    assert comment["댓글ID"] == "196083684"
    assert comment["내용"] == truth["comments"][0]["content_text"]
    ctx, _ = dialogue.final_metadata(answer, ctx, ev, listed_ids=ids)
    ctx, ev, _ = dialogue.gather("조민준은?", [], ctx)
    assert ctx["status"] == "피드백" and not ctx["displayed_post_ids"]
    answer = integrity.safe_answer(*integrity.fact_bank("조민준은?", ctx, ev)).text
    assert "피드백 업무는 0건" in answer and "제안:" not in answer
    ctx, ev, _ = dialogue.gather("조민준 전체 현황", [], ctx)
    assert "status" not in ctx
    answer = integrity.safe_answer(*integrity.fact_bank("조민준 전체 현황", ctx, ev)).text
    assert "미완료 2건 중 기한 경과 1건" in answer


@pytest.mark.parametrize("due,status,expected", [
    ("", "피드백", "기한 미등록"), ("20261302", "진행", "기한 형식 확인 필요"),
    ("20260230", "진행", "기한 형식 확인 필요"), ("20260909", "진행", "기한 경과 아님"),
    ("20260910", "진행", "기한 경과 아님"), ("20260908", "진행", "기한 경과"),
    ("20260908", "완료", "현재 상태는 기한 경과 집계 대상 아님"),
    ("20260908", "모니터링", "기한 경과"),
])
def test_deadline_validity(due, status, expected):
    assert tools.deadline_state(due, status, "20260909") == expected


@pytest.mark.parametrize("block", [
    {"kind": "fact", "ref": "bad", "text": ""},
    {"kind": "fact", "ref": "summary", "text": "모두 기한 경과"},
    {"kind": "interpretation", "ref": "summary", "text": "2건 모두 지연입니다"},
    {"kind": "interpretation", "ref": "summary", "text": "황유경이 작성했습니다"},
    {"kind": "interpretation", "ref": "summary", "text": "“없는 인용문”"},
    {"kind": "interpretation", "ref": "summary", "text": "모두 기한 경과입니다"},
])
def test_adversarial_draft_is_never_accepted(block):
    with pytest.raises(ValueError):
        integrity.validate_draft({"blocks": [block]}, {"summary": {"text": "원본"}})


def test_semantic_rejection_retries_once_then_facts_only(truth, monkeypatch):
    ctx, ev, _ = dialogue.gather("조민준 전체 현황", [], {})
    calls = []
    def fake(messages, schema, **kwargs):
        calls.append(schema)
        if schema == integrity.REVIEW_SCHEMA:
            return {"valid": False, "reason": "원인 추정"}
        return {"blocks": [{"kind": "suggestion", "ref": "post:83859879", "text": "담당자의 역량 부족이 원인이니 교육을 지시하세요."}]}
    monkeypatch.setattr(llm_backend, "structured_chat", fake)
    result = integrity.generate("내가 뭘 결정해야 해?", ctx, ev, deadline=time.monotonic()+5)
    assert result.validation["outcome"] == "fallback"
    assert result.validation["attempts"] == 2 and len(calls) == 4
    assert "역량 부족" not in result.text


def test_invalid_first_draft_does_not_enter_retry_context(truth, monkeypatch):
    ctx, ev, _ = dialogue.gather("조민준 전체 현황", [], {})
    calls = []
    def fake(messages, schema, **kwargs):
        calls.append(list(messages))
        if len(calls) == 1:
            return {"blocks": [{"kind": "fact", "ref": "summary", "text": "비밀초안모두지연"}]}
        return {"blocks": [{"kind": "fact", "ref": "summary", "text": ""}]}
    monkeypatch.setattr(llm_backend, "structured_chat", fake)
    result = integrity.generate("조민준 전체 현황", ctx, ev, deadline=time.monotonic()+5)
    assert result.validation["attempts"] == 2 and result.validation["outcome"] == "verified"
    assert "비밀초안" not in json.dumps(calls, ensure_ascii=False) + result.text


def test_http_deadline_no_late_answer_or_log(truth, monkeypatch, tmp_path):
    local_log = tmp_path / "turns.md"
    monkeypatch.setenv("DORIDANG_BOT_LOG_PATH", str(local_log))
    monkeypatch.setattr(server, "ANSWER_TIMEOUT_SECONDS", .3)
    release = threading.Event()
    def slow(*a, **kw):
        release.wait(3)
        return integrity.VerifiedAnswer("늦은답변유출", {"outcome": "verified"}, [])
    monkeypatch.setattr(integrity, "generate", slow)
    http = server.ThreadingHTTPServer(("127.0.0.1", 0), server.DoridangBotHandler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{http.server_port}/api/chat", data=json.dumps(
            {"message": "조민준 전체 현황", "session_id": "deadline"}).encode(), headers={"Content-Type": "application/json"})
        start = time.monotonic()
        with urllib.request.urlopen(req) as response:
            body = response.read().decode()
        assert time.monotonic() - start < 2
        release.set()
        events = [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: {")]
        answer = next(e for e in events if e["type"] == "answer")
        assert answer["validation"]["outcome"] == "fallback"
        assert [e["text"] for e in events if e["type"] == "delta"] == [answer["text"]]
        assert "늦은답변유출" not in body + local_log.read_text(encoding="utf-8")
    finally:
        release.set()
        http.shutdown()
        http.server_close()


def test_request_snapshot_pins_tables_across_file_update(monkeypatch, tmp_path):
    paths = []
    for name in ["PROJECT", "POST", "COMMENT"]:
        path = tmp_path / (name + ".parquet")
        pd.DataFrame([dict(project_id="2926716", value="old")]).to_parquet(path)
        monkeypatch.setattr(tools, "FLOW_" + name + "_PARQUET", path)
        paths.append(path)
    monkeypatch.setattr(tools, "_SNAPSHOT_CACHE", None)
    with tools.request_snapshot():
        assert tools._load_posts().iloc[0]["value"] == "old"
        pd.DataFrame([dict(project_id="2926716", value="new")]).to_parquet(paths[1])
        assert tools._load_posts().iloc[0]["value"] == "old"
    with tools.request_snapshot():
        assert tools._load_posts().iloc[0]["value"] == "new"


def test_browser_context_survives_server_memory_reset(truth):
    context, evidence, _ = dialogue.gather("차보령 피드백 업무 자세히", [], {})
    result = integrity.safe_answer(*integrity.fact_bank("차보령 피드백 업무 자세히", context, evidence))
    context, _ = dialogue.final_metadata(result.text, context, evidence, listed_ids=result.post_ids)
    server._SESSION_MEMORY.clear()
    current, evidence, _ = dialogue.gather("조민준은?", [], json.loads(json.dumps(context)))
    assert current["worker"] == "조민준" and current["status"] == "피드백"
    assert evidence[0]["data"]["post_count"] == 0


def test_structured_backend_budget_and_client_cleanup(monkeypatch):
    import ollama
    from types import SimpleNamespace
    seen, closed = [], []
    class Client:
        def __init__(self, **kwargs):
            seen.append(kwargs)
            self._client = SimpleNamespace(close=lambda: closed.append(True))
        def chat(self, **kwargs):
            seen.append(kwargs)
            return SimpleNamespace(done_reason="stop", message=SimpleNamespace(content='{"valid": true}'))
    monkeypatch.setattr(ollama, "Client", Client)
    assert llm_backend.structured_chat([], integrity.REVIEW_SCHEMA, deadline=time.monotonic()+2) == {"valid": True}
    assert 0 < seen[0]["timeout"] <= 2
    assert seen[1]["model"] == llm_backend.ANSWER_MODEL and seen[1]["stream"] is False
    assert closed == [True]


def test_model_queue_obeys_remaining_deadline(monkeypatch):
    slot = threading.BoundedSemaphore(1)
    slot.acquire()
    monkeypatch.setattr(llm_backend, "_CHAT_SLOTS", slot)
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        llm_backend.structured_chat([], {}, deadline=start+.02)
    assert time.monotonic()-start < .5


def test_review_reason_cannot_leak_draft():
    result = integrity.safe_answer({"s": {"text": "검증된 사실"}}, ["s"], [],
                                   reason="ValueError: 거짓 초안 전문", attempts=2)
    assert "거짓" not in json.dumps(result.validation, ensure_ascii=False)


@pytest.mark.parametrize("question", ["기한 없는 것만", "기한이 없는 업무", "마감일 미등록", "기한 안 잡힌 것"])
def test_missing_due_switches_previous_overdue_filter(truth, question):
    ctx, evidence, _ = dialogue.gather(question, [], {"worker": "조민준", "due": "overdue", "displayed_post_ids": ["83859879"]})
    assert ctx["due"] == "none"
    assert [p["post_id"] for p in evidence[0]["data"]["posts"]] == ["81528325"]


def test_open_request_does_not_mean_completed(truth):
    ctx, evidence, _ = dialogue.gather("조민준 미완료 업무 전체", [], {})
    assert ctx["status"] == "미완료"
    assert evidence[0]["data"]["post_count"] == 2
    assert all(p["task_status"] == "진행" for p in evidence[0]["data"]["posts"])
