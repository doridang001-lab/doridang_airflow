"""대상 유지·근거 연결·전송 실패를 고정하는 대화 회귀 검사."""
import json
import threading
import urllib.request

import pytest

from modules.transform.doridang_bot import conversation, dialogue, integrity, llm_backend, server


@pytest.fixture
def evidence_tools(monkeypatch):
    from contextlib import nullcontext
    monkeypatch.setattr(dialogue.tools, "request_snapshot", nullcontext)
    names = {"차보령", "황유경", "조민준"}
    monkeypatch.setattr(dialogue.tools, "is_known_person", lambda n: n in names)
    monkeypatch.setattr(dialogue.tools, "resolve_post_reference", lambda *a: [])
    monkeypatch.setattr(dialogue.tools, "detect_worker_name", lambda s: next((n for n in names if n in s), None))
    monkeypatch.setattr(dialogue.tools, "data_freshness", lambda: {"latest": "2026-09-09", "stale": False})
    calls = []
    def execute(name, args):
        calls.append((name, args))
        posts = [{"post_id": str(i), "project_id": "2926716", "title": f"업무{i}",
                  "post_url": f"https://flow.team/l/p{i}", "task_status": "피드백",
                  "content_text": "디자인 수정 요청이 남아 있음"} for i in range(1, 5)]
        if name == "get_post_thread":
            posts = [p for p in posts if p["post_id"] == args["post_id"]]
        return {"posts": posts, "open_count": 4, "comments": [{"content_text": "디자인 피드백 대기"}]}
    monkeypatch.setattr(dialogue.tools, "execute_tool", execute)
    return calls


def test_leader_four_turns_keep_worker_and_fetch_threads(evidence_tools):
    context, history = {}, []
    for question in ["차보령 업무 어때?", "이 담당자는 뭘 못했어?", "왜?", "내가 뭘 결정해야 해?"]:
        context, evidence, direct = dialogue.gather(question, history, context)
        assert context["worker"] == "차보령"
        assert not direct
        assert 1 <= len(evidence) <= 4
        if question != "차보령 업무 어때?":
            assert len(evidence) == 4
        messages = dialogue.answer_messages(question, history, context, evidence)
        if history:
            assert messages[1]["content"] == history[0]["text"]
        context, sources = dialogue.final_metadata("업무2 다음 업무1", context, evidence)
        assert context["displayed_post_ids"] == ["2", "1"]
        history.extend([{"role": "user", "text": question}, {"role": "assistant", "text": "업무2 다음 업무1"}])


def test_changed_worker_keeps_topic_and_clears_old_selection(evidence_tools):
    context, evidence, _ = dialogue.gather("황유경은 어때?", [], {"worker": "차보령", "due": "overdue", "displayed_post_ids": ["3"]})
    assert context["worker"] == "황유경"
    assert context["due"] == "overdue"
    assert context["displayed_post_ids"] == []
    context, evidence, _ = dialogue.gather("그중 기한 지난 것만", [], context)
    assert evidence[0]["arguments"]["worker"] == "황유경"
    assert evidence[0]["arguments"]["due"] == "overdue"


def test_ordinal_uses_display_order_and_keeps_focus(evidence_tools):
    context, evidence, _ = dialogue.gather("두 번째 것 자세히", [], {"worker": "차보령", "displayed_post_ids": ["3", "1"]})
    assert evidence[0]["arguments"] == {"post_id": "1"}
    context, evidence, _ = dialogue.gather("왜?", [], context)
    assert evidence[0]["arguments"] == {"post_id": "1"}


def test_unknown_ordinal_asks_without_query(evidence_tools):
    _, evidence, direct = dialogue.gather("두 번째 자세히", [], {})
    assert not evidence and "제목" in direct
    assert not evidence_tools


def test_restart_recovers_user_subject_not_assistant_mention(evidence_tools):
    history = [{"role": "user", "text": "차보령은 어때?"}, {"role": "assistant", "text": "황유경과 논의가 필요합니다."}]
    context, _, _ = dialogue.gather("이 담당자는 뭘 못했어?", history, {})
    assert context["worker"] == "차보령"


def test_sources_only_allowed_flow_and_mentioned_posts(evidence_tools):
    evidence = [{"data": {"posts": [
        {"post_id": "1", "project_id": "2926716", "title": "업무1", "post_url": "javascript:alert(1)"},
        {"post_id": "2", "project_id": "outside", "title": "비공개", "post_url": "https://evil.test"}]}}]
    context, sources = dialogue.final_metadata("업무1 비공개", {}, evidence)
    assert context["displayed_post_ids"] == ["1"]
    assert all(s["url"].startswith("https://flow.team/") for s in sources)


def test_qwen_answer_budget_does_not_change_shared_settings():
    assert llm_backend._chat_options("qwen2.5:14b", "answer")["num_predict"] == 1200
    assert llm_backend.qwen_client.LLM_CHAT_OPTIONS["num_predict"] == 220


@pytest.mark.parametrize("fail", [False, True])
def test_http_stream_completion_and_local_log(evidence_tools, monkeypatch, tmp_path, fail):
    path = tmp_path / "turns.md"
    monkeypatch.setenv("DORIDANG_BOT_LOG_PATH", str(path))
    captured = []
    def stream(messages, schema, **kwargs):
        captured.extend(messages)
        if fail:
            raise TimeoutError("test")
        return {"blocks": [{"kind": "fact", "ref": "post:1", "text": ""}]}
    monkeypatch.setattr(llm_backend, "structured_chat", stream)
    http = server.ThreadingHTTPServer(("127.0.0.1", 0), server.DoridangBotHandler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        payload = {"message": "이 담당자는 뭘 못했어?", "session_id": "test", "message_id": "a1",
                   "context": {"worker": "차보령"}, "history": [{"role": "user", "text": "차보령 업무 어때?"}]}
        request = urllib.request.Request(f"http://127.0.0.1:{http.server_port}/api/chat",
            data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request) as response:
            text = response.read().decode()
        events = [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: {")]
        assert any(e["type"] == "answer" for e in events)
        assert path.exists()
        assert all(m["role"] != "assistant" for m in captured)
        answer = next(e for e in events if e["type"] == "answer")
        assert answer["validation"]["outcome"] == ("fallback" if fail else "verified")
        if not fail:
            answer = next(e for e in events if e["type"] == "answer")
            assert answer["message_id"] == "a1"
            assert answer["context"]["worker"] == "차보령"
            assert "https://flow.team/l/p1" in answer["text"]
    finally:
        http.shutdown()
        http.server_close()


def test_default_log_is_local():
    assert "OneDrive" not in str(conversation._write_log_path())


@pytest.mark.parametrize("question", ["더 쉽게 설명해", "그게 무슨 뜻이야?", "한 줄로 말해줘", "계속해"])
def test_non_keyword_followup_keeps_context(evidence_tools, question):
    context, evidence, direct = dialogue.gather(question, [], {"worker": "차보령"})
    assert context["worker"] == "차보령"
    assert evidence[0]["arguments"]["worker"] == "차보령"


def test_numbered_list_order_wins_over_intro(evidence_tools):
    _, evidence, _ = dialogue.gather("차보령 업무 어때?", [], {})
    context, _ = dialogue.final_metadata("업무1과 업무2입니다.\n1. 업무2 먼저\n2. 업무1 다음", {}, evidence)
    assert context["displayed_post_ids"] == ["2", "1"]


def test_missing_cause_and_past_due_are_explicit_in_model_input(evidence_tools):
    context, evidence, _ = dialogue.gather("차보령 왜?", [], {})
    text = dialogue.answer_messages("왜?", [], context, evidence)[-1]["content"]
    assert '"기한경과": false' in text
    assert "확인되지 않는다고" in text
    assert "추정하면 안 됨" in text


def test_explicit_title_takes_priority(evidence_tools, monkeypatch):
    monkeypatch.setattr(dialogue.tools, "resolve_post_reference", lambda *a: ["3"])
    context, evidence, _ = dialogue.gather("업무3 자세히", [], {"worker": "차보령", "displayed_post_ids": ["1"]})
    assert evidence[0]["arguments"] == {"post_id": "3"}
    assert context["focused_post_id"] == "3"


def test_ambiguous_title_asks(evidence_tools, monkeypatch):
    monkeypatch.setattr(dialogue.tools, "resolve_post_reference", lambda *a: ["1", "2"])
    _, evidence, direct = dialogue.gather("같은 업무 자세히", [], {"worker": "차보령"})
    assert not evidence and "여러 개" in direct


def test_long_history_has_bounded_budget(evidence_tools):
    context, evidence, _ = dialogue.gather("차보령 업무 어때?", [], {})
    history = [{"role": "user", "text": "긴 질문입니다." * 1000} for _ in range(20)]
    messages = dialogue.answer_messages("왜?", history, context, evidence)
    assert sum(len(m["content"].encode()) for m in messages) < 22000


@pytest.mark.parametrize("ending", ["length", "failure"])
def test_partial_answer_never_continues_with_second_model(monkeypatch, ending):
    calls = []
    class Client:
        def chat(self, **kwargs):
            calls.append(kwargs["model"])
            yield {"message": {"content": "부분 답변"}}
            if ending == "failure":
                raise TimeoutError("disconnected")
            yield {"message": {}, "done_reason": "length"}
    monkeypatch.setattr(llm_backend.qwen_client, "get_ollama_client_with_candidates", lambda: (Client(), ["qwen2.5:14b", "gpt-oss:20b"]))
    stream = llm_backend.chat_stream([])
    assert next(stream)["text"] == "부분 답변"
    with pytest.raises((RuntimeError, TimeoutError)):
        next(stream)
    assert calls == ["qwen2.5:14b"]


def test_internal_thinking_is_not_emitted(monkeypatch):
    class Client:
        def chat(self, **kwargs):
            yield {"message": {"thinking": "내부 추론"}}
            yield {"message": {"content": "사용자 답변"}}
    monkeypatch.setattr(llm_backend.qwen_client, "get_ollama_client_with_candidates", lambda: (Client(), ["qwen2.5:14b"]))
    assert list(llm_backend.chat_stream([])) == [{"type": "delta", "text": "사용자 답변"}]


@pytest.mark.parametrize("question, expected", [
    ("완료된 거 전체 보여줘", {"status": "완료"}),
    ("기한 지난 것 전부 보여줘", {"due": "overdue"}),
    ("전체 보여줘", {}),
])
def test_all_preserves_current_filters(evidence_tools, question, expected):
    context = dialogue.resolve_context(question, [], {"worker": "차보령", "status": "대기", "due": "none", "focused_post_id": "1"})
    assert context["worker"] == "차보령"
    assert {k: context[k] for k in ("status", "due") if k in context} == expected
    assert "focused_post_id" not in context


def test_filter_followup_does_not_return_to_old_focused_post(evidence_tools):
    context, evidence, _ = dialogue.gather("완료된 것은?", [], {"worker": "차보령", "focused_post_id": "1"})
    context, evidence, _ = dialogue.gather("왜?", [], context)
    assert evidence[0]["name"] == "filter_posts"
    assert evidence[0]["arguments"]["status"] == "완료"
    assert "focused_post_id" not in context


@pytest.mark.parametrize("question", ["1번 다시 설명해", "첫 번째 자세히", "1번째는?"])
def test_natural_numbered_reference(evidence_tools, question):
    context, evidence, _ = dialogue.gather(question, [], {"worker": "차보령", "displayed_post_ids": ["3", "1"]})
    assert evidence[0]["arguments"] == {"post_id": "3"}


def test_zero_is_not_last_post(evidence_tools):
    _, evidence, direct = dialogue.gather("0번째", [], {"worker": "차보령", "displayed_post_ids": ["1", "2"]})
    assert not evidence and direct


def test_focused_post_excludes_siblings_from_answer(evidence_tools):
    _, evidence, _ = dialogue.gather("차보령 업무 어때?", [], {})
    data = dialogue.grounded_data(evidence, {"focused_post_id": "3", "displayed_post_ids": ["1", "2"]})
    assert [p["post_id"] for p in data["업무표본"]] == ["3"]


def test_explicit_full_list_does_not_invent_completion_date(evidence_tools):
    context, evidence, _ = dialogue.gather("완료된 거 전체 보여줘", [], {"worker": "차보령"})
    text, ids = dialogue.requested_list_answer("완료된 거 전체 보여줘", evidence)
    assert len(ids) == 4
    assert all(f"업무{i}" in text for i in range(1, 5))
    assert "오늘" not in text
    assert evidence[0]["name"] == "filter_posts"
    context, _ = dialogue.final_metadata(text, context, evidence, listed_ids=ids)
    assert context["displayed_post_ids"] == ids


def test_list_limit_is_disclosed():
    text, _ = dialogue.requested_list_answer("전체 보여줘", [{"data": {"posts": [], "task_count": 80}}])
    assert "전체 목록은 아닙니다" in text


def test_record_list_count_uses_matching_posts():
    result = {"posts": [{"post_id": "1", "title": "회의 기록", "task_status": "회의록"}],
              "post_count": 1, "task_count": 0}
    text, _ = dialogue.requested_list_answer("회의록 전체 보여줘", [{"data": result}])
    assert "업무는 1건" in text
