"""실제 Chromium에서 스트리밍 세션 분리·재시도·한글 입력을 확인한다."""
import threading

import pytest

from modules.transform.doridang_bot import dialogue, llm_backend, server


def test_browser_conversation_controls(monkeypatch, tmp_path):
    from contextlib import nullcontext
    monkeypatch.setattr(dialogue.tools, "request_snapshot", nullcontext)
    playwright = pytest.importorskip("playwright.sync_api")
    monkeypatch.setenv("DORIDANG_BOT_LOG_PATH", str(tmp_path / "conversation.md"))
    monkeypatch.setattr(dialogue.tools, "is_known_person", lambda n: n == "차보령")
    monkeypatch.setattr(dialogue.tools, "resolve_post_reference", lambda *a: [])
    monkeypatch.setattr(dialogue.tools, "detect_worker_name", lambda s: "차보령" if "차보령" in s else None)
    monkeypatch.setattr(dialogue.tools, "data_freshness", lambda: {"latest": "2026-09-09", "stale": False})
    monkeypatch.setattr(dialogue.tools, "execute_tool", lambda *a: {"posts": [
        {"post_id": "1", "project_id": "2926716", "title": "업무1", "post_url": "https://flow.team/l/a"}]})
    release = threading.Event()
    def stream(*args, **kwargs):
        release.wait(10)
        return {"blocks": [{"kind": "fact", "ref": "post:1", "text": ""}]}
    monkeypatch.setattr(llm_backend, "structured_chat", stream)
    http = server.ThreadingHTTPServer(("127.0.0.1", 0), server.DoridangBotHandler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        with playwright.sync_playwright() as p:
            browser = p.chromium.launch(headless=True, channel="chrome")
            page = browser.new_page(viewport={"width": 1280, "height": 720})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{http.server_port}/")
            page.locator("#message-input").fill("차보령 업무 어때?")
            page.locator("#message-input").dispatch_event("keydown", {"key": "Enter", "isComposing": True})
            assert page.locator(".message.user").count() == 0
            page.locator("#send-button").click()
            page.wait_for_function("document.querySelector('.assistant')?.textContent.includes('검증')")
            assert page.evaluate("activeSession().messages.at(-1).text") == ""
            old_id = page.evaluate("activeSessionId")
            page.locator("#new-session-button").click()
            new_id = page.evaluate("activeSessionId")
            assert new_id != old_id
            release.set()
            page.wait_for_function("pendingRequest === null")
            assert page.locator(".message").count() == 0
            page.evaluate("id => setActiveSession(id)", old_id)
            assert page.locator(".assistant").count() == 1
            assert page.evaluate("activeSession().messages.at(-1).status") == "complete"
            assert page.evaluate("activeSession().messages.at(-1).context.worker") == "차보령"
            assert page.locator(".assistant a").count() == 1
            assert page.evaluate("document.querySelector('#chat-form').getBoundingClientRect().bottom <= innerHeight")
            # 중단 후 같은 질문을 재시도해도 사용자 발화가 중복되지 않는다.
            release.clear()
            page.locator("#message-input").fill("이 담당자는 뭘 못했어?")
            page.locator("#send-button").click()
            page.wait_for_function("document.querySelector('.assistant:last-child')?.textContent.includes('검증')")
            assert page.evaluate("activeSession().messages.at(-1).text") == ""
            page.locator("#send-button").click()
            page.wait_for_function("pendingRequest === null")
            assert page.evaluate("activeSession().messages.at(-1).status") == "cancelled"
            release.set()
            count = page.locator(".message.user").count()
            page.locator(".retry-button").click()
            page.wait_for_function("pendingRequest === null && activeSession().messages.at(-1).status === 'complete'")
            assert page.locator(".message.user").count() == count
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.querySelector('#chat-form').getBoundingClientRect().bottom <= innerHeight")
            page.reload()
            assert page.locator(".message.user").count() == count
            assert not errors
            browser.close()
    finally:
        release.set()
        http.shutdown()
        http.server_close()
