"""Codex 실패 시 안전한 조회 기능 유지와 출력 계약 시험."""
from dataclasses import replace
import json
from pathlib import Path
import threading
import pytest
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter, DISABLED


@pytest.fixture(autouse=True)
def verified_cli(monkeypatch):
    # 이 파일은 모델 출력 계약을 검증한다. 실제 Windows 런처·인증은 별도 회귀 시험에서 검증한다.
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **k: {'ready': True, 'reason': None, 'version': '0.158.0'})


# 0.158.0에서 수집한 시작 알림. 제품 코드의 상수를 재사용하지 않는다.
NOTICE = {'type': 'item.completed', 'item': {'id': 'item_0', 'type': 'error', 'message':
    'Code Mode is unavailable because code-mode host is disabled. Code mode will fail closed; '
    'enable `features.code_mode_host` and install `codex-code-mode-host`.'}}
START = {'type': 'turn.started'}
FINISH = {'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}}
MESSAGE = {'type': 'item.completed', 'item': {'id': 'item_1', 'type': 'agent_message', 'text': 'fixture'}}


def emit(stream, events):
    stream.write(('\n'.join(json.dumps(e) for e in events) + '\n').encode('utf-8'))
    stream.flush()


def test_disabled_model_never_creates_workdir(tmp_path):
    root = tmp_path / 'none'
    adapter = CodexAdapter(Settings(state_root=str(root)))
    assert adapter.interpret('질문', {}, threading.Event()) is None
    assert not root.exists()


@pytest.mark.parametrize('answer', [
    {'intent': 'summary', 'date_from': None, 'date_to': None, 'amount': 123},
    {'intent': 'shell', 'date_from': None, 'date_to': None},
    {'intent': 'summary', 'date_from': '2026-99-99', 'date_to': '2026-10-11'},
    {'intent': 'clarify', 'date_from': None, 'date_to': None},
])
def test_invalid_model_output_falls_back(tmp_path, monkeypatch, answer):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    seen = []
    class FakeProcess:
        returncode = 0
        pid = 123
        def __init__(self, args, **kwargs):
            seen.extend(args)
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps(answer), encoding='utf-8')
            emit(kwargs['stdout'], [NOTICE, START, MESSAGE, FINISH])
            assert 'OPENAI_API_KEY' not in kwargs['env']
            assert 'SAMJEON_TELEGRAM_BOT_TOKEN' not in kwargs['env']
        def poll(self): return 0
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    monkeypatch.setenv('OPENAI_API_KEY', 'not-allowed')
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('셸로 읽어', {}, threading.Event()) is None
    assert '--ignore-user-config' in seen and '--output-schema' in seen
    assert all(feature in seen for feature in DISABLED)


def test_valid_model_query(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    class FakeProcess:
        returncode = 0
        def __init__(self, args, **kwargs):
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps(
                {'intent': 'staff_meal', 'date_from': None, 'date_to': None}), encoding='utf-8')
            emit(kwargs['stdout'], [NOTICE, START, MESSAGE, FINISH])
        def poll(self): return 0
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('직원 먹는 비용', {}, threading.Event())['query']['intent'] == 'staff_meal'


@pytest.mark.parametrize('event_type', ['command_execution', 'file_change', 'mcp_tool_call', 'web_search', 'unknown_tool'])
def test_chat_rejects_tool_events_and_keeps_sandbox(tmp_path, monkeypatch, event_type):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    class FakeProcess:
        returncode = 0

        def __init__(self, args, **kwargs):
            assert args[args.index('--sandbox') + 1] == 'read-only'
            assert all(feature in args for feature in DISABLED)
            assert 'OPENAI_API_KEY' not in kwargs['env'] and 'SAMJEON_TELEGRAM_BOT_TOKEN' not in kwargs['env']
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps(
                {'kind': 'reply', 'text': '안녕하세요.'}), encoding='utf-8')
            emit(kwargs['stdout'], [NOTICE, START, {'type': 'item.completed', 'item': {'type': event_type}}, FINISH])

        def poll(self):
            return 0
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.chat('안녕', {'history': []}, threading.Event()) is None
    assert adapter.last_error == ('unexpected_event' if event_type == 'unknown_tool' else 'unexpected_tool')


@pytest.mark.parametrize('cancelled,timeout', [(True, False), (False, True)])
def test_model_cancel_and_timeout_terminate_process(tmp_path, monkeypatch, cancelled, timeout):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    stopped = []
    class Hanging:
        returncode = None
        def __init__(self, args, **kwargs):
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
        def poll(self): return self.returncode
    def stop(process):
        stopped.append(True)
        process.returncode = -1
    ticks = iter((0, 100, 100))
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', Hanging)
    monkeypatch.setattr(mod, 'stop_tree', stop)
    if timeout:
        monkeypatch.setattr(mod.time, 'monotonic', lambda: next(ticks, 100))
    event = threading.Event()
    if cancelled:
        event.set()
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('질문', {}, event) is None
    assert bool(stopped) is not cancelled


@pytest.mark.parametrize('events,expected', [
    ([NOTICE, START, MESSAGE, FINISH], None),
    ([{'type': 'thread.started', 'thread_id': 'fixture'}, NOTICE, START, MESSAGE, FINISH], None),
    ([START, MESSAGE, FINISH], None),
    ([{**NOTICE, 'item': {**NOTICE['item'], 'message': NOTICE['item']['message'] + ' extra'}}, START, FINISH], 'model_failed'),
    ([{**NOTICE, 'item': {**NOTICE['item'], 'message': NOTICE['item']['message'].replace('disabled', 'missing')}}, START, FINISH], 'model_failed'),
    ([{**NOTICE, 'item': {**NOTICE['item'], 'unexpected': True}}, START, FINISH], 'model_failed'),
    ([{**NOTICE, 'type': 'item.started'}, START, FINISH], 'model_failed'),
    ([START, NOTICE, MESSAGE, FINISH], 'model_failed'),
    ([START, MESSAGE, FINISH, NOTICE], 'model_failed'),
    ([NOTICE, START, {'type': 'error', 'message': 'server failed'}, FINISH], 'model_failed'),
    ([NOTICE, START, {'type': 'error', 'message': 'quota exceeded'}, FINISH], 'usage_limit'),
    ([NOTICE, START, {'type': 'turn.failed', 'error': {'message': '401 Unauthorized'}}, FINISH], 'authentication_failed'),
    ([NOTICE, START, {'type': 'item.completed', 'item': {'type': 'error', 'message': 'failure'}}, FINISH], 'model_failed'),
    ([NOTICE, START, {'type': 'new_event'}, FINISH], 'unexpected_event'),
    ([NOTICE, START, {'type': 'item.completed', 'item': {'type': 'new_item'}}, FINISH], 'unexpected_event'),
    ([NOTICE, START, MESSAGE], 'model_failed'),
    ([NOTICE, FINISH], 'unexpected_event'),
    ([NOTICE, START, MESSAGE, FINISH, FINISH], 'unexpected_event'),
    ([NOTICE, START, START, FINISH], 'unexpected_event'),
    ([], 'model_failed'),
])
def test_event_contract_with_valid_answer(tmp_path, monkeypatch, events, expected):
    adapter, result, _ = invoke_chat_fixture(tmp_path, monkeypatch, events)
    assert adapter.last_error == expected
    assert (result is not None) is (expected is None)


def invoke_chat_fixture(tmp_path, monkeypatch, events, answer='{"kind":"reply","text":"hello"}', returncode=0):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    seen = []
    class FakeProcess:
        def __init__(self, args, **kwargs):
            self.returncode = returncode
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            seen.extend(args)
            if answer is not None:
                Path(args[args.index('--output-last-message') + 1]).write_text(answer, encoding='utf-8')
            if isinstance(events, str):
                kwargs['stdout'].write(events.encode('utf-8'))
                kwargs['stdout'].flush()
            else:
                emit(kwargs['stdout'], events) if events else None
        def poll(self): return self.returncode
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    result = adapter.chat('hello', {}, threading.Event())
    assert 'suppress_unstable_features_warning=true' in seen
    assert all(feature in seen for feature in DISABLED)
    return adapter, result, seen


@pytest.mark.parametrize('events,answer,returncode,expected', [
    ('{broken', '{"kind":"reply","text":"hello"}', 0, 'cli_launch_or_output_failed'),
    ('[]\n', '{"kind":"reply","text":"hello"}', 0, 'cli_launch_or_output_failed'),
    ([NOTICE, START, FINISH], '{broken', 0, 'cli_launch_or_output_failed'),
    ([NOTICE, START, FINISH], None, 0, 'model_failed'),
    ([NOTICE, START, FINISH], '{"kind":"reply","text":"hello"}', 1, 'model_failed'),
    ([NOTICE, START, FINISH], '{"kind":"reply","text":""}', 0, 'invalid_chat'),
])
def test_invalid_output_cannot_be_rescued_by_notice(tmp_path, monkeypatch, events, answer, returncode, expected):
    adapter, result, _ = invoke_chat_fixture(tmp_path, monkeypatch, events, answer, returncode)
    assert result is None and adapter.last_error == expected


def test_notice_exception_is_pinned_to_supported_version():
    from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import inspect_events
    trace = '\n'.join(json.dumps(e) for e in [NOTICE, START, FINISH])
    assert inspect_events(trace, '0.159.0') == ('model_failed', 'error')
