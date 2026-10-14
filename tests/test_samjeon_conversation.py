"""대표님 자연어 질문의 회귀 시험. 실계정·Telegram·모델을 호출하지 않는다."""
import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
import json
from pathlib import Path
import threading

import pytest

from test_samjeon_material_service import service, conversation, update, messages, NOW
from modules.transform.pipelines.db.samjeon_material_service.conversation import (
    DATA_UNAVAILABLE, AI_UNAVAILABLE, CLARIFY, period, route)
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter, validate_chat
from modules.transform.pipelines.db.samjeon_material_service.storage import Store


class ChatModel:
    last_error = None

    def __init__(self):
        self.calls = []

    def chat(self, question, context, cancelled):
        self.calls.append((question, context))
        return {'kind': 'reply', 'text': '안녕하세요. 함께 이야기해요.' if not context['history'] else '아까 함께 이야기하자고 말씀드렸어요.'}

    def plan(self, *args):
        raise AssertionError('단순 매출 질문과 일반 대화에는 분석 모델 호출 금지')


def enable(service):
    service.settings = replace(service.settings, ai_enabled=True, ai_verified=True)
    service.model = ChatModel()
    return service.model


def missing(service):
    calls = []

    def fail():
        calls.append(True)
        raise ValueError('synthetic_private_path_or_hash_error')
    service.repo.current = fail
    return calls


def test_greeting_followup_without_snapshot_and_restart(service):
    model = enable(service)
    calls = missing(service)
    asyncio.run(conversation(service, update(1, '안녕'), update(2, '아까 말한 내용 다시 설명해줘')))
    assert not calls
    assert model.calls[1][1]['history'][0]['question'] == '안녕'
    root = service.store.root
    service.store.close()
    service.store = Store(root)
    asyncio.run(conversation(service, update(3, '왜?')))
    assert len(model.calls[2][1]['history']) == 2
    assert service.store.context(22, NOW) is None


@pytest.mark.parametrize('text', ['어제 매출은?', '원재료 매입 알려줘', '/summary', '보고서 보여줘', 'PDF'])
def test_missing_data_has_exact_notice_and_no_model(service, text):
    model = enable(service)
    missing(service)
    asyncio.run(conversation(service, update(1, text), update(2, '왜?')))
    assert messages(service) == [DATA_UNAVAILABLE, DATA_UNAVAILABLE]
    assert not model.calls
    assert all(method != 'sendDocument' for method, _ in service.transport.calls)


def test_yesterday_sales_uses_one_day_without_ai(service):
    enable(service)
    asyncio.run(conversation(service, update(1, '어제 매출은?')))
    assert service.repo.queries[0][:3] == ('summary', '2026-10-11', '2026-10-11')
    assert 'YYYY-MM-DD' not in messages(service)[0]


def test_stale_snapshot_does_not_substitute_older_sales(service):
    enable(service)
    service.clock = lambda: NOW + timedelta(days=3)
    asyncio.run(conversation(service, update(1, '어제 매출은?')))
    assert not service.repo.queries
    assert '2026-10-14' in messages(service)[0]
    assert '준비되지' in messages(service)[0]
    assert service.store.context(22, service.clock()) is None


def test_followup_to_unavailable_notice_does_not_become_analysis(service):
    enable(service)
    current = service.repo.current
    missing(service)
    asyncio.run(conversation(service, update(1, '어제 매출은?')))
    service.repo.current = current
    asyncio.run(conversation(service, update(2, '왜?')))
    assert '앞선 질문에서는' in messages(service)[-1]
    assert not service.repo.queries and not service.model.calls


def test_receipt_timestamp_survives_midnight_queue(service):
    event = update(1, '어제 매출은?')
    event['message']['date'] = int(NOW.timestamp())
    service.clock = lambda: NOW + timedelta(days=1)
    asyncio.run(conversation(service, event))
    assert service.repo.queries[0][1:3] == ('2026-10-11', '2026-10-11')


@pytest.mark.parametrize('text,today,expected', [
    ('어제 매출은?', '2027-01-01', ('2026-12-31', '2026-12-31')),
    ('그제 매출', '2026-03-01', ('2026-02-27', '2026-02-27')),
    ('오늘 매출', '2026-10-13', ('2026-10-13', '2026-10-13')),
    ('지난주 매출', '2026-10-13', ('2026-10-05', '2026-10-11')),
    ('최근 7일 매출', '2026-10-13', ('2026-10-06', '2026-10-12')),
    ('이번 달 매출', '2026-10-01', ('2026-10-01', '2026-10-01')),
    ('이번 달 매출', '2026-10-13', ('2026-10-01', '2026-10-12')),
])
def test_relative_dates(text, today, expected):
    assert period(text, today) == expected


@pytest.mark.parametrize('flags', [(False, False), (True, False), (False, True)])
def test_disabled_chat_never_calls_model_or_snapshot(service, flags):
    model = enable(service)
    calls = missing(service)
    service.settings = replace(service.settings, ai_enabled=flags[0], ai_verified=flags[1])
    asyncio.run(conversation(service, update(1, '안녕')))
    assert messages(service) == [AI_UNAVAILABLE]
    assert not calls and not model.calls


def test_ambiguous_question_then_explicit_switch(service):
    enable(service)
    calls = missing(service)
    asyncio.run(conversation(service, update(1, '얼마야?'), update(2, '안녕'), update(3, '왜?')))
    assert messages(service)[0] == CLARIFY
    assert len(service.model.calls) == 2
    assert not calls
    assert route('왜?', 'data') == 'data'
    assert route('안녕', 'data') == 'chat'


@pytest.mark.parametrize('command', ['/start', '/help'])
def test_help_tells_actual_readiness(service, command):
    missing(service)
    asyncio.run(conversation(service, update(1, command)))
    assert '일반 대화: 연결 준비 중' in messages(service)[0]
    assert DATA_UNAVAILABLE in messages(service)[0]
    assert 'PDF를 받' not in messages(service)[0]


@pytest.mark.parametrize('bad', [None, {}, {'kind': 'shell', 'text': 'hello'}, {'kind': 'reply', 'text': '```powershell\nGet-Content secret\n```'}])
def test_invalid_chat_survives_without_private_error(service, bad):
    model = enable(service)
    model.chat = lambda *args: bad
    missing(service)
    asyncio.run(conversation(service, update(1, '안녕')))
    assert messages(service) == [AI_UNAVAILABLE]
    assert not service.store.chat_turns(22, NOW)


def test_chat_exception_and_uncertain_delivery_not_remembered(service):
    model = enable(service)
    def fail(*args):
        raise RuntimeError('private-secret-401')
    model.chat = fail
    asyncio.run(conversation(service, update(1, '안녕')))
    assert messages(service) == [AI_UNAVAILABLE]
    enable(service)
    service.transport.fail = True
    asyncio.run(conversation(service, update(2, '안녕')))
    assert not service.store.chat_turns(22, NOW)


def test_chat_history_limits_isolation_and_token_redaction(service, monkeypatch):
    model = enable(service)
    monkeypatch.setenv(service.settings.token_env, 'synthetic-sensitive-token')
    asyncio.run(conversation(service, update(1, '안녕 synthetic-sensitive-token')))
    assert 'synthetic-sensitive-token' not in str(model.calls)
    assert 'synthetic-sensitive-token' not in str(service.store.chat_turns(22, NOW))
    for i in range(15):
        service.store.remember_chat(22, str(i), '답변', NOW)
    assert len(service.store.chat_turns(22, NOW)) == 10
    assert not service.store.chat_turns(44, NOW)
    assert not service.store.chat_turns(22, NOW + timedelta(hours=24))


def test_unknown_user_and_group_chat_do_not_invoke_chat(service):
    model = enable(service)
    group = update(2, '안녕')
    group['message']['chat']['type'] = 'group'
    asyncio.run(conversation(service, update(1, '안녕', user=99), group))
    assert not messages(service) and not model.calls


def test_unverified_operating_numbers_are_rerouted():
    assert validate_chat({'kind': 'reply', 'text': '어제 매출은 999만원입니다.'})['kind'] == 'data_request'


def test_doctor_separates_disabled_deployment_from_runtime(service, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service.__main__ import doctor
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **kw: {'ready': True, 'reason': None})
    monkeypatch.setenv(service.settings.token_env, 'synthetic-token')
    config = replace(service.settings, auto_send=False, enabled=False, snapshot_root='')
    result = doctor(config)
    assert result['conversation_deployable']
    assert not result['conversation_ready'] and not result['data_ready'] and not result['ready']
    assert '유효한 v3 게시본 없음' in result['blocked_reasons']


def test_single_day_query_followup_keeps_period_and_history(service):
    from test_samjeon_grounded import Analyst
    service.settings = replace(service.settings, ai_enabled=True, ai_verified=True)
    service.model = Analyst()
    asyncio.run(conversation(service, update(1, '어제 매출은?'), update(2, '왜?')))
    planned = service.model.calls[0][1]
    assert planned['date_from'] == planned['date_to'] == '2026-10-11'
    assert planned['history'][0]['question'] == '어제 매출은?'


def test_planner_cannot_replace_resolved_yesterday_period(service):
    from test_samjeon_grounded import Analyst, plan
    service.settings = replace(service.settings, ai_enabled=True, ai_verified=True)
    service.model = Analyst(plan(start='2026-10-01', end='2026-10-10'))
    asyncio.run(conversation(service, update(1, '어제 매출이 왜 그래?')))
    assert service.repo.queries[0][1:3] == ('2026-10-11', '2026-10-11')


@pytest.mark.parametrize('reason', ['authentication_failed', 'usage_limit', 'model_timeout'])
def test_cli_failure_has_safe_notice_and_can_recover(service, reason):
    model = enable(service)
    model.last_error = reason
    model.chat = lambda *a: None
    asyncio.run(conversation(service, update(1, '안녕')))
    assert messages(service) == [AI_UNAVAILABLE]
    enable(service)
    asyncio.run(conversation(service, update(2, '안녕')))
    assert '안녕하세요' in messages(service)[-1]


@pytest.mark.parametrize('failure', ['missing', 'pointer', 'hash'])
def test_real_v3_failure_blocks_numbers_and_pdf_after_prior_success(service, tmp_path, failure):
    import shutil
    from test_samjeon_material_mart import _fixture
    from test_samjeon_material_mart_v3 import build
    from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
    plan, _ = _fixture(tmp_path)
    path = build(plan)
    published = tmp_path / 'published'
    target = published / 'snapshots' / path.name
    shutil.copytree(path, target)
    manifest = json.loads((target / 'manifest.json').read_text(encoding='utf-8'))
    pointer = {'schema_version': 3, 'snapshot_id': manifest['snapshot_id'],
               'as_of_date': manifest['as_of_date'], 'manifest': f'snapshots/{path.name}/manifest.json'}
    latest = published / 'latest.json'
    latest.write_text(json.dumps(pointer), encoding='utf-8')
    service.repo = Repository(replace(service.settings, snapshot_root=str(published)))
    asyncio.run(conversation(service, update(1, '/summary')))
    assert DATA_UNAVAILABLE not in messages(service)[0]
    if failure == 'missing':
        latest.unlink()
    elif failure == 'pointer':
        pointer['snapshot_id'] = 'different'
        latest.write_text(json.dumps(pointer), encoding='utf-8')
    else:
        (target / 'agent_context.json').write_text('{}', encoding='utf-8')
    asyncio.run(conversation(service, update(2, '어제 매출은?'), update(3, '/report')))
    from modules.transform.pipelines.db.samjeon_material_service.repository import SnapshotUnavailable
    expected = str(SnapshotUnavailable(code='snapshot_missing' if failure == 'missing' else 'snapshot_invalid'))
    assert messages(service)[-2:] == [expected, expected]
    assert not any(method == 'sendDocument' for method, _ in service.transport.calls)


def test_cancel_chat_and_duplicate_update_do_not_remember_or_resend(service):
    model = enable(service)
    started = threading.Event()
    def wait_for_cancel(question, context, cancelled):
        started.set()
        assert cancelled.wait(5)
        return {'kind': 'reply', 'text': '취소된 답변'}
    model.chat = wait_for_cancel
    async def run():
        event = update(1, '안녕')
        await service.ingest([event])
        job = asyncio.create_task(service.handle(service.store.next_update()))
        assert await asyncio.to_thread(started.wait, 3)
        await service.ingest([update(2, '/cancel')])
        await job
        await service.ingest([event])
    asyncio.run(run())
    assert len(messages(service)) == 1 and '취소했습니다' in messages(service)[0]
    assert not service.store.chat_turns(22, NOW)


def test_verify_ai_requires_two_turn_memory_without_changing_settings(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service.__main__ import main
    seen = []
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **kw: {'ready': True})
    monkeypatch.setattr(CodexAdapter, 'plan', lambda *a: {'action': 'query'})
    monkeypatch.setattr(CodexAdapter, 'explain', lambda *a: {'fact_ids': ['F1'], 'hypotheses': [], 'questions': []})
    def chat(self, question, context, cancelled):
        seen.append(context)
        return {'kind': 'reply', 'text': '기억 단어는 해바라기입니다.'}
    monkeypatch.setattr(CodexAdapter, 'chat', chat)
    config = tmp_path / 'service.json'
    config.write_text(json.dumps({'state_root': str(tmp_path / 'state')}), encoding='utf-8')
    before = config.read_bytes()
    result = main(['verify-ai', '--config', str(config), '--require-ready'])
    assert result['ready'] and result['followup_valid'] and not result['settings_changed']
    assert len(seen) == 2 and len(seen[1]['history']) == 1
    assert config.read_bytes() == before and not (tmp_path / 'state').exists()


def test_chat_adapter_validates_reply_and_bounds_history(service, monkeypatch):
    seen = []
    def invoke(self, schema, instruction, question, context, cancelled):
        seen.append(context)
        return {'kind': 'reply', 'text': '반갑습니다.'}
    monkeypatch.setattr(CodexAdapter, '_invoke', invoke)
    adapter = CodexAdapter(service.settings)
    result = adapter.chat('안녕', {'history': [{'question': '가' * 4000, 'answer': '나' * 2000}] * 15}, threading.Event())
    assert result['text'] == '반갑습니다.'
    assert len(seen[0]['history']) == 10
    assert len(json.dumps(seen[0], ensure_ascii=False)) < 30000
