"""자연어 계획·계산 근거·후속 대화의 서비스 인수 시험."""
import asyncio
import copy
from dataclasses import replace
from datetime import timedelta
import threading

import pytest

from test_samjeon_material_service import service, conversation, update, messages, NOW
from modules.transform.pipelines.db.samjeon_material_service.grounded import validate_plan, render_answer, make_facts
from modules.transform.pipelines.db.samjeon_material_service.storage import Store


def plan(intent='summary', product=None, start=None, end=None):
    return {'action': 'query', 'queries': [{'intent': intent, 'date_from': start,
            'date_to': end, 'product_name': product}], 'clarification': None}


class Analyst:
    last_error = None
    def __init__(self, planned=None, answer=None):
        self.planned = planned or plan()
        self.answer = answer
        self.calls = []

    def plan(self, question, context, cancelled):
        self.calls.append(('plan', context))
        return copy.deepcopy(self.planned)

    def explain(self, question, context, cancelled):
        self.calls.append(('explain', context))
        return copy.deepcopy(self.answer or {'fact_ids': ['F1'], 'hypotheses': [
            {'text': '행사 준비를 위한 선매입 가능성이 있습니다.', 'fact_ids': ['F1'], 'note_ids': []}],
            'questions': ['행사 준비나 배송 일정 변경이 있었나요?']})


def enable(service, model=None):
    service.settings = replace(service.settings, ai_enabled=True, ai_verified=True)
    service.model = model or Analyst()
    return service.model


def test_natural_question_uses_planner_before_keyword_routing(service):
    model = enable(service, Analyst(plan('compare')))
    asyncio.run(conversation(service, update(1, '이번 달 매입률이 왜 올랐어?')))
    assert service.repo.queries[0][0] == 'compare'
    assert [r[0] for r in model.calls] == ['plan', 'explain']
    assert '확인 전 가설' in messages(service)[-1]
    assert '선매입' in messages(service)[-1]


def test_commands_do_not_consume_model_calls(service):
    model = enable(service)
    asyncio.run(conversation(service, update(1, '/summary')))
    assert model.calls == []


@pytest.mark.parametrize('bad', [
    {'fact_ids': ['F999'], 'hypotheses': [], 'questions': []},
    {'fact_ids': ['F1'], 'hypotheses': [{'text': '매입이 999원 늘었습니다.', 'fact_ids': ['F1'], 'note_ids': []}], 'questions': []},
    {'fact_ids': ['F1'], 'hypotheses': [{'text': '매입이 백만원 늘었습니다.', 'fact_ids': ['F1'], 'note_ids': []}], 'questions': []},
    {'fact_ids': ['F1'], 'hypotheses': [{'text': '폐기가 확실합니다.', 'fact_ids': ['F1'], 'note_ids': []}], 'questions': []},
    {'fact_ids': ['F1'], 'hypotheses': [{'text': '원인이 확실합니다.', 'fact_ids': [], 'note_ids': []}], 'questions': []},
    {'fact_ids': ['F1'], 'hypotheses': [], 'questions': [], 'shell': 'anything'},
])
def test_unverified_model_answer_falls_back_to_calculation(service, bad):
    enable(service, Analyst(answer=bad))
    asyncio.run(conversation(service, update(1, '원인을 분석해줘')))
    assert '확인된 조회 결과' in messages(service)[-1]
    assert '999원' not in messages(service)[-1]
    assert service.store.get('ai_status')['error'] == 'invalid_answer'


def test_incomplete_data_suppresses_hypothesis(service):
    enable(service)
    original = service.repo.query
    def pending(*a, **k):
        result = original(*a, **k)
        result['status'] = '잠정'
        return result
    service.repo.query = pending
    asyncio.run(conversation(service, update(1, '원인을 분석해줘')))
    assert '판단을 보류' in messages(service)[-1]
    assert '선매입 가능성' not in messages(service)[-1]


def test_incomplete_comparison_is_not_hidden_by_current_period_status(service):
    result = service.repo.query(None, 'compare', '2026-10-01', '2026-10-11')
    result['result']['seven_day']['complete'] = False
    result['result']['seven_day']['reason'] = '지난 비교 기간 미확정'
    assert make_facts([result])[0]['status'] == '잠정'


def test_history_persists_but_answer_invalidates_on_note_or_focus(service):
    model = enable(service)
    asyncio.run(conversation(service, update(1, '원인을 분석해줘'), update(2, '그러면 무엇을 확인할까?')))
    assert model.calls[2][1]['history'][0]['question'] == '원인을 분석해줘'
    analysis = service.store.context(22, NOW)
    assert service.store.grounded_answer(22, analysis, [])
    service.store.note(22, analysis['id'], 11, '행사 때문에 미리 샀어')
    assert service.store.grounded_answer(22, analysis, service.store.notes(22, analysis['id'])) is None
    assert service.store.turns(22, analysis, NOW + timedelta(hours=25)) == []
    assert service.store.turns(44, analysis, NOW) == []
    root = service.store.root
    service.store.close()
    service.store = Store(root)
    assert len(service.store.turns(22, analysis, NOW)) == 2
    service.store.focus(analysis['id'], 22, 'other')
    assert service.store.turns(22, service.store.analysis(analysis['id'], 22), NOW) == []
    service.store.close()


def test_pdf_uses_same_saved_analysis(service, monkeypatch):
    model = enable(service)
    captured = []
    def pdf(settings, repo, analysis, notes, grounded=None):
        captured.append((analysis, grounded))
        return service.store.root / 'reports' / 'test.pdf'
    monkeypatch.setattr('modules.transform.pipelines.db.samjeon_material_service.runtime.create_pdf', pdf)
    asyncio.run(conversation(service, update(1, '원인을 분석해줘'), update(2, '/report')))
    assert '확인된 자료' in captured[0][1]
    assert len(model.calls) == 2
    assert any(m == 'sendDocument' for m, _ in service.transport.calls)


def test_three_query_limit_and_malformed_plan():
    raw = plan()
    raw['queries'] *= 4
    with pytest.raises(ValueError):
        validate_plan(raw)
    raw = plan()
    raw['queries'][0]['intent'] = 'shell'
    with pytest.raises(ValueError):
        validate_plan(raw)


def test_planner_failure_does_not_invoke_legacy_model_again(service):
    model = enable(service)
    model.plan = lambda *a: None
    model.last_error = 'usage_limit'
    model.interpret = lambda *a: pytest.fail('질문당 모델 재시도 금지')
    asyncio.run(conversation(service, update(1, '매출 요약')))
    assert 'AI 연결을 사용할 수 없어' in messages(service)[-1]
    assert model.calls == []


def test_cancel_while_explanation_does_not_send_result(service):
    model = enable(service)
    started, release = threading.Event(), threading.Event()
    original = model.explain
    def slow(*a):
        started.set()
        release.wait(5)
        return original(*a)
    model.explain = slow
    async def run():
        request = update(1, '원인을 분석해줘')
        await service.ingest([request])
        task = asyncio.create_task(service.handle(request))
        while not started.is_set():
            await asyncio.sleep(.01)
        await service.ingest([update(2, '/cancel')])
        release.set()
        await task
    asyncio.run(run())
    assert len(messages(service)) == 1 and '취소' in messages(service)[0]
    assert service.store.get('turns-v1-22') is None


def test_unknown_product_clarifies_instead_of_using_whole_store(service):
    enable(service, Analyst(plan('compare', '없는품목')))
    service.repo.search_items = lambda *a: []
    asyncio.run(conversation(service, update(1, '없는품목은 왜 늘었어?')))
    assert service.repo.queries == []
    assert '찾지 못했습니다' in messages(service)[-1]
