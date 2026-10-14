"""자동 세션 경계와 전달 완료 기억의 실제 서비스 흐름."""
import asyncio
from datetime import datetime, timedelta, timezone

from test_samjeon_legacy import service, callback, NOW
from test_samjeon_material_service import conversation, update, messages
from modules.transform.pipelines.db.samjeon_material_service.storage import Store


def test_exact_72_hour_boundary_restart_and_long_absence(tmp_path):
    clock = datetime(2026, 10, 14, 0, tzinfo=timezone.utc)
    store = Store(tmp_path)
    try:
        first, changed = store.memory.select(1, 11, 22, clock)
        assert not changed
        before, changed = store.memory.select(2, 11, 22, clock + timedelta(hours=72, microseconds=-1))
        assert before['id'] == first['id'] and not changed
        boundary, changed = store.memory.select(3, 11, 22, clock + timedelta(hours=72))
        assert changed and boundary['id'] != first['id']
        duplicate, changed = store.memory.select(3, 11, 22, clock + timedelta(days=10))
        assert duplicate['id'] == boundary['id'] and not changed
    finally:
        store.close()
    store = Store(tmp_path)
    try:
        next_session, changed = store.memory.select(4, 11, 22, clock + timedelta(days=30))
        assert changed and next_session['previous'] == boundary['id']
        assert store.memory.active(11, 22)['id'] == next_session['id']
        assert store.memory.active(99, 22) is None
    finally:
        store.close()


def test_transition_uses_fallback_when_single_summary_call_fails(service):
    current = [NOW]
    service.clock = lambda: current[0]
    calls = []

    def fail_summary(context, cancelled):
        calls.append(context)
        raise RuntimeError('synthetic unavailable')

    service.model.summarize = fail_summary
    asyncio.run(conversation(service, update(1, '안녕')))
    old = service.store.memory.active(11, 22)
    current[0] = NOW + timedelta(hours=72)
    asyncio.run(conversation(service, update(2, '안녕')))
    new = service.store.memory.active(11, 22)
    assert new['id'] != old['id'] and new['previous'] == old['id']
    assert new['summary_attempted'] and '안녕' in new['summary']
    assert len(calls) == 1 and len(messages(service)) == 2
    asyncio.run(conversation(service, update(3, '안녕')))
    assert len(calls) == 1 and service.store.memory.active(11, 22)['id'] == new['id']


def test_data_context_survives_transition_and_easy_followup(service):
    current = [NOW]
    service.clock = lambda: current[0]
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘'),
                             update(2, '어떤 품목을 많이 샀어?'),
                             update(3, '그 구매 근거 보여줘')))
    previous = service.store.memory.active(11, 22)
    assert previous['analysis_id'] and previous['last_query']['intent'] == 'evidence'
    current[0] = NOW + timedelta(hours=73)
    asyncio.run(conversation(service, update(4, '쉽게 다시 설명해줘')))
    active = service.store.memory.active(11, 22)
    assert active['id'] != previous['id']
    assert active['analysis_id'] and active['last_query']['intent'] == 'evidence'
    assert '구매 기록' in messages(service)[-1] and '자료 기준일' in messages(service)[-1]
    assert len(service.store.memory.model_context(active['id'])['history']) == 1


def test_recall_shows_historical_basis_and_rejects_other_owner(service):
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘')))
    asyncio.run(conversation(service, update(2, '지난번 10월 분석 가져와')))
    assert '당시 자료 기준일 2026-10-11' in messages(service)[-1]
    assert '10월 전체 실적은 아닙니다' in messages(service)[-1]
    assert not service.store.memory.search(99, 22, '지난번 10월 분석 가져와')
    assert len(service.store.memory.recent(service.store.memory.active(11, 22)['id'])) == 1


def test_old_button_does_not_replace_new_session_analysis(service):
    current = [NOW]
    service.clock = lambda: current[0]
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘')))
    old_aid = service.store.memory.active(11, 22)['analysis_id']
    current[0] = NOW + timedelta(hours=73)
    asyncio.run(conversation(service, update(2, '2026년 9월 분석해줘')))
    newer = service.store.memory.active(11, 22)
    assert newer['analysis_id'] != old_aid
    asyncio.run(conversation(service, callback(3, old_aid + ':items')))
    assert service.store.memory.active(11, 22)['analysis_id'] == newer['analysis_id']


def test_uncertain_delivery_is_not_recorded_as_memory(tmp_path):
    store = Store(tmp_path)
    try:
        at = datetime(2026, 10, 14, tzinfo=timezone.utc)
        store.memory.select(1, 11, 22, at)
        store.enqueue('update-1:0000', 22, {'method': 'sendMessage', 'text': '답변'})
        store.sent_state('update-1:0000', 'uncertain')
        assert not store.memory.record(1, 11, 22, '질문', '답변', at)
        assert not store.memory.recent(store.memory.active(11, 22)['id'])
    finally:
        store.close()


def test_existing_delivered_turn_is_linked_once_without_reset(tmp_path):
    at = datetime(2026, 10, 14, tzinfo=timezone.utc)
    store = Store(tmp_path)
    try:
        store.receive([update(1, '안녕')], at)
        store.update_status(1, 'done')
        store.enqueue('update-1:0000', 22, {'method': 'sendMessage', 'text': '반갑습니다.'})
        store.sent_state('update-1:0000', 'sent', 10)
        store.add_analysis('old-analysis', 22, '/synthetic', 'old', '2026-10-01', '2026-10-11')
        store.remember(22, 'old-analysis', at + timedelta(hours=24))
        current, changed = store.memory.select(2, 11, 22, at + timedelta(hours=73))
        assert changed and current['analysis_id'] == 'old-analysis'
        old = store.memory.session(current['previous'])
        assert old['title'] == '이전 대화'
        assert [item['answer'] for item in store.memory.recent(old['id'])] == ['반갑습니다.']
        marker = store.get('memory-migrated-v1-11-22')
    finally:
        store.close()
    store = Store(tmp_path)
    try:
        assert store.get('memory-migrated-v1-11-22') == marker
        assert len(store.memory.recent(marker)) == 1
        assert store.db.execute('PRAGMA user_version').fetchone()[0] == 1
    finally:
        store.close()


def test_multiple_historical_candidates_require_selection(service):
    current = [NOW]
    service.clock = lambda: current[0]
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘')))
    current[0] = NOW + timedelta(hours=73)
    asyncio.run(conversation(service, update(2, '송파삼전점 10월을 분석해줘'),
                             update(3, '지난번 10월 분석 가져와')))
    assert '이전 기록이 여러 개입니다' in messages(service)[-1]
    asyncio.run(conversation(service, update(4, '1')))
    assert '당시 자료 기준일 2026-10-11' in messages(service)[-1]
    assert not service.store.memory.recent(service.store.memory.active(11, 22)['id'])[-1]['question'] == '1'


def test_queued_message_keeps_received_session_at_processing_boundary(service):
    current = [NOW]
    service.clock = lambda: current[0]
    asyncio.run(conversation(service, update(1, '안녕')))
    first = service.store.memory.active(11, 22)
    current[0] = NOW + timedelta(hours=71)
    asyncio.run(service.ingest([update(2, '안녕')]))
    current[0] = NOW + timedelta(hours=73)
    asyncio.run(service.handle(service.store.next_update()))
    assert service.store.memory.active(11, 22)['id'] == first['id']


def test_recall_filters_topic_and_distinguishes_chat_date(service):
    asyncio.run(conversation(service, update(1, '안녕'),
                             update(2, '송파삼전점 10월을 분석해줘'),
                             update(3, '어떤 품목을 많이 샀어?'),
                             update(4, '그 구매 근거 보여줘')))
    matches = service.store.memory.search(11, 22, '지난번 양파 근거 보여줘')
    assert len(matches) == 1 and '근거' in matches[0]['answer']
    asyncio.run(conversation(service, update(5, '지난번 2026-10-14 대화')))
    assert '당시 대화 2026-10-14' in messages(service)[-1]
    assert '자료 기준일' not in messages(service)[-1]


def test_explicit_preference_survives_transition(service):
    current = [NOW]
    service.clock = lambda: current[0]
    asyncio.run(conversation(service, update(1, '앞으로 설명은 짧게 해줘')))
    old = service.store.memory.active(11, 22)
    assert old['preferences'][0]['update_id'] == 1
    current[0] = NOW + timedelta(hours=73)
    asyncio.run(conversation(service, update(2, '안녕')))
    new = service.store.memory.active(11, 22)
    assert '앞으로 설명은 짧게 해줘' in service.store.memory.model_context(new['id'])['preferences']


def test_failed_note_delivery_does_not_persist_note(service):
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘')))
    aid = service.store.memory.active(11, 22)['analysis_id']
    service.transport.fail = True
    asyncio.run(conversation(service, update(2, '행사 때문에 미리 샀어')))
    assert not service.store.notes(22, aid)
