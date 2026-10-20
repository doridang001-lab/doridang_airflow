"""지속 대화·읽기 전용 자유 조회·전달 상태의 회귀 시험."""
import asyncio
from dataclasses import replace
import json
import threading
from datetime import timedelta

import pytest

from test_samjeon_legacy import fixture, NOW
from test_samjeon_material_service import Transport, conversation, update, messages
from modules.transform.pipelines.db.samjeon_material_service.analysis_tools import AnalysisTools
from modules.transform.pipelines.db.samjeon_material_service.appserver import AppServer, AppServerError
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
from modules.transform.pipelines.db.samjeon_material_service.runtime import Service
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.presentation import chunks, telegram_plain, telegram_answer
from modules.transform.pipelines.db.samjeon_material_service.daily_briefing import due, send_daily_briefing
from modules.transform.pipelines.db.samjeon_material_service.workspace_files import WorkspaceFiles
from modules.transform.pipelines.db.samjeon_material_service.brain_files import BrainFiles


@pytest.fixture
def settings(tmp_path):
    fixture(tmp_path / 'published')
    return Settings(snapshot_root=str(tmp_path / 'published'), state_root=str(tmp_path / 'state'),
                    codex_backend='wsl', wsl_distribution='Ubuntu', wsl_user='samjeon',
                    wsl_codex_executable='/opt/samjeon-node/bin/codex', conversation_engine='appserver',
                    enabled=True, ai_enabled=True, ai_verified=True, allowed_users=(11,), allowed_chats=(22,))


def test_flexible_sql_and_evidence(settings):
    tool = AnalysisTools(settings, Repository(settings))
    catalog = tool.call('data_catalog', {})
    assert 'source_file' not in catalog['tables']['purchase_lines']['columns']
    result = tool.call('query_data', {'sql': 'WITH grouped AS (SELECT category, sum(amount_won) AS amount '
        'FROM purchase_lines GROUP BY category) SELECT category, amount, '
        'round(100.0*amount/sum(amount) OVER (), 2) FROM grouped ORDER BY amount DESC'})
    assert sum(row[1] for row in result['rows']) == 8850
    assert result['snapshot_id'] == catalog['snapshot_id']
    assert tool.call('get_evidence', {'evidence_ids': ['E0000']})['rows']
    assert tool.call('query_data', {'sql': 'SELECT stdev(amount_won) FROM purchase_lines'})['rows'][0][0] > 0
    # 재생성 없이 불변 캐시를 재사용하고 잠긴 파일을 남기지 않는다.
    other = AnalysisTools(settings, Repository(settings))
    other.prepare()
    assert other.path == tool.path
    other.path.rename(other.path.with_suffix('.checked'))


@pytest.mark.parametrize('sql', [
    'DELETE FROM purchase_lines', 'DROP TABLE sales_daily',
    "ATTACH DATABASE ':memory:' AS other", 'PRAGMA writable_schema=ON',
    'SELECT * FROM sqlite_master', "SELECT load_extension('bad')",
    'CREATE TABLE bad(x)', 'SELECT 1; SELECT 2',
    "SELECT * FROM pragma_table_info('sales_daily')",
])
def test_sql_cannot_write_or_escape(settings, sql):
    tool = AnalysisTools(settings, Repository(settings))
    assert 'error' in tool.call('query_data', {'sql': sql})
    assert tool.call('query_data', {'sql': 'SELECT sum(amount_won) FROM sales_daily'})['rows'] == [[10000]]


def test_query_limits_and_cancel(settings):
    tool = AnalysisTools(settings, Repository(settings))
    result = tool.call('query_data', {'sql': 'WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL '
        'SELECT x+1 FROM n WHERE x<2000) SELECT x FROM n'})
    assert len(result['rows']) == 100 and result['truncated']
    tool.cancelled.set()
    assert tool.call('data_catalog', {}) == {'error': 'cancelled'}


class Agent:
    ready = True
    def __init__(self):
        self.opened, self.turns, self.analysis_modes = [], [], []
        self.fail = False

    async def open_thread(self, thread_id=None, *, analysis_only=False):
        self.opened.append(thread_id)
        self.analysis_modes.append(analysis_only)
        return thread_id or 'thread-' + str(len(self.opened))

    async def turn(self, thread_id, text, call, cancelled, progress, context, started):
        self.turns.append((thread_id, text, context))
        started('turn-' + str(len(self.turns)))
        if self.fail:
            raise AppServerError('model_failed')
        if '분석' in text:
            data = await call('query_data', {'sql': 'SELECT sum(amount_won) FROM sales_daily'})
            assert data['rows'] == [[10000]]
        return {'text': '먼저 미분류 구매가 어떤 용도인지 확인해 보세요. 원인은 아직 단정할 수 없습니다.',
                'turn_id': 'turn-' + str(len(self.turns)), 'thread_id': thread_id}

    async def close(self):
        pass


def service(settings):
    svc = Service(settings, Store(settings.state_root), Transport(), Repository(settings), model=object(), clock=lambda: NOW)
    svc.agent = Agent()
    return svc


def test_natural_followup_bypasses_templates_and_reuses_thread(settings):
    svc = service(settings)
    try:
        asyncio.run(conversation(svc, update(1, '10월 분석해줘'), update(2, '그럼 난 뭘 해야 해?')))
        assert len(svc.agent.turns) == 2
        assert svc.agent.opened == [None, 'thread-1']
        assert all('구매 관측액 순' not in text for text in messages(svc))
        sid = svc.store.memory.active(11, 22)['id']
        assert len(svc.store.memory.recent(sid)) == 2
        assert svc.store.get('codex-request-v1-2')['status'] == 'sent'
        assert svc.store.get('codex-analysis-v1-' + sid)['queries']
    finally:
        svc.store.close()


def test_telegram_plain_is_delivered_and_remembered(settings):
    svc = service(settings)
    async def turn(thread_id, text, call, cancelled, progress, context, started):
        started('turn-1')
        return {'text': '10월 매출은 14,000원입니다.\n\n## 10월 분석\n\n**매출**은 `14,000원`입니다.\n\n- [근거](https://example.com/a) 확인',
                'thread_id': thread_id, 'turn_id': 'turn-1'}
    svc.agent.turn = turn
    try:
        asyncio.run(conversation(svc, update(1, '분석해줘')))
        answer = '10월 매출은 14,000원입니다.\n\n# 10월 분석\n- 매출은 14,000원입니다.\n\n- 근거 (https://example.com/a) 확인'
        assert messages(svc) == [answer]
        sid = svc.store.memory.active(11, 22)['id']
        assert svc.store.memory.recent(sid)[0]['answer'] == answer
        assert svc.store.get('codex-request-v1-1')['answer'] == answer
    finally:
        svc.store.close()


def test_telegram_plain_preserves_literals_and_chunk_boundaries():
    assert telegram_plain('**핵심**\n\n```sql\nSELECT 1\n```\n\n매출_10월 2 * 3') == \
        '핵심\n\nSELECT 1\n\n매출_10월 2 * 3'
    text = ('첫 문단 ' * 9) + '\n\n' + ('둘째 문단 🐔' * 8) + '\n\n' + ('셋째 문단 ' * 8)
    parts = list(chunks(text, limit=100))
    assert len(parts) >= 3
    assert ''.join(parts) == text
    assert all(len(part.encode('utf-16-le')) // 2 <= 100 for part in parts)
    assert parts[0].endswith('\n\n')
    formatted = telegram_answer('핵심 결론입니다.\n## 매출 관점\n\n* 매출 10,000원\n\n## 확인할 일\n- 전일 자료 확인')
    assert formatted == '핵심 결론입니다.\n\n# 매출 관점\n- 매출 10,000원\n\n# 확인할 일\n- 전일 자료 확인'


def test_agent_progress_does_not_send_separate_chat_message(settings):
    svc = service(settings)
    async def turn(thread_id, text, call, cancelled, progress, context, started):
        assert progress is None
        started('turn-1')
        return {'text': '자료 기준일을 확인했습니다.\n\n# 자료 상태\n- 10월 11일까지 잠정 자료입니다.'}
    svc.agent.turn = turn
    try:
        asyncio.run(conversation(svc, update(1, '자료 상태 알려줘')))
        assert len(messages(svc)) == 1
        assert '# 자료 상태' in messages(svc)[0]
    finally:
        svc.store.close()


def test_daily_briefing_uses_provisional_data_once_at_nine(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22)
    svc = service(settings)
    svc.clock = lambda: NOW.replace(hour=9, minute=0)
    async def daily_turn(thread_id, text, call, cancelled, progress, context, started):
        assert progress is None and context['briefing']
        assert (await call('data_catalog', {}))['as_of_date'] == '2026-10-11'
        assert (await call('query_data', {'sql': 'SELECT sum(amount_won) FROM sales_daily'}))['rows'] == [[10000]]
        return {'text': '10월 매출은 11일까지 잠정 10,000원입니다.\n\n# 실적 현황\n- 매출: 10,000원 (10/1~11, 잠정)\n\n# AI 판단\n- 이후 자료와 비교 기간 확인이 필요합니다.'}
    svc.agent.turn = daily_turn
    try:
        asyncio.run(svc.monitor_once())
        asyncio.run(svc.monitor_once())
        assert len(messages(svc)) == 1
        assert messages(svc)[0].startswith('10월 매출은')
        assert '# 실적 현황\n- 매출:' in messages(svc)[0]
        assert svc.agent.analysis_modes == [True]
        assert svc.store.get('daily-briefing-v1-22-2026-10-14')['source'] == 'astra'
    finally:
        svc.store.close()


def test_daily_briefing_catches_up_before_noon_and_skips_after(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22)
    svc = service(settings)
    try:
        svc.clock = lambda: NOW.replace(hour=12, minute=0)
        asyncio.run(svc.monitor_once())
        assert not messages(svc)
        svc.clock = lambda: NOW.replace(hour=10, minute=15)
        asyncio.run(svc.monitor_once())
        assert len(messages(svc)) == 1
        svc.clock = lambda: NOW.replace(hour=11, minute=30)
        asyncio.run(svc.monitor_once())
        assert len(messages(svc)) == 1
    finally:
        svc.store.close()


def test_daily_briefing_model_failure_sends_verified_status(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22)
    svc = service(settings)
    svc.clock = lambda: NOW.replace(hour=9, minute=0)
    svc.agent.fail = True
    try:
        asyncio.run(send_daily_briefing(svc))
        asyncio.run(send_daily_briefing(svc))
        assert len(messages(svc)) == 1
        assert '2026-10-11' in messages(svc)[0]
        assert '# 자료 상태' in messages(svc)[0]
        assert svc.store.get('daily-briefing-v1-22-2026-10-14')['source'] == 'verified_status'
    finally:
        svc.store.close()


def test_daily_briefing_without_snapshot_sends_status(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22, snapshot_root='')
    svc = service(settings)
    svc.clock = lambda: NOW.replace(hour=9, minute=0)
    try:
        asyncio.run(send_daily_briefing(svc))
        assert len(messages(svc)) == 1
        assert '수치 분석을 보류' in messages(svc)[0]
        assert '# 자료 상태' in messages(svc)[0]
    finally:
        svc.store.close()


def test_daily_briefing_uncertain_delivery_is_not_resent(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22)
    svc = service(settings)
    svc.clock = lambda: NOW.replace(hour=9, minute=0)
    svc.transport.fail = True
    try:
        assert asyncio.run(send_daily_briefing(svc)) is False
        assert asyncio.run(send_daily_briefing(svc)) is False
        assert len([m for m, _ in svc.transport.calls if m == 'sendMessage']) == 1
        assert svc.store.get('daily-briefing-v1-22-2026-10-14')['status'] == 'undelivered'
    finally:
        svc.store.close()


def test_daily_briefing_needs_recipient(settings):
    with pytest.raises(ValueError, match='수신자'):
        replace(settings, daily_briefing_enabled=True).validate()
    assert due(NOW.replace(hour=9, minute=0), '09:00')
    assert not due(NOW.replace(hour=8, minute=59), '09:00')
    assert not due(NOW.replace(hour=12, minute=0), '09:00')
    with pytest.raises(ValueError, match='정오'):
        replace(settings, daily_briefing_time='12:00').validate()


def test_rotation_preserves_analysis_and_restarts_native_thread(settings):
    svc = service(settings)
    try:
        asyncio.run(conversation(svc, update(1, '10월 분석해줘')))
        svc.clock = lambda: NOW + timedelta(hours=72)
        asyncio.run(conversation(svc, update(2, '그럼 난 뭘 해야 해?')))
        assert svc.agent.opened == [None, None]
        assert svc.agent.turns[-1][2]['previous_analysis']['queries']
        assert svc.agent.turns[-1][2]['inherited_summary']
        notices = [text for text in messages(svc) if '새 대화 세션' in text]
        assert len(notices) == 1
        assert '이전 대화의 요약과 선호' in notices[0]
        assert len(svc.store.memory.recent(svc.store.memory.active(11, 22)['id'])) == 1
    finally:
        svc.store.close()


def test_daily_briefing_uses_only_recipient_business_conversation(settings):
    settings = replace(settings, daily_briefing_enabled=True, recipient=22,
                       allowed_users=(22, 33), allowed_chats=(22, 33))
    svc = service(settings)
    svc.clock = lambda: NOW.replace(hour=9, minute=0)
    for uid, user, chat, question, topic in (
            (1, 22, 22, '앞으로 보고는 매출 변화에 집중해줘', 'data'),
            (2, 22, 22, '10월 매출이 왜 줄었어?', 'data'),
            (3, 22, 22, 'WSL 창 숨겨줘', 'chat'),
            (4, 33, 33, '원재료 가격을 자세히 보고해줘', 'data')):
        svc.store.receive([{'update_id': uid, 'message': {'text': question}}], received_at=NOW)
        svc.store.memory.select(uid, user, chat, NOW)
        asyncio.run(svc.send(f'update-{uid}', chat, '확인했습니다.'))
        assert svc.store.memory.record(uid, user, chat, question, '확인했습니다.', NOW, topic=topic)

    async def daily_turn(thread_id, text, call, cancelled, progress, context, started):
        focus = context['conversation_focus']
        assert focus['preferences'] == ['앞으로 보고는 매출 변화에 집중해줘']
        assert focus['recent_business_questions'] == ['앞으로 보고는 매출 변화에 집중해줘', '10월 매출이 왜 줄었어?']
        assert '원재료 가격' not in str(focus)
        assert 'WSL' not in str(focus)
        assert (await call('query_data', {'sql': 'SELECT sum(amount_won) FROM sales_daily'}))['rows'] == [[10000]]
        return {'text': '매출 변화는 오늘 자료로 확인했습니다.\n\n# 매출 관점\n- 최신 매출: 10,000원'}

    svc.agent.turn = daily_turn
    try:
        assert asyncio.run(send_daily_briefing(svc))
    finally:
        svc.store.close()


def test_failed_delivery_does_not_become_conversation_memory(settings):
    svc = service(settings)
    try:
        svc.transport.fail = True
        asyncio.run(conversation(svc, update(1, '안녕')))
        sid = svc.store.memory.active(11, 22)['id']
        assert not svc.store.memory.recent(sid)
        assert svc.store.get('codex-thread-v3-' + sid)['dirty']
        svc.transport.fail = False
        asyncio.run(conversation(svc, update(2, '다시 해줘')))
        assert svc.agent.opened == [None, None]
        assert 'delivery_notice' in svc.agent.turns[-1][2]
    finally:
        svc.store.close()


def test_model_failure_is_recorded_without_table_fallback(settings):
    svc = service(settings)
    try:
        svc.agent.fail = True
        asyncio.run(conversation(svc, update(1, '매출 분석해줘')))
        assert svc.store.get('codex-request-v1-1')['status'] == 'failed'
        assert '응답을 완료하지 못했습니다' in messages(svc)[0]
        assert '매출 관측액' not in messages(svc)[0]
    finally:
        svc.store.close()


def test_resume_after_service_restart_and_private_recall(settings):
    first = service(settings)
    try:
        asyncio.run(conversation(first, update(1, '10월 분석해줘')))
        sid = first.store.memory.active(11, 22)['id']
        assert first.store.memory.search_agent(11, 22, '10월')
        assert not first.store.memory.search_agent(12, 22, '10월')
        assert not first.store.memory.search_agent(11, 23, '10월')
    finally:
        first.store.close()
    resumed = service(settings)
    try:
        asyncio.run(conversation(resumed, update(2, '그럼 난 뭘 해야 해?')))
        assert resumed.agent.opened == ['thread-1']
        assert resumed.store.memory.active(11, 22)['id'] == sid
    finally:
        resumed.store.close()


def test_protocol_accepts_dynamic_tools_and_only_final_message(settings):
    async def run():
        agent = AppServer(settings)
        responses = []
        async def rpc(method, params):
            assert method == 'turn/start'
            for event in [
                {'id': 90, 'method': 'item/tool/call', 'params': {'threadId': 't', 'turnId': 'u',
                    'tool': 'data_catalog', 'arguments': {}}},
                {'method': 'item/completed', 'params': {'threadId': 't', 'turnId': 'u',
                    'item': {'id': 'a', 'type': 'agentMessage', 'phase': 'commentary', 'text': '확인 중입니다.'}}},
                {'method': 'item/completed', 'params': {'threadId': 't', 'turnId': 'u',
                    'item': {'id': 'b', 'type': 'agentMessage', 'phase': 'final_answer', 'text': '최종 답변'}}},
                {'method': 'turn/completed', 'params': {'threadId': 't', 'turn': {'id': 'u', 'status': 'completed'}}},
            ]:
                await agent.events.put(event)
            return {'turn': {'id': 'u'}}
        async def send(value):
            responses.append(value)
        async def call(name, args):
            assert name == 'data_catalog'
            return {'tables': []}
        agent.rpc, agent.send = rpc, send
        result = await agent.turn('t', '질문', call, threading.Event())
        assert result['text'] == '최종 답변'
        assert responses[0]['result']['contentItems'][0]['type'] == 'inputText'
    asyncio.run(run())


def test_workspace_files_allow_text_edit_without_delete(tmp_path):
    files = WorkspaceFiles(tmp_path)
    assert files.call('write_workspace_file', {'path': 'notes/analysis.md', 'content': '초안',
                                               'expected_sha256': ''})['created']
    read = files.call('read_workspace_file', {'path': 'notes/analysis.md'})
    assert read['content'] == '초안'
    assert files.call('write_workspace_file', {'path': 'notes/analysis.md', 'content': '수정',
                                               'expected_sha256': read['sha256']})['created'] is False
    assert files.call('write_workspace_file', {'path': 'notes/analysis.md', 'content': '덮어쓰기',
                                               'expected_sha256': read['sha256']})['error'] == 'file_changed'
    assert files.call('delete_workspace_file', {'path': 'notes/analysis.md'})['error'] == 'tool_not_allowed'
    assert (tmp_path / 'notes/analysis.md').read_text(encoding='utf-8') == '수정'
    assert files.call('read_workspace_file', {'path': '../outside.md'})['error'] == 'file_unavailable'
    assert files.call('write_workspace_file', {'path': '.env', 'content': 'secret',
                                               'expected_sha256': ''})['error'] == 'file_unavailable'
    assert files.call('write_workspace_file', {'path': 'OneDrive/report.md', 'content': '자료',
                                               'expected_sha256': ''})['error'] == 'file_unavailable'


def test_brain_files_find_nested_text_and_never_write(tmp_path):
    root = tmp_path / 'brain'
    path = root / 'Brand_History' / 'history.md'
    path.parent.mkdir(parents=True)
    path.write_text('도리당은 브랜드의 역사와 운영 원칙을 기록합니다.', encoding='utf-8')
    (root / 'secret.json').write_text('{}', encoding='utf-8')
    (root / 'sales.xlsx').write_bytes(b'not opened')
    brain = BrainFiles(root)
    assert [item['path'] for item in brain.call('list_brain', {})['files']] == [
        'Brand_History/history.md', 'sales.xlsx']
    assert brain.call('search_brain', {'query': '브랜드 역사'})['matches'][0]['path'] == 'Brand_History/history.md'
    assert '운영 원칙' in brain.call('read_brain_file', {'path': 'Brand_History/history.md'})['content']
    assert brain.call('read_brain_file', {'path': '../secret.json'})['error'] == 'brain_unavailable'
    assert brain.call('write_brain_file', {'path': 'Brand_History/history.md', 'content': '변경'})['error'] == 'tool_not_allowed'
    assert path.read_text(encoding='utf-8').startswith('도리당은')


def test_brain_excel_requires_filename_and_masks_sensitive_cells(tmp_path):
    from openpyxl import Workbook
    root = tmp_path / 'brain'
    root.mkdir()
    path = root / 'october.xlsx'
    book = Workbook()
    sheet = book.active
    sheet.title = '거래'
    sheet.append(['품목', '매출', '고객명', '전화번호', '비고'])
    sheet.append(['닭', 12345, '홍길동', '010-1234-5678', 'abc@example.com'])
    book.save(path)
    assert BrainFiles(root).call('inspect_brain_workbook', {'path': 'october.xlsx'})['error'] == \
        'excel_explicit_request_required'
    brain = BrainFiles(root, 'october.xlsx 분석해줘')
    assert brain.call('inspect_brain_workbook', {'path': 'october.xlsx'})['sheets'][0]['name'] == '거래'
    result = brain.call('read_brain_sheet', {'path': 'october.xlsx', 'sheet': '거래'})
    assert result['rows'] == [['닭', '12345', '[민감정보]', '[민감정보]', '[민감정보]']]
    assert brain.call('read_brain_sheet', {'path': '../october.xlsx', 'sheet': '거래'})['error'] == \
        'brain_unavailable'
    assert path.is_file()


def test_new_thread_receives_non_deleting_file_tools(settings):
    async def run():
        agent = AppServer(settings)
        agent.start = lambda: asyncio.sleep(0)
        agent.work = '/tmp/samjeon'
        async def rpc(method, params):
            assert method == 'thread/start'
            names = {tool['name'] for tool in params['dynamicTools']}
            assert {'list_workspace', 'read_workspace_file', 'propose_learning', 'list_learning'} <= names
            assert 'write_workspace_file' not in names
            assert {'list_brain', 'search_brain', 'read_brain_file'} <= names
            assert {'inspect_brain_workbook', 'read_brain_sheet'} <= names
            assert not any('delete' in name for name in names)
            return {'thread': {'id': 'new-thread'}}
        agent.rpc = rpc
        assert await agent.open_thread() == 'new-thread'
    asyncio.run(run())


def test_daily_thread_gets_only_read_only_data_tools(settings):
    async def run():
        agent = AppServer(settings)
        agent.start = lambda: asyncio.sleep(0)
        agent.work = '/tmp/samjeon'
        async def rpc(method, params):
            assert method == 'thread/start'
            assert params['ephemeral'] is True
            assert {tool['name'] for tool in params['dynamicTools']} == {
                'data_catalog', 'query_data', 'get_evidence',
                'search_brain', 'read_brain_file', 'read_assistant_knowledge'}
            return {'thread': {'id': 'daily-thread'}}
        agent.rpc = rpc
        assert await agent.open_thread(analysis_only=True) == 'daily-thread'
    asyncio.run(run())
