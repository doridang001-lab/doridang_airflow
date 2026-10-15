"""실제 v1 구조를 재현한다. 실자료·모델·Telegram에 쓰지 않는 인수 시험."""
import asyncio
from dataclasses import replace
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from test_samjeon_material_service import Transport, conversation, update, messages
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository, SnapshotUnavailable
from modules.transform.pipelines.db.samjeon_material_service.runtime import Service
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.legacy import QUERIES, render
from modules.transform.pipelines.db.samjeon_material_service.grounded import plan_schema, validate_plan
from modules.transform.pipelines.db.samjeon_material_service.conversation import period, route

NOW = datetime(2026, 10, 14, 12, tzinfo=ZoneInfo('Asia/Seoul'))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')


def rehash(path):
    manifest = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
    for name in manifest['files']:
        manifest['files'][name] = hashlib.sha256((path / name).read_bytes()).hexdigest()
    write_json(path / 'manifest.json', manifest)


def fixture(root, count=45):
    path = root / 'snapshots' / 'fixture-v1'
    path.mkdir(parents=True)
    context = {'snapshot_id': path.name, 'as_of_date': '2026-10-11', 'window_start': '2026-08-16',
               'rule_version': 'samjeon-mart-v1', 'status': '잠정', 'forecast': {'must_not_use': 999999}}
    rows = []
    for i in range(count):
        product = i % 3
        rows.append(dict(purchase_date='2026-10-01', source='food_guide', account='fixture',
                         product_key='p' + str(product), product_code=str(product),
                         item_name='양파' if product < 2 else '식사', spec=['1kg', '2kg', '1인'][product],
                         unit='개', amount_won=-50 if i == 0 else 100 * (product + 1),
                         category='현장 직원식대' if product == 2 else '미분류',
                         classification_status='계정확정' if product == 2 else '검수필요',
                         source_file='C:/private/source.xlsx', source_sha256='a' * 64, source_row=i,
                         evidence_id=f'E{i:04}', closed=False, qty='1'))
    lines = pd.DataFrame(rows)
    def aggregate(keys):
        return lines.groupby(keys, as_index=False).agg(amount_won=('amount_won', 'sum'), row_count=('amount_won', 'size'))
    tables = {'purchase_lines': lines,
              'purchase_daily': aggregate(['purchase_date', 'source', 'category']),
              'item_daily': aggregate(['purchase_date', 'source', 'account', 'category', 'product_key', 'item_name', 'spec']),
              'sales_daily': pd.DataFrame([dict(sale_date='2026-10-01', source='sales', amount_won=10000, row_count=3)]),
              'quality_daily': pd.DataFrame([dict(date=str(d.date()), source=s, status='잠정', reason='C:/private/quality', row_count=1)
                                            for d in pd.date_range('2026-08-16', '2026-10-11')
                                            for s in ('sales', 'food_guide', 'logistics_dam')])}
    for name, frame in tables.items():
        frame.to_parquet(path / (name + '.parquet'), index=False)
    write_json(path / 'agent_context.json', context)
    (path / 'review_candidates.csv').write_text('product_key\np0\n', encoding='utf-8-sig')
    manifest = {**context, 'files': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in path.iterdir()}}
    write_json(path / 'manifest.json', manifest)
    write_json(root / 'latest.json', {'snapshot_id': path.name, 'as_of_date': context['as_of_date'],
                                     'manifest': f'snapshots/{path.name}/manifest.json'})
    return path, tables


class Model:
    last_error = None

    def chat(self, *args):
        return {'kind': 'reply', 'text': '안녕하세요.'}

    def explain(self, question, context, cancelled):
        assert context['format'] == 'legacy_v1'
        assert context['supported_queries'] == list(QUERIES)
        assert 'C:/private' not in json.dumps(context)
        return None  # 설명 실패에도 계산값은 반드시 전달한다.

    def plan(self, *args):
        return None


@pytest.fixture
def service(tmp_path):
    root = tmp_path / 'published'
    fixture(root)
    settings = Settings(snapshot_root=str(root), state_root=str(tmp_path / 'state'),
                        enabled=True, ai_enabled=True, ai_verified=True, allowed_users=(11,), allowed_chats=(22,), recipient=22)
    svc = Service(settings, Store(settings.state_root), Transport(), Repository(settings), Model(), clock=lambda: NOW)
    yield svc
    svc.store.close()


def callback(uid, data, chat=22):
    return {'update_id': uid, 'callback_query': {'id': 'callback', 'data': data, 'from': {'id': 11},
                                               'message': {'chat': {'id': chat, 'type': 'private'}}}}


def test_legacy_totals_and_capabilities(service):
    path, context = service.repo.current()
    assert context['format'] == 'legacy_v1' and 'forecast' not in context
    assert context['available_metrics'] == ['sales_won', 'purchase_won']
    result = service.repo.query(path, 'summary', '2026-10-01', '2026-10-11')
    assert result['result']['sales_won'] == 10000
    assert result['result']['purchase_won'] == 8850
    assert result['result']['categories'] == {'미분류': 4350, '현장 직원식대': 4500}
    assert service.repo.period_context(path, context['snapshot_id'], '2026-10-01', '2026-10-11')['format'] == 'legacy_v1'
    assert service.repo.compare_item(path, 'p0', '2026-10-01', '2026-10-11')['format'] == 'legacy_v1'
    assert len(service.repo.search_items(path, '양파')) == 2
    assert len(service.repo.items(path, '양파는 얼마야')) == 2
    assert service.repo.query(path, 'forecast')['status'] == '미지원'
    assert 'C:/private' not in render(result)


def test_empty_sales_is_not_advertised_as_available(service):
    path, _ = service.repo.current()
    sales_path = path / 'sales_daily.parquet'
    pd.read_parquet(sales_path).iloc[:0].to_parquet(sales_path, index=False)
    rehash(path)
    context = service.repo.context(path)
    assert context['available_metrics'] == ['purchase_won']


def test_month_followup_evidence_and_outside_day(service):
    asyncio.run(conversation(service, update(1, '송파삼전점 10월을 분석해줘'),
                             update(2, '어떤 품목을 많이 샀어?'), update(3, '그 구매 근거 보여줘')))
    texts = messages(service)
    assert len(texts) == 3
    assert '2026년 10월 자료는 현재 10월 1일~11일까지 있습니다.' in texts[0]
    assert '10월 전체 실적은 아닙니다.' in texts[0] and '10,000원' in texts[0]
    assert 'AI 설명을 완료하지 못해' in texts[0]
    assert '구매 관측액 순위' in texts[1] and '직전 목록' in texts[2]
    assert 'C:/private' not in ''.join(texts)
    record = service.store.context(22, NOW)
    assert (record['date_from'], record['date_to']) == ('2026-10-01', '2026-10-11')
    scope = service.store.get('legacy-view-v1-' + record['id'])
    assert scope['requested_period'] == ['2026-10-01', '2026-10-31']
    asyncio.run(conversation(service, update(4, '어제 매출은?'), update(5, '10월 전체 확정 실적은?'), update(6, '안녕')))
    assert '2026-10-13' in messages(service)[3] and '2026-10-11' in messages(service)[3]
    assert '월 전체 실적은 아닙니다' in messages(service)[4]
    assert messages(service)[5] == '안녕하세요.'


@pytest.mark.parametrize('count', [21, 101, 501])
def test_pages_do_not_skip_or_repeat(tmp_path, count):
    root = tmp_path / 'published'
    fixture(root, count)
    settings = Settings(snapshot_root=str(root), state_root=str(tmp_path / 'state'), allowed_users=(11,), allowed_chats=(22,))
    svc = Service(settings, Store(settings.state_root), Transport(), Repository(settings), Model(), clock=lambda: NOW)
    try:
        asyncio.run(conversation(svc, update(1, '/evidence')))
        seen = []
        uid = 1
        while True:
            import re
            seen.extend(re.findall(r'근거 (E\d+)', messages(svc)[-1]))
            sent = [p for m, p in svc.transport.calls if m == 'sendMessage'][-1]
            next_button = next((b for row in sent['reply_markup']['inline_keyboard'] for b in row if b['text'] == '다음'), None)
            if not next_button:
                break
            uid += 1
            asyncio.run(conversation(svc, callback(uid, next_button['callback_data'])))
        assert seen == [f'E{i:04}' for i in range(count)]
        path, _ = svc.repo.current()
        assert len(svc.repo.query(path, 'evidence', limit=500)['result']['rows']) == min(500, count)
    finally:
        svc.store.close()


def test_item_selection_preserves_evidence_request(service):
    asyncio.run(conversation(service, update(1, '10월 양파 구매 근거 보여줘')))
    payload = [p for m, p in service.transport.calls if m == 'sendMessage'][-1]
    buttons = payload['reply_markup']['inline_keyboard']
    assert '1kg' in buttons[0][0]['text'] and '2kg' in buttons[1][0]['text']
    asyncio.run(conversation(service, callback(2, buttons[1][0]['callback_data'])))
    assert '구매 근거' in messages(service)[-1] and '2kg' in messages(service)[-1]
    assert '1kg' not in messages(service)[-1] and '비교 기간:' not in messages(service)[-1]
    asyncio.run(conversation(service, update(3, '왜?')))
    assert '2kg' in messages(service)[-1] and '1kg' not in messages(service)[-1]


@pytest.mark.parametrize('text', ['2025년 10월 분석', '12월 분석', '2026-10-13 매출'])
def test_unavailable_periods(service, text):
    asyncio.run(conversation(service, update(1, text)))
    assert '보유 범위' in messages(service)[-1] and '2026-10-11' in messages(service)[-1]


@pytest.mark.parametrize('text', ['/forecast', '/anomalies', '/report', '월말 예측', '고급 이상 탐지'])
def test_unsupported_and_old_pdf_button(service, text):
    asyncio.run(conversation(service, update(1, text)))
    assert '지원하지 않습니다' in messages(service)[-1]
    asyncio.run(conversation(service, update(2, '/summary')))
    record = service.store.context(22, NOW)
    asyncio.run(conversation(service, callback(3, record['id'] + ':pdf')))
    assert '지원하지 않습니다' in messages(service)[-1]
    assert not any(m == 'sendDocument' for m, _ in service.transport.calls)


def test_readiness_and_no_auto_send(service, monkeypatch, tmp_path):
    from modules.transform.pipelines.db.samjeon_material_service.__main__ import doctor, main
    from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter
    monkeypatch.setenv('SAMJEON_TELEGRAM_BOT_TOKEN', 'synthetic')
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda self, **kw: {'ready': True, 'auth': 'chatgpt'})
    monkeypatch.setattr(CodexAdapter, 'available', lambda self: True)
    state = doctor(service.settings)
    assert state['data_query_ready'] and state['conversation_ready']
    assert not state['data_ready'] and not state['ready'] and state['data_format'] == 'legacy_v1'
    from dataclasses import asdict
    config = tmp_path / 'service.json'
    write_json(config, asdict(service.settings))
    assert main(['doctor', '--config', str(config), '--require-data-query-ready'])['_exit_code'] == 0
    assert main(['doctor', '--config', str(config), '--require-ready'])['_exit_code'] == 2
    service.settings = replace(service.settings, auto_send=True)
    asyncio.run(service.monitor_once())
    assert not service.transport.calls
    asyncio.run(conversation(service, update(1, '/help'), update(2, '/status')))
    assert all('전체 운영 준비: 미완료' in s for s in messages(service))


@pytest.mark.parametrize('damage', ['hash', 'identity', 'date', 'columns', 'null', 'fraction', 'duplicate', 'aggregate', 'escape', 'unknown', 'version_conflict'])
def test_corruption_is_blocked(tmp_path, damage):
    root = tmp_path / 'published'
    path, _ = fixture(root)
    manifest = json.loads((path / 'manifest.json').read_text(encoding='utf-8'))
    if damage in ('identity', 'date'):
        context = json.loads((path / 'agent_context.json').read_text(encoding='utf-8'))
        context['snapshot_id' if damage == 'identity' else 'as_of_date'] = 'other' if damage == 'identity' else '2026-10-12'
        write_json(path / 'agent_context.json', context)
        rehash(path)
    elif damage in ('columns', 'null', 'fraction', 'duplicate', 'aggregate'):
        file = path / ('purchase_daily.parquet' if damage == 'aggregate' else 'purchase_lines.parquet')
        frame = pd.read_parquet(file)
        if damage == 'columns':
            frame = frame.drop(columns=['category'])
        elif damage == 'duplicate':
            frame.loc[1, 'evidence_id'] = frame.loc[0, 'evidence_id']
        else:
            frame['amount_won'] = frame.amount_won.astype(float)
            frame.loc[0, 'amount_won'] = {'null': None, 'fraction': 1.5, 'aggregate': 999}[damage]
        frame.to_parquet(file, index=False)
        rehash(path)
    elif damage == 'hash':
        (path / 'agent_context.json').write_text('{}', encoding='utf-8')
    elif damage == 'escape':
        manifest['files']['../secret'] = 'a' * 64
        write_json(path / 'manifest.json', manifest)
    elif damage == 'unknown':
        manifest['rule_version'] = 'unrecognized'
        write_json(path / 'manifest.json', manifest)
    else:
        pointer = json.loads((root / 'latest.json').read_text(encoding='utf-8'))
        pointer['schema_version'] = 3
        write_json(root / 'latest.json', pointer)
    with pytest.raises(SnapshotUnavailable) as caught:
        Repository(Settings(snapshot_root=str(root))).current()
    assert caught.value.code == ('unsupported_format' if damage == 'unknown' else 'snapshot_invalid')
    assert 'secret' not in str(caught.value)


def test_month_parser_and_plan_capabilities():
    assert period('2025년 10월 분석', '2026-10-14') == ('2025-10-01', '2025-10-31')
    assert period('10월 분석', '2026-10-14') == ('2026-10-01', '2026-10-31')
    assert route('10월 전체 확정 실적은?') == 'data'
    assert route('10월') == 'clarify'
    for text in ('13월 분석', '110월 분석', '25년 10월 분석', '10월과 11월 분석', '10월 어제 매출', '2026-10-01 어제 매출'):
        with pytest.raises(ValueError):
            period(text, '2026-10-14')
    schema = plan_schema(QUERIES)
    assert 'pdf' not in schema['properties']['action']['enum']
    assert schema['properties']['queries']['items']['properties']['intent']['enum'] == list(QUERIES)
    with pytest.raises(ValueError):
        validate_plan({'action': 'query', 'queries': [{'intent': 'forecast', 'date_from': None, 'date_to': None, 'product_name': None}], 'clarification': None}, QUERIES)


def test_exact_length_comparison_and_independent_ranking(tmp_path):
    path, tables = fixture(tmp_path / 'published', 6)
    lines = tables['purchase_lines']
    lines.loc[:2, 'purchase_date'] = '2026-09-20'
    lines['amount_won'] = [1000, 10, 100, 1100, 900, -50]
    lines.to_parquet(path / 'purchase_lines.parquet', index=False)
    for table, keys in [('purchase_daily', ['purchase_date', 'source', 'category']),
                        ('item_daily', ['purchase_date', 'source', 'account', 'category', 'product_key', 'item_name', 'spec'])]:
        lines.groupby(keys, as_index=False).agg(amount_won=('amount_won', 'sum'), row_count=('amount_won', 'size')).to_parquet(path / (table + '.parquet'), index=False)
    rehash(path)
    repo = Repository(Settings(snapshot=str(path)))
    compared = repo.query(path, 'compare', '2026-10-01', '2026-10-11')['result']
    assert compared['previous_from'] == '2026-09-20' and compared['previous_to'] == '2026-09-30'
    assert compared['current_won'] == 1950 and compared['previous_won'] == 1110 and compared['difference_won'] == 840
    assert repo.query(path, 'items', '2026-10-01', '2026-10-11')['result']['items'][0]['product_key'] == 'p0'
    assert repo.compare_item(path, 'p1', '2026-10-01', '2026-10-11')['result']['difference_won'] == 890
    no_rows = render(repo.query(path, 'summary', '2026-10-02', '2026-10-03'))
    assert '실제 0원 확정 아님' in no_rows


def test_original_receipt_date_survives_queue(service):
    message = update(1, '10월 매출 분석')
    message['message']['date'] = datetime(2025, 10, 14, tzinfo=ZoneInfo('Asia/Seoul')).timestamp()
    asyncio.run(conversation(service, message))
    assert '2025-10-01~2025-10-31' in messages(service)[-1]


def test_legacy_buttons_pin_snapshot_and_validate_current(service):
    asyncio.run(conversation(service, update(1, '/evidence')))
    aid = service.store.context(22, NOW)['id']
    asyncio.run(conversation(service, callback(2, aid + ':e999')))
    assert '확인하지 못했습니다' in messages(service)[-1]
    service.settings = replace(service.settings, allowed_chats=(22, 44))
    asyncio.run(conversation(service, callback(3, aid + ':e20', chat=44)))
    assert '확인하지 못했습니다' in messages(service)[-1]
    path, _ = service.repo.current()
    (path / 'agent_context.json').write_text('{}', encoding='utf-8')
    asyncio.run(conversation(service, callback(4, aid + ':e20')))
    assert messages(service)[-1] == str(SnapshotUnavailable(code='snapshot_invalid'))


def test_model_exception_preserves_calculated_answer(service):
    def fail(*args):
        raise RuntimeError('C:/private/raw-error')
    service.model.explain = fail
    asyncio.run(conversation(service, update(1, '10월 분석')))
    assert '10,000원' in messages(service)[-1] and 'C:/private' not in messages(service)[-1]
    state = json.loads((service.store.root / 'health.json').read_text(encoding='utf-8'))
    assert state['data_query_ready'] and not state['data_ready'] and not state['full_data_ready']


@pytest.mark.parametrize('kind', ['pdf', 'invented_product'])
def test_legacy_plan_cannot_enable_unsupported_function(service, kind):
    def plan(question, context, cancelled):
        assert context['format'] == 'legacy_v1' and context['supported_queries'] == list(QUERIES)
        return ({'action': 'pdf', 'queries': [], 'clarification': None} if kind == 'pdf' else
                {'action': 'query', 'queries': [{'intent': 'items', 'date_from': None, 'date_to': None, 'product_name': '양파'}], 'clarification': None})
    service.model.plan = plan
    asyncio.run(conversation(service, update(1, '삼전점 알려줘')))
    assert not any(m == 'sendDocument' for m, _ in service.transport.calls)
    assert ('조회할 기간' if kind == 'pdf' else '직접 알려주세요') in messages(service)[-1]


def test_cancel_and_duplicate_legacy_analysis(service):
    import threading
    entered, release = threading.Event(), threading.Event()
    def explain(question, context, cancelled):
        entered.set()
        release.wait(5)
        return None
    service.model.explain = explain
    async def exercise():
        task = asyncio.create_task(conversation(service, update(1, '10월 분석')))
        assert await asyncio.to_thread(entered.wait, 5)
        await service.ingest([update(2, '/cancel')])
        release.set()
        await task
        await conversation(service, update(1, '10월 분석'))
    asyncio.run(exercise())
    assert all('10,000원' not in text for text in messages(service))
    assert not service.store.get('turns-v1-22', [])


def test_dynamic_plan_schema_is_carried_to_wsl(monkeypatch):
    import threading
    from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter
    from modules.transform.pipelines.db.samjeon_material_service.wsl_transport import WslTransport
    monkeypatch.setattr('shutil.which', lambda _: 'wsl.exe')
    monkeypatch.setattr(WslTransport, 'diagnose', lambda self: {'ready': True, 'reason': None})
    def invoke(self, schema, instruction, question, context, cancelled):
        assert schema == plan_schema(QUERIES)
        return {'answer': {'action': 'query', 'queries': [{'intent': 'items', 'date_from': None, 'date_to': None, 'product_name': None}], 'clarification': None}}
    monkeypatch.setattr(WslTransport, 'invoke', invoke)
    adapter = CodexAdapter(Settings(codex_backend='wsl', wsl_distribution='UbuntuCodex', wsl_user='test',
                                    wsl_codex_executable='/opt/codex', ai_enabled=True, ai_verified=True))
    answer = adapter.plan('구매 품목', {'format': 'legacy_v1', 'supported_queries': list(QUERIES)}, threading.Event())
    assert answer['queries'][0]['intent'] == 'items'
