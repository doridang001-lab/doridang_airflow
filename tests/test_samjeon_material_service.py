"""대화 흐름, 단일 상태 소유권, 알림 중복과 실패의 인수 시험."""
import asyncio
import copy
from dataclasses import replace
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from zoneinfo import ZoneInfo

import pytest

from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.runtime import Service
from modules.transform.pipelines.db.samjeon_material_service.telegram import DeliveryError
from modules.transform.pipelines.db.samjeon_material_service.conversation import parse, validate_query
from modules.transform.pipelines.db.samjeon_material_service.presentation import chunks, alert_items, render_query, summary
from modules.transform.pipelines.db.samjeon_material_service.pipeline import enqueue, valid_job
from test_samjeon_material_mart_v3 import analytical_tables
from modules.transform.pipelines.db.samjeon_material_agent.analytics import build_context

NOW = datetime(2026, 10, 12, 9, 30, tzinfo=ZoneInfo('Asia/Seoul'))


def context():
    plan, tables = analytical_tables(sales_won=1000000, purchase_won=500000)
    return build_context(plan, tables, 'test', {'target_start_date': '2026-10-01', 'amount_basis_confirmed': True})


class Repo:
    def __init__(self):
        self.data = context()
        self.queries = []

    def current(self):
        return Path('/synthetic-snapshot'), copy.deepcopy(self.data)

    def context(self, path, expected=None):
        if expected and expected != self.data['snapshot_id']:
            raise ValueError('판본 변경')
        return copy.deepcopy(self.data)

    def query(self, path, intent, start=None, end=None, **filters):
        self.queries.append((intent, start, end, filters))
        values = {'summary': self.data['month_to_date'], 'staff_meal': self.data['month_to_date'],
                  'compare': {'seven_day': self.data['seven_day'], 'staff_meal_seven_day': self.data['staff_meal_seven_day'], 'top_increase_items': []},
                  'anomalies': self.data['alerts'], 'forecast': self.data['forecast'],
                  'status': {'period': self.data['month_to_date'], 'quality': []},
                  'evidence': {'rows': [], 'offset': 0, 'total': 0, 'next_offset': None}}
        return {'intent': intent, 'date_from': start, 'date_to': end, 'status': '정상', 'result': copy.deepcopy(values[intent])}


class Transport:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def call(self, method, payload):
        self.calls.append((method, payload))
        if self.fail:
            raise DeliveryError(True)
        return {'message_id': len(self.calls)}


class Model:
    def interpret(self, *args):
        return None


def update(uid, text, user=11, chat=22):
    return {'update_id': uid, 'message': {'from': {'id': user}, 'chat': {'id': chat, 'type': 'private'}, 'text': text}}


@pytest.fixture
def service(tmp_path):
    settings = Settings(state_root=str(tmp_path), allowed_users=(11,), allowed_chats=(22,), recipient=22, auto_send=True)
    store, transport, repo = Store(tmp_path), Transport(), Repo()
    svc = Service(settings, store, transport, repo, Model(), clock=lambda: NOW)
    yield svc
    store.close()


async def conversation(svc, *updates):
    for u in updates:
        await svc.ingest([u])
        while pending := svc.store.next_update():
            await svc.handle(pending)


def messages(svc):
    return [p['text'] for m, p in svc.transport.calls if m == 'sendMessage']


def test_conversation_explains_then_items_then_note(service):
    asyncio.run(conversation(service, update(1, '지금 뭐가 의심돼?'), update(2, '어떤 품목이야?'),
                             update(3, '행사 때문에 미리 샀어'), update(4, '왜?')))
    texts = messages(service)
    assert len(texts) == 4
    assert '목표' in texts[0]
    assert service.repo.queries[0][0] == 'compare'
    assert '대표님 설명: 행사 때문에 미리 샀어' in texts[-1]
    assert len(service.store.db.execute('SELECT * FROM notes').fetchall()) == 1


def test_duplicate_update_and_unauthorized_user(service):
    asyncio.run(conversation(service, update(1, '/summary'), update(1, '/summary'), update(2, '/summary', user=99)))
    assert len(messages(service)) == 1
    assert service.store.get('offset') == 3
    assert service.store.db.execute('SELECT status FROM updates WHERE id=2').fetchone()[0] == 'unauthorized'


def test_callback_cannot_access_another_chat(service):
    asyncio.run(conversation(service, update(1, '/summary')))
    aid = service.store.context(22, NOW)['id']
    service.settings = replace(service.settings, allowed_users=(11, 33), allowed_chats=(22, 44))
    callback = {'update_id': 2, 'callback_query': {'id': 'cb', 'from': {'id': 33}, 'data': aid + ':pdf',
                'message': {'chat': {'id': 44, 'type': 'private'}}}}
    asyncio.run(conversation(service, callback))
    assert not any(method == 'sendDocument' for method, _ in service.transport.calls)
    assert service.store.db.execute('SELECT status FROM updates WHERE id=2').fetchone()[0] == 'failed'


def test_context_expires_and_callback_remains_pinned(service):
    asyncio.run(conversation(service, update(1, '/summary')))
    aid = service.store.context(22, NOW)['id']
    assert service.store.context(22, NOW + timedelta(hours=25)) is None
    callback = {'update_id': 2, 'callback_query': {'id': 'cb', 'from': {'id': 11}, 'data': aid + ':meal',
                'message': {'chat': {'id': 22, 'type': 'private'}}}}
    asyncio.run(conversation(service, callback))
    assert service.repo.queries[-1][0] == 'staff_meal'


def test_cancel_is_received_while_query_runs(service):
    started, release = threading.Event(), threading.Event()
    original = service.repo.query
    def slow(*args, **kwargs):
        started.set()
        release.wait(5)
        return original(*args, **kwargs)
    service.repo.query = slow
    async def scenario():
        u = update(1, '/summary')
        await service.ingest([u])
        task = asyncio.create_task(service.handle(u))
        while not started.is_set():
            await asyncio.sleep(.01)
        await service.ingest([update(2, '/cancel')])
        release.set()
        await task
    asyncio.run(scenario())
    assert len(messages(service)) == 1 and '취소' in messages(service)[0]
    assert service.store.db.execute('SELECT status FROM updates WHERE id=1').fetchone()[0] == 'cancelled'


def test_uncertain_delivery_is_never_resent_after_restart(tmp_path):
    settings = Settings(state_root=str(tmp_path))
    store, transport = Store(tmp_path), Transport(fail=True)
    service = Service(settings, store, transport, Repo(), Model())
    assert asyncio.run(service.send('x', 22, '첫 메시지')) is False
    store.close()
    store = Store(tmp_path)
    service = Service(settings, store, transport, Repo(), Model())
    assert asyncio.run(service.send('x', 22, '첫 메시지')) is False
    assert len(transport.calls) == 1
    assert store.outgoing('x')[0]['state'] == 'uncertain'
    store.close()


def test_morning_only_once_and_new_snapshot_same_alert_not_repeated(service):
    async def scenario():
        await service.monitor_once()
        await service.monitor_once()
        service.repo.data['snapshot_id'] = 'another-version'
        await service.monitor_once()
    asyncio.run(scenario())
    assert len(messages(service)) == 1


def test_new_anomaly_and_night_deferred(service):
    asyncio.run(service.monitor_once())
    service.repo.data['alerts'].append({'type': 'weekday_purchase_increase', 'baseline_won': 100000,
        'increase_won': 200000, 'sample_count': 4})
    service.repo.data['snapshot_id'] = 'new'
    service.clock = lambda: NOW.replace(hour=22)
    asyncio.run(service.monitor_once())
    assert len(messages(service)) == 1
    service.clock = lambda: NOW.replace(hour=16)
    asyncio.run(service.monitor_once())
    assert len(messages(service)) == 2 and '같은 요일' in messages(service)[-1]


def test_missing_data_wait_once_and_final_once_when_ready(service):
    normal = copy.deepcopy(service.repo.data)
    service.repo.data['month_to_date']['complete'] = False
    service.repo.data['alerts'] = []
    async def scenario():
        await service.monitor_once()
        await service.monitor_once()
        service.repo.data = normal
        service.repo.data['snapshot_id'] = 'ready'
        await service.monitor_once()
    asyncio.run(scenario())
    assert len(messages(service)) == 2
    assert '아직' in messages(service)[0]


@pytest.mark.parametrize('slot,allowed', [('09:00', False), ('09:30', True), ('12:00', True), ('12:30', False)])
def test_queue_slots_and_dry_run(tmp_path, slot, allowed):
    hour, minute = map(int, slot.split(':'))
    instant = NOW.replace(hour=hour, minute=minute)
    root = tmp_path / 'not-created'
    result = enqueue(root, instant, dry_run=True)
    assert not root.exists()
    assert bool(result.get('dry_run')) == allowed
    assert enqueue(root, instant, manual=True)['queued'] is False
    if allowed:
        result = enqueue(root, instant)
        job = json.loads(Path(result['job_path']).read_text(encoding='utf-8'))
        assert valid_job(job, instant)
        assert not valid_job(job, instant + timedelta(days=1))


def test_query_validation_and_followup_dates():
    assert parse('지난주 매출', '2026-10-11')['query']['date_from'] == '2026-09-28'
    assert parse('/evidence 2026-10-01 2026-10-11 limit=5 source=food_guide', '2026-10-11')['query']['limit'] == 5
    with pytest.raises(ValueError):
        validate_query({'intent': 'summary', 'shell': 'dir'})
    with pytest.raises(ValueError):
        validate_query({'intent': 'evidence', 'limit': True})


def test_korean_emoji_message_splitting():
    text = '대표님🐔' * 2000
    parts = list(chunks(text))
    assert ''.join(parts) == text
    assert all(len(p.encode('utf-16-le')) // 2 <= 3500 for p in parts)


def test_unclassified_is_not_reported_as_zero_purchase():
    period = context()['month_to_date']
    period.update(unclassified_row_count=3, unclassified_won=123000, observed_purchase_won=0,
                  purchase_rate_pct=None, complete=False, target_comparison_status='보류')
    text = summary(period)
    assert '전체 확정 매입액은 보류' in text and '123,000원' in text


def test_schema_future_version_is_not_reset(tmp_path):
    import sqlite3
    db = sqlite3.connect(tmp_path / 'state.sqlite3')
    db.execute('PRAGMA user_version=99')
    db.close()
    with pytest.raises(ValueError):
        Store(tmp_path)


def test_doctor_from_fresh_process_creates_no_files(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    for name in ('LOCAL_DB', 'TEMP_DIR', 'DOWN_DIR', 'ANALYTICS_DB', 'MART_DB', 'REPORT_SALES_DB'):
        env[name] = str(tmp_path / name)
    result = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-m',
         'modules.transform.pipelines.db.samjeon_material_service', 'doctor'],
         cwd=root, env=env, capture_output=True, text=True, encoding='utf-8')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['files_written'] is False
    assert list(tmp_path.iterdir()) == []


def test_mart_hash_excludes_service_code(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_agent import mart
    core = tmp_path / 'core'
    core.mkdir()
    (core / 'mart.py').write_text('value = 1', encoding='utf-8')
    sibling = tmp_path / 'service'
    sibling.mkdir()
    monkeypatch.setattr(mart, '__file__', str(core / 'mart.py'))
    first = mart._implementation_hash()
    (sibling / 'telegram.py').write_text('value = 2', encoding='utf-8')
    assert mart._implementation_hash() == first
    (core / 'mart.py').write_text('value = 3', encoding='utf-8')
    assert mart._implementation_hash() != first


def test_item_selection_and_followup_keeps_product(service):
    service.repo.items = lambda *args: [{'product_key': 'a', 'item_name': '닭', 'spec': '1kg'},
                                       {'product_key': 'b', 'item_name': '닭', 'spec': '2kg'}]
    asyncio.run(conversation(service, update(1, '닭이 얼마나 늘었어?')))
    aid = service.store.context(22, NOW)['id']
    cb = {'update_id': 2, 'callback_query': {'id': 'cb', 'from': {'id': 11}, 'data': aid + ':pick1',
          'message': {'chat': {'id': 22, 'type': 'private'}}}}
    asyncio.run(conversation(service, cb, update(3, '왜?')))
    assert service.repo.queries[-1][3]['product_key'] == 'b'
    assert '여러 품목' in messages(service)[0]


def test_note_survives_snapshot_change_but_not_product_change(service):
    asyncio.run(conversation(service, update(1, '/summary'), update(2, '행사 때문에 미리 샀어')))
    original = service.store.context(22, NOW)
    service.repo.data['snapshot_id'] = 'revised'
    asyncio.run(conversation(service, update(3, '지금 뭐가 의심돼?')))
    current = service.store.context(22, NOW)
    assert current['id'] != original['id']
    assert '행사 때문에' in messages(service)[-1]
    service.store.focus(current['id'], 22, 'different-item')
    assert service.store.notes(22, current['id']) == []


def test_item_comparison_uses_core_and_excludes_staff_meals(tmp_path):
    from test_samjeon_material_mart import _fixture
    from test_samjeon_material_mart_v3 import build
    from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
    plan, _ = _fixture(tmp_path)
    path = build(plan)
    repo = Repository(Settings(snapshot=str(path)))
    selected = repo.items(path, '쌀이 얼마나 늘었어?')
    assert len(selected) == 1
    result = repo.query(path, 'compare', '2026-10-01', '2026-10-11', product_key=selected[0]['product_key'])
    assert result['result']['seven_day']['current_observed_purchase_won'] == 30000
    assert result['result']['seven_day']['complete'] is False


def test_malformed_and_expired_job_does_not_send(service):
    queue = service.store.root / 'queue'
    queue.mkdir()
    (queue / 'bad.json').write_text('{', encoding='utf-8')
    enqueue(service.store.root, NOW - timedelta(days=1))
    asyncio.run(service.accept_jobs())
    assert messages(service) == []
    assert len(list((queue / 'expired').glob('*.json'))) == 2


def test_restart_keeps_done_updates_and_marks_inflight(tmp_path):
    store = Store(tmp_path)
    store.receive([update(1, '/summary'), update(2, '/compare')])
    store.update_status(1, 'done')
    store.update_status(2, 'processing')
    store.enqueue('x:0000', 22, {'text': '메시지'})
    store.sent_state('x:0000', 'sending')
    store.close()
    store = Store(tmp_path)
    assert store.next_update() is None
    assert store.get('offset') == 3
    assert store.outgoing('x')[0]['state'] == 'uncertain'
    store.close()


def test_partial_message_failure_preserves_first_part_and_never_retries(service):
    class Partial(Transport):
        def call(self, method, payload):
            self.calls.append((method, payload))
            if len(self.calls) == 2:
                raise DeliveryError(True)
            return {'message_id': len(self.calls)}
    service.transport = Partial()
    text = '가' * 8000
    assert asyncio.run(service.send('long', 22, text)) is False
    assert asyncio.run(service.send('long', 22, text)) is False
    rows = service.store.outgoing('long')
    assert [r['state'] for r in rows] == ['sent', 'uncertain', 'pending']
    assert rows[0]['message_id'] == 1
    assert len(service.transport.calls) == 2
