"""CEO 모드의 지연 조회·검증·기억·기존 연결 격리를 검사한다."""
import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import threading

import pytest

from modules.transform.pipelines.db.samjeon_material_service.ceo_tools import CeoTools
from modules.transform.pipelines.db.samjeon_material_service.ceo_memory import command, relevant
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
from modules.transform.pipelines.db.samjeon_material_service.runtime import Service
from modules.transform.pipelines.db.samjeon_material_service.appserver import AppServer
from modules.transform.pipelines.db.samjeon_material_service.__main__ import doctor
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter


def publish(root, manifest=True):
    dataset = root / '삼전점'
    for folder in ('00_요약', '01_홀매출/메뉴', '02_배달매출/메뉴'):
        for month in ('09', '10'):
            path = dataset / folder / f'2026_{month}.csv'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('날짜,메뉴코드,매출원,자료상태\n'
                            f'2026-{month}-01,001,100,정상\n2026-{month}-02,002,200,잠정\n', encoding='utf-8-sig')
    (root / 'AGENTS.md').write_text('# 공통\n매입은 사용원가가 아니다.', encoding='utf-8')
    (dataset / 'AGENTS.md').write_text('# 매출\n매출원은 원 단위.\n# 품질\n잠정 상태를 표시한다.', encoding='utf-8')
    if manifest:
        data = {'schema_version': 1, 'files': {p.relative_to(dataset).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                              for p in dataset.rglob('*.csv')}}
        (dataset / 'manifest.json').write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    return dataset


@pytest.fixture
def settings(tmp_path):
    root = tmp_path / 'data'
    publish(root)
    return Settings(ceo_data_root=str(root), state_root=str(tmp_path / 'state'),
                    conversation_engine='appserver', codex_backend='wsl', wsl_distribution='Ubuntu',
                    wsl_user='ceo', wsl_codex_executable='/usr/local/bin/codex',
                    enabled=True, ai_enabled=True, ai_verified=True, allowed_users=(11,), allowed_chats=(22,))


def prepare(tool, tables=None, start='2026-10-01', end='2026-10-02'):
    return tool.call('prepare_data', {'dataset': '삼전점', 'tables': tables or ['00_요약'], 'date_from': start, 'date_to': end})


def test_catalog_and_guides_never_read_csv(settings, monkeypatch):
    original = Path.read_bytes
    def guarded(path):
        assert path.suffix != '.csv'
        return original(path)
    monkeypatch.setattr(Path, 'read_bytes', guarded)
    tool = CeoTools(settings)
    assert tool.call('data_catalog', {})['datasets'] == ['삼전점']
    assert len(tool.call('data_catalog', {'dataset': '삼전점'})['tables']) == 3
    assert tool.call('read_guide', {'dataset': '삼전점', 'query': '매출'})['sections']
    assert not Path(settings.state_root).exists()


def test_only_selected_months_and_tables_are_read(settings, monkeypatch):
    original = Path.read_bytes
    seen = []
    def track(path):
        if path.suffix == '.csv':
            seen.append(path.relative_to(Path(settings.ceo_data_root)).as_posix())
        return original(path)
    monkeypatch.setattr(Path, 'read_bytes', track)
    tool = CeoTools(settings)
    result = prepare(tool)
    assert 'error' not in result
    assert set(seen) == {'삼전점/00_요약/2026_10.csv'}
    table = result['prepared'][0]['sql_table']
    answer = tool.call('query_data', {'sql': f'SELECT SUM(매출원), MIN(메뉴코드) FROM {table}'})
    assert answer['rows'] == [[300, '001']]
    assert tool.call('query_data', {'sql': f'SELECT SUM(매출원), MIN(메뉴코드) FROM {table}'}) == answer
    assert tool.cache_hits == 1
    prepare(tool, ['01_홀매출/메뉴'])
    assert not any('02_배달매출' in x for x in seen)


def test_compare_period_filter_and_cache(settings):
    tool = CeoTools(settings)
    result = prepare(tool, start='2026-09-02', end='2026-10-01')
    alias = result['prepared'][0]['sql_table']
    assert tool.query(f'SELECT 날짜, 매출원 FROM {alias} ORDER BY 날짜')['rows'] == [['2026-09-02', 200], ['2026-10-01', 100]]
    other = CeoTools(settings)
    assert prepare(other, start='2026-09-02', end='2026-10-01')['snapshot_id'] == result['snapshot_id']
    assert other.cache_hits == 1


@pytest.mark.parametrize('sql', ['DELETE FROM {t}', 'DROP TABLE {t}', "ATTACH ':memory:' AS bad", 'PRAGMA database_list',
                               'SELECT * FROM sqlite_master', 'SELECT * FROM d0.data', "SELECT load_extension('bad')",
                               'SELECT 1; SELECT 2', "SELECT * FROM pragma_table_info('data')"])
def test_read_only_sql(settings, sql):
    tool = CeoTools(settings)
    alias = prepare(tool)['prepared'][0]['sql_table']
    assert 'error' in tool.call('query_data', {'sql': sql.format(t=alias)})
    assert tool.query(f'SELECT sum(매출원) FROM {alias}')['rows'] == [[300]]


def test_result_cap_does_not_truncate_aggregation(settings):
    tool = CeoTools(settings)
    prepare(tool)
    sql = 'WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<150) SELECT x FROM n'
    assert len(tool.query(sql)['rows']) == 20
    assert tool.query(sql)['truncated']
    assert len(tool.query(sql, detail=True)['rows']) == 100
    assert tool.query(sql.replace('SELECT x FROM n', 'SELECT SUM(x) FROM n'))['rows'] == [[11325]]


def test_manifest_failure_and_unrelated_corruption(settings):
    root = Path(settings.ceo_data_root) / '삼전점'
    (root / '02_배달매출/메뉴/2026_10.csv').write_text('broken', encoding='utf-8')
    tool = CeoTools(settings)
    assert 'error' not in prepare(tool)
    assert prepare(tool, ['02_배달매출/메뉴'])['error'] == 'file_hash_mismatch'
    assert prepare(tool, start='2027-01-01', end='2027-01-02')['error'] == 'period_unavailable'


def test_generic_new_folder_and_guide_change(settings):
    root = Path(settings.ceo_data_root)
    new = root / '새매장'
    new.mkdir()
    (new / '2026_10.csv').write_text('날짜,매출원\n2026-10-01,900\n', encoding='utf-8-sig')
    assert '새매장' in CeoTools(settings).catalog()['datasets']
    tool = CeoTools(settings)
    tool.guide('삼전점', '매출')
    first = prepare(tool)['snapshot_id']
    (root / '삼전점/AGENTS.md').write_text('# 매출\n새 정의', encoding='utf-8')
    other = CeoTools(settings)
    other.guide('삼전점', '매출')
    assert prepare(other)['snapshot_id'] != first
    result = other.prepare('새매장', ['.'], '2026-10-01', '2026-10-01')
    assert result['prepared'][0]['verification'] == '원천 검증 정보 없음'


def test_schema_change_cancel_and_escape(settings):
    tool = CeoTools(settings)
    assert 'error' in tool.call('data_catalog', {'dataset': '../escape'})
    tool.cancelled.set()
    assert prepare(tool)['error'] == 'cancelled'
    root = Path(settings.ceo_data_root) / '삼전점'
    (root / 'manifest.json').unlink()
    (root / '00_요약/2026_09.csv').write_text('날짜,다른열\n2026-09-01,x\n', encoding='utf-8')
    assert prepare(CeoTools(settings), start='2026-09-01')['error'] == 'columns_changed'


def test_memory_explicit_updates_scoped_and_persistent(settings):
    store = Store(settings.state_root)
    try:
        assert command(store, 11, 22, '매출이 줄었네', 1) is None
        assert '기억했습니다' in command(store, 11, 22, '/기억 매출비교=동일 기간으로 비교', 2)
        assert relevant(store, 11, 22, '매출비교 해줘')[0]['text'] == '동일 기간으로 비교'
        assert relevant(store, 12, 22, '매출비교 해줘') == []
        assert '기억했습니다' in command(store, 11, 22, '기억해: 답변은 짧게 세 줄로 해줘', 5)
        assert command(store, 11, 22, '아까 말한 기준을 기억해', 6).startswith('기억할 기준')
    finally:
        store.close()
    store = Store(settings.state_root)
    try:
        assert '동일 기간' in command(store, 11, 22, '/기억목록', 3)
        command(store, 11, 22, '/기억삭제 매출비교', 4)
        assert not any(r['name'] == '매출비교' for r in relevant(store, 11, 22, '매출비교 해줘'))
    finally:
        store.close()


def test_doctor_help_idle_no_repository_or_inference(settings, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('대기 중 자료 읽기 또는 모델 추론 발생')
    monkeypatch.setattr(Repository, 'current', forbidden)
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda self: {'ready': True})
    assert doctor(settings)['data_status'] == '미조회'
    store = Store(settings.state_root)
    service = Service(settings, store, object())
    try:
        assert '요약' in asyncio.run(service.help())
        asyncio.run(service.monitor_once())
        service.write_health()
        assert not (Path(settings.state_root) / 'analysis-cache').exists()
    finally:
        store.close()


def test_existing_bot_config_keeps_workspace_and_brain_tools(settings, monkeypatch):
    settings.validate()
    with pytest.raises(ValueError):
        replace(settings, auto_send=True).validate()
    with pytest.raises(ValueError):
        replace(settings, state_root=settings.ceo_data_root).validate()
    agent = AppServer(settings)
    seen = []
    async def start():
        pass
    async def rpc(method, value):
        seen.append(value)
        return {'thread': {'id': 'test'}}
    monkeypatch.setattr(agent, 'start', start)
    monkeypatch.setattr(agent, 'rpc', rpc)
    asyncio.run(agent.open_thread())
    names = {x['name'] for x in seen[0]['dynamicTools']}
    assert 'prepare_data' in names
    assert 'write_workspace_file' not in names
    assert {'propose_learning', 'list_learning'} <= names
    assert 'search_brain' in names
    assert len(names) == len(seen[0]['dynamicTools'])


def test_complete_conversation_rotates_and_remembers(settings, monkeypatch):
    from test_samjeon_appserver import service
    from test_samjeon_material_service import conversation, update, messages
    svc = service(settings)
    contexts = []
    async def turn(thread_id, text, call, cancelled, progress, context, started):
        contexts.append(context)
        started('t1')
        assert (await call('data_catalog', {}))['datasets'] == ['삼전점']
        result = await call('prepare_data', {'dataset': '삼전점', 'tables': ['00_요약'],
                                             'date_from': '2026-10-01', 'date_to': '2026-10-02'})
        alias = result['prepared'][0]['sql_table']
        result = await call('query_data', {'sql': f'SELECT SUM(매출원) FROM {alias}'})
        assert result['rows'] == [[300]]
        return {'text': '선택 기간 매출은 300원입니다. 일부 자료는 잠정입니다.', 'thread_id': thread_id, 'turn_id': 't1'}
    svc.agent.turn = turn
    try:
        asyncio.run(conversation(svc, update(1, '/기억 매출비교=같은 기간 비교')))
        asyncio.run(conversation(svc, *(update(i, '매출비교 해줘') for i in range(2, 15))))
        assert svc.agent.opened[0] is None and svc.agent.opened[12] is None
        assert contexts[-1]['remembered_rules'][0]['text'] == '같은 기간 비교'
        assert len(contexts[-1]['handoff']) <= 3
        assert svc.store.get('ceo-usage-v1-14')['csv_files_read'] == 1
        assert svc.store.get('ceo-usage-v1-14')['usage_available'] is False
        assert all('300원' in x for x in messages(svc)[1:]), messages(svc)
        asyncio.run(conversation(svc, update(15, '/기억삭제 매출비교'), update(16, '매출비교 해줘')))
        assert contexts[-1]['remembered_rules'] == []
        assert svc.agent.opened[-1] is None
    finally:
        svc.store.close()


def test_run_start_and_idle_do_not_touch_data(settings, monkeypatch):
    store = Store(settings.state_root)
    class Transport:
        def call(self, method, args):
            return []
    svc = Service(settings, store, Transport())
    visited = []
    async def forbidden():
        visited.append(True)
        raise AssertionError('조회 금지')
    async def start():
        svc.agent.ready = True
    async def close():
        pass
    monkeypatch.setattr(svc, 'current', forbidden)
    monkeypatch.setattr(svc, 'accept_jobs', forbidden)
    monkeypatch.setattr(svc.agent, 'start', start)
    monkeypatch.setattr(svc.agent, 'close', close)
    async def run():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(svc.run(), .05)
    try:
        asyncio.run(run())
        assert not visited
        assert not (Path(settings.state_root) / 'analysis-cache').exists()
    finally:
        store.close()


def test_failed_refresh_never_uses_old_prepared_data(settings):
    tool = CeoTools(settings)
    prepare(tool)
    path = Path(settings.ceo_data_root) / '삼전점/00_요약/2026_10.csv'
    path.write_text('broken', encoding='utf-8')
    assert prepare(tool)['error'] == 'file_hash_mismatch'
    assert tool.call('query_data', {'sql': 'SELECT 1'})['error'] == 'prepare_data_required'


def test_unrelated_growth_does_not_increase_query_context(settings):
    first = CeoTools(settings)
    a = prepare(first)
    alias = a['prepared'][0]['sql_table']
    answer = first.query(f'SELECT SUM(매출원) FROM {alias}')
    root = Path(settings.ceo_data_root)
    for index in range(50):
        folder = root / f'무관한자료{index}'
        folder.mkdir()
        (folder / '2026_10.csv').write_text('날짜,금액원\n2026-10-01,1\n', encoding='utf-8')
    second = CeoTools(settings)
    b = prepare(second)
    assert b == a
    assert second.query(f'SELECT SUM(매출원) FROM {alias}') == answer
    assert second.read_files == first.read_files == 1


def test_unmanifested_sync_change_is_rejected(settings, monkeypatch):
    root = Path(settings.ceo_data_root) / '삼전점'
    (root / 'manifest.json').unlink()
    path = root / '00_요약/2026_10.csv'
    original = Path.read_bytes
    seen = 0
    def change(target):
        nonlocal seen
        value = original(target)
        if target == path:
            seen += 1
            if seen == 2:
                return value + b'\n'
        return value
    monkeypatch.setattr(Path, 'read_bytes', change)
    assert prepare(CeoTools(settings))['error'] == 'source_changed'


def test_root_only_enables_existing_bot_and_legacy_profile_still_loads(settings, tmp_path):
    from dataclasses import asdict
    assert settings.assistant_profile == 'legacy' and settings.lazy_data
    settings.validate()
    original = replace(settings, ceo_data_root='')
    assert not original.lazy_data
    compatible = replace(settings, assistant_profile='ceo_data')
    compatible.validate()
    path = tmp_path / 'service.json'
    path.write_text(json.dumps(asdict(settings)), encoding='utf-8')
    loaded = Settings.load(path)
    assert loaded.token_env == original.token_env
    assert loaded.state_root == original.state_root
    assert loaded.allowed_users == original.allowed_users
    assert loaded.wsl_user == original.wsl_user
    assert loaded.lazy_data


def test_next_request_sees_new_month_dataset_guide_and_changed_values(settings):
    root = Path(settings.ceo_data_root)
    dataset = root / '삼전점'
    (dataset / 'manifest.json').unlink()
    first = CeoTools(settings)
    old = prepare(first)
    csv = dataset / '00_요약/2026_10.csv'
    csv.write_text('날짜,메뉴코드,매출원,자료상태\n2026-10-01,001,900,정상\n', encoding='utf-8')
    (dataset / '00_요약/2026_11.csv').write_text(csv.read_text(encoding='utf-8').replace('10-01', '11-01'), encoding='utf-8')
    (dataset / 'AGENTS.md').write_text('# 변경된 기준\n매출은 환불 차감 후 금액입니다.', encoding='utf-8')
    (root / '추가자료').mkdir()
    next_request = CeoTools(settings)
    assert '추가자료' in next_request.catalog()['datasets']
    assert '2026_11' in next_request.catalog('삼전점')['tables'][0]['months']
    assert '환불 차감' in str(next_request.guide('삼전점', '매출'))
    new = prepare(next_request)
    assert new['snapshot_id'] != old['snapshot_id']
    alias = new['prepared'][0]['sql_table']
    assert next_request.query(f'SELECT SUM(매출원) FROM {alias}')['rows'] == [[900]]


def test_same_bot_transition_preserves_history_and_dispatches_existing_tools(settings, monkeypatch):
    from test_samjeon_appserver import service
    from test_samjeon_material_service import conversation, update, messages
    from modules.transform.pipelines.db.samjeon_material_service.brain_files import BrainFiles
    from modules.transform.pipelines.db.samjeon_material_service.workspace_files import WorkspaceFiles
    svc = service(settings)
    calls = []
    monkeypatch.setattr(BrainFiles, 'call', lambda self, name, args: calls.append(name) or {'text': '운영 기준'})
    monkeypatch.setattr(WorkspaceFiles, 'call', lambda self, name, args: calls.append(name) or {'text': '코드 안내'})
    def forbidden(*args, **kwargs):
        pytest.fail('일반 대화에서 업무 자료를 읽었습니다')
    monkeypatch.setattr(CeoTools, 'call', forbidden)
    try:
        svc.store.receive([update(1, '기존 대화')], svc.clock())
        sid, _ = svc.store.memory.select(1, 11, 22, svc.clock())
        asyncio.run(svc.send('update-1', 22, '기존 답변'))
        assert svc.store.memory.record(1, 11, 22, '기존 대화', '기존 답변', svc.clock(), topic='chat')
        svc.store.update_status(1, 'done')
        svc.store.put('codex-thread-v3-' + sid['id'], {'thread_id': 'old-thread', 'dirty': False})
        contexts = []
        async def turn(thread_id, text, call, cancelled, progress, context, started):
            contexts.append(context)
            assert (await call('search_conversation', {'query': '기존'}))['records']
            assert (await call('search_brain', {'query': '운영'}))['text'] == '운영 기준'
            assert (await call('read_workspace_file', {'path': 'AGENTS.md'}))['text'] == '코드 안내'
            return {'text': '이전 대화를 이어갑니다.', 'thread_id': thread_id, 'turn_id': 't1'}
        svc.agent.turn = turn
        asyncio.run(conversation(svc, update(2, '운영 기준과 코드 안내를 알려줘')))
        assert svc.agent.opened == [None]
        assert contexts[0]['handoff'][0]['answer'] == '기존 답변'
        assert calls == ['search_brain', 'read_workspace_file']
        assert svc.store.get('ceo-usage-v1-2')['csv_files_read'] == 0
        assert messages(svc)
    finally:
        svc.store.close()


def test_transferred_state_keeps_offset_memories_and_deduplication(settings, tmp_path):
    import shutil
    from test_samjeon_appserver import service
    from test_samjeon_material_service import conversation, update, messages
    svc = service(settings)
    asyncio.run(conversation(svc, update(101, '/기억 답변=세 줄로 요약')))
    expected_offset = svc.store.get('offset')
    svc.store.close()
    target = tmp_path / 'notebook-state'
    shutil.copytree(settings.state_root, target)
    migrated = service(replace(settings, state_root=str(target)))
    try:
        assert migrated.store.get('offset') == expected_offset
        assert relevant(migrated.store, 11, 22, '답변')[0]['text'] == '세 줄로 요약'
        asyncio.run(conversation(migrated, update(101, '/기억 답변=세 줄로 요약')))
        assert messages(migrated) == []
        asyncio.run(conversation(migrated, update(102, '/기억목록')))
        assert '세 줄로 요약' in messages(migrated)[0]
    finally:
        migrated.store.close()
