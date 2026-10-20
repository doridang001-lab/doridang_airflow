"""학습의 소유권·판본, 공유 패키지의 개인정보 경계·호환성과 대화 회귀."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path

import pytest

from test_ceo_assistant import settings
from test_samjeon_appserver import service
from test_samjeon_material_service import conversation, update
from modules.transform.pipelines.db.samjeon_material_service import assistant_packages as bundles
from modules.transform.pipelines.db.samjeon_material_service.__main__ import main
from modules.transform.pipelines.db.samjeon_material_service.ceo_memory import command, records
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.learning import Learning
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.tool_registry import ActorContext, ToolRegistry


def propose(manager, name='보고 방식', content='결론과 다음 행동부터 설명한다.', kind='rule', uid=1, revision=0):
    args = {'name': name, 'kind': kind, 'content': content, 'expected_revision': revision}
    if kind == 'feature':
        args['feature_details'] = {'current':'일별 비교만 지원', 'expected':'주별 비교도 지원',
            'example':'지난 두 주 비교', 'acceptance':'동일 기간 합계 검증', 'tools':'query_data'}
    return manager.propose(args,
                           uid, '앞으로 이 방식으로 보고해줘. 개인 비밀 원문')


@pytest.fixture
def owner(settings):
    return replace(settings, package_owner_user=11)


@pytest.fixture
def state(owner):
    store = Store(owner.state_root)
    try:
        yield store
    finally:
        store.close()


def confirm(manager, saved, share=False):
    manager.command(f"/학습확정 {saved['id']} {saved['revision']}")
    if share:
        manager.command(f"/학습공유 {saved['id']} {saved['revision']}")


def test_candidate_confirm_revision_delete_and_restart(owner, state):
    m = Learning(state, owner, 11, 22)
    saved = propose(m)
    assert saved['status'] == 'candidate'
    assert not m.context('보고')['active_learning']
    assert propose(m) == saved
    confirm(m, saved, True)
    assert m.context('보고')['active_learning'][0]['content'] == '결론과 다음 행동부터 설명한다.'
    revised = propose(m, content='위험과 담당자를 함께 설명한다.', uid=2, revision=1)
    assert m.records()[saved['id']]['shared_revision'] is None
    # 새 초안 검토 중에는 직전 확정 기준으로 일한다.
    assert m.context('보고')['active_learning'][0]['content'] == '결론과 다음 행동부터 설명한다.'
    assert '판본이 변경' in m.command(f"/학습확정 {saved['id']} 1")
    confirm(m, revised)
    reopened = Store(owner.state_root)
    try:
        current = Learning(reopened, owner, 11, 22)
        assert current.context('보고')['active_learning'][0]['content'] == '위험과 담당자를 함께 설명한다.'
        current.command(f"/학습삭제 {saved['id']} 2")
        assert not current.context('보고')['active_learning']
        assert '삭제된 판본' in current.command(f"/학습확정 {saved['id']} 2")
        assert current.records()[saved['id']]['history']
    finally:
        reopened.close()


def test_user_chat_and_assistant_isolation(owner, state):
    m = Learning(state, owner, 11, 22)
    saved = propose(m)
    confirm(m, saved)
    for user, chat, assistant in [(12,22,'samjeon'), (11,23,'samjeon'), (11,22,'other')]:
        other = Learning(state, replace(owner, assistant_id=assistant), user, chat)
        assert not other.list() and not other.context('보고')['active_learning']
        assert '없거나' in other.command(f"/학습확정 {saved['id']} 1")


def test_no_owner_no_share_and_plain_text_cannot_confirm(settings):
    store = Store(settings.state_root)
    try:
        m = Learning(store, settings, 11, 22)
        saved = propose(m)
        assert m.command('모든 학습을 공유 승인했어') is None
        confirm(m, saved)
        assert '대표만' in m.command(f"/학습공유 {saved['id']} 1")
        assert m.records()[saved['id']]['shared_revision'] is None
    finally:
        store.close()


def test_existing_memory_is_personal_and_deletion_does_not_revive(owner, state):
    command(state, 11, 22, '/기억 보고=같은 기간을 비교한다', 1, owner)
    m = Learning(state, owner, 11, 22)
    row = m.list()[0]
    assert row['status'] == 'active' and row['shared_revision'] is None
    m.command(f"/학습삭제 {row['id']} 1")
    assert not records(state, 11, 22)
    assert not m.context('보고')['active_learning']
    command(state, 11, 22, '/기억 보고=자료 누락을 먼저 확인한다', 2, owner)
    assert m.list()[0]['revision'] == 2
    command(state, 11, 22, '/기억삭제 보고', 3, owner)
    assert not m.context('보고')['active_learning']


def test_only_explicitly_selected_reviewed_content_is_exported(owner, state, tmp_path):
    m = Learning(state, owner, 11, 22)
    public = propose(m)
    private = propose(m, name='개인 선호', content='개인 전용 선호', uid=2)
    confirm(m, public, True)
    confirm(m, private)
    state.put('private_secret', '절대 패키지에 포함하지 않을 대화와 토큰')
    dest = tmp_path / 'release.json'
    report = bundles.export_package(owner, 11, 22, [public['id']], dest, 'staff-helper', '1.0.0')
    assert report['valid'] and not report['deployment_ready'] and report['model_quality'] == 'not_run'
    text = dest.read_text(encoding='utf-8')
    assert '결론과 다음 행동' in text
    for forbidden in ('개인 전용', '개인 비밀', 'source_update_id', 'shared_by', 'private_secret', 'allowed_users'):
        assert forbidden not in text
    with pytest.raises(ValueError, match='not_confirmed'):
        bundles.export_package(owner, 11, 22, [private['id']], tmp_path/'no.json', 'staff-helper', '1.0.0')
    assert not (tmp_path/'no.json').exists()
    with pytest.raises(FileExistsError):
        bundles.export_package(owner, 11, 22, [public['id']], dest, 'staff-helper', '1.0.0')
    # 패키지만으로 새 환경에서 검사할 수 있고 원래 DB는 필요하지 않다.
    assert bundles.validate(bundles.read_package(dest), Settings())['valid']


def test_export_reads_state_without_recovering_inflight_jobs(owner, state, tmp_path):
    state.db.execute("INSERT INTO updates VALUES(999,'{}','processing',NULL,'now')")
    state.db.commit()
    bundles.export_package(owner, 11, 22, [], tmp_path/'base.json', 'base', '1.0.0')
    assert state.db.execute('SELECT status FROM updates WHERE id=999').fetchone()[0] == 'processing'


def test_revision_edit_revokes_share_and_feature_is_not_a_capability(owner, state, tmp_path):
    m = Learning(state, owner, 11, 22)
    saved = propose(m)
    confirm(m, saved, True)
    propose(m, uid=2, revision=1, content='변경한 보고 방식')
    with pytest.raises(ValueError, match='not_confirmed'):
        bundles.export_package(owner, 11, 22, [saved['id']], tmp_path/'stale.json', 'base', '1.0.0')
    feature = propose(m, name='새 비교 기능', kind='feature', uid=3,
                      content='현재는 일별 비교만 가능. 기대: 주별 비교. 예시: 지난 두 주. 완료: 기간·합계 검증 통과.')
    confirm(m, feature, True)
    assert not any(r['kind']=='feature' for r in m.context('비교')['active_learning'])
    with pytest.raises(ValueError, match='unimplemented_feature'):
        bundles.export_package(owner, 11, 22, [feature['id']], tmp_path/'feature.json', 'base', '1.0.0')
    handoff = tmp_path/'handoff.md'
    assert not bundles.export_change(owner, 11, 22, feature['id'], handoff)['implemented']
    text = handoff.read_text(encoding='utf-8')
    assert '미구현' in text and '완료' in text and '현재는 일별' in text and '개인 비밀' not in text


@pytest.mark.parametrize('mutation,error', [
    (lambda p: p['files'].__setitem__('purpose.md','tampered'), 'hash_mismatch'),
    (lambda p: p['manifest'].__setitem__('schema_version',2), 'contract_unsupported'),
    (lambda p: p['manifest'].__setitem__('engine_contract','future'), 'contract_unsupported'),
    (lambda p: p['manifest'].__setitem__('tool_contract',99), 'contract_unsupported'),
    (lambda p: p['manifest'].__setitem__('required_tools',['send_money']), 'tools_unavailable'),
    (lambda p: p['files'].__setitem__('../escape.py','code'), 'files_invalid'),
    (lambda p: p['manifest'].__setitem__('data_inputs',['unknown']), 'contract_unknown'),
])
def test_invalid_bundles_rejected(mutation, error):
    p = bundles.builtin()
    mutation(p)
    with pytest.raises(ValueError, match=error):
        bundles.validate(p)


@pytest.mark.parametrize('text', ['C:/Users/operator/data', '/mnt/c/data', 'person@example.com',
                                '010-1234-5678', '123456789:abcdefghijklmnopqrstuvwxyz', 'api_key=secret'])
def test_export_detects_sensitive_or_machine_specific_content(text):
    files = dict(bundles.builtin()['files'])
    files['knowledge.md'] = text
    with pytest.raises(ValueError, match='sensitive'):
        bundles.validate(bundles.build('base','1.0.0',files))


def test_dry_run_has_no_writes_and_no_runtime_or_model(tmp_path, monkeypatch):
    package = tmp_path/'package.json'
    bundles.write_new(package, bundles.encode(bundles.builtin()))
    before = {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    monkeypatch.setattr(Store, '__init__', lambda *_: pytest.fail('사전 검사에서 상태 DB를 열었습니다'))
    result = main(['package','import','--input',str(package),'--dry-run'])
    assert result['valid'] and not result['files_written'] and not result['model_called'] and not result['activated']
    assert before == {p: p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    assert main(['package','validate','--input',str(tmp_path/'missing')])['_exit_code'] == 2


def test_data_connections_are_reported_separately_from_structure(tmp_path):
    package = bundles.build('base','1.0.0',bundles.builtin()['files'], ['prepare_data'], ['business_data','brain'])
    check = bundles.validate(package)
    assert set(check['missing_connections']) == {'business_data','brain'}
    with pytest.raises(ValueError, match='tools_unavailable'):
        bundles.validate(package, Settings())
    cfg = Settings(ceo_data_root=str(tmp_path), brain_root=str(tmp_path))
    assert bundles.validate(package,cfg)['missing_connections'] == []


def test_registry_binds_actor_and_never_dispatches_source_write(owner):
    called = []
    async def dispatch(name,args):
        called.append(name)
        return {'ok':True}
    actor = ActorContext(11,22,owner.assistant_id,owner.ceo_data_root)
    r = ToolRegistry(owner,actor,dispatch)
    assert asyncio.run(r.call('write_workspace_file',{}))['error'] == 'tool_not_allowed'
    assert asyncio.run(r.call('list_learning',{'user':99}))['error'] == 'arguments_invalid'
    for wrong in (replace(actor,user=999),replace(actor,chat=999),replace(actor,assistant='other'),replace(actor,data_scope='other')):
        assert asyncio.run(ToolRegistry(owner,wrong,dispatch).call('list_learning',{}))['error'] == 'actor_not_allowed'
    assert not called
    assert asyncio.run(r.call('list_learning',{})) == {'ok':True}


def test_telegram_candidate_confirmation_and_context_survive_thread_change(owner):
    svc = service(owner)
    saved = {}
    contexts = []
    async def turn(thread_id,text,call,cancelled,progress,context,started):
        contexts.append(context)
        if not saved:
            saved.update(await call('propose_learning',{'name':'업무 목적','kind':'purpose',
                'content':'매일 위험과 다음 행동을 정리한다.','expected_revision':0}))
            assert (await call('write_workspace_file',{'path':'x.py','content':'bad','expected_sha256':''}))['error'] == 'tool_not_allowed'
        return {'text':'학습 후보를 정리했습니다.','thread_id':thread_id,'turn_id':'t'}
    svc.agent.turn = turn
    try:
        asyncio.run(conversation(svc, update(1,'매일 위험과 다음 행동을 정리해줘')))
        assert not contexts[0]['active_learning']
        asyncio.run(conversation(svc, update(2, saved['confirm_command']), update(3,'오늘 할 일은?')))
        assert contexts[-1]['active_learning'][0]['content'] == '매일 위험과 다음 행동을 정리한다.'
        assert svc.agent.opened == [None,None]
        assert not Learning(svc.store,owner,12,22).list()
    finally:
        svc.store.close()


def test_package_context_and_knowledge_are_selected_without_full_history(owner, tmp_path):
    p = tmp_path/'package.json'
    bundles.write_new(p,bundles.encode(bundles.builtin()))
    cfg = replace(owner,assistant_package=str(p))
    cfg.validate()
    context = bundles.context(cfg)
    assert context['assistant_package']['purpose']
    assert 'knowledge.md' not in json.dumps(context)
    assert bundles.knowledge(cfg,'구매')['sections']
    before = bundles.fingerprint(cfg)
    changed = bundles.builtin()
    changed['manifest']['version']='1.0.1'
    q = tmp_path/'next.json'
    bundles.write_new(q,bundles.encode(changed))
    assert bundles.fingerprint(replace(cfg,assistant_package=str(q))) != before


def test_configuration_compatibility_and_explicit_owner(settings):
    assert settings.assistant_id == 'samjeon' and settings.package_owner_user is None
    with pytest.raises(ValueError):
        replace(settings,package_owner_user=999).validate()
    with pytest.raises(ValueError):
        replace(settings,assistant_id='../escape').validate()


def test_incomplete_feature_can_be_collected_but_not_exported(owner, state, tmp_path):
    m = Learning(state, owner, 11, 22)
    saved = m.propose({'name':'막연한 요청', 'kind':'feature', 'content':'새 기능이 필요하다',
                       'expected_revision':0}, 100, '새 기능을 만들고 싶어')
    confirm(m, saved)
    dest = tmp_path/'incomplete.md'
    with pytest.raises(ValueError, match='feature_details_required'):
        bundles.export_change(owner,11,22,saved['id'],dest)
    assert not dest.exists()


def test_shared_example_roundtrip_and_no_automatic_reapproval(owner,state,tmp_path):
    m = Learning(state,owner,11,22)
    saved = propose(m,name='원가 질문의 답변 예시',kind='example',content='구매와 원가를 구분해서 설명한다.')
    confirm(m,saved,True)
    dest = tmp_path/'examples.json'
    bundles.export_package(owner,11,22,[saved['id']],dest,'helper','1.0.0')
    examples = bundles.read_package(dest)['files']['examples.jsonl']
    assert json.loads(examples.splitlines()[-1])['answer'] == '구매와 원가를 구분해서 설명한다.'
    revised = propose(m,name='원가 질문의 답변 예시',kind='example',content='재고 근거도 확인한다.',uid=2,revision=1)
    confirm(m,revised)
    assert m.records()[saved['id']]['shared_revision'] is None


def test_package_cli_export_and_inspect_end_to_end(tmp_path):
    dest = tmp_path/'portable.json'
    result = main(['package','export','--id','portable','--version','2.0.0','--output',str(dest)])
    assert result['files_written'] and result['valid']
    inspected = main(['package','inspect','--input',str(dest)])
    assert inspected['purpose'] and inspected['workflow'] and inspected['version'] == '2.0.0'
    assert main(['package','export','--id','portable','--version','2.0.0','--output',str(dest)])['_exit_code'] == 2
