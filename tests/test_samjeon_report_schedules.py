"""예약 권한·수신 이력 보존·정시 실행·발송 불명 및 읽기 도구 격리."""
import asyncio
from datetime import datetime, timedelta
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_ceo_assistant import settings
from test_samjeon_material_service import Transport
from modules.transform.pipelines.db.samjeon_material_service.storage import Store
from modules.transform.pipelines.db.samjeon_material_service.runtime import Service
from modules.transform.pipelines.db.samjeon_material_service import report_schedules as reports

NOW = datetime(2026, 10, 19, 8, 0, tzinfo=reports.KST)


def register(store, settings, uid=1, **kw):
    args = {'time': '09:00', 'prompt': reports.DEFAULT_PROMPT}
    args.update(kw)
    return reports.Schedules(store, settings).call('set_report_schedule', args, user=11, chat=22,
        request_id=uid, text='매일 오전 9시에 KPI 보고를 보내줘', now=NOW)


def test_persistence_revision_idempotency_and_cancel(settings):
    store = Store(settings.state_root)
    store.put('offset', 123)
    first = register(store, settings)
    again = register(store, settings, time='11:00')
    assert first == again
    assert first['revision'] == 1
    assert first['next_run'].startswith('2026-10-19T09:00')
    edited = register(store, settings, uid=2, time='10:00')
    assert edited['revision'] == 2
    store.close()
    store = Store(settings.state_root)
    manager = reports.Schedules(store, settings)
    assert manager.get(22, NOW)['time'] == '10:00'
    assert store.get('offset') == 123
    result = manager.call('cancel_report_schedule', {}, user=11, chat=22, request_id=3,
                          text='보고 예약 해제해줘', now=NOW)
    assert result['saved'] and not result['enabled']
    assert manager.get(22, NOW)['next_run'] is None


@pytest.mark.parametrize('user,chat,text', [
    (99,22,'매일 보고 예약해줘'), (11,99,'매일 보고 예약해줘'),
    (11,22,'이번 달 매출은?'), (11,22,'매주 월요일 보고 예약해줘'),
    (11,22,'내일만 보고 보내줘'), (11,22,'예약된 보고가 있니?'),
    (11,22,'보고 예약 해제해줘')])
def test_no_mutation_from_non_request_or_other_recipient(settings, user, chat, text):
    store = Store(settings.state_root)
    manager = reports.Schedules(store, settings)
    result = manager.call('set_report_schedule', {'time':'09:00','prompt':'보고'},
                          user=user, chat=chat, request_id=1, text=text, now=NOW)
    assert 'error' in result and not manager.all()


@pytest.mark.parametrize('time,prompt', [('24:00','보고'), ('9:00','보고'), ('09:00',''), ('09:00','x'*2001)])
def test_bad_inputs_do_not_save(settings,time,prompt):
    store = Store(settings.state_root)
    assert 'error' in register(store,settings,time=time,prompt=prompt)


def test_due_window_restart_and_repeated_claim(settings):
    store = Store(settings.state_root)
    register(store,settings)
    manager = reports.Schedules(store,settings)
    schedule = manager.all()[0]
    assert manager.claim(schedule,NOW) is None
    key = manager.claim(schedule,NOW + timedelta(hours=1))
    assert key.endswith('2026-10-19')
    assert manager.claim(schedule,NOW + timedelta(hours=2)) is None
    manager.recover(NOW + timedelta(hours=2))
    assert store.get(key)['status'] == 'interrupted'
    assert manager.claim(schedule,NOW + timedelta(hours=2)) is None
    assert manager.claim(schedule,NOW + timedelta(days=1,hours=4)) is None
    assert store.get('daily-briefing-v1-22-2026-10-20')['status'] == 'skipped'
    assert manager.claim(schedule,NOW + timedelta(days=2,hours=3,minutes=59))


def test_outbox_blocks_legacy_duplicate_and_new_registration_is_future(settings):
    store = Store(settings.state_root)
    register(store,settings)
    store.enqueue('daily-briefing-v1-22-2026-10-19:0000',22,{'method':'sendMessage','text':'기존 보고'})
    manager = reports.Schedules(store,settings)
    assert manager.claim(manager.all()[0],NOW + timedelta(hours=1)) is None
    changed = manager.call('set_report_schedule',{'time':'09:00','prompt':'새 보고'},user=11,chat=22,
                           request_id=2,text='보고 시간 변경해줘',now=NOW + timedelta(hours=2))
    assert changed['next_run'].startswith('2026-10-20T09:00')


def test_due_send_and_uncertain_delivery_never_repeat(settings,monkeypatch):
    async def preview(*_):
        return {'text':'검증 보고','as_of_date':'2026-10-18','query_count':1}
    monkeypatch.setattr(reports,'preview_report',preview)
    for fail in (False,True):
        cfg = replace(settings,state_root=str(Path(settings.state_root) / str(fail)))
        store = Store(cfg.state_root)
        register(store,cfg)
        transport = Transport(fail=fail)
        service = Service(cfg,store,transport,clock=lambda: NOW + timedelta(hours=1))
        async def run():
            await reports.run_due_reports(service)
            await reports.run_due_reports(service)
        asyncio.run(run())
        assert len(transport.calls) == 1
        assert store.get('daily-briefing-v1-22-2026-10-19')['status'] == ('undelivered_or_uncertain' if fail else 'sent')
        assert bool(store.get('last-scheduled-report-v1-11-22')) is not fail


def test_change_while_generating_suppresses_send(settings,monkeypatch):
    store = Store(settings.state_root)
    register(store,settings)
    transport = Transport()
    service = Service(settings,store,transport,clock=lambda: NOW + timedelta(hours=1))
    async def preview(*_):
        reports.Schedules(store,settings).call('cancel_report_schedule',{},user=11,chat=22,
            request_id=2,text='보고 예약 해제해줘',now=service.clock())
        return {'text':'생성 결과'}
    monkeypatch.setattr(reports,'preview_report',preview)
    asyncio.run(reports.run_due_reports(service))
    assert not transport.calls
    assert store.get('daily-briefing-v1-22-2026-10-19')['status'] == 'cancelled_before_send'


def test_no_source_reads_until_due_and_monitor_does_not_block(settings,monkeypatch):
    store = Store(settings.state_root)
    register(store,settings)
    def forbidden(*_):
        raise AssertionError('예약 전 자료 조회')
    monkeypatch.setattr(reports,'CeoTools',forbidden)
    service = Service(settings,store,Transport(),clock=lambda: NOW)
    async def run():
        await service.monitor_once()
        await service._report_task
        reports.Schedules(store,settings).get(22,NOW)
    asyncio.run(run())


def test_report_readonly_tools_and_kpi_refresh(settings):
    brain = Path(settings.state_root).parent / 'brain' / '직영점_KPI'
    brain.mkdir(parents=True)
    (brain/'KPI.md').write_text('11월 목표 25000000원',encoding='utf-8')
    cfg = replace(settings,brain_root=str(brain.parent))
    seen=[]
    class Agent:
        async def open_thread(self,**kw):
            assert kw['analysis_only']
            return 'preview'
        async def turn(self,thread,prompt,call,cancelled,progress,context,started):
            seen.append(context['kpi_document']['content'])
            assert (await call('set_report_schedule',{}))['error'] == 'tool_not_allowed'
            assert (await call('write_workspace_file',{}))['error'] == 'tool_not_allowed'
            assert (await call('read_brain_sheet',{}))['error'] == 'tool_not_allowed'
            p = await call('prepare_data',{'dataset':'삼전점','tables':['00_요약'],
                                        'date_from':'2026-10-01','date_to':'2026-10-02'})
            alias = p['prepared'][0]['sql_table']
            rows = await call('query_data',{'sql':f'SELECT SUM(매출원) FROM {alias}'})
            assert rows['rows'] == [[300]]
            return {'text':'자료 기준 10월 2일, 잠정 매출 300원. 10월 목표 없음. 11월 목표 준비 현황.'}
    service=SimpleNamespace(settings=cfg,agent=Agent(),clock=lambda:NOW)
    first=asyncio.run(reports.preview_report(service,{'prompt':reports.DEFAULT_PROMPT}))
    assert first['kpi_available'] and first['query_count']==1
    (brain/'KPI.md').write_text('11월 목표 30000000원',encoding='utf-8')
    asyncio.run(reports.preview_report(service,{'prompt':reports.DEFAULT_PROMPT}))
    assert seen==['11월 목표 25000000원','11월 목표 30000000원']


def test_kpi_missing_target_period_and_overachievement():
    from datetime import date
    october = reports.progress(10000000,25000000,'2026-11',date(2026,10,19))
    assert october['attainment_percent'] is None and october['remaining_won'] is None
    november = reports.progress(12500000,25000000,'2026-11',date(2026,11,16))
    assert november['attainment_percent']==50
    assert november['remaining_calendar_days']==15
    assert november['required_daily_won']==round(12500000/15,2)
    exceeded = reports.progress(30000000,25000000,'2026-11',date(2026,11,30))
    assert exceeded['attainment_percent']==120
    assert exceeded['remaining_won']==exceeded['required_daily_won']==0
    with pytest.raises(ValueError):
        reports.progress(100,float('nan'),'2026-11',date(2026,11,30))


def test_chat_schedule_tools_save_and_return_actual_state(settings):
    from test_samjeon_appserver import service
    from test_samjeon_material_service import conversation, update, messages
    svc = service(settings)
    async def turn(thread, text, call, cancelled, progress, context, started):
        if '해제' in text:
            result=await call('cancel_report_schedule',{})
        elif '변경' in text:
            saved=await call('get_report_schedule',{})
            result=await call('set_report_schedule',{'time':'10:00','prompt':saved['prompt']})
        else:
            result=await call('set_report_schedule',{'time':'09:00','prompt':reports.DEFAULT_PROMPT})
        assert result['saved']
        return {'text':'도구 결과와 다른 말을 해도 저장 결과로 응답한다.'}
    svc.agent.turn=turn
    asyncio.run(conversation(svc,update(1,'매일 오전 9시 KPI 보고 예약해줘'),
                             update(2,'오전 10시로 변경해줘'),update(3,'보고 예약 해제해줘')))
    assert reports.Schedules(svc.store,settings).get(22,NOW)['enabled'] is False
    sent=messages(svc)
    assert '09:00' in sent[0] and '10:00' in sent[1] and '해제' in sent[2]


def test_analysis_thread_excludes_mutation_and_memory_tools(settings,monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service.appserver import AppServer
    agent=AppServer(settings)
    seen=[]
    async def start():
        pass
    async def rpc(method,args):
        seen.append(args)
        return {'thread':{'id':'daily'}}
    monkeypatch.setattr(agent,'start',start)
    monkeypatch.setattr(agent,'rpc',rpc)
    asyncio.run(agent.open_thread(analysis_only=True))
    names={x['name'] for x in seen[0]['dynamicTools']}
    assert names=={'data_catalog','read_guide','prepare_data','query_data','search_brain','read_brain_file'}


def test_one_off_revision_does_not_change_future_reports(settings):
    store=Store(settings.state_root)
    register(store,settings)
    manager=reports.Schedules(store,settings)
    for text in ('이 리포트 내용을 수정해줘','이번 보고만 짧게 다시 써줘'):
        result=manager.call('set_report_schedule',{'time':'09:00','prompt':'간략 보고'},user=11,chat=22,
                            request_id=2,text=text,now=NOW)
        assert 'error' in result
    result=manager.call('set_report_schedule',{'time':'09:00','prompt':'홀·배달과 오늘 할 일만 간략히'},user=11,chat=22,
                        request_id=3,text='앞으로 리포트 내용을 짧게 수정해줘',now=NOW)
    assert result['saved'] and result['time']=='09:00'


def test_followup_sees_only_own_delivered_report(settings):
    from modules.transform.pipelines.db.samjeon_material_service.ceo_memory import context
    store=Store(settings.state_root)
    store.put('last-scheduled-report-v1-11-22',{'text':'발송 완료 보고','as_of_date':'2026-10-18'})
    session={'id':'none'}
    assert context(store,11,22,session,'이 보고서 다시 써줘')['latest_scheduled_report']['text']=='발송 완료 보고'
    assert context(store,12,22,session,'보고서')['latest_scheduled_report'] is None
    assert context(store,11,22,session,'안녕')['latest_scheduled_report'] is None
