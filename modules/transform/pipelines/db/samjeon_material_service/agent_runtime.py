"""자연어는 Codex 세션으로 전달하고 발송 완료 여부를 별도 기록한다."""
import asyncio
import json
import time
import hashlib
from zoneinfo import ZoneInfo

from .analysis_tools import AnalysisTools
from .appserver import AppServerError
from .codex_adapter import redact
from .presentation import telegram_answer
from .learning import Learning
from .workspace_files import WorkspaceFiles
from .tool_registry import ActorContext, ToolRegistry
from . import assistant_packages
from .brain_files import BrainFiles


async def handle_agent(service, uid, user, chat, text, session, cancelled):
    store, agent = service.store, service.agent
    session_key = 'codex-thread-v3-' + session['id']
    request_key = 'codex-request-v1-' + str(uid)
    binding = store.get(session_key, {})
    ceo = service.settings.lazy_data
    learning = Learning(store, service.settings, user, chat)
    began = time.monotonic()
    rotated = ceo and binding.get('completed_turns', 0) >= 12
    # 미전달·중단 턴이 남은 스레드를 대화 기억으로 사용하지 않는다.
    toolset = ('assistant-learning-v1-' + str(ceo) + '-' + assistant_packages.fingerprint(service.settings)
               + '-' + hashlib.sha256(json.dumps(learning.list(), ensure_ascii=False, sort_keys=True).encode()).hexdigest())
    changed_tools = ((ceo or 'toolset' in binding) and binding.get('toolset') != toolset)
    fresh = not binding.get('thread_id') or binding.get('dirty', False) or rotated or changed_tools
    request_date = str(service._received_at.astimezone(ZoneInfo('Asia/Seoul')).date())
    context = {'request_date': request_date, 'timezone': 'Asia/Seoul'}
    context.update(learning.context(text))
    context.update(assistant_packages.context(service.settings))
    if ceo:
        from .ceo_memory import context as ceo_context
        context.update(ceo_context(store, user, chat, session, text))
        managed_names = {r['name'] for r in learning.list()}
        context['remembered_rules'] = [r for r in context['remembered_rules'] if r['name'] not in managed_names]
        context['remembered_rules'] += [{'name': r['name'], 'text': r['content']} for r in context['active_learning']]
        if not fresh:
            context.pop('handoff', None)
            context.pop('inherited_preferences', None)
            context.pop('inherited_summary', None)
    elif fresh:
        context.update(service.model_context())
        previous = session.get('previous') or session['id']
        context['previous_analysis'] = store.get('codex-analysis-v1-' + previous)
        if binding.get('dirty'):
            context['delivery_notice'] = '이전 생성 결과의 전달이 완료되지 않아 전달 완료 기록만 승계했습니다.'
    if ceo:
        from .ceo_tools import CeoTools
        tools = CeoTools(service.settings, cancelled=cancelled)
    else:
        tools = AnalysisTools(service.settings, service.repo, cancelled)
    brain = BrainFiles(service.settings.brain_root, text)
    files = WorkspaceFiles()
    calls = []
    output_chars = 0
    schedule_result = None

    async def dispatch(name, arguments):
        nonlocal output_chars, schedule_result
        if cancelled.is_set():
            return {'error': 'cancelled'}
        if name in ('get_report_schedule', 'set_report_schedule', 'cancel_report_schedule') and ceo:
            from .report_schedules import Schedules
            result = Schedules(store, service.settings).call(name, arguments, user=user, chat=chat,
                request_id=uid, text=text, now=service.clock())
            if result.get('saved'):
                schedule_result = result
        elif name == 'search_conversation':
            query = arguments.get('query', '') if isinstance(arguments, dict) else ''
            if not isinstance(query, str) or len(query) > 1000:
                return {'error': 'query_invalid'}
            records = store.memory.search_agent(user, chat, query)
            if ceo:
                records = [{**r, 'question': r.get('question', '')[:400], 'answer': r.get('answer', '')[:700]}
                           for r in records[:3]]
            result = {'records': [{k: r.get(k) for k in
                ('question', 'answer', 'created_at', 'snapshot_date', 'period')} for r in records]}
        elif name == 'list_learning':
            offset = arguments.get('offset', 0)
            if type(offset) is not int or not 0 <= offset <= 100:
                return {'error': 'offset_invalid'}
            items = learning.list()
            result = {'items': items[offset:offset + 10], 'next_offset': offset + 10 if offset + 10 < len(items) else None}
        elif name == 'propose_learning':
            result = learning.propose(arguments, uid, text)
        elif name == 'read_assistant_knowledge':
            result = assistant_packages.knowledge(service.settings, arguments.get('query'))
        elif name in ('list_workspace', 'read_workspace_file'):
            result = await asyncio.to_thread(files.call, name, arguments)
        elif name in ('list_brain', 'search_brain', 'read_brain_file',
                      'inspect_brain_workbook', 'read_brain_sheet'):
            result = await asyncio.to_thread(brain.call, name, arguments)
        else:
            result = await asyncio.to_thread(tools.call, name, arguments)
        calls.append({'tool': name, 'error': result.get('error'), 'query_id': result.get('query_id')})
        output_chars += len(json.dumps(result, ensure_ascii=False))
        store.put(request_key, {**store.get(request_key, {}), 'tools': calls})
        return result

    registry = ToolRegistry(service.settings, ActorContext(user, chat, service.settings.assistant_id,
        service.settings.ceo_data_root or service.settings.snapshot_root or service.settings.snapshot), dispatch)
    tool_call = registry.call

    async def typing():
        while True:
            try:
                await asyncio.to_thread(service.transport.call, 'sendChatAction', {'chat_id': chat, 'action': 'typing'})
            except Exception:
                pass
            await asyncio.sleep(4)

    typing_task = asyncio.create_task(typing())
    try:
        try:
            thread_id = await agent.open_thread(None if fresh else binding['thread_id'])
        except AppServerError:
            if fresh:
                raise
            # 사라진 스레드는 사용자에게 전달된 기록으로 재구성한다.
            if not ceo:
                context.update(service.model_context())
            else:
                context.update(ceo_context(store, user, chat, session, text))
            context['delivery_notice'] = '이전 실행 세션을 복원하지 못해 전달 완료 대화로 재개했습니다.'
            thread_id = await agent.open_thread()
        store.put(session_key, {'thread_id': thread_id, 'dirty': True, 'toolset': toolset})
        store.put(request_key, {'status': 'running', 'thread_id': thread_id,
                               'session_id': session['id'], 'tools': []})

        def started(turn_id):
            store.put(request_key, {**store.get(request_key), 'turn_id': turn_id})

        result = await agent.turn(thread_id, text, tool_call, cancelled, None, context, started)
        if ceo:
            store.put('ceo-usage-v1-' + str(uid), {
                'elapsed_seconds': round(time.monotonic() - began, 3), 'csv_files_read': tools.read_files,
                'query_count': len(tools.trace), 'cache_hits': tools.cache_hits,
                'context_characters': len(json.dumps(context, ensure_ascii=False)),
                'tool_output_characters': output_chars, 'usage': result.get('usage'),
                'usage_available': result.get('usage') is not None})
        answer = telegram_answer(redact(result['text'], service.settings.token))
        if schedule_result:
            answer = ('매일 보고 예약을 해제했습니다.' if not schedule_result['enabled'] else
                      f"매일 한국시간 {schedule_result['time']} 보고를 예약했습니다.\n"
                      f"다음 실행: {schedule_result['next_run']}\n보고 내용: {schedule_result['prompt']}")
        store.put(request_key, {**store.get(request_key), 'status': 'generated', 'answer': answer})
        delivered = await service.send(f'update-{uid}', chat, answer, cancelled=cancelled)
        store.put(request_key, {**store.get(request_key), 'status': 'sent' if delivered else 'undelivered'})
        if delivered and not cancelled.is_set():
            store.put(session_key, {'thread_id': thread_id, 'dirty': False, 'toolset': toolset,
                                   'completed_turns': (0 if fresh else binding.get('completed_turns', 0)) + 1})
            basis = tools.context['as_of_date'] if tools.context else None
            store.memory.record(uid, user, chat, text, answer, service._received_at,
                                topic='data' if tools.context else 'chat', snapshot_date=basis)
            if tools.context:
                if ceo:
                    service.data_readiness = {'data_format': 'ceo_monthly_csv', 'data_query_ready': True,
                                              'data_ready': False, 'data_status': '선택 자료 조회 완료',
                                              'available_date_to': basis}
                store.put('codex-analysis-v1-' + session['id'], {
                    'snapshot_id': tools.context['snapshot_id'], 'as_of_date': basis,
                    'queries': tools.trace, 'last_question': text, 'answer': answer[:8000]})
            store.remember_topic(chat, 'data' if tools.context else 'chat', service.clock())
        store.put('ai_status', {'stage': 'agent', 'error': None, 'checked_at': service.clock().isoformat(),
                                'tool_calls': len(calls), 'delivered': delivered})
    except (AppServerError, OSError, ValueError, asyncio.TimeoutError) as exc:
        error = str(exc) if isinstance(exc, AppServerError) else 'agent_unavailable'
        if error not in ('cancelled', 'model_failed', 'empty_answer', 'appserver_disconnected',
                         'appserver_rpc_failed', 'appserver_rpc_timeout'):
            error = 'agent_unavailable'
        store.put(request_key, {**store.get(request_key, {}), 'status': 'failed', 'error': error})
        store.put('ai_status', {'stage': 'agent', 'error': error, 'checked_at': service.clock().isoformat()})
        store.update_status(uid, 'failed', error)
        if error != 'cancelled' and not cancelled.is_set():
            message = '지금 Codex 응답을 완료하지 못했습니다. 잠시 후 다시 말씀해 주세요. 이전 대화 기록은 보존돼 있습니다.'
            delivered = await service.send(f'update-{uid}', chat, message, cancelled=cancelled)
            if delivered:
                store.memory.record(uid, user, chat, text, message, service._received_at, topic='unavailable')
        elif not store.cancelled(uid):
            await service.send(f'agent-timeout-{uid}', chat, '분석 시간 제한에 도달해 중단했습니다. 질문 범위를 좁혀 이어갈 수 있습니다.')
    finally:
        typing_task.cancel()
        await asyncio.gather(typing_task, return_exceptions=True)
