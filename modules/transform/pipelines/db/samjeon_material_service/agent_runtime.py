"""자연어는 Codex 세션으로 전달하고 발송 완료 여부를 별도 기록한다."""
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from .analysis_tools import AnalysisTools
from .appserver import AppServerError
from .codex_adapter import redact
from .presentation import telegram_answer
from .workspace_files import WorkspaceFiles
from .brain_files import BrainFiles


async def handle_agent(service, uid, user, chat, text, session, cancelled):
    store, agent = service.store, service.agent
    session_key = 'codex-thread-v3-' + session['id']
    request_key = 'codex-request-v1-' + str(uid)
    binding = store.get(session_key, {})
    # 미전달·중단 턴이 남은 스레드를 대화 기억으로 사용하지 않는다.
    fresh = not binding.get('thread_id') or binding.get('dirty', False)
    request_date = str(service._received_at.astimezone(ZoneInfo('Asia/Seoul')).date())
    context = {'request_date': request_date, 'timezone': 'Asia/Seoul'}
    if fresh:
        context.update(service.model_context())
        previous = session.get('previous') or session['id']
        context['previous_analysis'] = store.get('codex-analysis-v1-' + previous)
        if binding.get('dirty'):
            context['delivery_notice'] = '이전 생성 결과의 전달이 완료되지 않아 전달 완료 기록만 승계했습니다.'
    tools = AnalysisTools(service.settings, service.repo, cancelled)
    files = WorkspaceFiles()
    brain = BrainFiles(service.settings.brain_root, text)
    calls = []

    async def tool_call(name, arguments):
        if cancelled.is_set():
            return {'error': 'cancelled'}
        if name == 'search_conversation':
            query = arguments.get('query', '') if isinstance(arguments, dict) else ''
            if not isinstance(query, str) or len(query) > 1000:
                return {'error': 'query_invalid'}
            records = store.memory.search_agent(user, chat, query)
            result = {'records': [{k: r.get(k) for k in
                ('question', 'answer', 'created_at', 'snapshot_date', 'period')} for r in records]}
        elif name in ('list_workspace', 'read_workspace_file', 'write_workspace_file'):
            result = await asyncio.to_thread(files.call, name, arguments)
        elif name in ('list_brain', 'search_brain', 'read_brain_file',
                      'inspect_brain_workbook', 'read_brain_sheet'):
            result = await asyncio.to_thread(brain.call, name, arguments)
        else:
            result = await asyncio.to_thread(tools.call, name, arguments)
        calls.append({'tool': name, 'error': result.get('error'), 'query_id': result.get('query_id')})
        store.put(request_key, {**store.get(request_key, {}), 'tools': calls})
        return result

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
            context.update(service.model_context())
            context['delivery_notice'] = '이전 실행 세션을 복원하지 못해 전달 완료 대화로 재개했습니다.'
            thread_id = await agent.open_thread()
        store.put(session_key, {'thread_id': thread_id, 'dirty': True})
        store.put(request_key, {'status': 'running', 'thread_id': thread_id,
                               'session_id': session['id'], 'tools': []})

        def started(turn_id):
            store.put(request_key, {**store.get(request_key), 'turn_id': turn_id})

        result = await agent.turn(thread_id, text, tool_call, cancelled, None, context, started)
        answer = telegram_answer(redact(result['text'], service.settings.token))
        store.put(request_key, {**store.get(request_key), 'status': 'generated', 'answer': answer})
        delivered = await service.send(f'update-{uid}', chat, answer, cancelled=cancelled)
        store.put(request_key, {**store.get(request_key), 'status': 'sent' if delivered else 'undelivered'})
        if delivered and not cancelled.is_set():
            store.put(session_key, {'thread_id': thread_id, 'dirty': False})
            basis = tools.context['as_of_date'] if tools.context else None
            store.memory.record(uid, user, chat, text, answer, service._received_at,
                                topic='data' if tools.context else 'chat', snapshot_date=basis)
            if tools.context:
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
