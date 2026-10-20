"""검증된 최신 관측값을 Astra로 해석해 오전 한 번 발송한다."""
import asyncio
from datetime import datetime
import logging
import threading
from zoneinfo import ZoneInfo

from .analysis_tools import AnalysisTools
from .brain_files import BrainFiles
from .codex_adapter import redact
from .presentation import telegram_answer


KST = ZoneInfo('Asia/Seoul')
log = logging.getLogger(__name__)


def due(now, start):
    """정시 이후 정오 전까지 놓친 오전 브리핑을 한 번 보충한다."""
    local = now.astimezone(KST)
    return start <= local.strftime('%H:%M') < '12:00'


def fallback(context):
    if context:
        date = context['as_of_date']
        status = context.get('status', '자료 상태 확인 필요')
        return (f'{date} 기준 삼전점 자료는 확인했지만 오늘의 AI 인사이트를 완성하지 못했습니다.\n\n'
                f'# 자료 상태\n- 최신 확인일: {date} ({status})\n- 확인되지 않은 기간의 수치는 포함하지 않았습니다.\n\n'
                '# 확인할 일\n- 자료 갱신·분류 상태를 확인해 주세요.\n- 구체적인 항목은 질문으로 다시 조회할 수 있습니다.')
    return ('오늘 오전 브리핑은 게시 자료를 검증하지 못해 수치 분석을 보류합니다.\n\n'
            '# 자료 상태\n- 검증된 최신 판본을 확인할 수 없습니다.\n\n'
            '# 확인할 일\n- 자료 게시·동기화 상태를 확인해 주세요.')


async def send_daily_briefing(service):
    settings = service.settings
    if not settings.daily_briefing_enabled or not due(service.clock(), settings.daily_briefing_time):
        return False
    now = service.clock().astimezone(KST)
    chat = settings.recipient
    key = f'daily-briefing-v1-{chat}-{now.date()}'
    if service.store.get(key) or service.store.outgoing(key):
        return False
    # 생성 중 중단돼도 같은 날 임의 재생성으로 중복 발송하지 않는다.
    service.store.put(key, {'status': 'generating', 'started_at': now.isoformat()})
    tools = AnalysisTools(settings, service.repo, threading.Event())
    brain = BrainFiles(settings.brain_root)
    context = None
    try:
        await asyncio.to_thread(tools.prepare)
        context = tools.context

        # 수신자 자신의 발송 완료 대화만 사용한다. 개인 대화 ID와 사용자 ID가 같은 경우에 한정한다.
        memory = (service.store.memory.briefing_context(chat, chat, now)
                  if chat in settings.allowed_users and chat in settings.allowed_chats
                  else {'preferences': [], 'recent_business_questions': []})

        async def tool_call(name, arguments):
            if name in ('data_catalog', 'query_data', 'get_evidence'):
                return await asyncio.to_thread(tools.call, name, arguments)
            if name in ('search_brain', 'read_brain_file'):
                return await asyncio.to_thread(brain.call, name, arguments)
            if name == 'read_assistant_knowledge':
                from .assistant_packages import knowledge
                return knowledge(settings, arguments.get('query'))
            return {'error': 'tool_not_allowed'}

        thread_id = await service.agent.open_thread(analysis_only=True)
        prompt = (f'{now.date()} 오전 브리핑을 작성하세요. 최신 확인 가능 자료 기준일은 '
                  f'{context["as_of_date"]}입니다. 자료 상태와 보유 기간을 밝히고, '
                  '실적 현황·비용 관점·확인할 일 중 실제 자료로 설명할 수 있는 항목만 짧게 제시하세요. '
                  '전일 자료가 없어도 확인 가능한 잠정 관측값을 분석하세요. '
                  '최근 경영 질문과 보고 선호가 있으면 관련 지표의 우선순위와 설명 깊이에 반영하세요. '
                  '대화에서 나온 추정이나 숫자를 현재 사실로 취급하지 말고 오늘 자료로 다시 검증하세요.')
        from .assistant_packages import context as package_context
        from .learning import Learning
        model_context = {'request_date': str(now.date()), 'timezone': 'Asia/Seoul',
                         'briefing': True, 'as_of_date': context['as_of_date'], 'conversation_focus': memory}
        model_context.update(package_context(settings))
        if chat in settings.allowed_users and chat in settings.allowed_chats:
            model_context.update(Learning(service.store, settings, chat, chat).context(prompt))
        result = await service.agent.turn(thread_id, prompt, tool_call, threading.Event(), None,
                                          model_context, lambda _: None)
        if not tools.trace:
            raise ValueError('briefing_without_data_query')
        answer = telegram_answer(redact(result['text'], settings.token))
        if not answer:
            raise ValueError('empty_briefing')
        source = 'astra'
    except Exception as exc:
        log.warning('오전 브리핑 분석 실패: %s', type(exc).__name__)
        answer = fallback(context)
        source = 'verified_status'
    delivered = await service.send(key, chat, answer)
    service.store.put(key, {'status': 'sent' if delivered else 'undelivered',
                            'source': source, 'snapshot_id': context.get('snapshot_id') if context else None,
                            'as_of_date': context.get('as_of_date') if context else None,
                            'checked_at': service.clock().isoformat()})
    return delivered
