"""단일 상태 소유자. 수신·취소는 분석과 PDF 생성 중에도 계속 처리한다."""
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import hashlib
import json
import logging
from pathlib import Path
import threading

from .storage import Store
from .repository import Repository, SnapshotUnavailable
from .telegram import Telegram, DeliveryError
from .codex_adapter import CodexAdapter, redact, validate_chat, safe_diagnostic
from .memory import utc
from .data_contract import planning_context
from .conversation import parse, route, period, help_text, DATA_UNAVAILABLE, AI_UNAVAILABLE, CLARIFY
from .presentation import briefing, alert_items, buttons, chunks, render_query
from .pdf_report import create_pdf
from .grounded import validate_plan, make_facts, render_answer

KST = ZoneInfo('Asia/Seoul')
log = logging.getLogger(__name__)


def now_kst():
    return datetime.now(KST)


def identity(update):
    callback = update.get('callback_query')
    message = callback.get('message', {}) if callback else update.get('message', {})
    user = callback.get('from', {}) if callback else message.get('from', {})
    chat = message.get('chat', {})
    return user.get('id'), chat.get('id'), chat.get('type'), callback, message


class Service:
    def __init__(self, settings, store, transport, repository=None, model=None, clock=now_kst):
        self.settings, self.store, self.transport = settings, store, transport
        self.repo = repository or Repository(settings)
        self.model = model or CodexAdapter(settings)
        self.agent = None
        if settings.conversation_engine == 'appserver':
            from .appserver import AppServer
            self.agent = AppServer(settings)
        if isinstance(self.repo, Repository):
            self.repo.last_good = self.store.get('last_valid_snapshot')
        self.clock = clock
        self.cancellations = {}
        self.delivery_lock = asyncio.Lock()
        self._session_id = None
        self._received_at = None
        self._user = None
        self._telegram_poll_at = None
        self._telegram_poll_error = None
        self._report_task = None

    def model_context(self):
        return self.store.memory.model_context(self._session_id)

    async def summarize_previous(self, session, cancelled):
        if self.settings.lazy_data:
            return
        claimed = self.store.memory.claim_summary(session['id'])
        if not claimed or not hasattr(self.model, 'summarize'):
            return
        previous = claimed['previous']
        context = {
            'previous_summary': self.store.memory.session(previous).get('summary', ''),
            'history': [{'question': item['question'][:1000], 'answer': item['answer'][:1500]}
                        for item in self.store.memory.recent(previous, 10)],
            'preferences': [item['text'] for item in claimed['preferences']],
            'unresolved_questions': [item['text'] for item in claimed['unresolved']],
        }
        try:
            value = await asyncio.to_thread(self.model.summarize, context, cancelled)
        except Exception:
            self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                                        **safe_diagnostic(self.model, 'summary', 'model_failed')})
            return
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                                    **safe_diagnostic(self.model, 'summary',
                                                      None if value else 'invalid_answer')})
        if not cancelled.is_set() and not self.store.cancelled(self._handling_uid):
            self.store.memory.save_summary(session['id'], value)

    async def recall(self, uid, user, chat, text, cancelled):
        if not self.store.memory.is_recall(text):
            pending = self.store.get(f'memory-pending-v1-{self._session_id}', [])
            if not (pending and text.strip().isdigit()):
                return False
            index = int(text.strip()) - 1
            if not 0 <= index < len(pending):
                await self.send(f'update-{uid}', chat, '표시한 번호 중 하나를 알려주세요.', cancelled=cancelled)
                return True
            choices = [pending[index]]
            self.store.put(f'memory-pending-v1-{self._session_id}', [])
        else:
            choices = self.store.memory.search(user, chat, text, self._received_at)
        if not choices:
            await self.send(f'update-{uid}', chat, '요청하신 이전 대화 기록을 찾지 못했습니다. 기간이나 주제를 더 알려주세요.', cancelled=cancelled)
            return True
        if len(choices) > 1:
            self.store.put(f'memory-pending-v1-{self._session_id}', choices)
            lines = ['이전 기록이 여러 개입니다. 번호를 알려주세요.']
            for i, candidate in enumerate(choices, 1):
                period = candidate.get('period')
                label = ' ~ '.join(period) if period else candidate['created_at'][:10]
                lines.append(f"{i}. {label}: {candidate['question'][:60]}")
            await self.send(f'update-{uid}', chat, '\n'.join(lines), cancelled=cancelled)
            return True
        candidate = choices[0]
        period = candidate.get('period')
        if candidate.get('snapshot_date'):
            label = f"당시 자료 기준일 {candidate['snapshot_date']}. 잠정 자료일 수 있습니다.\n\n"
        elif period:
            label = f"당시 분석 기간 {' ~ '.join(period)}. 자료 기준일은 기록에 남아 있지 않습니다.\n\n"
        else:
            label = f"당시 대화 {utc(candidate['created_at']).astimezone(KST).date()}.\n\n"
        await self.send(f'update-{uid}', chat, label + candidate['answer'], cancelled=cancelled)
        return True

    def allowed(self, update):
        user, chat, kind, _, _ = identity(update)
        return kind == 'private' and user in self.settings.allowed_users and chat in self.settings.allowed_chats

    def write_health(self):
        from modules.transform.pipelines.db.samjeon_material_agent.contracts import atomic_json
        from .data_contract import readiness
        data = getattr(self, 'data_readiness', readiness())
        conversation_ready = bool(self.settings.enabled and self.settings.ai_enabled and self.settings.ai_verified
                                  and self.settings.token and self.settings.allowed_users and self.settings.allowed_chats
                                  and not self.settings.auto_send)
        if self.agent:
            conversation_ready = conversation_ready and self.agent.ready
        elif isinstance(self.model, CodexAdapter):
            conversation_ready = conversation_ready and bool((self.model._probe or {}).get('ready'))
        schedules = []
        if self.settings.lazy_data:
            from .report_schedules import Schedules, next_run
            schedules = [{'enabled': s.get('enabled'), 'time': s.get('time'),
                          'next_run': next_run(s, self.clock())}
                         for s in Schedules(self.store, self.settings).all()]
        atomic_json(self.store.root / 'health.json', {'checked_at': self.clock().isoformat(),
                    'scheduled_reports': schedules,
                    'telegram_poll_at': self._telegram_poll_at,
                    'telegram_poll_error': self._telegram_poll_error,
                    'conversation_engine': self.settings.conversation_engine,
                    'daily_briefing_enabled': self.settings.daily_briefing_enabled,
                    'daily_briefing_time': self.settings.daily_briefing_time,
                    'agent_connected': bool(self.agent and self.agent.ready),
                    'last_snapshot': self.store.get('last_snapshot'),
                    'ai': self.store.get('ai_status'), 'data_notice': getattr(self.repo, 'fallback_reason', None),
                    **self.store.health(), **data, 'conversation_ready': conversation_ready,
                    'full_data_ready': data['data_ready']})

    async def current(self):
        try:
            result = await asyncio.to_thread(self.repo.current)
        except SnapshotUnavailable as exc:
            from .data_contract import readiness
            self.data_readiness = {**readiness(), 'data_error': exc.code}
            raise
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailable('게시본 검증 실패') from exc
        from .data_contract import readiness
        self.data_readiness = readiness(result[1])
        if not getattr(self.repo, 'fallback_reason', None):
            self.store.put('last_valid_snapshot', str(result[0]))
        return result

    def data_notice(self, context):
        notice = getattr(self.repo, 'fallback_reason', None)
        yesterday = str(self.clock().date() - timedelta(days=1))
        if context['as_of_date'] < yesterday:
            notice = (notice + ' ' if notice else '') + '최신 일자 자료가 아직 준비되지 않았습니다.'
        return f"\n\n자료 안내: {notice} 기준일 {context['as_of_date']}." if notice else ''

    async def pdf(self, uid, chat, analysis, cancelled):
        context = await asyncio.to_thread(self.repo.context, analysis['snapshot'], analysis['snapshot_id'])
        if context.get('format') == 'legacy_v1':
            from .legacy import UNSUPPORTED
            await self.send(f'update-{uid}', chat, UNSUPPORTED, cancelled=cancelled)
            return
        notes = self.store.notes(chat, analysis['id'])
        saved = self.store.grounded_answer(chat, analysis, notes)
        document = await asyncio.to_thread(create_pdf, self.settings, self.repo, analysis, notes, saved)
        if not cancelled.is_set():
            await self.send(f'update-{uid}', chat,
                f"{analysis['date_from']} ~ {analysis['date_to']} 상세 근거입니다. 대화와 같은 분석 판본입니다.",
                document=document, cancelled=cancelled)

    async def analyze_text(self, uid, user, chat, text, path, context, analysis, cancelled, request_date=None, resolved=None):
        plan = await asyncio.to_thread(self.model.plan, text, {
            **planning_context(context),
            'snapshot_date': context['as_of_date'], 'request_date': request_date,
            'resolved_period': list(resolved) if resolved else None, 'date_from': analysis['date_from'],
            'date_to': analysis['date_to'], 'selected_product': analysis.get('focus'),
            **self.model_context()}, cancelled)
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                       **safe_diagnostic(self.model, 'plan')})
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        if plan is None:
            return False
        plan = validate_plan(plan)
        if plan['action'] == 'clarify':
            # 질문은 질문으로만 표시하며 계산 결과로 취급하지 않는다.
            rendered = '확인 질문: ' + plan['clarification']
            delivered = await self.send(f'update-{uid}', chat, rendered, cancelled=cancelled)
            if delivered and not cancelled.is_set():
                self.store.memory.record(uid, user, chat, text, rendered, self._received_at,
                                         analysis, 'clarify', context['as_of_date'])
            return True
        if plan['action'] == 'pdf':
            await self.pdf(uid, chat, analysis, cancelled)
            return True
        if plan['action'] == 'note':
            delivered = await self.send(f'update-{uid}', chat, '이 분석에 설명을 기록했습니다. 다음 분석에 참고하며 원천 검수 승인으로 처리하지 않습니다.', cancelled=cancelled)
            if delivered and not cancelled.is_set():
                self.store.note(chat, analysis['id'], user, text)
            return True
        queries = plan['queries']
        if resolved:
            queries = [{**q, 'date_from': resolved[0], 'date_to': resolved[1]} for q in queries]
        periods = {(q['date_from'] or analysis['date_from'], q['date_to'] or analysis['date_to']) for q in queries}
        names = {q['product_name'] for q in queries if q['product_name']}
        if len(periods) != 1 or len(names) > 1:
            await self.send(f'update-{uid}', chat, '한 번에 확인할 기간과 품목을 하나씩 알려주세요.', cancelled=cancelled)
            return True
        start, end = next(iter(periods))
        if (start, end) != (analysis['date_from'], analysis['date_to']):
            analysis = self.analysis(chat, path, context, str(uid), start, end)
        focus = analysis.get('focus') if all(q['intent'] in ('compare', 'evidence') for q in queries) else None
        if names:
            name = next(iter(names))
            choices = await asyncio.to_thread(self.repo.search_items, path, name)
            if cancelled.is_set():
                return True
            if not choices:
                await self.send(f'update-{uid}', chat, '해당 품목을 자료에서 찾지 못했습니다. 구매내역의 품목명을 알려주세요.', cancelled=cancelled)
                return True
            if len(choices) > 1:
                self.store.put('choices-' + analysis['id'], choices)
                markup = {'inline_keyboard': [[{'text': f"{r['item_name']} {r['spec']}"[:55],
                    'callback_data': f"{analysis['id']}:pick{i}"}] for i, r in enumerate(choices)]}
                await self.send(f'update-{uid}', chat, '같은 이름으로 여러 품목이 있습니다. 어떤 품목을 확인할까요?', markup, cancelled=cancelled)
                return True
            focus = choices[0]['product_key']
            if any(q['intent'] not in ('compare', 'evidence') for q in queries):
                await self.send(f'update-{uid}', chat, '개별 품목은 구매 비교와 근거를 확인할 수 있습니다. 전체 매입률과 구분해 질문해 주세요.', cancelled=cancelled)
                return True
        if focus != analysis.get('focus'):
            analysis = self.analysis(chat, path, context, str(uid) + (focus or 'all'), start, end)
            self.store.focus(analysis['id'], chat, focus)
            analysis = self.store.analysis(analysis['id'], chat)
        results = []
        for query in queries:
            result = await asyncio.to_thread(self.repo.query, path, query['intent'], start, end,
                **({'product_key': focus} if focus else {}))
            if cancelled.is_set() or self.store.cancelled(uid):
                return True
            results.append(result)
        facts = make_facts(results)
        notes = self.store.notes(chat, analysis['id'])
        model_notes = notes[-10:]
        answer = await asyncio.to_thread(self.model.explain, text, {
            'facts': facts, 'notes': [{'id': f'N{i + 1}', 'text': n['text']} for i, n in enumerate(model_notes)],
            **self.model_context()}, cancelled)
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        error = getattr(self.model, 'last_error', None)
        try:
            rendered = render_answer(answer, facts, model_notes)
        except (ValueError, TypeError):
            error = error or 'invalid_answer'
            rendered = 'AI 설명을 완료하지 못해 확인된 조회 결과를 안내합니다.\n\n' + '\n\n'.join(f['text'] for f in facts)
        rendered += self.data_notice(context)
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                                    **safe_diagnostic(self.model, 'answer', error)})
        self.store.save_answer(chat, analysis, notes, rendered)
        delivered = await self.send(f'update-{uid}', chat, rendered,
                                    buttons(analysis['id'], context.get('format') == 'monthly_csv_v3'), cancelled=cancelled)
        if delivered and not cancelled.is_set():
            self.store.remember_turn(chat, analysis, text, rendered, self.clock())
            self.store.memory.record(uid, user, chat, text, rendered, self._received_at,
                                     analysis, 'data', context['as_of_date'])
        return True

    async def chat(self, uid, chat, text, cancelled):
        answer = None
        if self.settings.ai_enabled and self.settings.ai_verified and hasattr(self.model, 'chat'):
            try:
                answer = await asyncio.to_thread(self.model.chat, redact(text, self.settings.token),
                    redact(self.model_context(), self.settings.token), cancelled)
                answer = validate_chat(answer) if answer else None
            except Exception:
                answer = None
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        chat_error = (None if answer else
                      (self.model.last_error or 'chat_unavailable')
                      if isinstance(self.model, CodexAdapter) else 'chat_unavailable')
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                                    **safe_diagnostic(self.model, 'chat', chat_error)})
        if answer and answer['kind'] == 'data_request':
            return False
        rendered = AI_UNAVAILABLE if not answer else CLARIFY if answer['kind'] == 'clarify' else redact(answer['text'], self.settings.token)
        delivered = await self.send(f'update-{uid}', chat, rendered, cancelled=cancelled)
        if delivered and answer and answer['kind'] == 'reply' and not cancelled.is_set():
            self.store.remember_chat(chat, redact(text, self.settings.token), rendered, self.clock())
            self.store.memory.record(uid, self._user, chat, redact(text, self.settings.token),
                                     rendered, self._received_at, topic='chat')
            self.store.remember_topic(chat, 'chat', self.clock())
        elif delivered and answer and answer['kind'] == 'clarify' and not cancelled.is_set():
            self.store.memory.record(uid, self._user, chat, redact(text, self.settings.token),
                                     rendered, self._received_at, topic='clarify')
        return True

    async def help(self):
        if self.settings.lazy_data:
            return ('질문할 때 필요한 기간의 요약부터 확인합니다. 대기 중에는 업무 자료를 읽지 않습니다.\n'
                    '예: 이번 달 매출이 지난달 같은 기간과 비교해 어떻게 달라졌어?\n'
                    '보고 예약: 매일 오전 9시에 KPI 보고 보내줘 / 보고 예약 확인해줘 / 보고 예약 해제해줘\n'
                    '/기억 이름=기준 · /기억목록 · /기억삭제 이름\n'
                    '자료 검증: 질문 시 수행. 노트북 종료·절전 중에는 응답할 수 없습니다.')
        context = None
        error = None
        try:
            _, context = await self.current()
            data_ready = True
        except SnapshotUnavailable as exc:
            data_ready = False
            error = str(exc)
        ai_ready = self.settings.ai_enabled and self.settings.ai_verified
        if ai_ready and self.agent:
            ai_ready = self.agent.ready
        elif ai_ready and hasattr(self.model, 'diagnose'):
            ai_ready = (await asyncio.to_thread(self.model.diagnose))['ready']
        if self.agent:
            if context:
                source = (f"삼전점 자료: {context['window_start']}~{context['as_of_date']} "
                          f"({context.get('status', '품질 확인 필요')})")
            else:
                source = '삼전점 자료: 현재 확인이 필요합니다.'
            return ('Codex 대화: ' + ('준비됐습니다.' if ai_ready else '연결 확인 중입니다.') + '\n'
                    + source + '\n질문을 그대로 보내세요. 필요한 자료는 제가 조회하고 확인하겠습니다. '
                    '진행 중인 분석은 /cancel로 중단할 수 있습니다.')
        if context and context.get('format') == 'legacy_v1':
            return ('대표님, 삼전점 비서입니다.\n일반 대화: ' + ('준비됐습니다.' if ai_ready else '연결 준비 중입니다.')
                    + f"\n기존 잠정 자료 조회: 가능 / {context['window_start']}~{context['as_of_date']}"
                    + '\nv3 전체 운영 준비: 미완료. 월 전체 확정 실적을 뜻하지 않습니다.'
                    + '\n/summary 요약 · /items 구매액 순위 · /compare 기간 비교 · /staff_meal 직원식대'
                    + '\n/evidence 구매 근거 · /status 상태 · /help 도움말 · /cancel 취소\nPDF·예측·고급 이상 탐지는 지원하지 않습니다.')
        return help_text(ai_ready, data_ready) + ('\n' + error if error and error != DATA_UNAVAILABLE else '')

    async def ingest(self, updates):
        updates = redact(updates, self.settings.token)
        fresh = [u for u in updates if not self.store.db.execute('SELECT 1 FROM updates WHERE id=?', (u['update_id'],)).fetchone()]
        self.store.receive(updates, self.clock())
        for update in fresh:
            if not self.allowed(update):
                self.store.update_status(update['update_id'], 'unauthorized')
                continue
            user, chat, _, callback, message = identity(update)
            if callback:
                try:
                    await asyncio.to_thread(self.transport.call, 'answerCallbackQuery', {'callback_query_id': callback['id']})
                except DeliveryError:
                    pass
            if message.get('text', '').split('@')[0].strip() == '/cancel':
                ids = self.store.cancel(chat)
                for uid in ids:
                    if uid in self.cancellations:
                        self.cancellations[uid].set()
                self.store.update_status(update['update_id'], 'done')
                await self.send(f"cancel-{update['update_id']}", chat, '진행 중인 요청을 취소했습니다. 이미 전달된 메시지는 대화에 남습니다.')

    async def send(self, key, chat, text=None, markup=None, document=None, cancelled=None):
        text = redact(text, self.settings.token)
        if document is not None:
            self.store.enqueue(key + ':0000', chat, {'method': 'sendDocument', 'document': str(document),
                                                     'caption': text or '같은 분석 판본의 상세 자료입니다.'})
        else:
            parts = list(chunks(text or ''))
            for i, part in enumerate(parts):
                payload = {'method': 'sendMessage', 'text': part}
                if i == len(parts) - 1 and markup:
                    payload['reply_markup'] = markup
                self.store.enqueue(f'{key}:{i:04d}', chat, payload)
        async with self.delivery_lock:
            for row in self.store.outgoing(key):
                if cancelled and cancelled.is_set():
                    return False
                if row['state'] in ('uncertain', 'failed'):
                    return False
                if row['state'] == 'sent':
                    continue
                payload = json.loads(row['payload'])
                method = payload.pop('method')
                if method == 'sendDocument':
                    path = Path(payload['document']).resolve()
                    if not path.is_relative_to(self.store.root / 'reports'):
                        raise ValueError('첨부 경로가 서비스 보고서 폴더 밖입니다')
                    payload['document'] = path
                self.store.sent_state(row['key'], 'sending')
                try:
                    result = await asyncio.to_thread(self.transport.call, method, {'chat_id': chat, **payload})
                except DeliveryError as exc:
                    self.store.sent_state(row['key'], 'uncertain' if exc.uncertain else 'failed', error=str(exc))
                    return False
                except Exception:
                    self.store.sent_state(row['key'], 'uncertain', error='전송 호출 결과 확인 필요')
                    return False
                self.store.sent_state(row['key'], 'sent', result.get('message_id'))
        return True

    def analysis(self, chat, path, context, key, start=None, end=None):
        end = end or context['as_of_date']
        start = start or end[:7] + '-01'
        aid = hashlib.sha256(f'{chat}:{key}:{context["snapshot_id"]}:{start}:{end}'.encode()).hexdigest()[:20]
        if not self.store.analysis(aid, chat):
            self.store.add_analysis(aid, chat, path, context['snapshot_id'], start, end)
        self.store.remember(chat, aid, self.clock() + timedelta(hours=24))
        return self.store.analysis(aid, chat)

    async def handle(self, update):
        uid = update['update_id']
        if not self.allowed(update) or self.store.cancelled(uid):
            return
        user, chat, _, callback, message = identity(update)
        cancelled = self.cancellations.setdefault(uid, threading.Event())
        self.store.update_status(uid, 'processing')
        try:
            text = redact(message.get('text', '').strip(), self.settings.token)
            if len(text) > 4000:
                raise ValueError('질문을 4,000자 이내로 나눠 보내주세요')
            received_row = self.store.db.execute('SELECT created FROM updates WHERE id=?', (uid,)).fetchone()
            self._received_at = utc(received_row['created'] if received_row else self.clock())
            self._user, self._handling_uid = user, uid
            if self.settings.lazy_data:
                from .ceo_memory import command
                answer = command(self.store, user, chat, text, uid)
                if answer is not None:
                    await self.send(f'update-{uid}', chat, answer, cancelled=cancelled)
                    return
                if callback or text.startswith('/') and text.partition(' ')[0] not in ('/start', '/help', '/status'):
                    await self.send(f'update-{uid}', chat, '분석할 내용을 자연어로 말씀해 주세요.', cancelled=cancelled)
                    return
            session = None
            if not callback and text and text.partition(' ')[0].split('@')[0] not in ('/start', '/help', '/status'):
                session, new_session = self.store.memory.select(uid, user, chat, self._received_at)
                self._session_id = session['id']
                if new_session and not self.settings.lazy_data:
                    await self.summarize_previous(session, cancelled)
                    await self.send('session-rotation-v1-' + session['id'], chat,
                                    '새 대화 세션이 시작됐습니다. 이전 대화의 요약과 선호, 분석 맥락은 이어받았습니다.',
                                    cancelled=cancelled)
                if not self.agent and await self.recall(uid, user, chat, text, cancelled):
                    return
            else:
                session = self.store.memory.active(user, chat)
                self._session_id = session['id'] if session else None
            if self.agent and not callback and text and not text.startswith('/'):
                if not self.settings.ai_enabled or not self.settings.ai_verified:
                    await self.send(f'update-{uid}', chat, AI_UNAVAILABLE, cancelled=cancelled)
                    return
                from .agent_runtime import handle_agent
                await handle_agent(self, uid, user, chat, text, session, cancelled)
                return
            analysis = (self.store.analysis(session['analysis_id'], chat)
                        if session and session.get('analysis_id') else
                        self.store.context(chat, self.clock()) if session is None else None)
            previous_analysis = analysis
            topic = self.store.topic(chat, self.clock())
            if not callback:
                kind = route(text, topic.get('topic') or ('data' if analysis else None))
                if kind == 'help' or text.partition(' ')[0].split('@')[0] == '/status':
                    await self.send(f'update-{uid}', chat, await self.help(), cancelled=cancelled)
                    return
                if kind == 'clarify':
                    await self.send(f'update-{uid}', chat, CLARIFY, cancelled=cancelled)
                    return
                if kind == 'chat' and await self.chat(uid, chat, text, cancelled):
                    return
            self.store.remember_topic(chat, 'data', self.clock())
            # Telegram의 원래 수신 시각을 사용하여 대기 중 자정을 넘겨도 의미가 변하지 않는다.
            received = (datetime.fromtimestamp(message['date'], KST)
                        if isinstance(message.get('date'), (int, float))
                        else self._received_at.astimezone(KST))
            request_date = str(received.astimezone(KST).date())
            try:
                resolved = period(text, request_date) if not callback else None
            except ValueError:
                await self.send(f'update-{uid}', chat, '조회할 월 또는 기간을 하나만 정확히 알려주세요. 예: 2026년 10월 또는 2026-10-01 2026-10-11.', cancelled=cancelled)
                return
            if callback:
                token, separator, action = callback.get('data', '').partition(':')
                analysis = self.store.analysis(token, chat)
                owner = self.store.memory.analysis_owner(token)
                if owner and (owner['user'], owner['chat']) != (user, chat):
                    raise ValueError('이 대화에서 확인할 수 없는 버튼입니다')
                import re
                if not separator or not analysis or (action not in ('why', 'items', 'meal', 'pdf', 'evidence') and not re.fullmatch(r'(?:pick|e)\d+', action)):
                    raise ValueError('이 대화에서 확인할 수 없는 버튼입니다. 새로 질문해 주세요')
                if action.startswith('pick'):
                    choices = self.store.get('choices-' + token, [])
                    index = int(action[4:])
                    if not 0 <= index < len(choices):
                        raise ValueError('품목 선택 범위 오류')
                    request = {'action': 'query', 'query': {'intent': 'compare', 'product_key': choices[index]['product_key']}}
                elif action in ('why', 'items', 'meal', 'pdf'):
                    request = {'why': {'action': 'explain'}, 'items': {'action': 'query', 'query': {'intent': 'compare'}},
                               'meal': {'action': 'query', 'query': {'intent': 'staff_meal'}}, 'pdf': {'action': 'pdf'}}[action]
                else:
                    request = None
            else:
                request = None
                if resolved or topic.get('topic') == 'chat' or any(word in text for word in ('지금', '최신', '현재')):
                    analysis = None
            current_path, current_context = await self.current()
            if (not callback and topic.get('outcome') == 'unavailable'
                    and text.rstrip('?! .') in ('왜', '왜요', '왜 그래', '왜 그래요', '다시 설명해줘')):
                await self.send(f'update-{uid}', chat,
                    '앞선 질문에서는 요청하신 자료의 준비·검증이 끝나지 않아 답하지 못했습니다. 확인할 항목을 다시 말씀해 주세요.',
                    cancelled=cancelled)
                self.store.remember_topic(chat, 'data', self.clock(), 'unavailable')
                return
            if analysis:
                path = Path(analysis['snapshot'])
                try:
                    context = await asyncio.to_thread(self.repo.context, path, analysis['snapshot_id'])
                except SnapshotUnavailable:
                    raise
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    raise SnapshotUnavailable(code='snapshot_invalid') from exc
            else:
                path, context = current_path, current_context
            if context.get('format') == 'legacy_v1':
                from .legacy_runtime import handle
                legacy_previous = analysis
                if not analysis and previous_analysis and resolved and '그 품목' in text:
                    legacy_previous = previous_analysis
                await handle(self, uid, user, chat, text, path, context, legacy_previous, callback, cancelled, request_date, resolved)
                return
            if callback and (action == 'evidence' or re.fullmatch(r'e\d+', action)):
                raise ValueError('해당 자료 형식의 버튼이 아닙니다')
            if not analysis:
                if cancelled.is_set() or self.store.cancelled(uid):
                    return
                if resolved and resolved[1] > context['as_of_date']:
                    self.store.remember_topic(chat, 'data', self.clock(), 'unavailable')
                    await self.send(f'update-{uid}', chat, f'{resolved[0]} ~ {resolved[1]} 자료가 아직 준비되지 않았습니다. 준비되면 다시 질문해 주세요.', cancelled=cancelled)
                    return
                analysis = self.analysis(chat, path, context, str(uid), *(resolved or (None, None)))
                if resolved and previous_analysis and previous_analysis.get('focus') and topic.get('topic') == 'data' and any(w in text for w in ('그럼', '그 품목', '그제는')):
                    self.store.focus(analysis['id'], chat, previous_analysis['focus'])
                    analysis = self.store.analysis(analysis['id'], chat)
            if not callback:
                request = parse(text, analysis['date_to'], today=request_date)
                if request is None and resolved:
                    request = {'action': 'query', 'query': {'intent': 'compare' if analysis.get('focus') else 'summary', 'date_from': resolved[0], 'date_to': resolved[1]}}
                    if analysis.get('focus'):
                        request['query']['product_key'] = analysis['focus']
                simple_summary = request and request['action'] == 'query' and request['query']['intent'] == 'summary' and not any(w in text for w in ('왜', '원인', '분석'))
                if (not text.startswith('/') and self.settings.ai_enabled and self.settings.ai_verified
                        and hasattr(self.model, 'plan') and not simple_summary):
                    if await self.analyze_text(uid, user, chat, text, path, context, analysis, cancelled, request_date, resolved):
                        return
            if request is None and not (self.settings.ai_enabled and hasattr(self.model, 'plan')):
                request = await asyncio.to_thread(self.model.interpret, text,
                    {'date_from': analysis['date_from'], 'date_to': analysis['date_to'],
                     'snapshot_date': context['as_of_date']}, cancelled)
            if cancelled.is_set() or self.store.cancelled(uid):
                return
            aid = analysis['id']
            pending_note = False
            if request is None:
                answer = '어느 기간의 매출·구매·직원식대가 궁금하신가요? 예: “최근 7일 구매 비교”, “이번 달 직원식대”.'
            elif request['action'] == 'note':
                pending_note = True
                answer = '알려주신 설명을 이 분석에 기록했습니다. 이어서 설명드릴 때 함께 표시하겠습니다. 원천 금액과 품목·마감 검수 상태는 그대로 유지합니다.'
            elif request['action'] == 'help':
                answer = await self.help()
            elif request['action'] == 'pdf' and context.get('format') == 'monthly_csv_v3':
                answer = '이 자료는 날짜별 집계만 제공합니다. 상세 PDF는 지원하지 않습니다.'
            elif request['action'] == 'pdf':
                await self.pdf(uid, chat, analysis, cancelled)
                return
            elif request['action'] == 'explain':
                # 대화 기간이 바뀐 뒤에도 해당 기간의 조회를 사용한다.
                if context.get('format') == 'monthly_csv_v3':
                    result = await asyncio.to_thread(self.repo.query, path, 'summary', analysis['date_from'], analysis['date_to'])
                    answer = render_query(result)
                elif analysis.get('focus'):
                    result = await asyncio.to_thread(self.repo.query, path, 'compare', analysis['date_from'],
                                                     analysis['date_to'], product_key=analysis['focus'])
                    answer = render_query(result)
                elif (analysis['date_to'] != context['as_of_date'] or analysis['date_from'] != analysis['date_to'][:7] + '-01'):
                    result = await asyncio.to_thread(self.repo.query, path, 'anomalies', analysis['date_from'], analysis['date_to'])
                    answer = render_query(result)
                else:
                    answer = briefing(context)
            elif request['action'] == 'query':
                query = dict(request['query'])
                start = query.pop('date_from', None) or analysis['date_from']
                end = query.pop('date_to', None) or analysis['date_to']
                intent = query.pop('intent')
                if intent == 'compare' and not query.get('product_key') and not callback and hasattr(self.repo, 'items'):
                    choices = await asyncio.to_thread(self.repo.items, path, text)
                    if len(choices) > 1:
                        self.store.put('choices-' + aid, choices)
                        markup = {'inline_keyboard': [[{'text': f"{r['item_name']} {r['spec']}"[:55],
                                                       'callback_data': f'{aid}:pick{i}'}] for i, r in enumerate(choices)]}
                        await self.send(f'update-{uid}', chat, '같은 이름으로 여러 품목이 있습니다. 어떤 품목을 확인할까요?', markup, cancelled=cancelled)
                        return
                    if choices:
                        query['product_key'] = choices[0]['product_key']
                    elif analysis.get('focus') and any(w in text for w in ('그', '얼마', '얼만')):
                        query['product_key'] = analysis['focus']
                result = await asyncio.to_thread(self.repo.query, path, intent, start, end, **query)
                if cancelled.is_set() or self.store.cancelled(uid):
                    return
                answer = render_query(result)
                if result['status'] != '범위 밖' and (start, end) != (analysis['date_from'], analysis['date_to']):
                    analysis = self.analysis(chat, path, context, str(uid), start, end)
                    aid = analysis['id']
                if result['status'] != '범위 밖' and query.get('product_key'):
                    # 이전 전체 분석의 설명을 개별 품목으로 덮어 해석하지 않는다.
                    analysis = self.analysis(chat, path, context, str(uid) + query['product_key'], start, end)
                    aid = analysis['id']
                    self.store.focus(aid, chat, query['product_key'])
            else:
                answer = '조회 기간과 확인할 항목을 알려주세요.'
            notes = self.store.notes(chat, aid)
            if notes and request and request['action'] in ('explain', 'query'):
                answer += '\n\n대표님 설명: ' + notes[-1]['text'] + '\n(사용자가 알려준 설명입니다.)'
            if not cancelled.is_set() and not self.store.cancelled(uid):
                answer += self.data_notice(context)
                if not callback and not text.startswith('/') and self.settings.ai_enabled and getattr(self.model, 'last_error', None):
                    answer = 'AI 연결을 사용할 수 없어 기본 조회로 안내합니다.\n\n' + answer
                self.store.remember(chat, aid, self.clock() + timedelta(hours=24))
                delivered = await self.send(f'update-{uid}', chat, answer,
                                            buttons(aid, context.get('format') == 'monthly_csv_v3'), cancelled=cancelled)
                if delivered and pending_note and not cancelled.is_set():
                    self.store.note(chat, aid, user, text)
                if delivered and not callback and request and request['action'] in ('query', 'explain') and not cancelled.is_set():
                    self.store.remember_turn(chat, analysis, text, answer, self.clock())
                    self.store.memory.record(uid, user, chat, text, answer, self._received_at,
                                             analysis, 'data', context['as_of_date'])
        except SnapshotUnavailable as exc:
            self.store.update_status(uid, 'failed', exc.code)
            self.store.remember_topic(chat, 'data', self.clock(), 'unavailable')
            if not cancelled.is_set():
                await self.send(f'error-{uid}', chat, str(exc), cancelled=cancelled)
        except (ValueError, FileNotFoundError, KeyError) as exc:
            self.store.update_status(uid, 'failed', type(exc).__name__)
            if not cancelled.is_set():
                # 파일 경로·원천 상세·비밀값이 예외에 포함될 수 있어 그대로 보내지 않는다.
                msg = '요청 내용을 확인하지 못했습니다. 확인할 항목이나 기간을 조금 더 구체적으로 말씀해 주세요.'
                await self.send(f'error-{uid}', chat, msg)
        except Exception as exc:
            self.store.update_status(uid, 'failed', type(exc).__name__)
            log.error('요청 처리 실패: %s', type(exc).__name__)
            if not cancelled.is_set():
                await self.send(f'error-{uid}', chat, '요청을 처리하지 못했습니다. 운영자가 서비스 상태를 확인해야 합니다. /cancel 또는 다시 질문해 주세요.')
        finally:
            state = self.store.db.execute('SELECT status FROM updates WHERE id=?', (uid,)).fetchone()[0]
            if state == 'processing':
                self.store.update_status(uid, 'done')
            self.cancellations.pop(uid, None)
            self._session_id = None
            self._received_at = None
            self._user = None
            self.write_health()

    async def monitor_once(self):
        if self.settings.lazy_data and (self._report_task is None or self._report_task.done()):
            from .report_schedules import run_due_reports
            if self._report_task is not None:
                try:
                    self._report_task.result()
                except (Exception, asyncio.CancelledError):
                    log.warning('예약 보고 작업 중단')
            self._report_task = asyncio.create_task(run_due_reports(self))
        if self.settings.lazy_data:
            return
        if self.settings.daily_briefing_enabled:
            from .daily_briefing import send_daily_briefing
            await send_daily_briefing(self)
        if not self.settings.auto_send:
            try:
                await self.current()
            except SnapshotUnavailable:
                pass
            return
        now = self.clock()
        today, hhmm = str(now.date()), now.strftime('%H:%M')
        if not self.settings.notification_start <= hhmm < self.settings.notification_end:
            return
        chat = self.settings.recipient
        yesterday = str(now.date() - timedelta(days=1))
        morning = '09:30' <= hhmm <= '12:00'
        try:
            path, context = await self.current()
            if context.get('format') == 'legacy_v1':
                return
            fresh = context['as_of_date'] == yesterday and not getattr(self.repo, 'fallback_reason', None)
        except (OSError, ValueError, KeyError):
            path, context, fresh = None, None, False
        daily_key = f'daily-{chat}-{today}'
        if morning and (not fresh or not context['month_to_date']['complete']):
            await self.send(daily_key + '-waiting', chat,
                            f'대표님, {yesterday} 기준 자료의 수집·분류·마감 확인이 아직 끝나지 않았습니다. 정오까지 재확인하겠습니다. 관측값과 확인할 사항은 질문으로 확인하실 수 있습니다.')
        if not fresh:
            return
        items = alert_items(context)
        aid_record = None
        if morning and context['month_to_date']['complete'] and not self.store.get(daily_key):
            aid_record = self.analysis(chat, path, context, daily_key)
            delivered = await self.send(daily_key, chat, briefing(context),
                                        buttons(aid_record['id'], context.get('format') == 'monthly_csv_v3'))
            # 불명확한 발송도 처리 이력에 보존해 snapshot 변경으로 재전송하지 않는다.
            self.store.put(daily_key, {'attempted': True, 'delivered': delivered, 'snapshot': context['snapshot_id']})
            self.store.mark_alerts(chat, [x['key'] for x in items[:3]], aid_record['id'])
        last_snapshot = self.store.get('last_snapshot', {})
        unseen = set(self.store.unseen(chat, [x['key'] for x in items])) if last_snapshot.get('id') != context['snapshot_id'] else set()
        selected = [x for x in items if x['key'] in unseen][:3]
        if selected:
            key = 'alert-' + str(chat) + '-' + hashlib.sha256('|'.join(sorted(x['key'] for x in selected)).encode()).hexdigest()[:24]
            aid_record = self.analysis(chat, path, context, key)
            await self.send(key, chat, briefing(context, selected),
                            buttons(aid_record['id'], context.get('format') == 'monthly_csv_v3'))
            self.store.mark_alerts(chat, [x['key'] for x in selected], aid_record['id'])
        self.store.put('last_snapshot', {'id': context['snapshot_id'], 'checked_at': now.isoformat(), 'status': context['status']})

    async def accept_jobs(self):
        from .pipeline import valid_job
        queue = self.store.root / 'queue'
        if not queue.exists():
            return
        for path in sorted(queue.glob('*.json'))[:100]:
            try:
                job = json.loads(path.read_text(encoding='utf-8'))
                valid = valid_job(job, self.clock())
            except (ValueError, KeyError, OSError, TypeError):
                valid = False
            if valid:
                await self.monitor_once()
            completed = queue / ('done' if valid else 'expired')
            completed.mkdir(exist_ok=True)
            path.replace(completed / path.name)

    async def run(self):
        if self.settings.lazy_data:
            from .report_schedules import Schedules
            Schedules(self.store, self.settings).recover(self.clock())
        if self.agent:
            await self.agent.start()
            self.store.put('ai_status', {'stage': 'agent', 'error': None,
                                         'checked_at': self.clock().isoformat(),
                                         'status': 'connected'})
        elif hasattr(self.model, 'diagnose'):
            await asyncio.to_thread(self.model.diagnose)
        if not self.settings.lazy_data:
            try:
                await self.current()
            except SnapshotUnavailable:
                pass
        self.write_health()
        async def poll():
            while True:
                try:
                    updates = await asyncio.to_thread(self.transport.call, 'getUpdates',
                        {'offset': self.store.get('offset', 0), 'timeout': 25, 'allowed_updates': ['message', 'callback_query']})
                    self._telegram_poll_at = self.clock().isoformat()
                    self._telegram_poll_error = None
                    await self.ingest(updates)
                except DeliveryError:
                    self._telegram_poll_at = self.clock().isoformat()
                    self._telegram_poll_error = 'telegram_temporary'
                    log.warning('텔레그램 수신 실패. 5초 뒤 다시 연결합니다')
                    await asyncio.sleep(5)
        async def worker():
            while True:
                update = self.store.next_update()
                if update:
                    await self.handle(update)
                else:
                    await asyncio.sleep(.2)
        async def monitor():
            while True:
                try:
                    if not self.settings.lazy_data:
                        await self.accept_jobs()
                    await self.monitor_once()
                    self.write_health()
                except Exception as exc:
                    log.error('자료 확인 실패: %s', type(exc).__name__)
                await asyncio.sleep(self.settings.poll_seconds)
        try:
            await asyncio.gather(poll(), worker(), monitor())
        finally:
            if self._report_task is not None:
                self._report_task.cancel()
                await asyncio.gather(self._report_task, return_exceptions=True)
            if self.agent:
                await self.agent.close()
