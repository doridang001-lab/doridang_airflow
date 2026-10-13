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
from .codex_adapter import CodexAdapter, redact, validate_chat
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
        if isinstance(self.repo, Repository):
            self.repo.last_good = self.store.get('last_valid_snapshot')
        self.clock = clock
        self.cancellations = {}
        self.delivery_lock = asyncio.Lock()

    def allowed(self, update):
        user, chat, kind, _, _ = identity(update)
        return kind == 'private' and user in self.settings.allowed_users and chat in self.settings.allowed_chats

    def write_health(self):
        from modules.transform.pipelines.db.samjeon_material_agent.contracts import atomic_json
        atomic_json(self.store.root / 'health.json', {'checked_at': self.clock().isoformat(),
                    'last_snapshot': self.store.get('last_snapshot'),
                    'ai': self.store.get('ai_status'), 'data_notice': getattr(self.repo, 'fallback_reason', None),
                    **self.store.health()})

    async def current(self):
        try:
            result = await asyncio.to_thread(self.repo.current)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SnapshotUnavailable('게시본 검증 실패') from exc
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
        notes = self.store.notes(chat, analysis['id'])
        saved = self.store.grounded_answer(chat, analysis, notes)
        document = await asyncio.to_thread(create_pdf, self.settings, self.repo, analysis, notes, saved)
        if not cancelled.is_set():
            await self.send(f'update-{uid}', chat,
                f"{analysis['date_from']} ~ {analysis['date_to']} 상세 근거입니다. 대화와 같은 분석 판본입니다.",
                document=document, cancelled=cancelled)

    async def analyze_text(self, uid, user, chat, text, path, context, analysis, cancelled, request_date=None, resolved=None):
        history = self.store.turns(chat, analysis, self.clock())
        plan = await asyncio.to_thread(self.model.plan, text, {
            'snapshot_date': context['as_of_date'], 'request_date': request_date,
            'resolved_period': list(resolved) if resolved else None, 'date_from': analysis['date_from'],
            'date_to': analysis['date_to'], 'selected_product': analysis.get('focus'),
            'history': [{'question': r['question'], 'answer': r['answer']} for r in history]}, cancelled)
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(),
                       'stage': 'plan', 'error': getattr(self.model, 'last_error', None)})
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        if plan is None:
            return False
        plan = validate_plan(plan)
        if plan['action'] == 'clarify':
            # 질문은 질문으로만 표시하며 계산 결과로 취급하지 않는다.
            await self.send(f'update-{uid}', chat, '확인 질문: ' + plan['clarification'], cancelled=cancelled)
            return True
        if plan['action'] == 'pdf':
            await self.pdf(uid, chat, analysis, cancelled)
            return True
        if plan['action'] == 'note':
            self.store.note(chat, analysis['id'], user, text)
            await self.send(f'update-{uid}', chat, '이 분석에 설명을 기록했습니다. 다음 분석에 참고하며 원천 검수 승인으로 처리하지 않습니다.', cancelled=cancelled)
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
            'facts': facts, 'notes': [{'id': f'N{i + 1}', 'text': n['text']} for i, n in enumerate(model_notes)]}, cancelled)
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        error = getattr(self.model, 'last_error', None)
        try:
            rendered = render_answer(answer, facts, model_notes)
        except (ValueError, TypeError):
            error = error or 'invalid_answer'
            rendered = 'AI 설명을 완료하지 못해 확인된 조회 결과를 안내합니다.\n\n' + '\n\n'.join(f['text'] for f in facts)
        rendered += self.data_notice(context)
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(), 'stage': 'answer', 'error': error})
        self.store.save_answer(chat, analysis, notes, rendered)
        delivered = await self.send(f'update-{uid}', chat, rendered, buttons(analysis['id']), cancelled=cancelled)
        if delivered and not cancelled.is_set():
            self.store.remember_turn(chat, analysis, text, rendered, self.clock())
        return True

    async def chat(self, uid, chat, text, cancelled):
        answer = None
        if self.settings.ai_enabled and self.settings.ai_verified and hasattr(self.model, 'chat'):
            try:
                history = self.store.chat_turns(chat, self.clock())
                answer = await asyncio.to_thread(self.model.chat, redact(text, self.settings.token),
                    redact({'history': [{'question': r['question'], 'answer': r['answer']} for r in history]},
                           self.settings.token), cancelled)
                answer = validate_chat(answer) if answer else None
            except Exception:
                answer = None
        if cancelled.is_set() or self.store.cancelled(uid):
            return True
        self.store.put('ai_status', {'checked_at': self.clock().isoformat(), 'stage': 'chat',
                                    'error': None if answer else 'chat_unavailable'})
        if answer and answer['kind'] == 'data_request':
            return False
        rendered = AI_UNAVAILABLE if not answer else CLARIFY if answer['kind'] == 'clarify' else redact(answer['text'], self.settings.token)
        delivered = await self.send(f'update-{uid}', chat, rendered, cancelled=cancelled)
        if delivered and answer and answer['kind'] == 'reply' and not cancelled.is_set():
            self.store.remember_chat(chat, redact(text, self.settings.token), rendered, self.clock())
            self.store.remember_topic(chat, 'chat', self.clock())
        return True

    async def help(self):
        try:
            await self.current()
            data_ready = True
        except SnapshotUnavailable:
            data_ready = False
        ai_ready = self.settings.ai_enabled and self.settings.ai_verified
        if ai_ready and hasattr(self.model, 'diagnose'):
            ai_ready = (await asyncio.to_thread(self.model.diagnose))['ready']
        return help_text(ai_ready, data_ready)

    async def ingest(self, updates):
        updates = redact(updates, self.settings.token)
        fresh = [u for u in updates if not self.store.db.execute('SELECT 1 FROM updates WHERE id=?', (u['update_id'],)).fetchone()]
        self.store.receive(updates)
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
            analysis = self.store.context(chat, self.clock())
            previous_analysis = analysis
            topic = self.store.topic(chat, self.clock())
            if not callback:
                kind = route(text, topic.get('topic') or ('data' if analysis else None))
                if kind == 'help':
                    await self.send(f'update-{uid}', chat, await self.help(), cancelled=cancelled)
                    return
                if kind == 'clarify':
                    await self.send(f'update-{uid}', chat, CLARIFY, cancelled=cancelled)
                    return
                if kind == 'chat' and await self.chat(uid, chat, text, cancelled):
                    return
            self.store.remember_topic(chat, 'data', self.clock())
            # Telegram의 원래 수신 시각을 사용하여 대기 중 자정을 넘겨도 의미가 변하지 않는다.
            received = datetime.fromtimestamp(message['date'], KST) if isinstance(message.get('date'), (int, float)) else self.clock()
            request_date = str(received.astimezone(KST).date())
            resolved = period(text, request_date) if not callback else None
            if callback:
                token, separator, action = callback.get('data', '').partition(':')
                analysis = self.store.analysis(token, chat)
                if not separator or not analysis or (action not in ('why', 'items', 'meal', 'pdf') and not action.startswith('pick')):
                    raise ValueError('이 대화에서 확인할 수 없는 버튼입니다. 새로 질문해 주세요')
                if action.startswith('pick'):
                    choices = self.store.get('choices-' + token, [])
                    index = int(action[4:])
                    if not 0 <= index < len(choices):
                        raise ValueError('품목 선택 범위 오류')
                    request = {'action': 'query', 'query': {'intent': 'compare', 'product_key': choices[index]['product_key']}}
                else:
                    request = {'why': {'action': 'explain'}, 'items': {'action': 'query', 'query': {'intent': 'compare'}},
                               'meal': {'action': 'query', 'query': {'intent': 'staff_meal'}}, 'pdf': {'action': 'pdf'}}[action]
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
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    raise SnapshotUnavailable('대화 판본 검증 실패') from exc
            else:
                path, context = current_path, current_context
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
            if request is None:
                answer = '어느 기간의 매출·구매·직원식대가 궁금하신가요? 예: “최근 7일 구매 비교”, “이번 달 직원식대”.'
            elif request['action'] == 'note':
                self.store.note(chat, aid, user, text)
                answer = '알려주신 설명을 이 분석에 기록했습니다. 이어서 설명드릴 때 함께 표시하겠습니다. 원천 금액과 품목·마감 검수 상태는 그대로 유지합니다.'
            elif request['action'] == 'help':
                answer = await self.help()
            elif request['action'] == 'pdf':
                await self.pdf(uid, chat, analysis, cancelled)
                return
            elif request['action'] == 'explain':
                # 대화 기간이 바뀐 뒤에도 해당 기간의 조회를 사용한다.
                if analysis.get('focus'):
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
                delivered = await self.send(f'update-{uid}', chat, answer, buttons(aid), cancelled=cancelled)
                if delivered and not callback and request and request['action'] in ('query', 'explain') and not cancelled.is_set():
                    self.store.remember_turn(chat, analysis, text, answer, self.clock())
        except SnapshotUnavailable:
            self.store.update_status(uid, 'failed', 'snapshot_unavailable')
            self.store.remember_topic(chat, 'data', self.clock(), 'unavailable')
            if not cancelled.is_set():
                await self.send(f'error-{uid}', chat, DATA_UNAVAILABLE, cancelled=cancelled)
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
            self.write_health()

    async def monitor_once(self):
        if not self.settings.auto_send:
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
            delivered = await self.send(daily_key, chat, briefing(context), buttons(aid_record['id']))
            # 불명확한 발송도 처리 이력에 보존해 snapshot 변경으로 재전송하지 않는다.
            self.store.put(daily_key, {'attempted': True, 'delivered': delivered, 'snapshot': context['snapshot_id']})
            self.store.mark_alerts(chat, [x['key'] for x in items[:3]], aid_record['id'])
        last_snapshot = self.store.get('last_snapshot', {})
        unseen = set(self.store.unseen(chat, [x['key'] for x in items])) if last_snapshot.get('id') != context['snapshot_id'] else set()
        selected = [x for x in items if x['key'] in unseen][:3]
        if selected:
            key = 'alert-' + str(chat) + '-' + hashlib.sha256('|'.join(sorted(x['key'] for x in selected)).encode()).hexdigest()[:24]
            aid_record = self.analysis(chat, path, context, key)
            await self.send(key, chat, briefing(context, selected), buttons(aid_record['id']))
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
        async def poll():
            while True:
                try:
                    updates = await asyncio.to_thread(self.transport.call, 'getUpdates',
                        {'offset': self.store.get('offset', 0), 'timeout': 25, 'allowed_updates': ['message', 'callback_query']})
                    await self.ingest(updates)
                except DeliveryError:
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
                    await self.accept_jobs()
                    await self.monitor_once()
                    self.write_health()
                except Exception as exc:
                    log.error('자료 확인 실패: %s', type(exc).__name__)
                await asyncio.sleep(self.settings.poll_seconds)
        await asyncio.gather(poll(), worker(), monitor())
