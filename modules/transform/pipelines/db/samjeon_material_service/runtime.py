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
from .repository import Repository
from .telegram import Telegram, DeliveryError
from .codex_adapter import CodexAdapter
from .conversation import parse, HELP
from .presentation import briefing, alert_items, buttons, chunks, render_query
from .pdf_report import create_pdf

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
        self.clock = clock
        self.cancellations = {}
        self.delivery_lock = asyncio.Lock()

    def allowed(self, update):
        user, chat, kind, _, _ = identity(update)
        return kind == 'private' and user in self.settings.allowed_users and chat in self.settings.allowed_chats

    def write_health(self):
        from modules.transform.pipelines.db.samjeon_material_agent.contracts import atomic_json
        atomic_json(self.store.root / 'health.json', {'checked_at': self.clock().isoformat(),
                    'last_snapshot': self.store.get('last_snapshot'), **self.store.health()})

    async def ingest(self, updates):
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
            text = message.get('text', '').strip()
            if len(text) > 4000:
                raise ValueError('질문을 4,000자 이내로 나눠 보내주세요')
            if text.split('@')[0] in ('/help', '/start'):
                await self.send(f'update-{uid}', chat, HELP, cancelled=cancelled)
                return
            analysis = self.store.context(chat, self.clock())
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
                if any(word in text for word in ('지금', '최신', '현재')):
                    analysis = None
            if analysis:
                path = Path(analysis['snapshot'])
                context = await asyncio.to_thread(self.repo.context, path, analysis['snapshot_id'])
            else:
                path, context = await asyncio.to_thread(self.repo.current)
                if cancelled.is_set() or self.store.cancelled(uid):
                    return
                analysis = self.analysis(chat, path, context, str(uid))
            if not callback:
                request = parse(text, analysis['date_to'])
            if request is None:
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
                answer = HELP
            elif request['action'] == 'pdf':
                document = await asyncio.to_thread(create_pdf, self.settings, self.repo, analysis, self.store.notes(chat, aid))
                if not cancelled.is_set():
                    await self.send(f'update-{uid}', chat,
                                    f"{analysis['date_from']} ~ {analysis['date_to']} 상세 근거입니다. 대화와 같은 분석 판본입니다.",
                                    document=document, cancelled=cancelled)
                return
            elif request['action'] == 'explain':
                # 대화 기간이 바뀐 뒤에도 해당 기간의 조회를 사용한다.
                if analysis.get('focus'):
                    result = await asyncio.to_thread(self.repo.query, path, 'compare', analysis['date_from'],
                                                     analysis['date_to'], product_key=analysis['focus'])
                    answer = render_query(result)
                elif analysis['date_to'] != context['as_of_date']:
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
                self.store.remember(chat, aid, self.clock() + timedelta(hours=24))
                await self.send(f'update-{uid}', chat, answer, buttons(aid), cancelled=cancelled)
        except (ValueError, FileNotFoundError, KeyError) as exc:
            self.store.update_status(uid, 'failed', type(exc).__name__)
            if not cancelled.is_set():
                # 파일 경로·원천 상세·비밀값이 예외에 포함될 수 있어 그대로 보내지 않는다.
                msg = '자료·조회 조건을 확인하지 못했습니다. 날짜를 YYYY-MM-DD 형식으로 확인해 주세요. '
                msg += '스냅샷이 없거나 판본·해시 검증에 실패하면 운영자 확인이 필요합니다.'
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
            path, context = await asyncio.to_thread(self.repo.current)
            fresh = context['as_of_date'] == yesterday
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
