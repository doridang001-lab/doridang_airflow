"""개인 대화별 논리 세션과 전달 완료 원문의 로컬 기록."""
from datetime import datetime, timedelta, timezone
import json
import re
import uuid


SESSION_HOURS = 72
SCHEMA = 1
PREFERENCE = re.compile(r'기억해|앞으로|항상|존댓말|말투|선호')
BRIEFING_PREFERENCE = re.compile(r'보고|브리핑|매출|원가|비용|주문|수익|원재료|인사이트|간결|짧게|자세히')
RECALL = re.compile(r'지난번|지난 대화|이전 대화|예전|그때|과거|기록\s*(?:찾|보여|가져)|분석\s*가져와')
KST = timezone(timedelta(hours=9))


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class Memory:
    def __init__(self, store):
        self.store = store

    def _put(self, key, value):
        self.store.db.execute(
            'INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)',
            (key, json.dumps(value, ensure_ascii=False)),
        )

    @staticmethod
    def _active_key(user, chat):
        return f'memory-active-v{SCHEMA}-{user}-{chat}'

    @staticmethod
    def _session_key(sid):
        return f'memory-session-v{SCHEMA}-{sid}'

    def session(self, sid):
        return self.store.get(self._session_key(sid)) if sid else None

    def active(self, user, chat):
        return self.session(self.store.get(self._active_key(user, chat)))

    def for_update(self, uid):
        return self.store.get(f'memory-request-v{SCHEMA}-{uid}')

    def _legacy_once(self, user, chat, received):
        marker = f'memory-migrated-v{SCHEMA}-{user}-{chat}'
        if self.store.get(marker):
            return
        rows = self.store.db.execute(
            "SELECT id,payload,created FROM updates WHERE status='done' ORDER BY id"
        ).fetchall()
        valid = []
        for row in rows:
            try:
                update = json.loads(row['payload'])
                message = update.get('message') or {}
                if (message.get('from', {}).get('id'), message.get('chat', {}).get('id')) != (user, chat):
                    continue
                sent = self.store.db.execute(
                    "SELECT payload,state FROM outbox WHERE key LIKE ? ORDER BY key",
                    (f"update-{row['id']}:%",),
                ).fetchall()
                if not sent or any(part['state'] != 'sent' for part in sent):
                    continue
                answer = ''.join(json.loads(part['payload']).get('text', '') for part in sent)
                question = message.get('text', '')
                if question and answer and answer not in (
                    '지금은 대화 기능을 사용할 수 없습니다. 잠시 후 다시 말씀해 주세요.',
                ):
                    valid.append((row, question, answer))
            except (ValueError, TypeError, KeyError):
                continue
        legacy_sid = None
        if valid:
            sid = uuid.uuid4().hex
            legacy_sid = sid
            old = self.store.db.execute('SELECT analysis FROM conversations WHERE chat=?', (chat,)).fetchone()
            analysis_id = old['analysis'] if old and self.store.analysis(old['analysis'], chat) else None
            session = dict(version=SCHEMA, id=sid, user=user, chat=chat,
                           created_at=utc(valid[0][0]['created']).isoformat(),
                           previous=None, title='이전 대화', summary='', preferences=[],
                           unresolved=[], analysis_id=analysis_id, last_query=None,
                           summary_attempted=True)
            self._put(self._session_key(sid), session)
            if analysis_id:
                self._put(f'memory-analysis-v{SCHEMA}-{analysis_id}', sid)
            for row, question, answer in valid:
                basis = re.search(r'자료 기준일\s*(\d{4}-\d{2}-\d{2})', answer)
                period = re.search(r'(\d{4}-\d{2}-\d{2})\s*[~∼]\s*(\d{4}-\d{2}-\d{2})', answer)
                self._put(f"memory-turn-v{SCHEMA}-{row['id']}", dict(
                    version=SCHEMA, update_id=row['id'], session_id=sid, user=user, chat=chat,
                    question=question, answer=answer, created_at=utc(row['created']).isoformat(),
                    analysis_id=None, snapshot_id=None,
                    snapshot_date=basis[1] if basis else None,
                    period=list(period.groups()) if period else None,
                    topic='data' if basis else 'legacy'))
        self._put(marker, legacy_sid or True)

    def select(self, uid, user, chat, received):
        """요청과 활성 세션을 한 트랜잭션에서 고정한다."""
        now = utc(received)
        with self.store.db:
            existing = self.for_update(uid)
            if existing:
                return self.session(existing), False
            self._legacy_once(user, chat, now)
            active = self.active(user, chat)
            legacy_sid = self.store.get(f'memory-migrated-v{SCHEMA}-{user}-{chat}')
            previous = active or (self.session(legacy_sid) if isinstance(legacy_sid, str) else None)
            rotate = active is None or now >= utc(active['created_at']) + timedelta(hours=SESSION_HOURS)
            if rotate:
                sid = uuid.uuid4().hex
                history = self.recent(previous['id'], 3) if previous else []
                fallback = (previous.get('summary', '') + '\n' +
                            '\n'.join(f"{turn['question'][:120]}: {turn['answer'][:240]}" for turn in history)
                            ).strip()[-2000:] if previous else ''
                session = dict(version=SCHEMA, id=sid, user=user, chat=chat,
                               created_at=now.isoformat(), previous=previous['id'] if previous else None,
                               title=None, summary=fallback,
                               preferences=list(previous.get('preferences', [])) if previous else [],
                               unresolved=list(previous.get('unresolved', [])) if previous else [],
                               analysis_id=previous.get('analysis_id') if previous else None,
                               last_query=previous.get('last_query') if previous else None,
                               summary_attempted=previous is None)
                self._put(self._session_key(sid), session)
                self._put(self._active_key(user, chat), sid)
            else:
                session = previous
            self._put(f'memory-request-v{SCHEMA}-{uid}', session['id'])
            return session, bool(rotate and previous)

    def claim_summary(self, sid):
        with self.store.db:
            session = self.session(sid)
            if not session or session['summary_attempted'] or not session['previous']:
                return None
            session['summary_attempted'] = True
            self._put(self._session_key(sid), session)
            return session

    def save_summary(self, sid, value):
        if not isinstance(value, str) or not value.strip() or len(value) > 2000:
            return False
        with self.store.db:
            session = self.session(sid)
            if not session:
                return False
            session['summary'] = value.strip()
            self._put(self._session_key(sid), session)
        return True

    def set_analysis(self, sid, aid):
        if not sid:
            return
        with self.store.db:
            session = self.session(sid)
            if session:
                session['analysis_id'] = aid
                self._put(self._session_key(sid), session)
                self._put(f'memory-analysis-v{SCHEMA}-{aid}', sid)

    def analysis_owner(self, aid):
        return self.session(self.store.get(f'memory-analysis-v{SCHEMA}-{aid}'))

    def record(self, uid, user, chat, question, answer, received, analysis=None, topic='chat',
               snapshot_date=None, query=None):
        sid = self.for_update(uid)
        if not sid or not question or not answer:
            return False
        with self.store.db:
            parts = self.store.db.execute(
                'SELECT state FROM outbox WHERE key LIKE ? ORDER BY key', (f'update-{uid}:%',)
            ).fetchall()
            if not parts or any(row['state'] != 'sent' for row in parts) or self.store.cancelled(uid):
                return False
            key = f'memory-turn-v{SCHEMA}-{uid}'
            if self.store.get(key):
                return True
            turn = dict(version=SCHEMA, update_id=uid, session_id=sid, user=user, chat=chat,
                        question=question, answer=answer, created_at=utc(received).isoformat(),
                        analysis_id=analysis['id'] if analysis else None,
                        snapshot_id=analysis['snapshot_id'] if analysis else None,
                        snapshot_date=snapshot_date,
                        period=[analysis['date_from'], analysis['date_to']] if analysis else None,
                        topic=topic)
            self._put(key, turn)
            session = self.session(sid)
            if PREFERENCE.search(question):
                entry = {'text': question[:300], 'update_id': uid}
                session['preferences'] = (session['preferences'] + [entry])[-5:]
            if topic == 'clarify' or '확인 질문:' in answer:
                session['unresolved'] = [{'text': answer[:300], 'update_id': uid}]
            elif session['unresolved']:
                session['unresolved'] = []
            if analysis:
                session['analysis_id'] = analysis['id']
                self._put(f'memory-analysis-v{SCHEMA}-{analysis["id"]}', sid)
            if query and topic == 'data':
                session['last_query'] = query
            self._put(self._session_key(sid), session)
        return True

    def recent(self, sid, limit=10):
        if not sid:
            return []
        rows = self.store.db.execute(
            'SELECT value FROM meta WHERE key LIKE ? ORDER BY key',
            (f'memory-turn-v{SCHEMA}-%',),
        ).fetchall()
        turns = sorted((json.loads(row['value']) for row in rows),
                       key=lambda turn: turn['update_id'])
        return [turn for turn in turns if turn['session_id'] == sid][-limit:]

    def model_context(self, sid):
        session = self.session(sid)
        if not session:
            return {'history': []}
        analysis = self.store.analysis(session['analysis_id'], session['chat']) if session['analysis_id'] else None
        active = ({key: analysis[key] for key in ('snapshot_id', 'date_from', 'date_to', 'focus')}
                  if analysis else None)
        if active and (legacy := self.store.get('legacy-view-v1-' + analysis['id'], {})):
            active.update(format=legacy.get('format'), requested_period=legacy.get('requested_period'))
        return {
            'history': [{'question': turn['question'][:1000], 'answer': turn['answer'][:1500]}
                        for turn in self.recent(sid)],
            'preferences': [entry['text'] for entry in session['preferences']],
            'inherited_summary': session['summary'][:2000],
            'unresolved_questions': [entry['text'] for entry in session['unresolved']],
            'active_analysis': active,
        }

    def briefing_context(self, user, chat, now):
        """해당 개인 대화에서 전달 완료된 경영 질문만 오전 보고의 관심사로 제공한다."""
        session = self.active(user, chat)
        preferences = ([entry['text'][:300] for entry in session['preferences']
                        if BRIEFING_PREFERENCE.search(entry['text'])][-5:] if session else [])
        cutoff = utc(now) - timedelta(days=14)
        rows = self.store.db.execute('SELECT value FROM meta WHERE key LIKE ?',
                                     (f'memory-turn-v{SCHEMA}-%',)).fetchall()
        turns = sorted((json.loads(row['value']) for row in rows),
                       key=lambda turn: turn['update_id'])
        questions = [turn['question'][:200] for turn in turns
                     if (turn['user'], turn['chat']) == (user, chat)
                     and turn['topic'] == 'data' and utc(turn['created_at']) >= cutoff]
        return {'preferences': preferences, 'recent_business_questions': questions[-5:]}

    def is_recall(self, text):
        return bool(RECALL.search(text))

    def search(self, user, chat, text, received=None):
        match = re.search(r'(?<!\d)(\d{1,2})월', text)
        year = re.search(r'(?<!\d)(20\d{2})년', text)
        dates = re.findall(r'(?<!\d)20\d{2}-\d{2}-\d{2}(?!\d)', text)
        rows = self.store.db.execute(
            'SELECT value FROM meta WHERE key LIKE ? ORDER BY key',
            (f'memory-turn-v{SCHEMA}-%',),
        ).fetchall()
        turns = sorted((json.loads(row['value']) for row in rows),
                       key=lambda turn: turn['update_id'])
        turns = [turn for turn in turns if (turn['user'], turn['chat']) == (user, chat)]
        if match:
            month = f'-{int(match[1]):02d}-'
            turns = [turn for turn in turns if
                     (month in str(turn['period']) if turn.get('period') else
                      match[0] in turn['question'] or match[0] in turn['answer'] or
                      month in turn['question'] or month in turn['answer'])]
        if year:
            turns = [turn for turn in turns if year[1] in str(turn.get('period') or '') or
                     year[1] in turn['question']]
        if dates:
            if len(dates) > 1:
                turns = [turn for turn in turns if turn.get('period') == dates[:2]]
            else:
                day = dates[0]
                turns = [turn for turn in turns if
                         utc(turn['created_at']).astimezone(KST).date().isoformat() == day or
                         day in (turn.get('period') or [])]
        elif received and '어제' in text:
            day = (utc(received).astimezone(KST).date() - timedelta(days=1)).isoformat()
            turns = [turn for turn in turns if
                     utc(turn['created_at']).astimezone(KST).date().isoformat() == day]
        if '대화' in text and '분석' not in text and not match:
            turns = [turn for turn in turns if turn['topic'] in ('chat', 'legacy')]
        cleaned = RECALL.sub(' ', text)
        cleaned = re.sub(r'20\d{2}-\d{2}-\d{2}|20\d{2}년|\d{1,2}월|지난|이번|어제|분석|자료|대화|답변|보여줘|가져와|찾아줘', ' ', cleaned)
        terms = re.findall(r'[가-힣A-Za-z]{2,}', cleaned)
        if terms:
            turns = [turn for turn in turns if all(
                term.casefold() in (turn['question'] + ' ' + turn['answer'] + ' ' +
                                    turn['topic']).casefold() for term in terms)]
        groups = {}
        for turn in turns:
            key = (turn['session_id'], tuple(turn.get('period') or ()), turn.get('snapshot_id'))
            groups.setdefault(key, turn)
        return list(groups.values())[-5:][::-1]

    def search_agent(self, user, chat, text):
        """같은 개인 대화에서 실제 전달된 원문 턴을 찾는다."""
        if not isinstance(text, str) or not text.strip():
            return []
        rows = self.store.db.execute(
            'SELECT value FROM meta WHERE key LIKE ?', (f'memory-turn-v{SCHEMA}-%',)).fetchall()
        terms = re.findall(r'[가-힣A-Za-z0-9]{2,}', text.casefold())
        terms = [term for term in terms if term not in
                 ('지난번', '지난', '이번', '대화', '분석', '기록', '보여줘', '찾아줘', '가져와')]
        turns = (json.loads(row['value']) for row in rows)
        matched = [turn for turn in turns if (turn.get('user'), turn.get('chat')) == (user, chat)
                   and (not terms or all(term in (turn['question'] + ' ' + turn['answer'] + ' ' +
                        str(turn.get('period') or '')).casefold() for term in terms))]
        return sorted(matched, key=lambda turn: turn['update_id'], reverse=True)[:8]
