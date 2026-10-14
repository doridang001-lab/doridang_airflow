"""단일 서비스 소유 SQLite와 메시지 발송 원장."""
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from .config import local_path


def stamp():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, root):
        self.root = local_path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'state.sqlite3')
        self.db.row_factory = sqlite3.Row
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version == 0 and self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchone():
            self.db.close()
            raise ValueError('판본이 없는 기존 DB는 자동으로 초기화하지 않습니다')
        if version not in (0, 1):
            self.db.close()
            raise ValueError('지원하지 않는 상태 DB 판본입니다. 초기화하지 않습니다')
        self.db.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS updates(id INTEGER PRIMARY KEY, payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', error TEXT, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS analyses(id TEXT PRIMARY KEY, chat INTEGER NOT NULL, snapshot TEXT NOT NULL,
            snapshot_id TEXT NOT NULL, date_from TEXT NOT NULL, date_to TEXT NOT NULL,
            focus TEXT, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS conversations(chat INTEGER PRIMARY KEY, analysis TEXT NOT NULL, expires TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, chat INTEGER NOT NULL,
            analysis TEXT NOT NULL, user INTEGER NOT NULL, text TEXT NOT NULL, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox(key TEXT PRIMARY KEY, chat INTEGER NOT NULL, payload TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending', message_id INTEGER, error TEXT, created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS alerts(chat INTEGER NOT NULL, alert_key TEXT NOT NULL, analysis TEXT NOT NULL,
            created TEXT NOT NULL, PRIMARY KEY(chat, alert_key));
        PRAGMA user_version=1;
        ''')
        # 전송 직후 중단됐을 수 있으므로 보내던 메시지는 재발송하지 않는다.
        with self.db:
            self.db.execute("UPDATE outbox SET state='uncertain', error='재시작 중 전송 결과 확인 필요' WHERE state='sending'")
            self.db.execute("UPDATE updates SET status='interrupted', error='서비스 재시작' WHERE status='processing'")

    def close(self):
        self.db.close()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', (key, json.dumps(value, ensure_ascii=False)))

    def receive(self, updates):
        with self.db:
            offset = self.get('offset', 0)
            for u in updates:
                uid = u['update_id']
                self.db.execute('INSERT OR IGNORE INTO updates(id,payload,created) VALUES(?,?,?)',
                                (uid, json.dumps(u, ensure_ascii=False), stamp()))
                offset = max(offset, uid + 1)
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', ('offset', json.dumps(offset)))

    def next_update(self):
        row = self.db.execute("SELECT * FROM updates WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
        return json.loads(row['payload']) if row else None

    def update_status(self, uid, status, error=None):
        with self.db:
            self.db.execute('UPDATE updates SET status=?, error=? WHERE id=?', (status, error, uid))

    def cancel(self, chat):
        cancelled = []
        with self.db:
            for row in self.db.execute("SELECT id,payload FROM updates WHERE status IN ('queued','processing')").fetchall():
                u = json.loads(row['payload'])
                msg = u.get('message') or u.get('callback_query', {}).get('message', {})
                if msg.get('chat', {}).get('id') == chat:
                    self.db.execute("UPDATE updates SET status='cancelled' WHERE id=?", (row['id'],))
                    cancelled.append(row['id'])
        return cancelled

    def cancelled(self, uid):
        row = self.db.execute('SELECT status FROM updates WHERE id=?', (uid,)).fetchone()
        return row is None or row[0] == 'cancelled'

    def analysis(self, aid, chat):
        row = self.db.execute('SELECT * FROM analyses WHERE id=? AND chat=?', (aid, chat)).fetchone()
        return dict(row) if row else None

    def add_analysis(self, aid, chat, snapshot, snapshot_id, start, end, focus=None):
        with self.db:
            self.db.execute('INSERT INTO analyses VALUES(?,?,?,?,?,?,?,?)',
                            (aid, chat, str(snapshot), snapshot_id, start, end, focus, stamp()))

    def context(self, chat, now):
        row = self.db.execute('SELECT * FROM conversations WHERE chat=?', (chat,)).fetchone()
        if row and datetime.fromisoformat(row['expires']) > now:
            return self.analysis(row['analysis'], chat)
        return None

    def remember(self, chat, aid, expires):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO conversations VALUES(?,?,?)', (chat, aid, expires.isoformat()))

    def note(self, chat, aid, user, message):
        with self.db:
            self.db.execute('INSERT INTO notes(chat,analysis,user,text,created) VALUES(?,?,?,?,?)',
                            (chat, aid, user, message[:2000], stamp()))

    def notes(self, chat, aid):
        current = self.analysis(aid, chat)
        if not current:
            return []
        # 동일 기간·품목의 다른 판본에서도 사용자 설명을 보존한다.
        return [dict(r) for r in self.db.execute('''SELECT n.user,n.text,n.created FROM notes n
            JOIN analyses a ON a.id=n.analysis WHERE n.chat=? AND a.date_from=? AND a.date_to=?
            AND COALESCE(a.focus,'')=COALESCE(?,'') ORDER BY n.id''',
            (chat, current['date_from'], current['date_to'], current['focus']))]

    def focus(self, aid, chat, product_key):
        with self.db:
            self.db.execute('UPDATE analyses SET focus=? WHERE id=? AND chat=?', (product_key, aid, chat))

    def turn_scope(self, analysis):
        scope = {k: analysis.get(k) for k in ('snapshot_id', 'date_from', 'date_to', 'focus')}
        legacy = self.get('legacy-view-v1-' + analysis['id'], {})
        if legacy:
            scope.update(format=legacy['format'], requested_period=legacy['requested_period'])
        return scope

    def turns(self, chat, analysis, now):
        scope = self.turn_scope(analysis)
        rows = self.get(f'turns-v1-{chat}', [])
        return [r for r in rows if r['scope'] == scope and
                (now - datetime.fromisoformat(r['created'])).total_seconds() < 86400][-10:]

    def remember_turn(self, chat, analysis, question, answer, now):
        rows = self.turns(chat, analysis, now)
        rows.append({'scope': self.turn_scope(analysis),
                     'question': question[:1000], 'answer': answer[:1500], 'created': now.isoformat()})
        self.put(f'turns-v1-{chat}', rows[-10:])

    def chat_turns(self, chat, now):
        return [r for r in self.get(f'chat-turns-v1-{chat}', [])
                if 0 <= (now - datetime.fromisoformat(r['created'])).total_seconds() < 86400][-10:]

    def remember_chat(self, chat, question, answer, now):
        rows = self.chat_turns(chat, now)
        rows.append({'question': question[:4000], 'answer': answer[:2000], 'created': now.isoformat()})
        self.put(f'chat-turns-v1-{chat}', rows[-10:])

    def topic(self, chat, now):
        value = self.get(f'topic-v1-{chat}', {})
        if value and 0 <= (now - datetime.fromisoformat(value['created'])).total_seconds() < 86400:
            return value
        return {}

    def remember_topic(self, chat, topic, now, outcome=None):
        self.put(f'topic-v1-{chat}', {'topic': topic, 'outcome': outcome, 'created': now.isoformat()})

    def save_answer(self, chat, analysis, notes, text):
        from .grounded import scope_signature
        self.put(f'answer-v1-{chat}-{analysis["id"]}',
                 {'signature': scope_signature(analysis, notes), 'text': text})

    def grounded_answer(self, chat, analysis, notes):
        from .grounded import scope_signature
        record = self.get(f'answer-v1-{chat}-{analysis["id"]}', {})
        return record.get('text') if record.get('signature') == scope_signature(analysis, notes) else None

    def enqueue(self, key, chat, payload):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO outbox(key,chat,payload,created) VALUES(?,?,?,?)',
                            (key, chat, json.dumps(payload, ensure_ascii=False), stamp()))

    def outgoing(self, prefix):
        return [dict(r) for r in self.db.execute('SELECT * FROM outbox WHERE key LIKE ? ORDER BY key', (prefix + ':%',))]

    def sent_state(self, key, state, message_id=None, error=None):
        with self.db:
            self.db.execute('UPDATE outbox SET state=?,message_id=?,error=? WHERE key=?', (state, message_id, error, key))

    def unseen(self, chat, keys):
        return [k for k in keys if not self.db.execute('SELECT 1 FROM alerts WHERE chat=? AND alert_key=?', (chat, k)).fetchone()]

    def mark_alerts(self, chat, keys, aid):
        with self.db:
            for key in keys:
                self.db.execute('INSERT OR IGNORE INTO alerts VALUES(?,?,?,?)', (chat, key, aid, stamp()))

    def health(self):
        return {name: [dict(r) for r in self.db.execute(f'SELECT {column},count(*) AS count FROM {name} GROUP BY {column}')]
                for name, column in (('updates', 'status'), ('outbox', 'state'))}
