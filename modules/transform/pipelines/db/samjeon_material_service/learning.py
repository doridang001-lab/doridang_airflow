"""사용자 소유 학습 기록. 모델은 후보만 만들고 확정은 원문 명령으로 처리한다."""
import hashlib
import json
import re

from .analysis_tools import spec
from .storage import stamp

KINDS = ('purpose', 'rule', 'workflow', 'example', 'feature')
TOOLS = [
    spec('list_learning', '현재 사용자의 학습 후보·확정 기준과 판본을 10개씩 조회한다.', {'offset': {'type': 'integer'}}),
    spec('propose_learning', '업무 방식 또는 기능 개선 후보를 저장한다. 확정·공유는 사용자가 수행한다.',
         {'name': {'type': 'string'}, 'kind': {'type': 'string', 'enum': list(KINDS)},
          'content': {'type': 'string'}, 'expected_revision': {'type': 'integer'},
          'feature_details': {'type': 'object', 'properties': {
              k: {'type': 'string'} for k in ('current', 'expected', 'example', 'acceptance', 'tools')},
              'additionalProperties': False}},
         ('name', 'kind', 'content', 'expected_revision')),
]


def key(user, chat, assistant='samjeon'):
    return f'assistant-learning-v1-{assistant}-{user}-{chat}'


class Learning:
    def __init__(self, store, settings, user, chat):
        self.store, self.settings, self.user, self.chat = store, settings, user, chat
        self.key = key(user, chat, settings.assistant_id)

    def records(self):
        return self.store.get(self.key, {})

    def _invalidate(self):
        session = self.store.memory.active(self.user, self.chat)
        if session:
            k = 'codex-thread-v3-' + session['id']
            self.store.put(k, {**self.store.get(k, {}), 'dirty': True})

    def list(self):
        return [{k: v for k, v in row.items() if k not in ('history', 'source_text')}
                for row in self.records().values()]

    def propose(self, args, uid, source):
        if not isinstance(args, dict):
            return {'error': 'arguments_invalid'}
        name, kind, content, expected = (args.get(k) for k in ('name', 'kind', 'content', 'expected_revision'))
        if (not isinstance(name, str) or not 1 <= len(name.strip()) <= 80 or kind not in KINDS
                or not isinstance(content, str) or not 1 <= len(content.strip()) <= 3000
                or type(expected) is not int or expected < 0 or not source):
            return {'error': 'learning_invalid'}
        name, content = name.strip(), content.strip()
        identifier = hashlib.sha256(name.encode()).hexdigest()[:12]
        rows = self.records()
        old = rows.get(identifier)
        request_hash = hashlib.sha256(json.dumps([uid, args], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if old and old.get('request_hash') == request_hash:
            return {'saved': True, 'id': identifier, 'revision': old['revision'], 'status': old['status'],
                    'confirm_command': f"/학습확정 {identifier} {old['revision']}"}
        if expected != (old['revision'] if old else 0):
            return {'error': 'learning_changed', 'revision': old['revision'] if old else 0}
        if not old and len(rows) >= 100:
            return {'error': 'learning_limit'}
        details = args.get('feature_details', {})
        if (not isinstance(details, dict) or set(details) - {'current','expected','example','acceptance','tools'}
                or any(not isinstance(v, str) or not 1 <= len(v.strip()) <= 1500 for v in details.values())
                or details and kind != 'feature'):
            return {'error': 'feature_details_invalid'}
        history = (old.get('history', []) + [{k: v for k, v in old.items() if k != 'history'}]) if old else []
        rows[identifier] = {'id': identifier, 'name': name, 'kind': kind, 'content': content,
                            'revision': expected + 1, 'status': 'candidate', 'shared_revision': None,
                            'source_update_id': uid, 'source_text': source[:4000],
                            'request_hash': request_hash, 'updated_at': stamp(), 'history': history}
        rows[identifier]['feature_details'] = details
        rows[identifier]['effective'] = ({k: old[k] for k in ('id', 'name', 'kind', 'content', 'revision')}
            if old and old['status'] == 'active' else old.get('effective') if old and old['status'] == 'candidate' else None)
        self.store.put(self.key, rows)
        self._invalidate()
        return {'saved': True, 'id': identifier, 'revision': expected + 1, 'status': 'candidate',
                'confirm_command': f'/학습확정 {identifier} {expected + 1}'}

    def command(self, text):
        if text.strip() == '/학습목록':
            return '\n\n'.join(f"{r['id']} v{r['revision']} [{r['status']}] {r['name']}\n{r['content'][:120]}"
                               for r in self.list()) or '저장된 학습 항목이 없습니다.'
        if text.startswith('/학습보기 '):
            row = self.records().get(text.partition(' ')[2].strip())
            return (f"{row['id']} v{row['revision']} [{row['status']}] {row['name']}\n{row['content']}\n"
                    + json.dumps(row.get('feature_details', {}), ensure_ascii=False)) if row else '학습 항목이 없습니다.'
        if not text.startswith(('/학습확정', '/학습공유', '/학습삭제')):
            return None
        parts = text.split()
        if len(parts) != 3 or not re.fullmatch(r'[0-9a-f]{12}', parts[1]) or not parts[2].isdigit():
            return '/학습확정 ID 판본, /학습공유 ID 판본, /학습삭제 ID 판본 형식으로 입력하세요.'
        action, identifier, revision = parts
        if action not in ('/학습확정', '/학습공유', '/학습삭제'):
            return '지원하지 않는 학습 명령입니다.'
        rows = self.records()
        row = rows.get(identifier)
        if not row or row['revision'] != int(revision):
            return '학습 항목이 없거나 판본이 변경되었습니다. /학습목록에서 확인하세요.'
        if row['status'] == 'deleted':
            return '삭제된 판본입니다. 새 후보를 작성해야 다시 확정할 수 있습니다.'
        if action == '/학습확정' and row['status'] == 'active':
            return '이미 확정된 판본입니다.'
        if action == '/학습공유':
            if self.settings.package_owner_user != self.user:
                return '공유 확정은 설정에 지정된 대표만 할 수 있습니다.'
            if row['status'] != 'active':
                return '먼저 현재 판본을 /학습확정으로 확정하세요.'
            row['shared_revision'] = row['revision']
            row['shared_by'] = self.user
            message = '공유 후보로 확정했습니다. 아직 내보내거나 배포하지 않았습니다.'
        else:
            row['history'].append({k: v for k, v in row.items() if k != 'history'})
            row['status'] = 'active' if action == '/학습확정' else 'deleted'
            row['effective'] = None
            row['shared_revision'] = None
            message = ('개인 업무 기준으로 확정했습니다.' if row['kind'] != 'feature' else
                       '기능 개발 요청으로 확정했습니다. 기능은 아직 구현되지 않았습니다.') if action == '/학습확정' else '학습 기준을 삭제했습니다.'
        row['updated_at'] = stamp()
        self.store.put(self.key, rows)
        # 이전 /기억으로 생성된 기준은 두 화면에서 같은 판본을 보여준다.
        legacy_key = f'ceo-rules-v1-{self.user}-{self.chat}'
        legacy = self.store.get(legacy_key, {})
        if row['name'] in legacy and action != '/학습공유':
            if row['status'] == 'deleted' or row['kind'] == 'feature':
                legacy.pop(row['name'], None)
            else:
                legacy[row['name']] = {'text': row['content'], 'update_id': row['source_update_id']}
            self.store.put(legacy_key, legacy)
        self._invalidate()
        return message

    def context(self, question):
        terms = re.findall(r'[가-힣A-Za-z0-9]{2,}', question.casefold())
        rows = list(self.records().values())
        effective = [r if r['status'] == 'active' else r.get('effective') for r in rows]
        active = [r for r in effective if r and r['kind'] != 'feature']
        core = [r for r in active if r['kind'] in ('purpose', 'workflow')]
        rest = [r for r in active if r not in core]
        rest.sort(key=lambda r: sum(t in (r['name'] + r['content']).casefold() for t in terms), reverse=True)
        selected, size = [], 0
        for row in core + rest:
            if size + len(row['content']) > 12000:
                continue
            selected.append({k: row[k] for k in ('id', 'name', 'kind', 'content', 'revision')})
            size += len(row['content'])
        return {'active_learning': selected,
                'omitted_learning_count': len(active) - len(selected),
                'retired_learning': [r['name'] for r in rows if r['status'] != 'active' and not r.get('effective')],
                'learning_notice': '현재 유효한 개인 기준만 적용합니다. 미구현 기능과 과거 수치는 사실로 쓰지 않습니다.'}
