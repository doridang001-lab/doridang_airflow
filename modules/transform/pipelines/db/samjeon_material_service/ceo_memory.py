"""명시적인 기억 명령만 저장하며 사용자·대화별로 분리한다."""
import re


def records(store, user, chat):
    return store.get(f'ceo-rules-v1-{user}-{chat}', {})


def command(store, user, chat, text, uid):
    key = f'ceo-rules-v1-{user}-{chat}'
    rules = records(store, user, chat)
    def invalidate():
        session = store.memory.active(user, chat)
        if session:
            binding_key = 'codex-thread-v3-' + session['id']
            binding = store.get(binding_key, {})
            store.put(binding_key, {**binding, 'dirty': True})
    if text.strip() == '/기억목록':
        return '\n'.join(f'{name}: {r["text"]}' for name, r in rules.items()) or '저장된 확정 기준이 없습니다.'
    if text.startswith('/기억삭제 '):
        name = text.partition(' ')[2].strip()
        rules.pop(name, None)
        store.put(key, rules)
        invalidate()
        return f'기준을 삭제했습니다: {name[:80]}'
    match = re.match(r'^(?:/기억\s+|기억해:\s*)([^=\n]{1,80})=(.{1,1000})$', text, re.S)
    if match is None:
        natural = re.fullmatch(r'(?:기억해(?:줘)?:\s*(.{5,1000})|(.{5,1000}?)\s*[.。]?\s*기억해(?:줘)?[.!]?)', text, re.S)
        if natural:
            value = next(v for v in natural.groups() if v).strip(' .\n')
            if any(word in value for word in ('아까 말한', '이것', '그것', '이거', '그거')):
                return '기억할 기준을 한 문장으로 적어 주세요. 예: 기억해: 매출은 같은 기간으로 비교'
            return command(store, user, chat, '/기억 ' + value[:40].replace('=', ' ') + '=' + value, uid)
    if match:
        name, value = (part.strip() for part in match.groups())
        if not name or not value:
            return '이름과 기준을 함께 적어 주세요: /기억 이름=기준'
        if len(rules) >= 100 and name not in rules:
            return '기준은 최대 100개입니다. 사용하지 않는 기준을 먼저 삭제해 주세요.'
        rules[name] = {'text': value, 'update_id': uid}
        store.put(key, rules)
        invalidate()
        return f'확정 기준을 기억했습니다: {name} = {value}'
    if text.startswith(('/기억', '기억해:')):
        return '/기억 이름=기준 형식으로 입력해 주세요.'
    return None


def relevant(store, user, chat, question):
    terms = re.findall(r'[가-힣A-Za-z0-9]{2,}', question.casefold())
    scored = []
    for name, entry in records(store, user, chat).items():
        content = name + ' ' + entry['text']
        score = sum(term in content.casefold() or name.casefold() in term for term in terms)
        if score or any(word in name for word in ('답변', '보고', '선호')):
            scored.append((score, name, entry['text']))
    return [{'name': name, 'text': text} for _, name, text in sorted(scored, reverse=True)[:5]]


def context(store, user, chat, session, question):
    previous = session.get('previous') or session['id']
    history = store.memory.recent(session['id'], 3) or store.memory.recent(previous, 3)
    last_report = (store.get(f'last-scheduled-report-v1-{user}-{chat}')
                   if re.search(r'보고|리포트|브리핑|이거|이것|이\s*내용', question) else None)
    return {'remembered_rules': relevant(store, user, chat, question),
            'latest_scheduled_report': last_report,
            'inherited_preferences': [entry['text'][:300] for entry in session.get('preferences', [])[-5:]],
            'inherited_summary': session.get('summary', '')[:1200],
            'handoff': [{'question': row['question'][:400], 'answer': row['answer'][:700],
                         'snapshot_date': row.get('snapshot_date')} for row in history],
            'notice': '이전 답변은 과거 참고 자료입니다. 현재 수치는 다시 조회하세요.'}
