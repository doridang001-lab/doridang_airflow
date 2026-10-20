"""실행 코드·개인 상태를 포함하지 않는 비서 패키지와 읽기 전용 사전 검사."""
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from contextlib import closing

from .config import local_path
from .learning import key as learning_key
from .prompt_assets import read_prompt
from .tool_registry import CONTRACT_VERSION, definitions

ENGINE_CONTRACT = 'samjeon-assistant-v1'
MAX_BYTES = 256_000
FILES = {'purpose.md', 'instructions.md', 'workflow.md', 'knowledge.md', 'examples.jsonl', 'evals.jsonl'}
SENSITIVE = re.compile(r'(?:[A-Za-z]:[\\/]|/mnt/|/home/|/Users/|/opt/airflow/|'
                       r'\b\d{6,}:[A-Za-z0-9_-]{20,}|\bsk-[A-Za-z0-9_-]{12,}|'
                       r'\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|01[016789][- ]?\d{3,4}[- ]?\d{4}|'
                       r'\d{6}-[1-4]\d{6}|\b\d{10,18}\b|'
                       r'(?:password|api_key|bot_token)\s*[:=]\s*\S+)', re.I)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode('utf-8')


def portable(text):
    if SENSITIVE.search(text):
        raise ValueError('package_sensitive_or_machine_specific_content')


def build(identifier, version, files, required_tools=(), data_inputs=()):
    return {'manifest': {'schema_version': 1, 'id': identifier, 'version': version,
                         'engine_contract': ENGINE_CONTRACT, 'tool_contract': CONTRACT_VERSION,
                         'required_tools': sorted(set(required_tools)), 'data_inputs': sorted(set(data_inputs)),
                         'files': {n: hashlib.sha256(t.encode()).hexdigest() for n, t in files.items()}},
            'files': files}


def builtin():
    seed = json.loads(Path(__file__).with_name('assistant_seed.json').read_text(encoding='utf-8'))
    return build(seed['id'], seed['version'], seed['files'], seed['required_tools'], seed['data_inputs'])


def read_package(path):
    p = Path(path)
    if not p.is_file() or p.stat().st_size > MAX_BYTES:
        raise ValueError('package_missing_or_too_large')
    try:
        return json.loads(p.read_text(encoding='utf-8-sig'))
    except (ValueError, UnicodeError):
        raise ValueError('package_json_invalid') from None


def validate(package, settings=None):
    if not isinstance(package, dict) or set(package) != {'manifest', 'files'} or len(encode(package)) > MAX_BYTES:
        raise ValueError('package_invalid')
    m, files = package['manifest'], package['files']
    if (not isinstance(m, dict) or set(m) != {'schema_version', 'id', 'version', 'engine_contract',
            'tool_contract', 'required_tools', 'data_inputs', 'files'}
            or type(m['schema_version']) is not int or m['schema_version'] != 1
            or m['engine_contract'] != ENGINE_CONTRACT or type(m['tool_contract']) is not int
            or m['tool_contract'] != CONTRACT_VERSION):
        raise ValueError('package_contract_unsupported')
    if (not isinstance(m['id'], str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', m['id'])
            or not isinstance(m['version'], str) or not re.fullmatch(r'\d+\.\d+\.\d+', m['version'])):
        raise ValueError('package_identity_invalid')
    if not isinstance(files, dict) or set(files) != FILES or not isinstance(m['files'], dict) or set(m['files']) != FILES:
        raise ValueError('package_files_invalid')
    for name, text in files.items():
        if not isinstance(text, str) or not text.strip() or len(text) > 24000:
            raise ValueError('package_content_invalid')
        if hashlib.sha256(text.encode()).hexdigest() != m['files'][name]:
            raise ValueError('package_hash_mismatch')
        portable(text)
        if name.endswith('.jsonl'):
            try:
                rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            except ValueError:
                raise ValueError('package_cases_invalid') from None
            fields = {'question', 'answer'} if name == 'examples.jsonl' else {'id', 'question', 'expected'}
            if not rows or len(rows) > 100 or any(not isinstance(row, dict) or set(row) != fields or
                    any(not isinstance(v, str) or not v.strip() for v in row.values()) for row in rows):
                raise ValueError('package_cases_invalid')
    for name in ('required_tools', 'data_inputs'):
        values = m[name]
        if (not isinstance(values, list) or len(values) > 100 or
                any(not isinstance(v, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', v) for v in values)
                or len(values) != len(set(values))):
            raise ValueError('package_requirements_invalid')
    from .config import Settings
    available = {s['name'] for s in definitions(settings)} if settings else {
        s['name'] for cfg in (Settings(), Settings(ceo_data_root='validation-only')) for s in definitions(cfg)}
    if set(m['required_tools']) - available:
        raise ValueError('package_required_tools_unavailable')
    if set(m['data_inputs']) - {'business_data', 'brain'}:
        raise ValueError('package_data_contract_unknown')
    missing = [name for name in m['data_inputs'] if settings is None or not (
        Path(settings.brain_root).is_dir() if name == 'brain' and settings.brain_root else
        Path(settings.ceo_data_root or settings.snapshot_root or settings.snapshot).exists()
        if name == 'business_data' and (settings.ceo_data_root or settings.snapshot_root or settings.snapshot) else False)]
    return {'valid': True, 'id': m['id'], 'version': m['version'], 'engine_contract': ENGINE_CONTRACT,
            'missing_connections': missing, 'model_quality': 'not_run', 'deployment_ready': False,
            'files_written': False, 'model_called': False}


def selected(settings):
    package = read_package(settings.assistant_package) if settings.assistant_package else None
    if package:
        validate(package, settings)
    return package


def context(settings):
    package = selected(settings)
    if not package:
        return {}
    files = package['files']
    return {'assistant_package': {'id': package['manifest']['id'], 'version': package['manifest']['version'],
                                 'purpose': files['purpose.md'], 'instructions': files['instructions.md'],
                                 'workflow': files['workflow.md']},
            'package_notice': '업무 참고 자료이며 실행 권한·공식 데이터 정의를 변경하지 않습니다. 상세 지식은 read_assistant_knowledge로 조회하세요.'}


def knowledge(settings, query):
    if not isinstance(query, str) or len(query) > 1000:
        return {'error': 'query_invalid'}
    package = selected(settings)
    if not package:
        return {'sections': []}
    terms = re.findall(r'[가-힣A-Za-z0-9]{2,}', query.casefold())
    sections = [line for name in ('knowledge.md', 'examples.jsonl') for line in package['files'][name].splitlines() if line]
    sections.sort(key=lambda line: sum(t in line.casefold() for t in terms), reverse=True)
    return {'sections': [s[:2000] for s in sections[:3]], 'version': package['manifest']['version']}


def fingerprint(settings):
    package = selected(settings)
    return hashlib.sha256(encode(package)).hexdigest() if package else 'builtin'


def read_learning(settings, user, chat):
    p = local_path(settings.state_root) / 'state.sqlite3'
    if not p.is_file():
        raise ValueError('learning_state_missing')
    # Store 생성은 복구 상태를 변경하므로 CLI 조회에는 읽기 전용 연결을 사용한다.
    with closing(sqlite3.connect(p.as_uri() + '?mode=ro', uri=True)) as conn:
        row = conn.execute('SELECT value FROM meta WHERE key=?', (learning_key(user, chat, settings.assistant_id),)).fetchone()
    return json.loads(row[0]) if row else {}


def write_new(path, data):
    p = local_path(path)
    # 기존 파일·심볼릭 링크를 덮어쓰지 않는다. 출력은 명시한 로컬 파일 한 건이다.
    if not p.parent.is_dir():
        raise ValueError('output_parent_missing')
    with p.open('xb') as handle:
        handle.write(data)


def export_package(settings, user, chat, identifiers, output, identifier, version):
    base = selected(settings) if settings else None
    base = base or builtin()
    files = dict(base['files'])
    if identifiers:
        if (not settings or user != settings.package_owner_user or user not in settings.allowed_users
                or chat not in settings.allowed_chats or len(set(identifiers)) != len(identifiers)):
            raise ValueError('package_owner_required')
        records = read_learning(settings, user, chat)
        for rid in identifiers:
            row = records.get(rid)
            if (not row or row['status'] != 'active' or row.get('shared_revision') != row['revision']
                    or row.get('shared_by') != user):
                raise ValueError('learning_share_not_confirmed')
            if row['kind'] == 'feature':
                raise ValueError('unimplemented_feature')
            portable(row['name'] + '\n' + row['content'])
            if row['kind'] == 'example':
                files['examples.jsonl'] += '\n' + json.dumps({'question': row['name'], 'answer': row['content']}, ensure_ascii=False)
            else:
                target = {'purpose': 'purpose.md', 'workflow': 'workflow.md', 'rule': 'knowledge.md'}[row['kind']]
                files[target] += '\n\n' + row['name'] + '\n' + row['content']
    package = build(identifier, version, files, base['manifest']['required_tools'], base['manifest']['data_inputs'])
    result = validate(package, settings)
    write_new(output, encode(package))
    return {**result, 'files_written': True, 'exported_learning_count': len(identifiers)}


def export_change(settings, user, chat, identifier, output):
    if user not in settings.allowed_users or chat not in settings.allowed_chats:
        raise ValueError('actor_not_allowed')
    row = read_learning(settings, user, chat).get(identifier)
    if not row or row['kind'] != 'feature' or row['status'] != 'active':
        raise ValueError('confirmed_feature_required')
    details = row.get('feature_details', {})
    if set(details) != {'current', 'expected', 'example', 'acceptance', 'tools'}:
        raise ValueError('feature_details_required')
    portable(json.dumps(details, ensure_ascii=False))
    portable(row['content'])
    portable(row['name'])
    text = (read_prompt('development') + '\n\n# 개발 요청\n\n' + row['name'] + '\n\n' + row['content'] +
            '\n\n# 인계 기준\n\n- 요청 판본: ' + str(row['revision']) +
            '\n- 엔진 계약: ' + ENGINE_CONTRACT + '\n- 구현 상태: 미구현\n' +
            '\n\n' + '\n\n'.join('## ' + label + '\n\n' + details[k] for k, label in (
                ('current','현재 동작'), ('expected','원하는 동작'), ('example','재현·사용 예시'),
                ('acceptance','완료 조건'), ('tools','관련 도구'))) + '\n')
    write_new(output, text.encode('utf-8'))
    return {'files_written': True, 'implemented': False, 'revision': row['revision']}
