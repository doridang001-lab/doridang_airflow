"""모델은 조회와 가설만 제안하고 사실 문장은 검증된 조회 결과에서 가져온다."""
import hashlib
import json
import re

from .conversation import INTENTS, validate_query
from .presentation import render_query


def object_schema(properties):
    return {'type': 'object', 'properties': properties, 'required': list(properties), 'additionalProperties': False}


NULL_TEXT = {'type': ['string', 'null']}
QUERY_SCHEMA = object_schema({
    'intent': {'type': 'string', 'enum': sorted(INTENTS)},
    'date_from': NULL_TEXT, 'date_to': NULL_TEXT, 'product_name': NULL_TEXT,
})
PLAN_SCHEMA = object_schema({
    'action': {'type': 'string', 'enum': ['query', 'clarify', 'note', 'pdf']},
    'queries': {'type': 'array', 'items': QUERY_SCHEMA, 'maxItems': 3},
    'clarification': NULL_TEXT,
})
IDS = {'type': 'array', 'items': {'type': 'string'}, 'maxItems': 3}
ANSWER_SCHEMA = object_schema({
    'fact_ids': IDS,
    'hypotheses': {'type': 'array', 'maxItems': 3, 'items': object_schema({
        'text': {'type': 'string'}, 'fact_ids': IDS, 'note_ids': IDS})},
    'questions': {'type': 'array', 'maxItems': 3, 'items': {'type': 'string'}},
})


def validate_plan(raw):
    if not isinstance(raw, dict) or set(raw) != {'action', 'queries', 'clarification'}:
        raise ValueError('분석 계획 형식 오류')
    if raw['action'] not in ('query', 'clarify', 'note', 'pdf') or not isinstance(raw['queries'], list):
        raise ValueError('분석 작업 오류')
    if len(raw['queries']) > 3 or (raw['action'] == 'query') != bool(raw['queries']):
        raise ValueError('조회 개수 오류')
    if raw['clarification'] is not None and (not isinstance(raw['clarification'], str) or len(raw['clarification']) > 400):
        raise ValueError('확인 질문 오류')
    if raw['action'] == 'clarify' and not raw['clarification']:
        raise ValueError('확인 질문 누락')
    for query in raw['queries']:
        if not isinstance(query, dict) or set(query) != set(QUERY_SCHEMA['properties']):
            raise ValueError('조회 계획 필드 오류')
        name = query['product_name']
        if name is not None and (not isinstance(name, str) or not name.strip() or len(name) > 100):
            raise ValueError('품목명 오류')
        if name and query['intent'] not in ('compare', 'evidence'):
            raise ValueError('개별 품목은 비교 또는 근거 조회를 사용하세요')
        validate_query({k: v for k, v in query.items() if k != 'product_name'})
    return raw


def bounded(value):
    """모델 입력에는 원천 물리 경로를 제외하고 표를 제한한다."""
    if isinstance(value, dict):
        return {k: bounded(v) for k, v in value.items() if k not in ('source_file', 'source_path', 'snapshot')}
    if isinstance(value, list):
        return [bounded(v) for v in value[:20]]
    return value


def make_facts(results):
    def incomplete(value):
        if isinstance(value, dict):
            return any(((k == 'complete' or k.endswith('_complete')) and v is False) or
                       (k == 'status' and v in ('보류', '잠정', '누락', '오래됨', '범위 밖', '검증 실패')) or
                       incomplete(v) for k, v in value.items())
        return isinstance(value, list) and any(incomplete(v) for v in value)
    return [{'id': f'F{i + 1}', 'text': render_query(result), 'status': '잠정' if incomplete(result) else result['status'],
             'data': bounded(result)} for i, result in enumerate(results)]


def scope_signature(analysis, notes):
    scope = {k: analysis.get(k) for k in ('snapshot_id', 'date_from', 'date_to', 'focus')}
    scope['notes'] = notes
    return hashlib.sha256(json.dumps(scope, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _prose(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 500:
        raise ValueError('분석 문장 길이 오류')
    # 수치·기간·근거 ID는 모델이 새로 적지 않고 아래 사실 블록에서만 표시한다.
    if (any(c.isnumeric() for c in value) or re.search(r'https?://|[A-Za-z]:[\\/]|<[^>]+>', value)
            or re.search(r'[영공일이삼사오육칠팔구십백천만억조]+\s*(?:원|퍼센트|배|개|건|개월|년)(?:\s|[,.?!]|$)', value)):
        raise ValueError('가설에 임의 수치 또는 외부 참조가 있습니다')
    return value.strip()


def render_answer(raw, facts, notes):
    if not isinstance(raw, dict) or set(raw) != {'fact_ids', 'hypotheses', 'questions'}:
        raise ValueError('분석 답변 형식 오류')
    known = {f['id']: f for f in facts}
    note_ids = {f'N{i + 1}' for i in range(len(notes))}
    def refs(values, allowed, required=False):
        if not isinstance(values, list) or len(values) > 3 or any(not isinstance(v, str) or v not in allowed for v in values):
            raise ValueError('존재하지 않는 근거 참조')
        if required and not values:
            raise ValueError('근거 없는 분석')
        return list(dict.fromkeys(values))
    selected = refs(raw['fact_ids'], known, True)
    if not isinstance(raw['hypotheses'], list) or len(raw['hypotheses']) > 3 or not isinstance(raw['questions'], list) or len(raw['questions']) > 3:
        raise ValueError('분석 항목 개수 오류')
    hypotheses = []
    incomplete = any(f['status'] != '정상' for f in facts)
    for h in raw['hypotheses']:
        if not isinstance(h, dict) or set(h) != {'text', 'fact_ids', 'note_ids'}:
            raise ValueError('가설 형식 오류')
        support = refs(h['fact_ids'], known, True)
        supplied = refs(h['note_ids'], note_ids)
        text = _prose(h['text'])
        if re.search(r'확실|틀림없|확정(?:됐|되었|되었습니다|입니다|이다)|원인(?:입니다|이다)', text):
            raise ValueError('확인되지 않은 원인 확정')
        for ref in support:
            if ref not in selected:
                selected.append(ref)
        if not incomplete:
            hypotheses.append(f'- {text} (확인 전 가설; 근거 {", ".join(support)}' +
                              ('; 사용자 설명 참고' if supplied else '') + ')')
    # 자료 상태와 미확정 수치가 선택에서 누락되지 않도록 모든 계산 결과를 포함한다.
    selected.extend(f for f in known if f not in selected)
    lines = ['확인된 자료', *[f"[{ref}] {known[ref]['text']}" for ref in selected]]
    if hypotheses:
        lines.extend(['\n가능한 원인과 확인할 점', *hypotheses,
                      '재고·폐기·발주 근거가 없으면 낭비나 손실로 확정할 수 없습니다.'])
    if incomplete:
        lines.append('\n자료 확인이 남아 있어 원인과 확정 수치 판단을 보류합니다.')
    if raw['questions']:
        lines.extend(['\n추가 확인 질문', *['- ' + _prose(q) for q in raw['questions']]])
    if notes:
        lines.extend(['\n사용자가 알려준 설명', notes[-1]['text'], '(원천 검수 승인이나 확인된 사실을 뜻하지 않습니다.)'])
    return '\n\n'.join(lines)
