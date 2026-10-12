"""명령·짧은 후속 질문을 처리한다. 미지원 질문은 추측해 실행하지 않는다."""
import re
from datetime import date, timedelta

INTENTS = {'summary', 'compare', 'anomalies', 'forecast', 'staff_meal', 'evidence', 'status'}
HELP = ('삼전점 자료를 함께 확인하겠습니다.\n'
        '“지금 뭐가 의심돼?”, “왜?”, “어떤 품목?”, “직원식대는?”, “지난주와 비교해줘”처럼 물어보세요.\n'
        '“행사 때문에 미리 샀어” 같은 설명도 해당 분석에 기록합니다.\n'
        '버튼이나 “보고서 보여줘”로 상세 PDF를 받습니다.\n'
        '/summary /compare /anomalies /forecast /staff_meal /evidence /status /help /cancel\n'
        '기간은 YYYY-MM-DD YYYY-MM-DD로, 근거는 evidence_id=값 product_key=값 source=값 offset=0 limit=100으로 지정할 수 있습니다.')


def validate_query(raw):
    if not isinstance(raw, dict) or set(raw) - {'intent', 'date_from', 'date_to', 'evidence_id', 'product_key', 'source', 'offset', 'limit'}:
        raise ValueError('허용되지 않은 조회 요청')
    if raw.get('intent') not in INTENTS:
        raise ValueError('조회 종류 오류')
    out = dict(raw)
    for k in ('date_from', 'date_to'):
        if out.get(k) is not None:
            d = date.fromisoformat(out[k])
            if str(d) != out[k]:
                raise ValueError('날짜는 YYYY-MM-DD 형식이어야 합니다')
    if bool(out.get('date_from')) != bool(out.get('date_to')):
        raise ValueError('시작일과 종료일을 함께 지정하세요')
    if out.get('date_from') and out['date_from'] > out['date_to']:
        raise ValueError('기간이 역전됐습니다')
    for k in ('evidence_id', 'product_key', 'source'):
        if out.get(k) is not None and (not isinstance(out[k], str) or len(out[k]) > 200):
            raise ValueError('근거 필터 오류')
    for k, default, minimum, maximum in (('offset', 0, 0, 1000000), ('limit', 100, 1, 500)):
        if k in out and (type(out[k]) is not int or not minimum <= out[k] <= maximum):
            raise ValueError('근거 페이지 범위 오류')
    return out


def parse(text, end):
    text = text.strip()
    command = text.split()[0].split('@')[0] if text else ''
    if command in ('/help', '/start'):
        return {'action': 'help'}
    if command == '/cancel':
        return {'action': 'cancel'}
    if command == '/report' or any(w in text for w in ('보고서', 'PDF', 'pdf')):
        return {'action': 'pdf'}
    if any(w in text for w in ('때문이야', '때문입니다', '미리 샀', '선매입했', '확인했', '확인 완료')) and not text.endswith('?'):
        return {'action': 'note'}
    if command.startswith('/'):
        intent = command[1:]
        if intent not in INTENTS:
            return None
    elif any(w in text for w in ('직원', '식대', '부식')):
        intent = 'staff_meal'
    elif any(w in text for w in ('어떤 품목', '품목', '얼마나', '얼만', '비교', '증가액')):
        intent = 'compare'
    elif any(w in text for w in ('왜', '의심', '이상', '문제')):
        return {'action': 'explain'}
    elif any(w in text for w in ('전망', '월말', '여력')):
        intent = 'forecast'
    elif any(w in text for w in ('근거', '증빙')):
        intent = 'evidence'
    elif any(w in text for w in ('상태', '준비')):
        intent = 'status'
    elif any(w in text for w in ('매출', '매입', '요약', '현황')):
        intent = 'summary'
    else:
        return None
    query = {'intent': intent}
    dates = re.findall(r'\d{4}-\d{2}-\d{2}', text)
    if len(dates) > 2:
        raise ValueError('조회 기간을 시작일·종료일 두 날짜로 알려주세요')
    if dates:
        query.update(date_from=dates[0], date_to=dates[-1])
    else:
        finish = date.fromisoformat(end)
        if '지난주' in text:
            finish -= timedelta(days=finish.weekday() + 1)
            query.update(date_from=str(finish - timedelta(days=6)), date_to=str(finish))
        elif '최근 7일' in text or '지난 7일' in text:
            query.update(date_from=str(finish - timedelta(days=6)), date_to=str(finish))
        elif '이번 달' in text or '이번달' in text:
            query.update(date_from=str(finish.replace(day=1)), date_to=str(finish))
    for key, value in re.findall(r'(evidence_id|product_key|source|offset|limit)=([^\s]+)', text):
        query[key] = int(value) if key in ('offset', 'limit') else value
    return {'action': 'query', 'query': validate_query(query)}
