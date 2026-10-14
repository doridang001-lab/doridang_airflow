"""명령·짧은 후속 질문을 처리한다. 미지원 질문은 추측해 실행하지 않는다."""
import re
import calendar
from datetime import date, timedelta

INTENTS = {'summary', 'compare', 'anomalies', 'forecast', 'staff_meal', 'evidence', 'status'}
DATA_UNAVAILABLE = '현재 검증된 삼전점 자료가 준비되지 않아 매출·원재료 수치를 답할 수 없습니다. 준비되면 다시 질문해 주세요.'
AI_UNAVAILABLE = '지금은 대화 기능을 사용할 수 없습니다. 잠시 후 다시 말씀해 주세요.'
CLARIFY = '일반적인 설명을 원하시나요, 아니면 삼전점 자료를 조회할까요?'


def route(text, topic=None):
    """자료를 읽거나 모델을 호출하기 전에 명확한 의도와 마지막 주제를 확인한다."""
    text = text.strip()
    command = text.split()[0].split('@')[0] if text else ''
    if command in ('/start', '/help'):
        return 'help'
    if command.startswith('/'):
        return 'data' if command[1:] in INTENTS | {'report', 'items'} else 'clarify'
    if re.match(r'^(안녕|반가워|고마워|고맙|감사|좋은 아침|다른 얘기|다른 이야기)', text):
        return 'chat'
    if re.search(r'\d+\s*월', text):
        return 'data' if topic == 'data' or any(w in text for w in ('분석', '실적', '매출', '구매', '매입', '확정', '전체')) else 'clarify'
    if re.search(r'이상\s*탐지|고급\s*분석', text):
        return 'data'
    if any(w in text for w in ('삼전', '매출', '매입', '원재료', '구매', '품목', '직원식대', '원가',
                              '식대', '부식', '증빙', 'pdf', 'PDF', '보고서', '선매입', '미리 샀',
                              '의심', '원인 분석', '원인을 분석', '월말', '증가액')):
        return 'data'
    if re.search(r'(?:닭|감자|양파|소스|마늘|고기|쌀|고춧가루).*?(?:얼마|늘었|줄었)', text):
        return 'data'
    if re.search(r'^(왜|얼마|그건|그럼|그러면|그게|그거|아까|다시|설명|비교|근거|준비|상태|확인|때문|오늘|어제|그제|지난주|최근|이번 달|이번달)', text) or text in ('일반적인 설명', '일반 대화', '자료 조회'):
        if text in ('일반적인 설명', '일반 대화'):
            return 'chat'
        if text == '자료 조회':
            return 'data'
        return topic if topic in ('chat', 'data') else 'clarify'
    return 'chat' if text else 'clarify'


def month_period(text, today):
    matches = list(re.finditer(r'(?<!\d)(?:(\d+)\s*년\s*)?(\d+)\s*월', text))
    if not matches:
        if '전체' in text and ('이번 달' in text or '이번달' in text):
            day = date.fromisoformat(str(today))
            return str(day.replace(day=1)), str(day.replace(day=calendar.monthrange(day.year, day.month)[1]))
        return None
    if len(matches) != 1 or re.search(r'\d{4}-\d{2}-\d{2}|오늘|어제|그제|지난\s*주|최근\s*7일|이번\s*달', text):
        raise ValueError('기간을 하나만 알려주세요')
    match = matches[0]
    if (match[1] and len(match[1]) != 4) or len(match[2]) > 2:
        raise ValueError('연월 형식 오류')
    if re.match(r'\s*\d+\s*일', text[match.end():]):
        raise ValueError('특정 날짜는 YYYY-MM-DD로 알려주세요')
    year = int(match[1] or date.fromisoformat(str(today)).year)
    month = int(match[2])
    return str(date(year, month, 1)), str(date(year, month, calendar.monthrange(year, month)[1]))


def period(text, today):
    """상대 날짜는 스냅샷 기준일이 아닌 요청 수신일(KST) 기준으로 한 번 확정한다."""
    monthly = month_period(text, today)
    if monthly:
        return monthly
    dates = re.findall(r'\d{4}-\d{2}-\d{2}', text)
    if len(dates) > 2:
        raise ValueError('조회 기간을 시작일·종료일 두 날짜로 알려주세요')
    if dates:
        if re.search(r'오늘|어제|그제|지난\s*주|최근\s*7일|이번\s*달', text):
            raise ValueError('조회 기간 충돌')
        checked = validate_query({'intent': 'summary', 'date_from': dates[0], 'date_to': dates[-1]})
        return checked['date_from'], checked['date_to']
    today = date.fromisoformat(str(today))
    for word, delta in (('그제', 2), ('어제', 1), ('오늘', 0)):
        if word in text:
            day = str(today - timedelta(days=delta))
            return day, day
    if '지난주' in text or '지난 주' in text:
        end = today - timedelta(days=today.weekday() + 1)
        return str(end - timedelta(days=6)), str(end)
    if '최근 7일' in text or '지난 7일' in text:
        end = today - timedelta(days=1)
        return str(end - timedelta(days=6)), str(end)
    if '이번 달' in text or '이번달' in text:
        start = today.replace(day=1)
        return str(start), str(max(start, today - timedelta(days=1)))
    return None


def help_text(ai_ready, data_ready):
    return ('대표님, 삼전점 비서입니다.\n'
            + ('일반 대화: 준비됐습니다. 편하게 말씀해 주세요.\n' if ai_ready else '일반 대화: 연결 준비 중입니다.\n')
            + ('삼전점 자료: 검증된 자료로 매출·매입·근거를 조회할 수 있습니다. “어제 매출은?”처럼 물어보세요.\n'
               if data_ready else DATA_UNAVAILABLE + '\n')
            + '/help 도움말 · /cancel 진행 중 요청 취소')
HELP = ('삼전점 자료를 함께 확인하겠습니다.\n'
        '“지금 뭐가 의심돼?”, “왜?”, “어떤 품목?”, “직원식대는?”, “지난주와 비교해줘”처럼 물어보세요.\n'
        '“행사 때문에 미리 샀어” 같은 설명도 해당 분석에 기록합니다.\n'
        '버튼이나 “보고서 보여줘”로 상세 PDF를 받습니다.\n'
        '/summary /compare /anomalies /forecast /staff_meal /evidence /status /help /cancel\n'
        '기간은 YYYY-MM-DD YYYY-MM-DD로, 근거는 evidence_id=값 product_key=값 source=값 offset=0 limit=100으로 지정할 수 있습니다.')


def validate_query(raw):
    if not isinstance(raw, dict) or set(raw) - {'intent', 'date_from', 'date_to', 'evidence_id', 'product_key', 'source', 'offset', 'limit'}:
        raise ValueError('허용되지 않은 조회 요청')
    if raw.get('intent') not in INTENTS | {'items'}:
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


def parse(text, end, today=None):
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
    resolved = period(text, today) if today is not None else None
    dates = re.findall(r'\d{4}-\d{2}-\d{2}', text)
    if len(dates) > 2:
        raise ValueError('조회 기간을 시작일·종료일 두 날짜로 알려주세요')
    if resolved:
        query.update(date_from=resolved[0], date_to=resolved[1])
    elif dates:
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
