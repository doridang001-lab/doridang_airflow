"""v1의 기간·품목·근거 화면을 v3 분석과 분리해 처리한다."""
import asyncio
import re

from .codex_adapter import safe_diagnostic
from .conversation import parse, month_period
from .grounded import make_facts, render_answer, validate_plan
from .legacy import FORMAT, QUERIES, UNSUPPORTED
from .presentation import render_query


async def model_call(service, stage, *args):
    try:
        result = await asyncio.to_thread(getattr(service.model, stage), *args)
        error = getattr(service.model, 'last_error', None)
    except Exception:
        result, error = None, 'model_failed'
    service.store.put('ai_status', {'checked_at': service.clock().isoformat(),
                                    **safe_diagnostic(service.model, stage, error)})
    return result


def markup(aid, page=None):
    rows = [[{'text': '구매액 순위', 'callback_data': aid + ':items'},
             {'text': '직원식대', 'callback_data': aid + ':meal'}],
            [{'text': '구매 근거', 'callback_data': aid + ':evidence'}]]
    if page:
        navigation = []
        for label, offset in (('이전', page.get('previous')), ('다음', page.get('next'))):
            if offset is not None:
                navigation.append({'text': label, 'callback_data': aid + ':e' + str(offset)})
        if navigation:
            rows.append(navigation)
    return {'inline_keyboard': rows}


def monthly_notice(scope):
    requested = scope.get('requested_period')
    actual = scope.get('actual_period')
    if scope.get('month_request') and requested != actual:
        year, month = map(int, requested[0][:7].split('-'))
        a, b = (int(d[-2:]) for d in actual)
        return f'{year}년 {month}월 자료는 현재 {month}월 {a}일~{b}일까지 있습니다. 해당 기간의 잠정 관측값으로 분석하며, {month}월 전체 실적은 아닙니다.\n\n'
    return ''


def easy_answer(result):
    """직전 조회값을 새로 계산한 뒤 짧은 관측 문장으로 표시한다."""
    value = result['result']
    header = f"{result['date_from']}~{result['date_to']} 잠정 자료입니다. 자료 기준일은 {result['as_of_date']}입니다."
    won = lambda amount: f'{int(amount):,}원'
    intent = result['intent']
    if intent in ('summary', 'status'):
        sales = (f"매출 관측액은 {won(value['sales_won'])}입니다."
                 if value['sales_rows'] else '매출 거래는 관측되지 않았습니다. 실제 0원으로 확정한 뜻은 아닙니다.')
        purchase = (f"구매 관측액은 {won(value['purchase_won'])}입니다."
                    if value['purchase_rows'] else '구매 거래는 관측되지 않았습니다. 실제 0원으로 확정한 뜻은 아닙니다.')
        return (f'{header}\n쉽게 말씀드리면 {sales} {purchase} 구매액에는 직원식대가 포함됩니다.'
                '\n미분류 구매의 용도와 손실 원인은 아직 단정할 수 없습니다.')
    if intent == 'items':
        items = value.get('items', [])[:3]
        if items:
            ranked = ', '.join(f"{item['item_name']} {won(item['amount_won'])}" for item in items)
            return f'{header}\n구매 관측액이 큰 품목은 {ranked} 순입니다. 수량 순위는 아닙니다.'
    if intent == 'evidence':
        rows = value.get('rows', [])[:3]
        if rows:
            shown = ', '.join(f"{row['item_name']} {won(row['amount_won'])} (근거 {row['evidence_id']})"
                              for row in rows)
            return f'{header}\n앞서 본 구매 기록 중 {shown}을 확인할 수 있습니다.'
    return render_query(result)


async def handle(service, uid, user, chat, text, path, context, previous, callback, cancelled, request_date, resolved):
    store, repo = service.store, service.repo
    old_scope = store.get('legacy-view-v1-' + previous['id'], {}) if previous else {}
    requested = list(resolved) if resolved else old_scope.get('requested_period')
    monthly = bool(month_period(text, request_date)) if not callback else old_scope.get('month_request', False)
    if not resolved:
        monthly = old_scope.get('month_request', False)
    start, end = resolved or ((previous['date_from'], previous['date_to']) if previous else
                             (context['as_of_date'][:7] + '-01', context['as_of_date']))
    requested = requested or [start, end]
    if resolved and monthly:
        start, end = max(start, context['window_start']), min(end, context['as_of_date'])
    if start > end or start < context['window_start'] or end > context['as_of_date']:
        await service.send(f'update-{uid}', chat,
            f"요청 기간 {requested[0]}~{requested[1]} 자료가 부족합니다. 현재 보유 범위는 {context['window_start']}~{context['as_of_date']}입니다. 다른 날짜로 대체하지 않았습니다.", cancelled=cancelled)
        store.remember_topic(chat, 'data', service.clock(), 'unavailable')
        return
    scope = {'version': 1, 'format': FORMAT, 'requested_period': requested, 'actual_period': [start, end],
             'month_request': monthly, 'supported_queries': list(QUERIES)}
    filters = {}
    action = callback.get('data', '').partition(':')[2] if callback else None
    intent, note = None, False
    easy = not callback and ('쉽게' in text or '간단히' in text)
    if callback:
        if action == 'pdf':
            await service.send(f'update-{uid}', chat, UNSUPPORTED, cancelled=cancelled)
            return
        if action.startswith('pick'):
            choices = store.get('choices-' + previous['id'], [])
            index = int(action[4:])
            if not 0 <= index < len(choices) or not old_scope.get('pending'):
                raise ValueError('품목 선택 오류')
            intent = old_scope['pending']['intent']
            filters = {**old_scope['pending']['filters'], 'product_key': choices[index]['product_key']}
            filters.pop('product_keys', None)
        elif re.fullmatch(r'e\d+', action):
            page = old_scope.get('page', {})
            offset = int(action[1:])
            if offset not in (page.get('previous'), page.get('next')):
                raise ValueError('근거 페이지 소유권 또는 범위 오류')
            intent, filters = 'evidence', {**old_scope.get('filters', {}), 'offset': offset, 'limit': 20}
        else:
            intent = {'why': 'summary', 'items': 'items', 'meal': 'staff_meal', 'evidence': 'evidence'}.get(action)
    else:
        if text.split()[0].split('@')[0] == '/report' or re.search(r'PDF|pdf|보고서|예측|전망|월말|여력|이상\s*탐지|고급\s*분석', text):
            await service.send(f'update-{uid}', chat, UNSUPPORTED, cancelled=cancelled)
            return
        parsed = parse(text, end, today=request_date)
        if parsed and parsed['action'] == 'query':
            intent = parsed['query']['intent']
            filters = {k: v for k, v in parsed['query'].items() if k not in ('intent', 'date_from', 'date_to')}
        if parsed and parsed['action'] == 'note':
            note = True
        if text.startswith('/items'):
            intent = 'items'
        elif not text.startswith('/'):
            if any(w in text for w in ('근거', '증빙')):
                intent = 'evidence'
            elif any(w in text for w in ('품목', '많이 샀', '많이 산')) and not re.search(r'비교|증가|늘|줄', text):
                intent = 'items'
            elif intent is None and (resolved or any(w in text for w in ('분석', '실적', '왜', '설명'))):
                intent = 'summary'
        if easy and not resolved:
            session = store.memory.session(service._session_id)
            last = session.get('last_query') if session else None
            if last and last.get('format') == FORMAT:
                intent = last['intent']
                filters = dict(last.get('filters', {}))
    analysis = service.analysis(chat, path, context, str(uid), start, end)
    aid = analysis['id']
    store.put('legacy-view-v1-' + aid, scope)
    if note:
        store.put('legacy-view-v1-' + aid, {**old_scope, **scope})
        delivered = await service.send(f'update-{uid}', chat, '이 기간의 설명을 기록했습니다. 원천 분류·마감 승인으로 처리하지 않습니다.', markup(aid), cancelled=cancelled)
        if delivered and not cancelled.is_set():
            store.note(chat, aid, user, text)
        return
    if intent is None and service.settings.ai_enabled and service.settings.ai_verified and hasattr(service.model, 'plan'):
        plan = await model_call(service, 'plan', text, {
            'format': FORMAT, 'supported_queries': list(QUERIES), 'snapshot_date': context['as_of_date'],
            'available_date_from': context['window_start'], 'available_date_to': context['as_of_date'],
            'request_date': request_date, 'resolved_period': [start, end], 'date_from': start, 'date_to': end,
            **service.model_context()}, cancelled)
        if cancelled.is_set() or store.cancelled(uid):
            return
        try:
            plan = validate_plan(plan, QUERIES)
        except (ValueError, TypeError):
            plan = None
        if plan and plan['action'] == 'query':
            # v1 한 화면은 하나의 조회 범위를 가진다. 복합 요청은 정형 월 요약으로 답한다.
            query = plan['queries'][0] if len(plan['queries']) == 1 else {'intent': 'summary', 'product_name': None}
            intent = query['intent']
            if query.get('product_name'):
                if query['product_name'] not in text:
                    await service.send(f'update-{uid}', chat, '확인할 구매 품목명을 직접 알려주세요.', cancelled=cancelled)
                    return
                choices = await asyncio.to_thread(repo.search_items, path, query['product_name'])
                if not choices:
                    await service.send(f'update-{uid}', chat, '해당 품목을 찾지 못했습니다. 구매내역의 품목명을 알려주세요.', cancelled=cancelled)
                    return
                scope['planned_choices'] = choices
        else:
            intent = 'summary' if '분석' in text else None
    if intent is None:
        await service.send(f'update-{uid}', chat, '조회할 기간과 매출·구매 품목·직원식대·구매 근거 중 확인할 항목을 알려주세요.', cancelled=cancelled)
        return
    if intent not in QUERIES:
        await service.send(f'update-{uid}', chat, UNSUPPORTED, cancelled=cancelled)
        return
    if intent == 'summary' and previous and previous.get('focus') and not resolved and any(w in text for w in ('왜', '설명', '그 품목')):
        intent = 'items'
    if intent in ('items', 'compare', 'evidence'):
        choices = scope.pop('planned_choices', None)
        if not callback and not filters.get('product_key'):
            choices = choices or await asyncio.to_thread(repo.items, path, text)
        if choices and len(choices) > 1:
            store.put('choices-' + aid, choices)
            scope['pending'] = {'intent': intent, 'filters': filters}
            store.put('legacy-view-v1-' + aid, scope)
            buttons = {'inline_keyboard': [[{'text': f"{r['item_name']} {r['spec']} {r['source']}"[:55],
                         'callback_data': f'{aid}:pick{i}'}] for i, r in enumerate(choices)]}
            delivered = await service.send(f'update-{uid}', chat, '같은 이름의 여러 품목이 있습니다. 품목·규격·원천을 선택해 주세요.', buttons, cancelled=cancelled)
            if delivered and not cancelled.is_set() and not callback:
                store.memory.set_analysis(service._session_id, aid)
            return
        if choices:
            filters['product_key'] = choices[0]['product_key']
        all_items = intent == 'items' and (action == 'items' or any(w in text for w in ('전체', '어떤 품목', '순위')))
        if not all_items and not filters.get('product_key') and previous and previous.get('focus') and (callback or not resolved or '그 품목' in text):
            filters['product_key'] = previous['focus']
        if intent == 'evidence':
            if not filters.get('product_key') and 'product_keys' not in filters and old_scope.get('shown_products') and not resolved:
                filters['product_keys'] = old_scope['shown_products']
            filters['limit'] = 20
        if filters.get('product_key'):
            store.focus(aid, chat, filters['product_key'])
            analysis = store.analysis(aid, chat)
    result = await asyncio.to_thread(repo.query, path, intent, start, end, **filters)
    result.update(requested_date_from=requested[0], requested_date_to=requested[1])
    if cancelled.is_set() or store.cancelled(uid):
        return
    scope['filters'] = filters
    value = result.get('result', {})
    if intent in ('items', 'summary'):
        scope['shown_products'] = [r['product_key'] for r in value.get('items', [])]
    elif old_scope.get('shown_products'):
        scope['shown_products'] = old_scope['shown_products']
    if intent == 'evidence':
        scope['page'] = {'previous': max(0, value['offset'] - 20) if value['offset'] else None,
                         'next': value['next_offset']}
    facts = make_facts([result])
    rendered = easy_answer(result) if easy else render_query(result)
    if (not easy and not callback and not text.startswith('/') and any(w in text for w in ('분석', '왜', '설명'))
            and service.settings.ai_enabled and service.settings.ai_verified and hasattr(service.model, 'explain')):
        notes = store.notes(chat, aid)[-10:]
        answer = await model_call(service, 'explain', text, {'format': FORMAT,
            'supported_queries': list(QUERIES), 'facts': facts,
            'notes': [{'id': f'N{i + 1}', 'text': n['text']} for i, n in enumerate(notes)],
            **service.model_context()}, cancelled)
        error = getattr(service.model, 'last_error', None)
        try:
            if not isinstance(answer, dict) or answer.get('hypotheses') or any(
                    not isinstance(q, str) or re.search(r'원가율|목표|손실|확정|PDF|pdf|예측|전망|이상\s*탐지', q)
                    for q in answer.get('questions', [])):
                raise ValueError('잠정 자료의 설명 범위 오류')
            rendered = render_answer(answer, facts, notes)
        except (ValueError, TypeError):
            error = error or 'invalid_answer'
            rendered = 'AI 설명을 완료하지 못해 계산된 관측 결과를 안내합니다.\n\n' + rendered
        store.put('ai_status', {'checked_at': service.clock().isoformat(),
                                **safe_diagnostic(service.model, 'answer', error)})
    if cancelled.is_set() or store.cancelled(uid):
        return
    rendered = monthly_notice(scope) + rendered
    if intent == 'evidence':
        rendered = ('선택한 품목의 구매 근거입니다.\n' if filters.get('product_key') else
                    '직전 목록에 표시한 품목의 구매 근거입니다.\n' if filters.get('product_keys') else
                    '해당 기간 전체 구매 근거입니다.\n') + rendered
    store.put('legacy-view-v1-' + aid, scope)
    delivered = await service.send(f'update-{uid}', chat, rendered, markup(aid, scope.get('page')), cancelled=cancelled)
    if delivered and not cancelled.is_set():
        store.remember_turn(chat, analysis, text, rendered, service.clock())
        if callback:
            owner = store.memory.analysis_owner(previous['id']) if previous else None
            if owner and owner['id'] == service._session_id:
                store.memory.set_analysis(service._session_id, aid)
        else:
            store.memory.record(uid, user, chat, text, rendered, service._received_at,
                                analysis, 'data', context['as_of_date'],
                                {'format': FORMAT, 'intent': intent, 'filters': filters})
        store.remember_topic(chat, 'data', service.clock())
