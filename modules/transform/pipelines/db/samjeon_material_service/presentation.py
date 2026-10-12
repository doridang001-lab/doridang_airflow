"""검증된 숫자와 명시적 확인 질문으로 대화 문장을 만든다."""
import hashlib
import json
from datetime import date


def money(value):
    return f'{int(value):,}원' if value is not None else '확인 보류'


def alert_items(context):
    output = []
    end = context['as_of_date']
    for row in context.get('alerts', []):
        kind = row['type']
        if kind == 'seven_day_purchase_increase':
            title = '최근 구매 집중이 의심됩니다.'
            fact = f"최근 7일 구매가 직전 7일보다 {money(row['increase_won'])}({row['increase_rate_pct']}%) 늘었습니다."
            question = '선매입 목적이나 판매량 변화가 있었는지 확인해 주세요.'
            period = row['date_from'] + ':' + row['date_to']
        elif kind == 'weekday_purchase_increase':
            title = '평소 같은 요일보다 구매가 집중된 것으로 보입니다.'
            fact = f"동일 요일 평균 {money(row['baseline_won'])}보다 {money(row['increase_won'])} 증가했습니다. 비교 표본은 {row['sample_count']}일입니다."
            question = '배송일 변경이나 행사 준비 구매인지 확인해 주세요.'
            period = end
        else:
            title = '목표보다 높은 매입 부담이 확인됩니다.'
            fact = f"월 누적 매입률이 목표보다 {row['target_gap_pp']}%p 높습니다."
            question = '증가 품목과 남은 기간 구매 계획을 확인해 주세요.'
            period = end[:7]
        key = hashlib.sha256(f'{kind}:{period}'.encode()).hexdigest()[:24]
        output.append({'key': key, 'title': title, 'fact': fact, 'question': question, 'kind': kind})
    # 품질 문제는 경영 이상과 명시적으로 구분하고 같은 원천·기간 문제를 묶는다.
    for issue in context.get('issues', []):
        key = hashlib.sha256(f"quality:{issue.get('source')}:{issue.get('reason')}:{end[:7]}".encode()).hexdigest()[:24]
        if any(r['key'] == key for r in output):
            continue
        source = {'sales': '매출', 'food_guide': '푸드가이드', 'logistics_dam': '물류담'}.get(issue.get('source'), '원천')
        output.append({'key': key, 'title': '자료 확인이 필요합니다.',
                       'fact': f"{issue.get('date', end)} / {source}: {issue['reason']}",
                       'question': '수집·마감·품목 검수 근거를 확인해야 정확히 판단할 수 있습니다.', 'kind': 'quality'})
    return output


def briefing(context, items=None):
    items = alert_items(context) if items is None else items
    lines = [f"대표님, {context['as_of_date']} 기준으로 확인했습니다. 자료 상태는 {context['status']}입니다."]
    if items:
        lines.append(f'우선 확인할 항목 {min(len(items), 3)}건을 말씀드릴게요.')
        for i, item in enumerate(items[:3], 1):
            lines.extend([f"\n{i}. {item['title']}", item['fact'], item['question']])
    else:
        lines.append('확인된 자료와 현재 탐지 기준에서는 새 이상 징후가 없습니다.')
    meals = context['month_to_date']
    lines.append(f"\n현장 직원식대 월 누적 관측액은 {money(meals['staff_meal_won'])}입니다." +
                 ('' if meals['staff_meal_complete'] else ' 수집·마감 확인이 남아 있습니다.'))
    lines.append('왜 의심되는지, 어떤 품목인지 이어서 물어보세요. 상세 표와 근거는 PDF로 받으실 수 있습니다.')
    return '\n'.join(lines)


def summary(period):
    lines = [f"{period['date_from']} ~ {period['date_to']} / {period['status']}",
             f"할인 후 매출 관측액: {money(period['observed_sales_won'])}"]
    if period['unclassified_row_count']:
        lines.extend([f"분류된 원재료·포장재 관측액: {money(period['observed_purchase_won'])}",
                      f"미분류: {period['unclassified_row_count']}행, 순액 {money(period['unclassified_won'])}. 전체 확정 매입액은 보류합니다."])
    else:
        lines.append(f"원재료·포장재 매입 관측액: {money(period['observed_purchase_won'])}")
    rate = period['purchase_rate_pct']
    if rate is not None:
        lines.append(f"매입률 {rate}% / 목표 비교 {period['target_comparison_status']} / 목표 차이 {period['target_gap_pp']}%p")
    elif period.get('provisional_purchase_rate_pct') is not None:
        lines.append(f"잠정 매입률 {period['provisional_purchase_rate_pct']}%이며 확정 판단은 보류합니다.")
    else:
        lines.append('확정 매입률과 목표 초과 판단은 보류합니다.')
    lines.append(f"현장 직원식대: {money(period['staff_meal_won'])}" + ('' if period['staff_meal_complete'] else ' (미확정 관측값)'))
    lines.extend(period.get('reasons', []))
    return '\n'.join(lines)


def render_query(result):
    if result.get('status') == '범위 밖':
        return f"이 스냅샷에는 필요한 기간이 없습니다. {result['required_date_from']} ~ {result['required_date_to']} 재계산이 필요합니다."
    value, intent = result['result'], result['intent']
    header = f"{result['date_from']} ~ {result['date_to']} / {result['status']}\n"
    if intent == 'summary':
        return summary(value)
    if intent == 'staff_meal':
        return header + f"현장 직원식대는 {money(value['staff_meal_won'])}입니다. " + ('마감 확인된 값입니다.' if value['staff_meal_complete'] else '아직 수집·마감 확인이 남은 관측값입니다.')
    if intent == 'compare':
        period = value['seven_day']
        if not period['complete']:
            return header + '비교 기간의 분류·마감이 끝나지 않아 전체 매입액과 증감 판단을 보류합니다. ' + period['reason'] + '\n현재 확인 가능한 개별 구매는 근거 조회와 상세 PDF에서 확인하실 수 있습니다.'
        lines = [header, f"최근 7일 {money(period['current_observed_purchase_won'])}, 직전 7일 {money(period['previous_observed_purchase_won'])}입니다."]
        if value.get('selected_item'):
            lines.insert(1, f"선택한 품목: {value['selected_item']}")
        if period['complete']:
            lines.append(f"차이는 {money(period['increase_won'])}입니다.")
            for item in value['top_increase_items']:
                lines.append(f"- {item['item_name']}: {money(item['previous_won'])} → {money(item['current_won'])} (증가 {money(item['increase_won'])})")
            lines.append('증가 원인은 이 금액만으로 확정할 수 없습니다. 선매입 목적과 재고를 확인해 주세요.')
        else:
            lines.append('분류·마감이 끝나지 않아 구매 증감 판단은 보류합니다. ' + period['reason'])
        return '\n'.join(lines)
    if intent == 'status':
        return summary(value['period']) + '\n자료별 상태:\n' + '\n'.join(
            f"{x['date']} {x['source']}: {x['status']}" for x in value['quality'][:20])
    if intent == 'evidence':
        lines = [header, f"근거 {value['total']}행 중 {value['offset'] + 1 if value['rows'] else 0}부터 {len(value['rows'])}행입니다."]
        for r in value['rows'][:20]:
            lines.append(f"{r.get('purchase_date', r.get('sale_date'))} / {r.get('item_name', r['source'])} / {money(r['amount_won'])} / 근거 {r['evidence_id']}")
        if len(value['rows']) > 20 or value['next_offset'] is not None:
            lines.append('표시한 근거 이후의 내역은 근거 조회 범위를 좁혀 확인해 주세요.')
        return '\n'.join(lines)
    if intent == 'forecast':
        labels = {'status': '전망 상태', 'reason': '판단 근거', 'assumption': '가정',
                  'projected_month_sales_won': '예상 월매출', 'projected_month_purchase_won': '예상 월매입',
                  'projected_purchase_rate_pct': '예상 매입률(%)', 'remaining_purchase_capacity_won': '잔여 매입 여력',
                  'actual_month_sales_won': '월 확정 매출', 'actual_month_purchase_won': '월 확정 매입',
                  'actual_purchase_rate_pct': '확정 매입률(%)', 'method': '계산 방법',
                  'sample_window_start': '표본 시작일', 'sample_window_end': '표본 종료일',
                  'capacity_status': '매입 여력 기준', 'date': '표본 확인 대상일'}
        return header + '\n'.join(f"{labels[k]}: {money(v) if k.endswith('_won') else v}" for k, v in value.items()
                                 if k in labels) + '\n최근 패턴 유지 가정이며 재고·행사·확정 발주를 반영한 예측이 아닙니다.'
    if intent == 'anomalies':
        if not value:
            return header + '현재 자료에서 확정할 수 있는 이상 신호가 없습니다. 자료 상태도 함께 확인해 주세요.'
        return header + '\n'.join(x['title'] + '\n' + x['fact'] + '\n' + x['question']
                                   for x in alert_items({'alerts': value, 'as_of_date': result['date_to']})[:3])
    raise ValueError('지원하지 않는 결과')


def chunks(text, limit=3500):
    # Telegram의 UTF-16 길이 제한 안에서 한글·이모지를 보존한다.
    part, size = '', 0
    for char in text:
        units = len(char.encode('utf-16-le')) // 2
        if size + units > limit:
            yield part
            part, size = '', 0
        part += char
        size += units
    if part:
        yield part


def buttons(aid):
    return {'inline_keyboard': [[{'text': '왜 의심해?', 'callback_data': f'{aid}:why'},
                                  {'text': '증가 품목 보기', 'callback_data': f'{aid}:items'}],
                                 [{'text': '현장 직원식대', 'callback_data': f'{aid}:meal'},
                                  {'text': '상세 PDF', 'callback_data': f'{aid}:pdf'}]]}
