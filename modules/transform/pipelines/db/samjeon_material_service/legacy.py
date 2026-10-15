"""검증된 samjeon-mart-v1의 잠정 관측값 전용 로더·조회·표시."""
import hashlib
from io import BytesIO
import json
import re
from datetime import date, timedelta
from numbers import Integral
from pathlib import Path

import pandas as pd

from .data_contract import SnapshotUnavailable

FORMAT = 'legacy_v1'
QUERIES = ('summary', 'items', 'compare', 'staff_meal', 'evidence', 'status')
UNSUPPORTED = '현재 잠정 자료는 요약·구매 품목·기간 비교·직원식대·구매 근거를 조회할 수 있습니다. 고급 이상 탐지·예측·PDF는 지원하지 않습니다.'
COLUMNS = {
    'sales_daily': 'sale_date source amount_won row_count',
    'purchase_daily': 'purchase_date source category amount_won row_count',
    'purchase_lines': 'purchase_date source account product_key item_name spec amount_won category classification_status evidence_id closed',
    'item_daily': 'purchase_date source account category product_key item_name spec amount_won row_count',
    'quality_daily': 'date source status reason',
}
FILES = {name + '.parquet' for name in COLUMNS} | {'agent_context.json', 'review_candidates.csv'}
CATEGORIES = {'원재료', '포장재', '기타', '미분류', '현장 직원식대'}


def checked_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) or str(date.fromisoformat(value)) != value:
        raise ValueError('날짜 형식 오류')
    return value


def load(directory, manifest):
    """해시를 검사한 바이트를 파싱해 검사와 읽기 사이의 파일 교체를 방지한다."""
    directory = Path(directory).resolve()
    if manifest.get('rule_version') != 'samjeon-mart-v1' or 'schema_version' in manifest:
        raise ValueError('v1 판본 오류')
    files = manifest['files']
    if not isinstance(files, dict) or not FILES <= files.keys():
        raise ValueError('필수 파일 누락')
    contents = {}
    for name, expected in files.items():
        if not isinstance(name, str) or '/' in name or '\\' in name or Path(name).name != name:
            raise ValueError('파일 경로 오류')
        path = (directory / name).resolve()
        if not path.is_relative_to(directory) or not isinstance(expected, str) or not re.fullmatch('[a-f0-9]{64}', expected):
            raise ValueError('파일 계약 오류')
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != expected:
            raise ValueError('파일 해시 오류')
        if name in FILES:
            contents[name] = data
    context = json.loads(contents['agent_context.json'])
    if not isinstance(context, dict) or 'schema_version' in context:
        raise ValueError('문맥 판본 오류')
    for field in ('snapshot_id', 'as_of_date', 'window_start', 'rule_version'):
        if context.get(field) != manifest[field]:
            raise ValueError('문맥 판본 불일치')
    if not isinstance(manifest['snapshot_id'], str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', manifest['snapshot_id']):
        raise ValueError('판본 식별자 오류')
    first, last = checked_date(manifest['window_start']), checked_date(manifest['as_of_date'])
    if first > last:
        raise ValueError('기간 역전')
    tables = {}
    for name, required in COLUMNS.items():
        frame = pd.read_parquet(BytesIO(contents[name + '.parquet']))
        if not frame.columns.is_unique or not set(required.split()) <= set(frame.columns):
            raise ValueError('필수 컬럼 오류')
        day = 'sale_date' if name == 'sales_daily' else 'date' if name == 'quality_daily' else 'purchase_date'
        if not all(first <= checked_date(v) <= last for v in frame[day]):
            raise ValueError('자료 날짜 범위 오류')
        for col in ('amount_won', 'row_count'):
            if col in frame and not all(isinstance(v, Integral) and not isinstance(v, bool) and
                                       (col != 'row_count' or v >= 0) for v in frame[col]):
                raise ValueError('금액 또는 행수 오류')
        for col in set(required.split()) - {day, 'amount_won', 'row_count', 'closed'}:
            if not all(isinstance(v, str) and (bool(v.strip()) or col in ('spec', 'reason')) for v in frame[col]):
                raise ValueError('문자열 컬럼 오류')
        if 'category' in frame and not set(frame.category) <= CATEGORIES:
            raise ValueError('비용 분류 오류')
        if 'closed' in frame and not pd.api.types.is_bool_dtype(frame.closed.dtype):
            raise ValueError('마감 형식 오류')
        tables[name] = frame
    lines = tables['purchase_lines']
    for field in ('product_key', 'evidence_id'):
        if not all(re.fullmatch(r'[A-Za-z0-9_-]{1,200}', v) for v in lines[field]):
            raise ValueError('자료 식별자 형식 오류')
    if not set(tables['quality_daily'].status) <= {'정상', '잠정', '누락', '오래됨', '검증 실패', '보류'}:
        raise ValueError('품질 상태 오류')
    if lines.evidence_id.duplicated().any():
        raise ValueError('근거 식별자 중복')
    identity = ['product_key', 'source', 'item_name', 'spec'] + (['unit'] if 'unit' in lines else [])
    if lines[identity].drop_duplicates().product_key.duplicated().any():
        raise ValueError('품목 식별자 충돌')
    for name, keys in (
        ('purchase_daily', ['purchase_date', 'source', 'category']),
        ('item_daily', ['purchase_date', 'source', 'account', 'category', 'product_key', 'item_name', 'spec'])):
        # Python 정수 합산으로 고정폭 정수 오버플로를 피한다.
        def grouped(frame):
            return frame.assign(amount_won=frame.amount_won.astype(object)).groupby(keys, dropna=False).amount_won.sum().sort_index()
        if not grouped(lines).equals(grouped(tables[name])):
            raise ValueError('구매 집계 불일치')
        counts = lines.groupby(keys, dropna=False).size().sort_index()
        reported = tables[name].groupby(keys, dropna=False).row_count.sum().sort_index()
        if not counts.equals(reported):
            raise ValueError('구매 행수 불일치')
        if tables[name].duplicated(keys).any():
            raise ValueError('집계 키 중복')
    if tables['sales_daily'].duplicated(['sale_date', 'source']).any() or tables['quality_daily'].duplicated(['date', 'source']).any():
        raise ValueError('일별 키 중복')
    # 기존 v1 context의 계산값·예측·물리 경로는 서비스로 전달하지 않는다.
    safe = {k: manifest[k] for k in ('snapshot_id', 'as_of_date', 'window_start', 'rule_version')}
    from .data_contract import available_metrics
    safe.update(format=FORMAT, status='잠정', supported_queries=list(QUERIES),
                available_metrics=available_metrics(tables))
    return manifest, safe, tables


def total(frame):
    return sum(int(v) for v in frame.amount_won)


def query(loaded, intent, start=None, end=None, *, product_key=None, source=None,
          evidence_id=None, offset=0, limit=100, product_keys=None):
    manifest, context, tables = loaded
    end = checked_date(end or context['as_of_date'])
    start = checked_date(start or end[:7] + '-01')
    if start > end or type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError('조회 범위 오류')
    result = {**context, 'intent': intent, 'date_from': start, 'date_to': end,
              'requested_date_from': start, 'requested_date_to': end,
              'actual_date_from': start, 'actual_date_to': end}
    if intent not in QUERIES:
        return {**result, 'status': '미지원', 'result': {}}
    if start < context['window_start'] or end > context['as_of_date']:
        return {**result, 'status': '범위 밖', 'result': {}}
    selected = {}
    for name, frame in tables.items():
        column = 'sale_date' if name == 'sales_daily' else 'date' if name == 'quality_daily' else 'purchase_date'
        rows = frame.loc[frame[column].between(start, end)]
        if source and 'source' in rows:
            rows = rows.loc[rows.source.eq(source)]
        selected[name] = rows
    purchases, lines, items = (selected[n] for n in ('purchase_daily', 'purchase_lines', 'item_daily'))
    for key, value in (('product_key', product_key), ('evidence_id', evidence_id)):
        if value:
            lines = lines.loc[lines[key].eq(value)]
    if product_keys is not None:
        if not isinstance(product_keys, list) or len(product_keys) > 10 or any(not isinstance(k, str) for k in product_keys):
            raise ValueError('품목 범위 오류')
        lines = lines.loc[lines.product_key.isin(product_keys)]
    if product_key:
        items = items.loc[items.product_key.eq(product_key)]
    item_keys = ['product_key', 'source', 'item_name', 'spec']
    ranked = items.assign(amount_won=items.amount_won.astype(object)).groupby(item_keys, as_index=False).amount_won.sum()
    ranked = ranked.sort_values(['amount_won', 'product_key'], ascending=[False, True]).head(10)
    top = [{**r, 'amount_won': int(r['amount_won'])} for r in ranked.to_dict('records')]
    categories = {category: total(rows) for category, rows in purchases.groupby('category')}
    quality = selected['quality_daily']
    # 원문 reason/evidence에는 원천 경로가 있을 수 있으므로 상태와 누락 여부만 제공한다.
    def quality_summary(first, last):
        quality = tables['quality_daily'].loc[tables['quality_daily'].date.between(first, last)]
        counts = [{'source': str(src), 'status': str(status), 'days': len(rows)}
                  for (src, status), rows in quality.groupby(['source', 'status'])]
        days = (date.fromisoformat(last) - date.fromisoformat(first)).days + 1
        for src in ('sales', 'food_guide', 'logistics_dam'):
            missing = days - quality.loc[quality.source.eq(src), 'date'].nunique()
            if missing > 0:
                counts.append({'source': src, 'status': '품질 기록 없음', 'days': int(missing)})
        return counts
    quality_counts = quality_summary(start, end)
    period = {'sales_won': total(selected['sales_daily']), 'sales_rows': len(selected['sales_daily']),
              'purchase_won': total(purchases), 'purchase_rows': len(purchases),
              'categories': categories, 'items': top, 'quality': quality_counts}
    if intent in ('summary', 'status', 'staff_meal'):
        value = period
    elif intent == 'items':
        value = {'items': top}
    elif intent == 'evidence':
        fields = ['purchase_date', 'source', 'product_key', 'item_name', 'spec', 'amount_won', 'evidence_id', 'category']
        rows = lines.sort_values(['purchase_date', 'evidence_id'])
        value = {'total': len(rows), 'offset': offset, 'limit': limit,
                 'rows': rows.iloc[offset:offset + limit][fields].to_dict('records'),
                 'next_offset': offset + limit if offset + limit < len(rows) else None}
    else:
        length = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        previous_end = date.fromisoformat(start) - timedelta(days=1)
        previous_start = previous_end - timedelta(days=length - 1)
        value = {'previous_from': str(previous_start), 'previous_to': str(previous_end), 'comparable': False}
        if str(previous_start) >= context['window_start']:
            frame = tables['item_daily'] if product_key else tables['purchase_daily']
            if product_key:
                frame = frame.loc[frame.product_key.eq(product_key)]
            if source:
                frame = frame.loc[frame.source.eq(source)]
            current = frame.loc[frame.purchase_date.between(start, end)]
            previous = frame.loc[frame.purchase_date.between(str(previous_start), str(previous_end))]
            value.update(current_won=total(current), previous_won=total(previous),
                         comparable=not current.empty and not previous.empty,
                         difference_won=total(current) - total(previous))
        value['quality'] = quality_summary(str(max(previous_start, date.fromisoformat(context['window_start']))), end)
    return {**result, 'result': value}


def render(result):
    start, end = result['date_from'], result['date_to']
    if result['status'] == '미지원':
        return UNSUPPORTED
    if result['status'] == '범위 밖':
        return f"요청 기간 {start}~{end} 자료가 부족합니다. 현재 보유 범위는 {result['window_start']}~{result['as_of_date']}입니다. 다른 날짜로 대체하지 않았습니다."
    value, intent = result['result'], result['intent']
    lines = [f"{start} ~ {end} / 잠정 관측값 (자료 기준일 {result['as_of_date']})"]
    won = lambda v: f'{int(v):,}원'
    def show_items(items):
        return [f"- {r['item_name']} / {r['spec']} / {r['source']}: {won(r['amount_won'])}" for r in items] or ['관측된 구매 품목이 없습니다. 실제 구매 0원으로 확정하지 않습니다.']
    if intent in ('summary', 'status'):
        lines += [f"매출 관측액: {won(value['sales_won'])}" if value['sales_rows'] else '매출 관측 거래 없음 (실제 0원 확정 아님)',
                  f"전체 구매 관측액: {won(value['purchase_won'])} (직원식대 포함)" if value['purchase_rows'] else '구매 관측 거래 없음 (실제 0원 확정 아님)']
        lines += [f"- {k}: {won(v)}" for k, v in sorted(value['categories'].items())]
        if not {'원재료', '포장재'} <= value['categories'].keys():
            lines.append('원재료·포장재: 분류 확인 전인 항목은 미분류에 표시합니다.')
        lines += ['주요 구매 품목 (구매 관측액 순):', *show_items(value['items'])]
    elif intent == 'staff_meal':
        lines.append('현장 직원식대 관측액: ' + won(value['categories']['현장 직원식대']) if '현장 직원식대' in value['categories'] else '직원식대 관측 거래 없음 (실제 0원 확정 아님)')
    elif intent == 'items':
        lines += ['구매 관측액 순위 (수량 순위 아님):', *show_items(value['items'])]
    elif intent == 'compare':
        lines.append(f"비교 기간: {value['previous_from']} ~ {value['previous_to']}")
        if value['comparable']:
            lines.append(f"현재 구매 관측액 {won(value['current_won'])}, 직전 {won(value['previous_won'])}, 차이 {won(value['difference_won'])}")
        else:
            lines.append('비교 기간의 자료 또는 관측 거래가 부족해 증감을 제시하지 않습니다.')
    elif intent == 'evidence':
        lines.append(f"구매 근거 {value['total']}행 중 {value['offset'] + 1 if value['rows'] else 0}부터 {min(20, len(value['rows']))}행")
        lines += [f"{r['purchase_date']} / {r['item_name']} / {r['spec']} / {r['source']} / {won(r['amount_won'])} / 근거 {r['evidence_id']}" for r in value['rows'][:20]]
        lines.append('매출 상세 근거는 이 게시본에 없습니다.')
    if value.get('quality'):
        lines += ['자료 품질: ' + ', '.join(f"{r['source']} {r['status']} {r['days']}일" for r in value['quality'])]
        if any(r['status'] != '정상' for r in value['quality']):
            lines.append('수집·분류·마감 확인이 남아 있습니다. 누락 거래나 아직 반영되지 않은 자료가 있을 수 있습니다.')
    lines.append('잠정 관측값이며 확정 원가율·목표 초과·손실 판단은 보류합니다.')
    return '\n'.join(lines)
