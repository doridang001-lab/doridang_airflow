"""불변 스냅샷과 제한된 조회의 서비스 어댑터."""
import json
import re
from datetime import date, timedelta
from pathlib import Path
from modules.transform.pipelines.db.samjeon_material_agent.query import load_snapshot, query_snapshot


class Repository:
    def __init__(self, settings):
        self.settings = settings
        self.last_good = None
        self.fallback_reason = None

    def current(self):
        self.fallback_reason = None
        try:
            result = self._current()
            self.last_good = str(result[0])
            return result
        except (OSError, ValueError, KeyError, TypeError):
            if not self.last_good or self.settings.snapshot:
                raise
            root = Path(self.settings.snapshot_root).resolve() / 'snapshots'
            candidate = Path(self.last_good).resolve()
            if not candidate.is_relative_to(root):
                raise ValueError('이전 정상 자료 경로가 게시 루트 밖입니다') from None
            _, context, _ = load_snapshot(candidate)
            self.fallback_reason = '최신 자료 동기화를 기다리는 중입니다. 이전에 검증한 자료를 표시합니다.'
            return candidate, context

    def _current(self):
        if self.settings.snapshot:
            path = Path(self.settings.snapshot).resolve()
        else:
            if not self.settings.snapshot_root:
                raise ValueError('v3 스냅샷 경로가 등록되지 않았습니다')
            root = Path(self.settings.snapshot_root).resolve()
            pointer = json.loads((root / 'latest.json').read_text(encoding='utf-8'))
            if pointer.get('schema_version') != 3:
                raise ValueError('최신 게시본이 v3가 아닙니다. v3 게시가 필요합니다')
            manifest = (root / pointer['manifest']).resolve()
            if not manifest.is_relative_to(root / 'snapshots') or manifest.name != 'manifest.json':
                raise ValueError('최신 포인터의 경로가 허용 범위 밖입니다')
            path = manifest.parent
        manifest, context, _ = load_snapshot(path)
        if not self.settings.snapshot and (manifest['snapshot_id'] != pointer['snapshot_id'] or
                                          manifest['as_of_date'] != pointer['as_of_date']):
            raise ValueError('최신 포인터와 스냅샷 판본 불일치')
        return path, context

    def context(self, path, expected=None):
        _, context, _ = load_snapshot(Path(path))
        if expected and context['snapshot_id'] != expected:
            raise ValueError('대화의 스냅샷 판본이 변경됐습니다')
        return context

    def query(self, path, intent, start=None, end=None, **filters):
        if intent == 'compare' and filters.get('product_key'):
            return self.compare_item(path, filters['product_key'], start, end)
        return query_snapshot(Path(path), intent=intent, date_from=start, date_to=end, **filters)

    def items(self, path, question):
        _, _, tables = load_snapshot(Path(path))
        frame = tables['item_daily']
        if frame.empty:
            return []
        names = frame[['product_key', 'item_name', 'spec', 'source']].drop_duplicates().to_dict('records')
        exact = [r for r in names if r['item_name'] and r['item_name'] in question]
        if exact:
            length = max(len(r['item_name']) for r in exact)
            return [r for r in exact if len(r['item_name']) == length][:10]
        match = re.search(r'([^\s?]+?)(?:은|는|이|가)\s*(?:얼마|얼만|왜|늘|증가)', question)
        if match and match[1] not in ('그것', '그게', '매입', '매출', '직원식대', '구매', '품목'):
            return [r for r in names if match[1] in r['item_name']][:10]
        return []

    def search_items(self, path, name):
        _, _, tables = load_snapshot(Path(path))
        rows = tables['item_daily'][['product_key', 'item_name', 'spec', 'source']].drop_duplicates()
        exact = rows.loc[rows['item_name'].eq(name)]
        selected = exact if not exact.empty else rows.loc[rows['item_name'].str.contains(name, regex=False, na=False)]
        return selected.head(10).to_dict('records')

    def compare_item(self, path, product_key, start, end):
        from modules.transform.pipelines.db.samjeon_material_agent.analytics import period_result, compare_periods
        check = query_snapshot(Path(path), intent='compare', date_from=start, date_to=end)
        if check['status'] == '범위 밖':
            return check
        _, context, tables = load_snapshot(Path(path))
        selected = tables['item_daily'].loc[tables['item_daily']['product_key'].eq(product_key)]
        if selected.empty:
            raise ValueError('해당 품목의 구매 근거가 없습니다')
        tables['purchase_daily'] = selected
        tables['purchase_lines'] = tables['purchase_lines'].loc[tables['purchase_lines']['product_key'].eq(product_key)]
        finish = date.fromisoformat(check['date_to'])
        current = period_result(str(finish - timedelta(days=6)), str(finish), tables, context['settings'])
        previous = period_result(str(finish - timedelta(days=13)), str(finish - timedelta(days=7)), tables, context['settings'])
        comparison = compare_periods(current, previous, 'observed_purchase_won', 'purchase_complete')
        first = selected.iloc[0]
        item = {'item_name': str(first['item_name']), 'product_key': product_key, 'source': str(first['source']),
                'current_won': current['observed_purchase_won'], 'previous_won': previous['observed_purchase_won'],
                'increase_won': comparison['increase_won']}
        check['result'] = {'seven_day': comparison, 'top_increase_items': [item] if comparison['complete'] else [],
                           'selected_item': item['item_name']}
        return check

    def period_context(self, path, expected, start, end):
        from modules.transform.pipelines.db.samjeon_material_agent.analytics import build_context
        manifest, context, tables = load_snapshot(Path(path))
        if context['snapshot_id'] != expected:
            raise ValueError('대화의 스냅샷 판본이 변경됐습니다')
        check = self.query(path, 'anomalies', start, end)
        if check['status'] == '범위 밖':
            raise ValueError('상세 분석에 필요한 과거 자료가 부족합니다')
        plan = {'as_of_date': end, 'start_date': manifest['window_start'], 'query_start': start,
                'snapshot_id': expected, 'guide_sha256': manifest['guide_sha256']}
        return build_context(plan, tables, context['classification_version'], context['settings'])
