"""기존 봇에서 질문에 선택된 월별 파일만 검증·적재하는 도구."""
from contextlib import closing
from datetime import date
from .prompt_assets import read_prompt
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid

import pandas as pd

from .analysis_tools import spec, Stdev
from .config import local_path


TOOLS = [
    spec('data_catalog', 'CSV 본문 없이 자료·월·테이블 목록을 확인한다. dataset을 생략하면 자료 목록만 반환한다.',
         {'dataset': {'type': 'string'}, 'offset': {'type': 'integer'}}),
    spec('read_guide', '선택 자료의 관련 설명을 최대 3부분, 6000자로 읽는다.',
         {'dataset': {'type': 'string'}, 'query': {'type': 'string'}, 'offset': {'type': 'integer'}}, ('dataset',)),
    spec('prepare_data', '필요한 테이블과 기간만 검증하고 로컬 SQL 조회를 준비한다. 전체 추세는 요약부터 준비한다. 반환된 테이블명을 SQL에서 사용한다.',
         {'dataset': {'type': 'string'}, 'tables': {'type': 'array', 'items': {'type': 'string'}, 'maxItems': 12},
          'date_from': {'type': 'string'}, 'date_to': {'type': 'string'}}, ('dataset', 'tables', 'date_from', 'date_to')),
    spec('query_data', '준비된 자료에 읽기 전용 SELECT를 수행한다. 합계는 SQL에서 계산한다. 잘린 목록은 전체 합계가 아니다.',
         {'sql': {'type': 'string'}, 'parameters': {'type': 'array', 'items': {'type': ['string', 'number', 'null', 'boolean']}},
          'detail': {'type': 'boolean'}}, ('sql',)),
    spec('search_conversation', '같은 사용자의 전달 완료 대화를 필요한 부분만 검색한다.', {'query': {'type': 'string'}}, ('query',)),
]

INSTRUCTIONS = read_prompt('data')


def fingerprint(value):
    return hashlib.sha256(value).hexdigest()


class CeoTools:
    def __init__(self, settings, repository=None, cancelled=None):
        self.settings = settings
        self.root = Path(settings.ceo_data_root).resolve()
        self.cancelled = cancelled or threading.Event()
        self.context = None
        self.trace = []
        self.prepared = {}
        self.query_cache = {}
        self.read_files = 0
        self.cache_hits = 0
        self.guide_versions = {}

    def _path(self, relative, base=None):
        base = base or self.root
        raw = Path(relative)
        if raw.is_absolute() or not raw.parts or any(p in ('.', '..') or p.startswith('.') for p in raw.parts):
            raise ValueError('path_not_allowed')
        path = base / raw
        if not path.resolve().is_relative_to(self.root) or any(p.is_symlink() for p in (path, *path.parents) if p != self.root):
            raise ValueError('path_not_allowed')
        return path

    def _manifest(self, dataset):
        path = self._path('manifest.json', dataset)
        if not path.exists():
            return None, None
        raw = path.read_bytes()
        manifest = json.loads(raw)
        if not isinstance(manifest, dict) or not isinstance(manifest.get('files'), dict):
            raise ValueError('manifest_invalid')
        return manifest, fingerprint(raw)

    def _files(self, dataset):
        manifest, version = self._manifest(dataset)
        names = list(manifest['files']) if manifest is not None else [p.relative_to(dataset).as_posix() for p in dataset.rglob('*.csv')]
        files = {}
        for name in names:
            path = self._path(name, dataset)
            if not re.fullmatch(r'\d{4}_(0[1-9]|1[0-2])\.csv', path.name):
                continue
            table = path.parent.relative_to(dataset).as_posix()
            files.setdefault(table, []).append((path, name))
        return files, manifest, version

    def catalog(self, dataset=None, offset=0):
        if type(offset) is not int or offset < 0:
            raise ValueError('offset_invalid')
        if not dataset:
            items = sorted(p.name for p in self.root.iterdir() if p.is_dir() and not p.is_symlink() and not p.name.startswith(('.', '_')))
            return {'datasets': items[offset:offset + 30], 'next_offset': offset + 30 if len(items) > offset + 30 else None}
        directory = self._path(dataset)
        files, manifest, _ = self._files(directory)
        tables = []
        for key, values in sorted(files.items()):
            months = sorted(p.stem for p, _ in values)
            tables.append({'table': key, 'months': months[-12:], 'month_count': len(months),
                           'available_from': months[0] if months else None,
                           'available_to': months[-1] if months else None, 'months_truncated': len(months) > 12})
        return {'dataset': dataset, 'tables': tables[offset:offset + 30],
                'next_offset': offset + 30 if len(tables) > offset + 30 else None,
                'verification': 'manifest' if manifest is not None else '원천 검증 정보 없음'}

    def guide(self, dataset, query='', offset=0):
        directory = self._path(dataset)
        if not isinstance(query, str) or len(query) > 1000 or type(offset) is not int or offset < 0:
            raise ValueError('guide_query_invalid')
        candidates = [self.root / 'AGENTS.md', directory / 'AGENTS.md']
        sections = []
        terms = re.findall(r'[가-힣A-Za-z0-9]{2,}', query.casefold())
        for path in candidates:
            if not path.is_file():
                continue
            path = self._path(path.relative_to(self.root).as_posix())
            if path.stat().st_size > 500_000:
                raise ValueError('guide_too_large')
            raw = path.read_bytes()
            self.guide_versions[path.relative_to(self.root).as_posix()] = fingerprint(raw)
            for section in re.split(r'(?m)(?=^#{1,4} )', raw.decode('utf-8-sig')):
                for start in range(0, len(section), 2000):
                    text = section[start:start + 2000]
                    sections.append((sum(term in text.casefold() for term in terms), path.relative_to(self.root).as_posix(), text))
        sections.sort(key=lambda item: -item[0])
        selected = sections[offset:offset + 3]
        return {'sections': [{'source': source, 'text': text} for _, source, text in selected],
                'versions': self.guide_versions, 'next_offset': offset + 3 if len(sections) > offset + 3 else None}

    def prepare(self, dataset, tables, date_from, date_to):
        first, last = date.fromisoformat(date_from), date.fromisoformat(date_to)
        if str(first) != date_from or str(last) != date_to or first > last:
            raise ValueError('period_invalid')
        if not isinstance(tables, list) or not 1 <= len(tables) <= 12 or not all(isinstance(x, str) for x in tables):
            raise ValueError('tables_invalid')
        directory = self._path(dataset)
        files, manifest, version = self._files(directory)
        # 가이드 본문은 모델에 필요한 부분만 보내되 캐시 판본은 매번 최신으로 고정한다.
        for guide in (self.root / 'AGENTS.md', directory / 'AGENTS.md'):
            if guide.is_file():
                guide = self._path(guide.relative_to(self.root).as_posix())
                if guide.stat().st_size > 500_000:
                    raise ValueError('guide_too_large')
                self.guide_versions[guide.relative_to(self.root).as_posix()] = fingerprint(guide.read_bytes())
        output, pending = [], {}
        for table in dict.fromkeys(tables):
            if table not in files:
                raise ValueError('table_unknown')
            selected = [(p, n) for p, n in files[table] if date_from[:7] <= p.stem.replace('_', '-') <= date_to[:7]]
            if not selected:
                raise ValueError('period_unavailable')
            hashes, frames, headers = {}, [], None
            for path, name in sorted(selected):
                if self.cancelled.is_set():
                    raise InterruptedError()
                raw = path.read_bytes()
                self.read_files += 1
                digest = fingerprint(raw)
                if manifest is not None:
                    expected = manifest['files'][name]
                    expected = expected.get('sha256') if isinstance(expected, dict) else expected
                    if digest != expected:
                        raise ValueError('file_hash_mismatch')
                hashes[name] = digest
                frame = pd.read_csv(io.BytesIO(raw), encoding='utf-8-sig', dtype=str, keep_default_na=False)
                if headers is not None and list(frame.columns) != headers:
                    raise ValueError('columns_changed')
                headers = list(frame.columns)
                if len(headers) > 100 or '날짜' not in headers:
                    raise ValueError('date_column_required')
                valid_dates = pd.to_datetime(frame['날짜'], format='%Y-%m-%d', errors='coerce')
                if valid_dates.isna().any() or not frame['날짜'].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all():
                    raise ValueError('date_invalid')
                frame = frame.loc[frame['날짜'].between(date_from, date_to)].copy()
                # 코드·이름을 숫자로 변환하지 않는다. 금액·수량 열만 확인하여 변환한다.
                for col in headers:
                    if col.endswith(('원', '수량', '주문수', '노출수', '클릭수', '고객수', '광고지출', '즉시할인', '수수료율')):
                        frame[col] = pd.to_numeric(frame[col].replace('', None), errors='raise')
                frames.append(frame)
            # 동기화 도중 서로 다른 판본을 묶지 않는다.
            for path, name in selected:
                if fingerprint(path.read_bytes()) != hashes[name]:
                    raise ValueError('source_changed')
            if self._manifest(directory)[1] != version:
                raise ValueError('manifest_changed')
            if any(fingerprint(self._path(name).read_bytes()) != digest for name, digest in self.guide_versions.items()):
                raise ValueError('guide_changed')
            identity = {'dataset': dataset, 'table': table, 'files': hashes, 'period': [date_from, date_to],
                        'guide': self.guide_versions, 'loader': 1}
            key = fingerprint(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode())
            alias = 't_' + fingerprint((dataset + '/' + table).encode())[:12]
            cache = local_path(self.settings.state_root) / 'analysis-cache'
            cache.mkdir(parents=True, exist_ok=True)
            target = cache / (key + '.sqlite3')
            combined = pd.concat(frames, ignore_index=True)
            if not target.exists():
                temporary = cache / (uuid.uuid4().hex + '.tmp')
                try:
                    with closing(sqlite3.connect(temporary)) as db:
                        combined.to_sql('data', db, index=False)
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
            else:
                self.cache_hits += 1
            pending[alias] = {'path': target, 'key': key}
            observed = sorted(combined['날짜'].unique().tolist())
            record = {'table': table, 'sql_table': alias, 'columns': headers, 'rows': len(combined),
                      'date_from': date_from, 'date_to': date_to,
                      'observed_from': observed[0] if observed else None, 'observed_to': observed[-1] if observed else None,
                      'observed_days': len(observed), 'requested_days': (last - first).days + 1,
                      'files': list(hashes), 'verification': '해시 검증 완료' if manifest is not None else '원천 검증 정보 없음',
                      'note': '관측일 수는 수집 완료나 무거래를 증명하지 않습니다. 상태 열과 가이드를 확인하세요.'}
            output.append(record)
        self.prepared.update(pending)
        self.query_cache.clear()
        self.context = {'snapshot_id': fingerprint(''.join(sorted(x['key'] for x in self.prepared.values())).encode()),
                        'as_of_date': max((r['observed_to'] or date_from) for r in output)}
        return {'prepared': output, 'snapshot_id': self.context['snapshot_id']}

    def query(self, sql, parameters=(), detail=False):
        if not self.prepared:
            raise ValueError('prepare_data_required')
        if not isinstance(sql, str) or len(sql) > 20000 or not re.match(r'^\s*(SELECT|WITH)\b', sql, re.I):
            raise ValueError('query_invalid')
        if not isinstance(parameters, (list, tuple)) or len(parameters) > 1000 or type(detail) is not bool:
            raise ValueError('parameters_invalid')
        key = json.dumps([self.context['snapshot_id'], sql, parameters, detail], ensure_ascii=False)
        if key in self.query_cache:
            self.cache_hits += 1
            return self.query_cache[key]
        with closing(sqlite3.connect(':memory:', uri=True)) as db:
            db.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 64)
            if len(self.prepared) > db.getlimit(sqlite3.SQLITE_LIMIT_ATTACHED):
                raise ValueError('too_many_prepared_tables')
            for i, (alias, entry) in enumerate(self.prepared.items()):
                db.execute(f'ATTACH DATABASE ? AS d{i}', (entry['path'].as_uri() + '?mode=ro&immutable=1',))
                db.execute(f'CREATE TEMP VIEW "{alias}" AS SELECT * FROM d{i}.data')
            db.execute('PRAGMA query_only=ON')
            db.enable_load_extension(False)
            db.create_aggregate('stdev', 1, Stdev)
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 262144)
            db.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 20000)
            allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
            def authorize(action, first, second, database, origin):
                if action not in allowed:
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and not (first in self.prepared or first == 'data' and origin in self.prepared):
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_FUNCTION and second in ('load_extension', 'readfile', 'writefile'):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            db.set_authorizer(authorize)
            deadline = time.monotonic() + 15
            db.set_progress_handler(lambda: int(self.cancelled.is_set() or time.monotonic() > deadline), 1000)
            cursor = db.execute(sql, parameters)
            columns = [c[0] for c in cursor.description]
            result = {'query_id': 'Q' + str(len(self.trace) + 1), **self.context,
                      'columns': columns, 'rows': [], 'truncated': False}
            cap, byte_cap = (100, 65536) if detail else (20, 16384)
            for row in cursor:
                values = [v.hex() if isinstance(v, bytes) else v for v in row]
                candidate = {**result, 'rows': result['rows'] + [values]}
                if len(result['rows']) >= cap or len(json.dumps(candidate, ensure_ascii=False).encode()) > byte_cap:
                    result['truncated'] = True
                    break
                result = candidate
        self.trace.append({'query_id': result['query_id'], 'rows': len(result['rows']), 'snapshot_id': self.context['snapshot_id']})
        self.query_cache[key] = result
        return result

    def call(self, name, arguments):
        try:
            if self.cancelled.is_set():
                return {'error': 'cancelled'}
            if not isinstance(arguments, dict):
                raise ValueError('arguments_invalid')
            action = {'data_catalog': self.catalog, 'read_guide': self.guide, 'prepare_data': self.prepare, 'query_data': self.query}.get(name)
            return action(**arguments) if action else {'error': 'tool_not_allowed'}
        except (ValueError, OSError, KeyError, TypeError, sqlite3.Error, InterruptedError) as exc:
            if name == 'prepare_data':
                self.prepared.clear()
                self.query_cache.clear()
                self.context = None
            safe = {'period_invalid', 'tables_invalid', 'table_unknown', 'period_unavailable', 'columns_changed',
                    'date_column_required', 'date_invalid', 'file_hash_mismatch', 'source_changed', 'manifest_changed',
                    'guide_changed', 'prepare_data_required', 'query_invalid', 'parameters_invalid', 'path_not_allowed'}
            return {'error': str(exc) if str(exc) in safe else 'data_unavailable',
                    'hint': '목록·기간·가이드를 확인하세요. 검증 실패 자료의 숫자를 추측하지 마세요.'}
