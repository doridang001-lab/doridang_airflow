"""검증된 게시본을 자유롭게 조회하는 읽기 전용 분석 도구."""
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from contextlib import closing

from .config import local_path
from .repository import checked_snapshot, SnapshotUnavailable


def spec(name, description, properties, required=()):
    return {'type': 'function', 'name': name, 'description': description,
            'inputSchema': {'type': 'object', 'properties': properties,
                            'required': list(required), 'additionalProperties': False}}


TOOLS = [
    spec('data_catalog', '현재 연결된 검증 자료의 테이블·컬럼·기간·품질을 확인한다. SQL 작성 전에 확인한다.', {}),
    spec('query_data', '현재 판본을 읽기 전용 SQLite SQL로 조회·집계·비교·계산한다. SELECT/WITH, JOIN, 윈도 함수, '
         'sqrt/pow/log와 stdev 집계가 가능하다. 최대 1000행. 숫자 날짜는 데이터에서 확인하며 구매액은 사용원가가 아니다.',
         {'sql': {'type': 'string'}, 'parameters': {'type': 'array', 'items': {
             'type': ['string', 'number', 'null', 'boolean']}}}, ('sql',)),
    spec('get_evidence', 'purchase_lines에서 evidence_id로 구매 원행을 확인한다.',
         {'evidence_ids': {'type': 'array', 'items': {'type': 'string'}, 'maxItems': 50}}, ('evidence_ids',)),
    spec('search_conversation', '같은 사용자와 대화의 전달 완료 과거 기록을 검색한다. 검색어 또는 날짜·품목을 사용한다.',
         {'query': {'type': 'string'}}, ('query',)),
]


class Stdev:
    def __init__(self):
        self.n, self.mean, self.m2 = 0, 0.0, 0.0

    def step(self, value):
        if value is not None:
            self.n += 1
            delta = float(value) - self.mean
            self.mean += delta / self.n
            self.m2 += delta * (float(value) - self.mean)

    def finalize(self):
        return math.sqrt(max(0, self.m2) / (self.n - 1)) if self.n > 1 else None


class AnalysisTools:
    """요청마다 생성해 판본을 고정한다. 운영 이력 DB에 연결하지 않는다."""
    def __init__(self, settings, repository, cancelled=None):
        self.settings, self.repository = settings, repository
        self.cancelled = cancelled or threading.Event()
        self.path = None
        self.context = None
        self.catalog = None
        self.trace = []

    def prepare(self):
        if self.path:
            return
        source, _ = self.repository.current()
        manifest, context, tables = checked_snapshot(source)
        fingerprint = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        root = local_path(self.settings.state_root) / 'analysis-cache'
        root.mkdir(parents=True, exist_ok=True)
        target = root / (fingerprint + '.sqlite3')
        # 게시 파일의 경로·개인 환경 정보는 분석 DB에도 복사하지 않는다.
        clean = {name: table.drop(columns=[c for c in table.columns
                    if c in ('source_file', 'source_path') or c.endswith('_path')], errors='ignore')
                 for name, table in tables.items()}
        if not target.exists():
            temporary = root / (uuid.uuid4().hex + '.tmp.sqlite3')
            try:
                with closing(sqlite3.connect(temporary)) as db:
                    for name, table in clean.items():
                        if self.cancelled.is_set():
                            raise InterruptedError('cancelled')
                        table.to_sql(name, db, index=False)
                    if 'purchase_lines' in clean and 'evidence_id' in clean['purchase_lines']:
                        db.execute('CREATE INDEX evidence_lookup ON purchase_lines(evidence_id)')
                    db.commit()
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
        self.path, self.context = target, context
        self.catalog = {
            'snapshot_id': context['snapshot_id'], 'as_of_date': context['as_of_date'],
            'available_from': context.get('window_start'), 'format': context.get('format'),
            'status': context.get('status', '품질 테이블 확인 필요'),
            'tables': {name: {'columns': list(table.columns), 'rows': len(table)} for name, table in clean.items()},
            'definitions': {'amount_won': '원 단위 관측 금액. 구매 금액은 재고 사용원가·손실이 아니다.',
                            'row_count': '원천 행수. 품목 수량과 다르다.',
                            'quality_daily': '원천·날짜별 품질. 행 부재를 정상 또는 0으로 해석하지 않는다.',
                            'category': '미분류를 원재료로 추정하지 않는다.',
                            'dates': '날짜 문자열은 YYYY-MM-DD. 보유 기간은 월 전체 완료를 뜻하지 않는다.'}}

    def query(self, sql, parameters=()):
        if not isinstance(sql, str) or len(sql) > 20000:
            raise ValueError('query_invalid')
        if not isinstance(parameters, (list, tuple)) or len(parameters) > 1000:
            raise ValueError('parameters_invalid')
        self.prepare()
        deadline = time.monotonic() + 15
        with closing(sqlite3.connect(self.path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.enable_load_extension(False)
            db.create_aggregate('stdev', 1, Stdev)
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 262144)
            db.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 20000)
            db.setlimit(sqlite3.SQLITE_LIMIT_COLUMN, 100)
            allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
            tables = set(self.catalog['tables'])

            def authorize(action, first, second, database, origin):
                if action not in allowed or (action == sqlite3.SQLITE_READ and first not in tables):
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_FUNCTION and second in ('load_extension', 'readfile', 'writefile'):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            db.set_authorizer(authorize)
            db.set_progress_handler(lambda: int(self.cancelled.is_set() or time.monotonic() > deadline), 1000)
            cursor = db.execute(sql, parameters)
            if cursor.description is None:
                raise ValueError('query_invalid')
            columns = [c[0] for c in cursor.description]
            rows, size, truncated = [], 0, False
            for row in cursor:
                values = [v.hex() if isinstance(v, bytes) else v for v in row]
                size += len(json.dumps(values, ensure_ascii=False).encode())
                if len(rows) == 1000 or size > 128000:
                    truncated = True
                    break
                rows.append(values)
        query_id = 'Q' + str(len(self.trace) + 1)
        result = {'query_id': query_id, 'snapshot_id': self.context['snapshot_id'],
                  'as_of_date': self.context['as_of_date'], 'status': self.catalog['status'],
                  'columns': columns, 'rows': rows, 'truncated': truncated}
        self.trace.append({'query_id': query_id, 'sql': sql, 'rows': len(rows),
                           'snapshot_id': self.context['snapshot_id']})
        return result

    def call(self, name, arguments):
        try:
            if self.cancelled.is_set():
                return {'error': 'cancelled'}
            if not isinstance(arguments, dict):
                return {'error': 'arguments_invalid'}
            if name == 'data_catalog':
                self.prepare()
                return self.catalog
            if name == 'query_data':
                return self.query(arguments.get('sql'), arguments.get('parameters', []))
            if name == 'get_evidence':
                ids = arguments.get('evidence_ids')
                if not isinstance(ids, list) or not 1 <= len(ids) <= 50 or not all(isinstance(i, str) for i in ids):
                    return {'error': 'evidence_ids_invalid'}
                return self.query('SELECT * FROM purchase_lines WHERE evidence_id IN (' + ','.join('?' for _ in ids) + ')', ids)
            return {'error': 'tool_not_allowed'}
        except SnapshotUnavailable as exc:
            return {'error': exc.code, 'message': str(exc)}
        except sqlite3.Error as exc:
            # SQL은 모델이 작성한 문장이다. 원천 파일 경로를 포함할 수 있는 원본 예외는 노출하지 않는다.
            code = 'query_failed'
            message = str(exc)
            if 'no such column' in message:
                code = 'unknown_column'
            elif 'no such table' in message:
                code = 'unknown_table'
            elif 'not authorized' in message or 'prohibited' in message:
                code = 'query_not_allowed'
            elif 'interrupt' in message:
                code = 'query_cancelled_or_timeout'
            return {'error': code, 'hint': 'data_catalog로 구조를 확인하고 읽기 전용 SELECT 한 문장으로 다시 조회하세요.'}
        except (ValueError, OSError, KeyError, TypeError, InterruptedError):
            return {'error': 'data_unavailable'}
