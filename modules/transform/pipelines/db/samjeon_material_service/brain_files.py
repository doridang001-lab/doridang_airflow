"""대표의 OneDrive 지식 폴더에서 Markdown·텍스트만 읽는다."""
from pathlib import Path
import re
from zipfile import BadZipFile, ZipFile

from .analysis_tools import spec


BRAIN_TOOLS = [
    spec('list_brain', '도리당 지식 폴더의 하위 Markdown·텍스트·Excel 파일 목록을 읽는다. Excel 셀 내용은 이 도구로 읽지 않는다.', {}),
    spec('search_brain', '도리당 지식 폴더의 Markdown·텍스트에서 질문 관련 문장을 찾는다.',
         {'query': {'type': 'string'}}, ('query',)),
    spec('read_brain_file', '도리당 지식 폴더의 한 파일을 일부 읽는다. 반환 경로는 출처로만 사용한다.',
         {'path': {'type': 'string'}, 'offset': {'type': 'integer'}}, ('path',)),
    spec('inspect_brain_workbook', '사용자가 파일명을 명시한 Excel(.xlsx)의 시트 목록만 읽는다.',
         {'path': {'type': 'string'}}, ('path',)),
    spec('read_brain_sheet', '사용자가 파일명을 명시한 Excel(.xlsx)의 지정 시트에서 최대 20행을 읽는다. 민감 열·값은 가린다.',
         {'path': {'type': 'string'}, 'sheet': {'type': 'string'}, 'offset': {'type': 'integer'}},
         ('path', 'sheet')),
]

SENSITIVE_HEADER = re.compile(r'이름|성명|고객명|담당자|전화|휴대폰|핸드폰|이메일|주민|생년월일|주소|계좌|카드번호|비밀번호|인증|토큰|api.?key|password|secret|phone|email|account', re.I)
SENSITIVE_VALUE = re.compile(r'(?<!\w)(?:[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|01[016789][- ]?\d{3,4}[- ]?\d{4}|\d{6}[- ]?[1-4]\d{6}|\d{10,18})(?!\w)')


class BrainFiles:
    def __init__(self, root, user_query=''):
        self.root = Path(root).resolve() if root else None
        self.user_query = user_query if isinstance(user_query, str) else ''

    def _files(self, include_excel=False):
        if not self.root or not self.root.is_dir():
            return []
        found = []
        for path in self.root.rglob('*'):
            if len(found) >= 500:
                break
            if (path.suffix.lower() not in (('.md', '.txt', '.xlsx') if include_excel else ('.md', '.txt'))
                    or not path.is_file()
                    or any(part.startswith('.') for part in path.relative_to(self.root).parts)
                    or path.is_symlink() or not path.resolve().is_relative_to(self.root)):
                continue
            found.append(path)
        return sorted(found, key=lambda p: str(p.relative_to(self.root)).casefold())

    def _path(self, value):
        if not isinstance(value, str) or not value or '\x00' in value or not self.root:
            raise ValueError('path_invalid')
        raw = Path(value)
        if raw.is_absolute() or '..' in raw.parts or any(p.startswith('.') for p in raw.parts):
            raise ValueError('path_not_allowed')
        path = self.root / raw
        if path.suffix.lower() not in ('.md', '.txt') or not path.is_file():
            raise ValueError('file_not_found')
        if any(p.is_symlink() for p in (path, *path.parents) if p != self.root):
            raise ValueError('path_not_allowed')
        if not path.resolve().is_relative_to(self.root):
            raise ValueError('path_not_allowed')
        return path

    def _workbook(self, value):
        if not isinstance(value, str) or not value or '\x00' in value or not self.root:
            raise ValueError('path_invalid')
        raw = Path(value)
        if (raw.is_absolute() or '..' in raw.parts or any(p.startswith('.') for p in raw.parts)
                or raw.suffix.lower() != '.xlsx'):
            raise ValueError('path_not_allowed')
        path = self.root / raw
        if not path.is_file() or any(p.is_symlink() for p in (path, *path.parents) if p != self.root):
            raise ValueError('file_not_found')
        if not path.resolve().is_relative_to(self.root):
            raise ValueError('path_not_allowed')
        if path.name.casefold() not in self.user_query.casefold():
            raise PermissionError('excel_explicit_request_required')
        if path.stat().st_size > 5_000_000:
            raise ValueError('file_too_large')
        with ZipFile(path) as archive:
            if len(archive.infolist()) > 1000 or sum(part.file_size for part in archive.infolist()) > 20_000_000:
                raise ValueError('file_too_large')
        return path

    @staticmethod
    def _safe_cell(value, sensitive=False):
        if value is None:
            return None
        if sensitive:
            return '[민감정보]'
        value = str(value)[:200]
        return SENSITIVE_VALUE.sub('[민감정보]', value)

    def _excel(self, name, arguments):
        from openpyxl import load_workbook
        path = self._workbook(arguments.get('path'))
        book = load_workbook(path, read_only=True, data_only=True, keep_links=False)
        try:
            relative = str(path.relative_to(self.root)).replace('\\', '/')
            if name == 'inspect_brain_workbook':
                return {'path': relative, 'sheets': [{'name': sheet.title,
                        'rows': sheet.max_row, 'columns': sheet.max_column} for sheet in book.worksheets[:30]]}
            sheet_name = arguments.get('sheet')
            offset = arguments.get('offset', 0)
            if (not isinstance(sheet_name, str) or sheet_name not in book.sheetnames
                    or type(offset) is not int or not 0 <= offset <= 100_000):
                return {'error': 'sheet_or_offset_invalid'}
            sheet = book[sheet_name]
            row_count, column_count = sheet.max_row or 0, sheet.max_column or 0
            if column_count > 100 or row_count > 100_000:
                return {'error': 'sheet_too_large'}
            rows = sheet.iter_rows(min_row=1, max_row=min(row_count, offset + 21),
                                   max_col=min(column_count, 20), values_only=True)
            header = next(rows, ())
            sensitive = [bool(SENSITIVE_HEADER.search(str(value or ''))) for value in header]
            data = list(sheet.iter_rows(min_row=offset + 2, max_row=min(row_count, offset + 21),
                                        max_col=min(column_count, 20), values_only=True))
            return {'path': relative, 'sheet': sheet_name, 'offset': offset,
                    'columns': [self._safe_cell(value) for value in header],
                    'rows': [[self._safe_cell(value, sensitive[i]) for i, value in enumerate(row)]
                             for row in data],
                    'next_offset': offset + len(data) if offset + len(data) < row_count - 1 else None}
        finally:
            book.close()

    def call(self, name, arguments):
        if not isinstance(arguments, dict):
            return {'error': 'arguments_invalid'}
        try:
            if not self.root or not self.root.is_dir():
                return {'error': 'brain_unavailable'}
            if name in ('inspect_brain_workbook', 'read_brain_sheet'):
                return self._excel(name, arguments)
            if name == 'list_brain':
                files = self._files(include_excel=True)
                return {'files': [{'path': str(path.relative_to(self.root)).replace('\\', '/'),
                                   'size': path.stat().st_size, 'type': path.suffix.lower().lstrip('.')}
                                  for path in files],
                        'truncated': len(files) >= 500}
            if name == 'read_brain_file':
                path = self._path(arguments.get('path'))
                offset = arguments.get('offset', 0)
                if type(offset) is not int or offset < 0 or offset > 500_000:
                    return {'error': 'offset_invalid'}
                if path.stat().st_size > 500_000:
                    return {'error': 'file_too_large'}
                content = path.read_text(encoding='utf-8-sig')
                return {'path': str(path.relative_to(self.root)).replace('\\', '/'),
                        'content': SENSITIVE_VALUE.sub('[민감정보]', content[offset:offset + 8000]),
                        'next_offset': offset + 8000
                        if offset + 8000 < len(content) else None}
            if name == 'search_brain':
                query = arguments.get('query')
                if not isinstance(query, str) or not 2 <= len(query.strip()) <= 100:
                    return {'error': 'query_invalid'}
                terms = [term.casefold() for term in re.findall(r'[가-힣A-Za-z0-9]{2,}', query)][:8]
                matches = []
                for path in self._files():
                    if path.stat().st_size > 500_000:
                        continue
                    content = path.read_text(encoding='utf-8-sig')
                    lowered = content.casefold()
                    positions = [lowered.find(term) for term in terms]
                    positions = [pos for pos in positions if pos >= 0]
                    if positions:
                        pos = min(positions)
                        matches.append({'path': str(path.relative_to(self.root)).replace('\\', '/'),
                                        'snippet': SENSITIVE_VALUE.sub('[민감정보]',
                                            content[max(0, pos - 120):pos + 300])})
                    if len(matches) >= 8:
                        break
                return {'matches': matches, 'truncated': len(matches) >= 8}
            return {'error': 'tool_not_allowed'}
        except PermissionError:
            return {'error': 'excel_explicit_request_required'}
        except (OSError, UnicodeError, ValueError, ImportError, KeyError, BadZipFile):
            return {'error': 'brain_unavailable'}
