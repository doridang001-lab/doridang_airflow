"""Telegram 대화에서 저장소 텍스트 파일만 읽고 생성·수정하는 도구."""
import hashlib
from pathlib import Path

from .analysis_tools import spec


FILE_TOOLS = [
    spec('list_workspace', '허용된 로컬 코드 저장소 안의 파일과 폴더를 나열한다.',
         {'path': {'type': 'string'}}),
    spec('read_workspace_file', '허용된 로컬 코드 저장소의 UTF-8 텍스트 파일과 수정 확인용 SHA-256을 읽는다.',
         {'path': {'type': 'string'}}, ('path',)),
    spec('write_workspace_file', '허용된 로컬 코드 저장소의 UTF-8 텍스트 파일을 생성하거나 수정한다. 삭제는 지원하지 않는다. '
         '기존 파일은 먼저 읽은 SHA-256을 expected_sha256에 넣고, 새 파일은 빈 문자열을 넣는다.',
         {'path': {'type': 'string'}, 'content': {'type': 'string'},
          'expected_sha256': {'type': 'string'}}, ('path', 'content', 'expected_sha256')),
]


class WorkspaceFiles:
    def __init__(self, root=None):
        self.root = (Path(root) if root else Path(__file__).resolve().parents[5]).resolve()
        if 'onedrive' in str(self.root).lower():
            raise ValueError('OneDrive는 별도 승인이 필요합니다')

    def _path(self, value):
        if not isinstance(value, str) or not value or '\x00' in value:
            raise ValueError('path_invalid')
        raw = Path(value)
        if (raw.is_absolute() or '..' in raw.parts or any(part.startswith('.') or
                'onedrive' in part.lower() for part in raw.parts)):
            raise ValueError('path_not_allowed')
        target = self.root / raw
        if any(part.is_symlink() for part in (target, *target.parents) if part != self.root):
            raise ValueError('symlink_not_allowed')
        if not target.resolve().is_relative_to(self.root):
            raise ValueError('path_not_allowed')
        if any(part.lower() in ('.git', '.venv', '.tmp', '__pycache__') for part in raw.parts):
            raise ValueError('path_not_allowed')
        return target

    def call(self, name, arguments):
        try:
            if not isinstance(arguments, dict):
                return {'error': 'arguments_invalid'}
            path = self._path(arguments.get('path', '.'))
            if name == 'list_workspace':
                if not path.is_dir():
                    return {'error': 'directory_not_found'}
                entries = [p for p in sorted(path.iterdir()) if not p.name.startswith('.')
                           and p.name != '__pycache__' and not p.is_symlink()]
                return {'path': str(path.relative_to(self.root)),
                        'entries': [{'name': p.name, 'type': 'directory' if p.is_dir() else 'file'}
                                    for p in entries[:100]], 'truncated': len(entries) > 100}
            if path.suffix.lower() not in ('.py', '.md', '.txt', '.json', '.yaml', '.yml', '.toml',
                                           '.ps1', '.csv', '.sql', '.html', '.css', '.js', '.ts',
                                           '.tsx', '.sh'):
                return {'error': 'file_type_not_allowed'}
            if name == 'read_workspace_file':
                data = path.read_bytes()
                if len(data) > 200_000:
                    return {'error': 'file_too_large'}
                return {'path': str(path.relative_to(self.root)), 'content': data.decode('utf-8'),
                        'sha256': hashlib.sha256(data).hexdigest()}
            if name == 'write_workspace_file':
                content, expected = arguments.get('content'), arguments.get('expected_sha256')
                if not isinstance(content, str) or not content.strip() or len(content.encode('utf-8')) > 200_000:
                    return {'error': 'content_invalid'}
                if not isinstance(expected, str):
                    return {'error': 'expected_sha256_invalid'}
                old = path.read_bytes() if path.exists() else None
                if old is not None and (not path.is_file() or len(old) > 200_000):
                    return {'error': 'file_not_editable'}
                actual = hashlib.sha256(old).hexdigest() if old is not None else ''
                if actual != expected:
                    return {'error': 'file_changed', 'sha256': actual}
                if old is not None:
                    old.decode('utf-8')
                path.parent.mkdir(parents=True, exist_ok=True)
                data = content.encode('utf-8')
                # 새 파일은 독점 생성하여 다른 프로세스의 파일을 덮어쓰지 않는다.
                if old is None:
                    with path.open('xb') as handle:
                        handle.write(data)
                else:
                    with path.open('wb') as handle:
                        handle.write(data)
                return {'path': str(path.relative_to(self.root)), 'sha256': hashlib.sha256(data).hexdigest(),
                        'created': old is None}
            return {'error': 'tool_not_allowed'}
        except (OSError, UnicodeError, ValueError):
            return {'error': 'file_unavailable'}
