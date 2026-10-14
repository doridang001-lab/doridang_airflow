"""WSL 표준입출력 실행기. Linux 자식 실행과 취소만 담당한다."""
import json
import os
from pathlib import Path
import sys
import tempfile
import threading

# -I 실행에서 지정된 저장소 코드만 사용한다. 사용자 PYTHONPATH는 무시한다.
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter
from modules.transform.pipelines.db.samjeon_material_service.config import Settings


def linux_environment(executable):
    import pwd
    home = pwd.getpwuid(os.getuid()).pw_dir
    return {'HOME': home, 'CODEX_HOME': str(Path(home) / '.codex'),
            'PATH': str(Path(executable).parent) + ':/usr/local/bin:/usr/bin:/bin',
            'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'}


def handle(request, cancelled):
    executable = request.get('codex_executable', '')
    candidate = Path(executable)
    if (not isinstance(executable, str) or not candidate.is_absolute()
            or candidate.resolve().as_posix().startswith('/mnt/')
            or candidate.suffix.lower() in ('.exe', '.cmd', '.ps1', '.bat')):
        return {'error': 'linux_cli_required'}
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        return {'error': 'cli_missing'}
    action = request.get('action')
    if action not in ('diagnose', 'invoke'):
        return {'error': 'wsl_protocol_error'}
    # 이 프로세스 안에서만 환경을 교체한다. 셸 프로필과 Windows 인증을 가져오지 않는다.
    env = linux_environment(executable)
    os.environ.clear()
    os.environ.update(env)
    settings = Settings(codex_executable=executable, ai_enabled=True, ai_verified=True)
    if action == 'diagnose':
        return {'diagnostic': CodexAdapter(settings).diagnose()}
    if (not isinstance(request.get('schema'), dict) or not isinstance(request.get('context'), dict)
            or not isinstance(request.get('instruction'), str) or not isinstance(request.get('question'), str)):
        return {'error': 'wsl_protocol_error'}
    from dataclasses import replace
    with tempfile.TemporaryDirectory(prefix='samjeon-codex-') as work:
        adapter = CodexAdapter(replace(settings, state_root=work))
        answer = adapter._invoke(request['schema'], request['instruction'], request['question'],
                                 request['context'], cancelled)
        return {'answer': answer, 'error': adapter.last_error, 'event_type': adapter.last_event_type}


def main():
    request_id = None
    cancelled = threading.Event()
    try:
        raw = sys.stdin.buffer.readline(256_001)
        if len(raw) > 256_000 or not raw.endswith(b'\n'):
            raise ValueError('request')
        request = json.loads(raw)
        request_id = request['request_id']
        if not isinstance(request_id, str) or len(request_id) != 32 or any(c not in '0123456789abcdef' for c in request_id):
            request_id = None
            raise ValueError('request')

        def watch_parent():
            # 두 번째 줄(취소) 또는 부모 종료(EOF)는 모두 현재 요청만 취소한다.
            sys.stdin.buffer.readline(256)
            cancelled.set()

        threading.Thread(target=watch_parent, daemon=True).start()
        response = handle(request, cancelled)
    except Exception:
        response = {'error': 'wsl_runner_failed'}
    print(json.dumps({'request_id': request_id, **response}, ensure_ascii=False), flush=True)
    # 표준입력 대기 스레드와 CPython 종료 중 buffered-reader lock 충돌을 피한다.
    os._exit(0)


if __name__ == '__main__':
    main()
