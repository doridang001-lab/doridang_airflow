"""Windows에서 동일 PC의 WSL 실행기에 요청한다. 셸과 원격 서버는 사용하지 않는다."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
import uuid


ERRORS = frozenset(('wsl_missing', 'wsl_distribution_missing', 'wsl_unavailable',
    'wsl_user_or_python_unavailable', 'wsl_path_unavailable', 'wsl_runner_failed',
    'wsl_protocol_error', 'wsl_timeout', 'linux_cli_required', 'cli_missing',
    'unsupported_cli', 'chatgpt_login_required', 'cli_probe_timeout', 'cli_launch_failed',
    'ai_disabled_or_cli_missing', 'cancelled', 'context_too_large', 'model_timeout',
    'model_output_too_large', 'usage_limit', 'authentication_failed', 'model_failed',
    'unexpected_tool', 'unexpected_event', 'cli_launch_or_output_failed',
    'connection_temporary', 'server_temporary'))


def safe_error(value):
    return value if isinstance(value, str) and value in ERRORS else 'wsl_runner_failed'


class WslTransport:
    def __init__(self, settings):
        self.settings = settings

    @staticmethod
    def environment():
        # WSLENV/Windows CODEX_HOME/API 키/봇 토큰을 전달하지 않는다.
        return {k: v for k, v in os.environ.items() if k.upper() in (
            'PATH', 'SYSTEMROOT', 'WINDIR', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA',
            'TEMP', 'TMP', 'COMSPEC', 'PATHEXT')}

    def prefix(self):
        return [shutil.which('wsl.exe') or 'wsl.exe', '--distribution',
                self.settings.wsl_distribution, '--user', self.settings.wsl_user, '--exec']

    def run(self, args):
        return subprocess.run(args, capture_output=True, timeout=20, env=self.environment(),
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)

    def preflight(self):
        if not shutil.which('wsl.exe'):
            return 'wsl_missing'
        try:
            result = self.run([shutil.which('wsl.exe'), '--list', '--quiet'])
            if result.returncode:
                return 'wsl_unavailable'
            raw = result.stdout
            names = raw.decode('utf-16' if raw.startswith((b'\xff\xfe', b'\xfe\xff')) else
                               'utf-16-le' if b'\x00' in raw else 'utf-8').splitlines()
            if self.settings.wsl_distribution.casefold() not in {n.strip().casefold() for n in names}:
                return 'wsl_distribution_missing'
            result = self.run(self.prefix() + ['/usr/bin/python3', '-I', '-c', 'print("ready")'])
            if result.returncode or result.stdout.strip() != b'ready':
                return 'wsl_user_or_python_unavailable'
        except (OSError, UnicodeError, subprocess.SubprocessError):
            return 'wsl_unavailable'
        return None

    def diagnose(self):
        error = self.preflight()
        result = {'ready': False, 'reason': error, 'version': None, 'auth': 'unknown',
                  'backend': 'wsl', 'model': self.settings.model,
                  'reasoning_effort': self.settings.model_reasoning_effort}
        if error:
            return result
        response = self.exchange({'action': 'diagnose'}, threading.Event())
        probe = response.get('diagnostic')
        if isinstance(probe, dict):
            ready = probe.get('ready') is True and probe.get('version') == '0.158.0' and probe.get('auth') == 'chatgpt'
            result.update(ready=ready,
                reason=None if ready else safe_error(probe.get('reason')),
                version='0.158.0' if probe.get('version') == '0.158.0' else None,
                auth='chatgpt' if probe.get('auth') == 'chatgpt' else 'unverified')
        else:
            result['reason'] = safe_error(response.get('error'))
        return result

    def invoke(self, schema, instruction, question, context, cancelled):
        return self.exchange({'action': 'invoke', 'schema': schema, 'instruction': instruction,
                              'question': question, 'context': context}, cancelled)

    def exchange(self, request, cancelled):
        if cancelled.is_set():
            return {'error': 'cancelled'}
        request = {**request, 'request_id': uuid.uuid4().hex,
                   'codex_executable': self.settings.wsl_codex_executable}
        encoded = (json.dumps(request, ensure_ascii=False) + '\n').encode('utf-8')
        if len(encoded) > 256_000:
            return {'error': 'context_too_large'}
        process = None
        reader = None
        try:
            source = str(Path(__file__).with_name('wsl_runner.py').resolve())
            mapped = self.run(self.prefix() + ['/usr/bin/wslpath', '-a', '-u', source])
            path = mapped.stdout.decode('utf-8').strip()
            if mapped.returncode or not path.startswith('/') or '\n' in path:
                return {'error': 'wsl_path_unavailable'}
            if cancelled.is_set():
                return {'error': 'cancelled'}
            process = subprocess.Popen(self.prefix() + ['/usr/bin/python3', '-I', '-B', path],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env=self.environment(), creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
            data = []
            finished = threading.Event()

            def receive():
                try:
                    data.append(process.stdout.read(65_537))
                except OSError:
                    pass
                finally:
                    finished.set()

            reader = threading.Thread(target=receive, daemon=True)
            reader.start()
            process.stdin.write(encoded)
            process.stdin.flush()  # 파이프는 종료/취소까지 열어 두어 EOF를 감시한다.
            deadline = time.monotonic() + (65 if request['action'] == 'diagnose' else 150)
            while not finished.wait(.05):
                if cancelled.is_set() or time.monotonic() >= deadline:
                    return {'error': 'cancelled' if cancelled.is_set() else 'wsl_timeout'}
            if cancelled.is_set():
                return {'error': 'cancelled'}
            if not data or len(data[0]) > 65_536:
                return {'error': 'wsl_protocol_error'}
            response = json.loads(data[0].decode('utf-8'))
            if not isinstance(response, dict) or response.get('request_id') != request['request_id']:
                return {'error': 'wsl_protocol_error'}
            if response.get('error'):
                response['error'] = safe_error(response['error'])
            allowed_events = ('command_execution', 'file_change', 'mcp_tool_call', 'web_search',
                              'todo_list', 'error', 'tool_call', 'function_call', 'unknown')
            if response.get('event_type') not in allowed_events:
                response['event_type'] = None
            if response.get('http_status') not in (400, 401, 403, 408, 429, 500, 502, 503, 504):
                response['http_status'] = None
            return response
        except (OSError, ValueError, UnicodeError, subprocess.SubprocessError):
            return {'error': 'wsl_runner_failed'}
        finally:
            if process is not None:
                # EOF로 Linux 실행기에 종료를 요청하고 자식 그룹 정리를 기다린다.
                try:
                    process.stdin.close()
                except OSError:
                    pass
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    from .codex_adapter import stop_tree
                    stop_tree(process)
                if reader:
                    reader.join(timeout=1)
                process.stdout.close()
