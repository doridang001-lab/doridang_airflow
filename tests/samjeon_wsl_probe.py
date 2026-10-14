"""표준 라이브러리만 사용하는 Linux 실행기 통합 시험. 실 Codex와 네트워크는 호출하지 않는다."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

RUNNER = Path(__file__).resolve().parents[1] / 'modules/transform/pipelines/db/samjeon_material_service/wsl_runner.py'
FAKE = '''#!/usr/bin/python3
import json, os, pathlib, subprocess, sys, time
root = pathlib.Path(__file__).parent
mode = (root / 'mode').read_text()
args = sys.argv[1:]
if args == ['--version']:
    print('codex-cli 0.158.0'); sys.exit(0)
if args == ['exec', '--help']:
    print('--ignore-user-config --ignore-rules --strict-config --output-schema --ephemeral'); sys.exit(0)
if args == ['login', 'status']:
    print('Logged in using ' + ('API key' if mode == 'api' else 'ChatGPT')); sys.exit(0)
assert not any(k in os.environ for k in ('OPENAI_API_KEY', 'SAMJEON_TELEGRAM_BOT_TOKEN', 'WSLENV'))
assert 'model_reasoning_effort="medium"' in args
assert args[args.index('--sandbox') + 1] == 'read-only'
if mode == 'hang':
    child = subprocess.Popen(['/usr/bin/python3', '-c', 'import time; time.sleep(300)'])
    (root / 'pids').write_text(json.dumps([os.getpid(), child.pid]))
    time.sleep(300)
prompt = sys.stdin.read()
assert '한글' in prompt
pathlib.Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps({'kind':'reply','text':'한글 답변입니다.'}))
print(json.dumps({'item': {'type': 'command_execution' if mode == 'tool' else 'agent_message'}}))
'''


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='samjeon-test-')
        self.root = Path(self.temp.name)
        self.cli = self.root / '한글 실행 파일'
        self.cli.write_text(FAKE, encoding='utf-8')
        self.cli.chmod(0o700)
        (self.root / 'mode').write_text('ok')
        self.process = None

    def tearDown(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait()
        for path in (self.root / 'pids',):
            if path.exists():
                for pid in json.loads(path.read_text()):
                    try:
                        os.kill(pid, 9)
                    except ProcessLookupError:
                        pass
        self.temp.cleanup()

    def start(self, action='invoke'):
        request = {'request_id': 'a' * 32, 'action': action, 'codex_executable': str(self.cli),
                   'schema': {}, 'instruction': '한글로 답하세요.', 'question': '한글' * 1500, 'context': {}}
        self.process = subprocess.Popen(['/usr/bin/python3', '-I', '-B', str(RUNNER)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={**os.environ, 'OPENAI_API_KEY': 'blocked', 'SAMJEON_TELEGRAM_BOT_TOKEN': 'blocked',
                 'WSLENV': 'OPENAI_API_KEY'})
        self.process.stdin.write((json.dumps(request, ensure_ascii=False) + '\n').encode())
        self.process.stdin.flush()

    def result(self):
        self.process.wait(timeout=15)
        stderr = self.process.stderr.read().decode()
        self.assertEqual(self.process.returncode, 0, stderr)
        response = json.loads(self.process.stdout.read())
        self.process.stdin.close()
        return response

    def test_chat_and_no_secrets(self):
        self.start()
        result = self.result()
        self.assertIsNone(result['error'])
        self.assertEqual(result['answer']['text'], '한글 답변입니다.')

    def test_api_login_rejected(self):
        (self.root / 'mode').write_text('api')
        self.start('diagnose')
        self.assertEqual(self.result()['diagnostic']['reason'], 'chatgpt_login_required')
        self.assertFalse((self.root / 'pids').exists())

    def test_real_tool_event_rejected(self):
        (self.root / 'mode').write_text('tool')
        self.start()
        result = self.result()
        self.assertEqual(result['error'], 'unexpected_tool')
        self.assertEqual(result['event_type'], 'command_execution')

    def test_parent_eof_kills_descendants_and_unblocks_stdin(self):
        (self.root / 'mode').write_text('hang')
        self.start()
        deadline = time.monotonic() + 10
        while not (self.root / 'pids').exists() and time.monotonic() < deadline:
            time.sleep(.05)
        self.assertTrue((self.root / 'pids').exists())
        pids = json.loads((self.root / 'pids').read_text())
        self.process.stdin.close()
        self.assertEqual(self.result()['error'], 'cancelled')
        for pid in pids:
            status = Path(f'/proc/{pid}/status')
            self.assertTrue(not status.exists() or '\nState:\tZ' in status.read_text())


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--fixture':
        import pwd
        with tempfile.TemporaryDirectory(prefix='samjeon-test-') as folder:
            root = Path(folder)
            cli = root / '한글 실행 파일'
            cli.write_text(FAKE, encoding='utf-8')
            cli.chmod(0o700)
            (root / 'mode').write_text(sys.argv[2])
            print(json.dumps({'executable': str(cli), 'user': pwd.getpwuid(os.getuid()).pw_name}), flush=True)
            sys.stdin.readline()
    else:
        unittest.main()
