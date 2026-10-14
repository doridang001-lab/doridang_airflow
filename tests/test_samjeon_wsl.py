"""실모델 없이 WSL 선택·진단·환경 격리 계약을 확인한다."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter
from modules.transform.pipelines.db.samjeon_material_service.wsl_transport import WslTransport


def settings(**kwargs):
    return replace(Settings(codex_backend='wsl', wsl_distribution='Ubuntu', wsl_user='samjeon',
                            wsl_codex_executable='/usr/local/bin/codex'), **kwargs)


@pytest.mark.parametrize('changes', [dict(codex_backend='ssh'), dict(model_reasoning_effort='middle'),
    dict(wsl_distribution=''), dict(wsl_user='root;echo'), dict(wsl_codex_executable='codex'),
    dict(wsl_codex_executable='/mnt/c/codex'), dict(wsl_codex_executable='/opt/../mnt/c/codex')])
def test_invalid_backend_settings(changes):
    with pytest.raises(ValueError):
        settings(**changes).validate()


def test_legacy_settings_stay_windows(tmp_path):
    p = tmp_path / 'config.json'
    p.write_text('{}', encoding='utf-8')
    assert Settings.load(p).codex_backend == 'windows'
    settings().validate()


def test_wsl_environment_drops_credentials(monkeypatch):
    for name in ('SAMJEON_TELEGRAM_BOT_TOKEN', 'OPENAI_API_KEY', 'CODEX_API_KEY', 'CODEX_HOME', 'WSLENV'):
        monkeypatch.setenv(name, 'secret')
        assert name not in WslTransport.environment()


@pytest.mark.parametrize('available,listing,exit_code,error', [
    (False, '', 0, 'wsl_missing'), (True, 'Ubuntu', 1, 'wsl_unavailable'),
    (True, 'Other', 0, 'wsl_distribution_missing'), (True, 'Ubuntu', 0, None)])
def test_preflight(monkeypatch, available, listing, exit_code, error):
    from modules.transform.pipelines.db.samjeon_material_service import wsl_transport as mod
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'wsl.exe' if available else None)
    transport = WslTransport(settings())
    def run(args):
        if '--list' in args:
            return subprocess.CompletedProcess(args, exit_code, listing.encode('utf-16-le'), b'')
        assert args[-3:] == ['-I', '-c', 'print("ready")']
        return subprocess.CompletedProcess(args, 0, b'ready\n', b'')
    monkeypatch.setattr(transport, 'run', run)
    assert transport.preflight() == error


def test_invalid_wsl_user_or_python(monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service import wsl_transport as mod
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'wsl.exe')
    monkeypatch.setattr(WslTransport, 'run', lambda self, args: subprocess.CompletedProcess(
        args, 0 if '--list' in args else 1, b'Ubuntu\n' if '--list' in args else b'', b'private error'))
    assert WslTransport(settings()).diagnose()['reason'] == 'wsl_user_or_python_unavailable'


def test_selected_backend_never_uses_windows_cli(monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    calls = []
    monkeypatch.setattr(mod.shutil, 'which', lambda x: calls.append(x) or 'wsl.exe')
    monkeypatch.setattr(WslTransport, 'diagnose', lambda self: {'ready': True, 'reason': None})
    def invoke(self, schema, instruction, question, context, cancelled):
        assert question == '[비밀값 삭제]' and context['history'] == []
        return {'answer': {'kind': 'reply', 'text': '안녕하세요.'}}
    monkeypatch.setenv('SAMJEON_TELEGRAM_BOT_TOKEN', 'private-test-token')
    monkeypatch.setattr(WslTransport, 'invoke', invoke)
    adapter = CodexAdapter(settings(ai_enabled=True, ai_verified=True))
    assert adapter.chat('private-test-token', {}, threading.Event())['text'] == '안녕하세요.'
    assert calls == ['wsl.exe']


def test_wsl_failure_is_not_retried_on_windows(monkeypatch):
    monkeypatch.setattr(WslTransport, 'diagnose', lambda self: {'ready': False, 'reason': 'cli_missing'})
    monkeypatch.setattr('shutil.which', lambda _: 'wsl.exe')
    adapter = CodexAdapter(settings(ai_enabled=True, ai_verified=True))
    assert adapter.chat('안녕', {}, threading.Event()) is None
    assert adapter.last_error == 'cli_missing'


def test_diagnose_only_exposes_safe_fields(monkeypatch):
    monkeypatch.setattr(WslTransport, 'preflight', lambda self: None)
    monkeypatch.setattr(WslTransport, 'exchange', lambda *a: {'diagnostic': {
        'ready': False, 'reason': 'private raw error', 'version': 'private', 'auth': 'private'}})
    result = WslTransport(settings()).diagnose()
    assert 'private' not in json.dumps(result)
    assert result['reason'] == 'wsl_runner_failed' and result['backend'] == 'wsl'


def test_precancel_does_not_launch(monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('취소된 요청이 실행됐습니다')
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    cancelled = threading.Event()
    cancelled.set()
    assert WslTransport(settings()).invoke({}, '', '', {}, cancelled) == {'error': 'cancelled'}


@pytest.mark.skipif(not os.environ.get('SAMJEON_TEST_WSL_DISTRIBUTION'), reason='명시한 WSL에서만 가짜 CLI 통합 시험')
def test_real_wsl_with_fake_cli():
    distribution = os.environ['SAMJEON_TEST_WSL_DISTRIBUTION']
    source = Path(__file__).with_name('samjeon_wsl_probe.py').resolve()
    prefix = ['wsl.exe', '-d', distribution, '--exec']
    mapped = subprocess.run(prefix + ['wslpath', '-a', '-u', str(source)], capture_output=True, check=True)
    result = subprocess.run(prefix + ['/usr/bin/python3', '-I', '-B', mapped.stdout.decode().strip()],
                            capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr.decode('utf-8', errors='replace')


@pytest.mark.skipif(not os.environ.get('SAMJEON_TEST_WSL_DISTRIBUTION'), reason='명시한 WSL에서만 가짜 CLI 통합 시험')
@pytest.mark.parametrize('mode', ['ok', 'hang', 'error', 'failed', 'unknown', 'tool', 'late_notice', 'incomplete'])
def test_windows_to_wsl_roundtrip(tmp_path, mode):
    distribution = os.environ['SAMJEON_TEST_WSL_DISTRIBUTION']
    source = Path(__file__).with_name('samjeon_wsl_probe.py').resolve()
    prefix = ['wsl.exe', '-d', distribution, '--exec']
    mapped = subprocess.run(prefix + ['wslpath', '-a', '-u', str(source)], capture_output=True, check=True)
    fixture = subprocess.Popen(prefix + ['/usr/bin/python3', '-I', '-B', mapped.stdout.decode().strip(), '--fixture', mode],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    timer = None
    try:
        native = json.loads(fixture.stdout.readline())
        adapter = CodexAdapter(settings(wsl_distribution=distribution, wsl_user=native['user'],
            wsl_codex_executable=native['executable'], ai_enabled=True, ai_verified=True,
            state_root=str(tmp_path / 'never-written')))
        assert adapter.diagnose()['ready']
        cancelled = threading.Event()
        if mode == 'hang':
            timer = threading.Timer(2, cancelled.set)
            timer.start()
        answer = adapter.chat('한글 질문', {}, cancelled)
        if mode == 'ok':
            assert answer['text'] == '한글 답변입니다.'
        else:
            expected = {'hang': 'cancelled', 'error': 'model_failed', 'failed': 'usage_limit',
                        'unknown': 'unexpected_event', 'tool': 'unexpected_tool',
                        'late_notice': 'model_failed', 'incomplete': 'model_failed'}
            assert answer is None and adapter.last_error == expected[mode]
        assert not (tmp_path / 'never-written').exists()
    finally:
        if timer:
            timer.cancel()
        fixture.stdin.close()
        fixture.wait(timeout=10)


def test_configure_wsl_preserves_existing_data_and_disables(tmp_path):
    from modules.transform.pipelines.db.samjeon_material_service.wsl_config import configure
    root = tmp_path / 'state'
    root.mkdir()
    db = root / 'state.sqlite3'
    db.write_bytes(b'preserve-database')
    receipt = root / 'deployment.json'
    receipt.write_text('{}')
    raw = {'state_root': str(root), 'allowed_users': [11], 'allowed_chats': [22],
           'recipient': 22, 'enabled': True, 'ai_enabled': True, 'ai_verified': True,
           'auto_send': True, 'snapshot_root': 'relative-data'}
    config = tmp_path / 'service.json'
    config.write_text(json.dumps(raw), encoding='utf-8')
    before = config.read_bytes()
    assert not configure(config, 'Ubuntu', 'samjeon', '/usr/local/bin/codex', True)['settings_changed']
    assert config.read_bytes() == before
    configure(config, 'Ubuntu', 'samjeon', '/usr/local/bin/codex')
    loaded = json.loads(config.read_text(encoding='utf-8'))
    for key in ('state_root', 'allowed_users', 'allowed_chats', 'recipient', 'snapshot_root'):
        assert loaded[key] == raw[key]
    assert not any(loaded[k] for k in ('enabled', 'ai_enabled', 'ai_verified', 'auto_send'))
    assert db.read_bytes() == b'preserve-database' and receipt.read_text() == '{}'
    assert not list(tmp_path.glob('.samjeon-settings-*'))


def test_configure_requires_stopped_service(tmp_path):
    from modules.transform.pipelines.db.samjeon_material_service.wsl_config import configure
    from modules.transform.pipelines.db.samjeon_material_service.__main__ import instance_lock
    config = tmp_path / 'service.json'
    config.write_text(json.dumps({'state_root': str(tmp_path / 'state')}), encoding='utf-8')
    before = config.read_bytes()
    with instance_lock(tmp_path / 'state'):
        with pytest.raises(RuntimeError):
            configure(config, 'Ubuntu', 'samjeon', '/usr/local/bin/codex')
    assert config.read_bytes() == before


def test_verify_keeps_first_stage_error(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service.__main__ import main
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **k: {'ready': True})
    def failed(self, *a):
        self.last_error, self.last_event_type = 'unexpected_tool', 'unknown'
        return None
    def good(self, *a):
        self.last_error, self.last_event_type = None, None
        return {'kind': 'reply', 'text': '해바라기'}
    monkeypatch.setattr(CodexAdapter, 'plan', failed)
    monkeypatch.setattr(CodexAdapter, 'chat', good)
    config = tmp_path / 'service.json'
    config.write_text('{}')
    result = main(['verify-ai', '--config', str(config), '--require-ready'])
    assert result['error'] == 'unexpected_tool'
    assert result['checks']['plan']['event_type'] == 'unknown'
    assert result['checks']['conversation']['error'] is None
    assert not result['ready']


@pytest.mark.skipif(os.name != 'nt', reason='Windows 숨김 시작 옵션 검증')
def test_background_start_is_hidden_without_launching_service(tmp_path):
    config = tmp_path / '한글 설정.json'
    config.write_text('{}', encoding='utf-8')
    result_file = tmp_path / 'launch.json'
    start = Path(__file__).resolve().parents[1] / 'scripts/start_samjeon_material_agent.ps1'
    def quote(path):
        return "'" + str(path).replace("'", "''") + "'"
    script = tmp_path / 'probe.ps1'
    script.write_text('function Start-Process {\n'
        'param($FilePath, $WindowStyle, $WorkingDirectory, $ArgumentList)\n'
        '@{file=$FilePath; window=$WindowStyle; args=$ArgumentList} | ConvertTo-Json | '
        'Set-Content -Encoding utf8 -LiteralPath ' + quote(result_file) + '\n}\n'
        '& ' + quote(start) + ' -Config ' + quote(config) + ' -Python ' + quote(sys.executable) + ' -Background\n',
        encoding='utf-8-sig')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(script)],
                            capture_output=True, timeout=15)
    assert result.returncode == 0
    launch = json.loads(result_file.read_text(encoding='utf-8-sig'))
    assert launch['window'] == 'Hidden' and launch['file'] == 'powershell.exe'
    assert '-Background' not in launch['args'] and str(config) in launch['args']


@pytest.mark.skipif(os.name != 'nt', reason='Windows 실제 자식 프로세스 이벤트 계약')
@pytest.mark.parametrize('mode,expected', [('ok', None), ('error', 'model_failed'),
    ('failed', 'usage_limit'), ('unknown', 'unexpected_event'), ('tool', 'unexpected_tool'),
    ('late_notice', 'model_failed'), ('incomplete', 'model_failed')])
def test_windows_native_startup_events(tmp_path, mode, expected):
    from samjeon_wsl_probe import FAKE
    fake = tmp_path / 'fake.py'
    fake.write_text(FAKE, encoding='utf-8')
    (tmp_path / 'mode').write_text(mode, encoding='ascii')
    launcher = tmp_path / 'codex.cmd'
    launcher.write_text('@echo off\n"' + sys.executable + '" -B -X utf8 "' + str(fake) + '" %*\n', encoding='utf-8')
    adapter = CodexAdapter(Settings(codex_executable=str(launcher), state_root=str(tmp_path / 'state'),
                                     ai_enabled=True, ai_verified=True))
    assert adapter.diagnose()['ready']
    answer = adapter.chat('한글 질문', {}, threading.Event())
    assert adapter.last_error == expected
    assert (answer is not None) is (expected is None)
