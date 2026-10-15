"""실제 Windows 런처와 최소 파일 배포·게시본 복구 검증."""
from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter, SUPPORTED_VERSION
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
from modules.transform.pipelines.db.samjeon_material_service.__main__ import doctor

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(os.name != 'nt', reason='Windows 런처 회귀 시험')
@pytest.mark.parametrize('auth,version,reason', [
    ('ChatGPT', SUPPORTED_VERSION, None), ('API key', SUPPORTED_VERSION, 'chatgpt_login_required'),
    ('ChatGPT', '0.0.1', 'unsupported_cli')])
def test_actual_cmd_launcher_with_spaces_and_korean(tmp_path, auth, version, reason):
    folder = tmp_path / '한글 실행 경로'
    folder.mkdir()
    launcher = folder / 'codex.cmd'
    launcher.write_text('@echo off\n'
        f'if "%1"=="--version" (echo codex-cli {version}& exit /b 0)\n'
        'if "%1"=="exec" (echo --ignore-user-config --ignore-rules --strict-config --output-schema --ephemeral& exit /b 0)\n'
        f'if "%1"=="login" (echo Logged in using {auth} 1>&2& exit /b 0)\nexit /b 1\n', encoding='ascii')
    adapter = CodexAdapter(Settings(codex_executable=str(launcher)))
    result = adapter.diagnose(refresh=True)
    assert result['reason'] == reason
    assert result['ready'] is (reason is None)


def test_diagnostics_missing_cli_and_environment_filter(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'never-pass')
    monkeypatch.setenv('SAMJEON_TELEGRAM_BOT_TOKEN', 'never-pass')
    result = CodexAdapter(Settings(codex_executable='missing-samjeon-codex')).diagnose()
    assert result['reason'] == 'cli_missing'
    assert 'OPENAI_API_KEY' not in CodexAdapter.environment()
    assert 'SAMJEON_TELEGRAM_BOT_TOKEN' not in CodexAdapter.environment()


def test_doctor_ready_exit_and_no_config_overwrite(tmp_path):
    config = tmp_path / 'service.json'
    config.write_text(json.dumps({'state_root': str(tmp_path / 'state'), 'codex_executable': 'missing-samjeon-codex'}), encoding='utf-8')
    before = config.read_bytes()
    result = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-m',
        'modules.transform.pipelines.db.samjeon_material_service', 'doctor', '--config', str(config), '--require-ready'],
        cwd=ROOT, capture_output=True, encoding='utf-8')
    assert result.returncode == 2
    assert json.loads(result.stdout)['blocked_reasons']
    assert config.read_bytes() == before
    assert not (tmp_path / 'state').exists()


def test_snapshot_sync_fallback_and_return_to_latest(tmp_path):
    from test_samjeon_material_mart import _fixture
    from test_samjeon_material_mart_v3 import build
    plan, _ = _fixture(tmp_path)
    path = build(plan)
    root = path.parent.parent
    # build는 local/snapshots 판본을 만들며 여기에서는 명시적인 테스트 게시 루트를 구성한다.
    published = tmp_path / 'published'
    target = published / 'snapshots' / path.name
    shutil.copytree(path, target)
    manifest = json.loads((target / 'manifest.json').read_text(encoding='utf-8'))
    pointer = {'schema_version': 3, 'snapshot_id': manifest['snapshot_id'],
               'as_of_date': manifest['as_of_date'], 'manifest': f'snapshots/{path.name}/manifest.json'}
    latest = published / 'latest.json'
    latest.write_text(json.dumps(pointer), encoding='utf-8')
    repo = Repository(Settings(snapshot_root=str(published)))
    assert repo.current()[0] == target
    latest.write_text('{', encoding='utf-8')
    with pytest.raises(ValueError):
        repo.current()
    restarted = Repository(repo.settings)
    restarted.last_good = repo.last_good
    with pytest.raises(ValueError):
        restarted.current()
    latest.write_text(json.dumps(pointer), encoding='utf-8')
    assert repo.current()[0] == target and repo.fallback_reason is None
    (target / 'agent_context.json').write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError):
        repo.current()


def test_runtime_sparse_clone_imports_without_other_repository_files(tmp_path):
    source, clone = tmp_path / 'source', tmp_path / 'clone'
    source.mkdir()
    patterns = (ROOT / 'config/samjeon-runtime.paths').read_text(encoding='utf-8').splitlines()
    for pattern in patterns:
        relative = pattern.lstrip('/')
        origin = ROOT / relative
        dest = source / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        if origin.is_dir():
            shutil.copytree(origin, dest, ignore=shutil.ignore_patterns('__pycache__'))
        else:
            shutil.copy2(origin, dest)
    (source / 'dags').mkdir()
    (source / 'dags/excluded.py').write_text('raise RuntimeError()', encoding='utf-8')
    def git(*args, cwd=source, **kwargs):
        return subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, encoding='utf-8', **kwargs)
    git('init', '-b', 'main')
    git('add', '-f', '.')
    git('-c', 'user.name=Runtime Test', '-c', 'user.email=runtime@example.invalid', 'commit', '-m', 'fixture')
    git('clone', '--no-checkout', '--single-branch', '--branch', 'main', str(source), str(clone))
    git('sparse-checkout', 'set', '--no-cone', '--stdin', cwd=clone, input='\n'.join(patterns) + '\n')
    git('checkout', 'main', cwd=clone)
    assert not (clone / 'dags').exists()
    assert not (clone / 'tests').exists()
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    config = tmp_path / 'minimal.json'
    config.write_text(json.dumps({'state_root': str(tmp_path / 'state'), 'codex_executable': 'missing-samjeon-codex'}), encoding='utf-8')
    result = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-m',
        'modules.transform.pipelines.db.samjeon_material_service', 'doctor', '--config', str(config)],
        cwd=clone, env=env, capture_output=True, encoding='utf-8')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['files_written'] is False
    assert git('status', '--porcelain', cwd=clone).stdout == ''


@pytest.mark.skipif(os.name != 'nt', reason='PowerShell 작업 스케줄러 설정')
def test_powershell_syntax_and_battery_settings():
    command = "$errors = @(); Get-ChildItem scripts/*samjeon*.ps1 | ForEach-Object { $tokens=$null; $e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile($_.FullName,[ref]$tokens,[ref]$e); $errors += $e }; if ($errors.Count) { $errors | Out-String | Write-Output; exit 1 }; $s=New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries; if ($s.DisallowStartIfOnBatteries -or $s.StopIfGoingOnBatteries) { exit 2 }"
    result = subprocess.run(['powershell.exe', '-NoProfile', '-Command', command], cwd=ROOT, capture_output=True, encoding='utf-8', errors='replace')
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(os.name != 'nt', reason='Windows 배포 통합 시험')
@pytest.mark.parametrize('conversation_only', [False, True])
def test_update_and_rollback_keep_state_and_control_task(tmp_path, conversation_only):
    """실제 Git·venv·PowerShell로 갱신/실패/복구. 작업 스케줄러 API만 가짜로 교체한다."""
    source, clone = tmp_path / 'origin', tmp_path / 'laptop'
    source.mkdir()
    scripts = source / 'scripts'
    scripts.mkdir()
    for name in ('update_samjeon_material_agent.ps1', 'set_samjeon_sparse.ps1'):
        shutil.copy2(ROOT / 'scripts' / name, scripts / name)
    (source / 'config').mkdir()
    (source / 'config/samjeon-runtime.paths').write_text('/scripts/\n/config/\n/modules/\n/requirements-samjeon.txt\n/version.txt\n/.gitignore\n', encoding='ascii')
    (source / '.gitignore').write_text('.venv/\n__pycache__/\n', encoding='ascii')
    (source / 'requirements-samjeon.txt').write_text('', encoding='ascii')
    package = source / 'modules/transform/pipelines/db/samjeon_material_service'
    package.mkdir(parents=True)
    (package / '__init__.py').write_text('', encoding='ascii')
    (package / 'config.py').write_text('import json\nfrom types import SimpleNamespace\nclass Settings:\n @staticmethod\n def load(p):\n  return SimpleNamespace(**json.load(open(p)))\n', encoding='ascii')
    (package / '__main__.py').write_text('import sys\nfrom contextlib import nullcontext\nfrom pathlib import Path\ndef instance_lock(root): return nullcontext()\nif __name__ == "__main__":\n print("diagnostic",sys.argv)\n raise SystemExit(2 if Path("version.txt").read_text() == "bad" else 0)\n', encoding='ascii')
    (source / 'version.txt').write_text('old', encoding='ascii')
    def git(*args, cwd=source):
        return subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, encoding='utf-8').stdout.strip()
    git('init', '-b', 'main')
    def commit(message):
        git('add', '.')
        git('-c', 'user.name=Runtime Test', '-c', 'user.email=runtime@example.invalid', 'commit', '-m', message)
    commit('old')
    git('clone', str(source), str(clone))
    subprocess.run([sys.executable, '-m', 'venv', str(clone / '.venv')], check=True, capture_output=True)
    state = tmp_path / 'state'
    state.mkdir()
    (state / 'state.sqlite3').write_bytes(b'preserve-outbox-and-conversation')
    config = tmp_path / 'service.json'
    config.write_text(json.dumps({'state_root': str(state)}), encoding='ascii')
    log = tmp_path / 'scheduler.txt'
    wrapper = tmp_path / 'run.ps1'
    wrapper.write_text('param([string]$Script,[string]$Config,[switch]$Rollback,[switch]$ConversationOnly,[string]$BootstrapPreviousCommit="")\n'
        'function Get-ScheduledTask { [pscustomobject]@{State="Ready"} }\n'
        'function Disable-ScheduledTask { param($TaskName); Add-Content $env:SAMJEON_TEST_LOG disable }\n'
        'function Stop-ScheduledTask { param($TaskName); Add-Content $env:SAMJEON_TEST_LOG stop }\n'
        'function Enable-ScheduledTask { param($TaskName); Add-Content $env:SAMJEON_TEST_LOG enable }\n'
        'function Start-ScheduledTask { param($TaskName); Add-Content $env:SAMJEON_TEST_LOG start }\n'
        '& $Script -Config $Config -Rollback:$Rollback -ConversationOnly:$ConversationOnly -BootstrapPreviousCommit $BootstrapPreviousCommit\n', encoding='ascii')
    env = dict(os.environ, SAMJEON_TEST_LOG=str(log))
    def deploy(rollback=False, bootstrap=None):
        return subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(wrapper),
            '-Script', str(clone / 'scripts/update_samjeon_material_agent.ps1'), '-Config', str(config),
            *(['-Rollback'] if rollback else []), *(['-ConversationOnly'] if conversation_only else []),
            *(['-BootstrapPreviousCommit', bootstrap] if bootstrap else [])], cwd=clone, env=env, capture_output=True, encoding='utf-8', errors='replace')
    (source / 'version.txt').write_text('new', encoding='ascii')
    commit('new')
    previous = git('rev-parse', 'HEAD', cwd=clone)
    if conversation_only:
        git('pull', '--ff-only', 'origin', 'main', cwd=clone)
    result = deploy(bootstrap=previous if conversation_only else None)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (clone / 'version.txt').read_text() == 'new'
    assert json.loads((state / 'deployment.json').read_text(encoding='utf-8-sig'))['status'] == 'updated'
    assert json.loads((state / 'deployment.json').read_text(encoding='utf-8-sig'))['previous_commit'] == previous
    assert log.read_text().splitlines() == (['disable', 'stop'] if conversation_only else ['disable', 'stop', 'enable', 'start'])
    assert ('--require-conversation-deployable' if conversation_only else '--require-ready') in result.stdout
    (source / 'version.txt').write_text('bad', encoding='ascii')
    commit('bad')
    result = deploy()
    assert result.returncode != 0
    assert log.read_text().splitlines()[-2:] == ['disable', 'stop']
    result = deploy(True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (clone / 'version.txt').read_text() == 'new'
    assert (state / 'state.sqlite3').read_bytes() == b'preserve-outbox-and-conversation'
    assert git('status', '--porcelain', cwd=clone) == ''


@pytest.mark.skipif(os.name != 'nt', reason='Windows 시작 스크립트 회귀 시험')
def test_start_default_python_reaches_disabled_guard_without_writing(tmp_path):
    config = tmp_path / 'service.json'
    config.write_text(json.dumps({'state_root': str(tmp_path / 'state')}), encoding='utf-8')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File',
        str(ROOT / 'scripts/start_samjeon_material_agent.ps1'), '-Config', str(config)],
        cwd=tmp_path, capture_output=True, encoding='utf-8', errors='replace', timeout=30)
    assert result.returncode != 0
    assert 'settings.ready()' in result.stderr
    assert 'Join-Path' not in result.stderr
    assert not (tmp_path / 'state').exists()


@pytest.mark.skipif(os.name != 'nt', reason='Windows 작업 스케줄러 실행 계약')
def test_scheduler_action_starts_default_python_in_isolated_runtime(tmp_path):
    import venv
    root = tmp_path / 'runtime with spaces'
    scripts = root / 'scripts'
    scripts.mkdir(parents=True)
    for name in ('start_samjeon_material_agent.ps1', 'register_samjeon_material_agent_task.ps1',
                 'watch_samjeon_material_agent.py'):
        shutil.copy2(ROOT / 'scripts' / name, scripts / name)
    venv.EnvBuilder(with_pip=False).create(root / '.venv')
    package = root / 'modules/transform/pipelines/db/samjeon_material_service'
    package.mkdir(parents=True)
    (package / '__main__.py').write_text('import sys,json\nprint(json.dumps(sys.argv[1:]))\n', encoding='ascii')
    config = tmp_path / 'service.json'
    config.write_text('{}', encoding='ascii')
    action = tmp_path / 'action.json'
    wrapper = tmp_path / 'scheduler.ps1'
    wrapper.write_text('param($Script,$Config,$Output)\n'
        'function New-ScheduledTaskAction { param($Execute,$Argument,$WorkingDirectory); [pscustomobject]@{Execute=$Execute;Argument=$Argument;WorkingDirectory=$WorkingDirectory} }\n'
        'function New-ScheduledTaskTrigger { param([switch]$AtLogOn,[switch]$Once,$At,$User,$RepetitionInterval,$RepetitionDuration); @{} }\n'
        'function New-ScheduledTaskPrincipal { param($UserId,$LogonType,$RunLevel); @{} }\n'
        'function New-ScheduledTaskSettingsSet { param($MultipleInstances,$RestartCount,$RestartInterval,$ExecutionTimeLimit,[switch]$StartWhenAvailable,[switch]$AllowStartIfOnBatteries,[switch]$DontStopIfGoingOnBatteries); [pscustomobject]@{Enabled=$false} }\n'
        'function New-ScheduledTask { param($Action,$Trigger,$Principal,$Settings); [pscustomobject]@{Action=$Action;Settings=$Settings} }\n'
        'function Register-ScheduledTask { param($TaskName,$InputObject,[switch]$Force); if ($TaskName -eq "DoridangSamjeonMaterialAgent") { $InputObject.Action | ConvertTo-Json | Set-Content -LiteralPath $Output -Encoding utf8 } }\n'
        '& $Script -Config $Config\n', encoding='ascii')
    result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', str(wrapper),
        '-Script', str(scripts / 'register_samjeon_material_agent_task.ps1'), '-Config', str(config), '-Output', str(action)],
        capture_output=True, encoding='utf-8', errors='replace', timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    command = json.loads(action.read_text(encoding='utf-8-sig'))
    assert command['Execute'].lower().endswith('pythonw.exe')
    assert 'start_samjeon_material_agent.ps1' not in command['Argument']
    started = subprocess.run('"' + command['Execute'][:-11] + 'python.exe" ' + command['Argument'],
        cwd=command['WorkingDirectory'],
        capture_output=True, encoding='utf-8', errors='replace', timeout=30, creationflags=subprocess.CREATE_NO_WINDOW)
    assert started.returncode == 0, started.stdout + started.stderr
    assert json.loads(started.stdout) == ['service', '--config', str(config)]
