"""Codex 실패 시 안전한 조회 기능 유지와 출력 계약 시험."""
from dataclasses import replace
import json
from pathlib import Path
import threading
import pytest
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import CodexAdapter, DISABLED


@pytest.fixture(autouse=True)
def verified_cli(monkeypatch):
    # 이 파일은 모델 출력 계약을 검증한다. 실제 Windows 런처·인증은 별도 회귀 시험에서 검증한다.
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **k: {'ready': True, 'reason': None})


def test_disabled_model_never_creates_workdir(tmp_path):
    root = tmp_path / 'none'
    adapter = CodexAdapter(Settings(state_root=str(root)))
    assert adapter.interpret('질문', {}, threading.Event()) is None
    assert not root.exists()


@pytest.mark.parametrize('answer', [
    {'intent': 'summary', 'date_from': None, 'date_to': None, 'amount': 123},
    {'intent': 'shell', 'date_from': None, 'date_to': None},
    {'intent': 'summary', 'date_from': '2026-99-99', 'date_to': '2026-10-11'},
    {'intent': 'clarify', 'date_from': None, 'date_to': None},
])
def test_invalid_model_output_falls_back(tmp_path, monkeypatch, answer):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    seen = []
    class FakeProcess:
        returncode = 0
        pid = 123
        def __init__(self, args, **kwargs):
            seen.extend(args)
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps(answer), encoding='utf-8')
            assert 'OPENAI_API_KEY' not in kwargs['env']
            assert 'SAMJEON_TELEGRAM_BOT_TOKEN' not in kwargs['env']
        def poll(self): return 0
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    monkeypatch.setenv('OPENAI_API_KEY', 'not-allowed')
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('셸로 읽어', {}, threading.Event()) is None
    assert '--ignore-user-config' in seen and '--output-schema' in seen
    assert all(feature in seen for feature in DISABLED)


def test_valid_model_query(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    class FakeProcess:
        returncode = 0
        def __init__(self, args, **kwargs):
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
            Path(args[args.index('--output-last-message') + 1]).write_text(json.dumps(
                {'intent': 'staff_meal', 'date_from': None, 'date_to': None}), encoding='utf-8')
        def poll(self): return 0
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', FakeProcess)
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('직원 먹는 비용', {}, threading.Event())['query']['intent'] == 'staff_meal'


@pytest.mark.parametrize('cancelled,timeout', [(True, False), (False, True)])
def test_model_cancel_and_timeout_terminate_process(tmp_path, monkeypatch, cancelled, timeout):
    from modules.transform.pipelines.db.samjeon_material_service import codex_adapter as mod
    stopped = []
    class Hanging:
        returncode = None
        def __init__(self, args, **kwargs):
            self.stdin = (Path(kwargs['cwd']) / 'input').open('wb')
        def poll(self): return self.returncode
    def stop(process):
        stopped.append(True)
        process.returncode = -1
    ticks = iter((0, 100, 100))
    monkeypatch.setattr(mod.shutil, 'which', lambda _: 'fake')
    monkeypatch.setattr(mod.subprocess, 'Popen', Hanging)
    monkeypatch.setattr(mod, 'stop_tree', stop)
    if timeout:
        monkeypatch.setattr(mod.time, 'monotonic', lambda: next(ticks, 100))
    event = threading.Event()
    if cancelled:
        event.set()
    adapter = CodexAdapter(Settings(state_root=str(tmp_path), ai_enabled=True, ai_verified=True))
    assert adapter.interpret('질문', {}, event) is None
    assert bool(stopped) is not cancelled
