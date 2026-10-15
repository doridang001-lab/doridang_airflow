"""실모델 검증의 독립 단계와 제한된 재시도 계약."""
import json
import threading
import pytest

from modules.transform.pipelines.db.samjeon_material_service.__main__ import main
from modules.transform.pipelines.db.samjeon_material_service.codex_adapter import (
    CodexAdapter, event_http_status, http_status, model_error, safe_diagnostic)
from modules.transform.pipelines.db.samjeon_material_service.config import Settings


@pytest.mark.parametrize('intent,start,passed', [
    ('summary', '2026-10-01', True), ('items', '2026-10-01', False),
    ('summary', '2026-09-01', False), ('forecast', '2026-10-01', False)])
def test_probe_uses_explicit_capabilities_and_checks_requested_query(tmp_path, monkeypatch, intent, start, passed):
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **kw: {'ready': True})

    def plan(self, question, context, cancelled):
        assert context['synthetic'] and context['format'] == 'legacy_v1'
        assert context['available_metrics'] == ['sales_won', 'purchase_won']
        assert 'summary' in context['supported_queries']
        assert context['resolved_period'] == ['2026-10-01', '2026-10-11']
        return {'action': 'query', 'queries': [{'intent': intent, 'date_from': start,
                'date_to': '2026-10-11', 'product_name': None}], 'clarification': None}

    monkeypatch.setattr(CodexAdapter, 'plan', plan)
    monkeypatch.setattr(CodexAdapter, 'explain', lambda *a: {
        'fact_ids': ['F1'], 'hypotheses': [], 'questions': []})
    monkeypatch.setattr(CodexAdapter, 'chat', lambda *a: {'kind': 'reply', 'text': '해바라기'})
    config = tmp_path / 'service.json'
    config.write_text('{}', encoding='utf-8')
    result = main(['verify-ai', '--config', str(config), '--require-ready'])
    assert result['checks']['plan']['passed'] is passed
    assert result['ready'] is passed


@pytest.mark.parametrize('format_name', ['legacy_v1', 'v3'])
def test_plan_instructions_are_scoped_to_format(monkeypatch, format_name):
    from modules.transform.pipelines.db.samjeon_material_service.legacy import QUERIES
    adapter = CodexAdapter(Settings())
    def invoke(schema, prompt, question, context, cancelled):
        assert ('compare는 기간 간 구매 관측액 비교' in prompt) == (format_name == 'legacy_v1')
        assert 'sales_won은 매출' in prompt
        assert ('pdf' in schema['properties']['action']['enum']) == (format_name == 'v3')
        return {'action': 'clarify', 'queries': [], 'clarification': '기간을 알려주세요.'}
    monkeypatch.setattr(adapter, '_invoke', invoke)
    assert adapter.plan('조회해줘', {'format': format_name, 'supported_queries': list(QUERIES)},
                        threading.Event())['action'] == 'clarify'


def test_clarify_plan_keeps_independent_explanation_and_chat(tmp_path, monkeypatch):
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **kw: {'ready': True})
    monkeypatch.setattr(CodexAdapter, 'plan', lambda *a: {
        'action': 'clarify', 'queries': [], 'clarification': '기간을 알려주세요.'})
    called = []

    def explain(self, question, context, cancelled):
        called.append(('answer', context['facts'][0]['id']))
        return {'fact_ids': ['F1'], 'hypotheses': [], 'questions': []}

    def chat(self, question, context, cancelled):
        called.append(('chat', len(context['history'])))
        return {'kind': 'reply', 'text': '해바라기'}

    monkeypatch.setattr(CodexAdapter, 'explain', explain)
    monkeypatch.setattr(CodexAdapter, 'chat', chat)
    config = tmp_path / 'service.json'
    config.write_text('{}', encoding='utf-8')
    result = main(['verify-ai', '--config', str(config), '--require-ready'])
    assert not result['ready'] and result['_exit_code'] == 2
    assert result['checks']['plan']['outcome'] == 'clarify'
    assert result['checks']['plan']['error'] is None
    assert result['checks']['answer']['passed'] and result['checks']['answer']['executed']
    assert result['checks']['followup']['passed']
    assert called == [('answer', 'F1'), ('chat', 0), ('chat', 1)]


def test_stage_failure_does_not_hide_later_results(tmp_path, monkeypatch):
    monkeypatch.setattr(CodexAdapter, 'diagnose', lambda *a, **kw: {'ready': True})

    def fail(self, *args):
        self.last_error, self.last_event_type = 'model_failed', 'error'
        return None

    monkeypatch.setattr(CodexAdapter, 'plan', fail)
    monkeypatch.setattr(CodexAdapter, 'explain', lambda *a: {
        'fact_ids': ['F1'], 'hypotheses': [], 'questions': []})
    monkeypatch.setattr(CodexAdapter, 'chat', lambda *a: {
        'kind': 'reply', 'text': '해바라기'})
    config = tmp_path / 'service.json'
    config.write_text('{}', encoding='utf-8')
    result = main(['verify-ai', '--config', str(config), '--require-ready'])
    assert result['error'] == 'model_failed'
    assert result['checks']['plan']['event_type'] == 'error'
    assert result['checks']['answer']['passed']
    assert result['checks']['conversation']['passed']


def test_only_confirmed_temporary_errors_retry(monkeypatch):
    adapter = CodexAdapter(Settings())
    attempts = []

    def invoke(self, *args):
        attempts.append(True)
        self.last_error = 'connection_temporary' if len(attempts) == 1 else None
        return None if len(attempts) == 1 else {'ok': True}

    monkeypatch.setattr(CodexAdapter, '_invoke_once', invoke)
    assert adapter._invoke({}, '', '', {}, threading.Event()) == {'ok': True}
    assert adapter.last_attempts == 2
    attempts.clear()
    assert adapter._invoke({}, '', '', {}, threading.Event(), allow_retry=False) is None
    assert adapter.last_attempts == 1

    def unknown(self, *args):
        attempts.append(True)
        self.last_error = 'model_failed'
        return None

    attempts.clear()
    monkeypatch.setattr(CodexAdapter, '_invoke_once', unknown)
    assert adapter._invoke({}, '', '', {}, threading.Event()) is None
    assert len(attempts) == 1


def test_safe_diagnostic_drops_raw_error_and_path():
    adapter = CodexAdapter(Settings())
    adapter.last_error = 'C:/private/token path'
    adapter.last_event_type = 'C:/private/path'
    adapter.last_http_status = 999
    result = safe_diagnostic(adapter, 'plan')
    assert result['error'] == 'model_failed'
    assert result['event_type'] is None and result['http_status'] is None
    assert model_error('HTTP 503 Service Unavailable') == 'server_temporary'
    assert model_error('connection reset by peer') == 'connection_temporary'
    assert model_error('HTTP 429') == 'usage_limit'
    assert model_error('server failed') == 'model_failed'
    assert http_status('HTTP status 503') == 503
    assert event_http_status(json.dumps({'type': 'error', 'message': 'HTTP status 503'})) == 503
