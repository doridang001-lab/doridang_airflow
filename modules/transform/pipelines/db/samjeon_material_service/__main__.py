"""python -B -m modules.transform.pipelines.db.samjeon_material_service <명령>"""
import argparse
import asyncio
from contextlib import contextmanager
import json
import logging
from pathlib import Path
import os

from .config import Settings, local_path
from .repository import Repository
from .presentation import briefing
from .data_contract import readiness, SnapshotUnavailable


@contextmanager
def instance_lock(root):
    root = local_path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'service.lock').open('a+b') as handle:
        handle.seek(0)
        # Windows는 다른 프로세스가 잠근 바이트의 읽기도 거부한다.
        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b'1')
            handle.flush()
        handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('이미 실행 중인 삼전점 서비스가 있습니다') from None
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def doctor(settings):
    from .codex_adapter import CodexAdapter
    from importlib.util import find_spec
    adapter = CodexAdapter(settings)
    cli = adapter.diagnose()
    result = {'enabled': settings.enabled, 'auto_send': settings.auto_send,
              'token_present': bool(settings.token), 'users_registered': bool(settings.allowed_users),
              'chat_registered': bool(settings.allowed_chats), 'ai_configured': adapter.available(),
              'codex': cli,
              'ai_live_verified': settings.ai_verified, 'pdf_available': find_spec('reportlab') is not None,
              'font_available': Path(settings.font_path).is_file(), 'files_written': False,
              'state_db_exists': (local_path(settings.state_root) / 'state.sqlite3').is_file()}
    result.update(readiness())
    health = local_path(settings.state_root) / 'health.json'
    if health.is_file():
        try:
            result['service_health'] = json.loads(health.read_text(encoding='utf-8'))
        except (ValueError, OSError):
            result['service_health'] = {'status': '진단 파일 확인 필요'}
    try:
        _, context = Repository(settings).current()
        result.update(readiness(context))
        result.update(snapshot_id=context['snapshot_id'], as_of_date=context['as_of_date'],
                      data_status=context['status'], data_complete=context.get('month_to_date', {}).get('complete', False))
    except (OSError, ValueError, KeyError) as exc:
        result.update(data_complete=False, data_status='게시 자료 확인 필요',
                      data_error=exc.code if isinstance(exc, SnapshotUnavailable) else 'snapshot_invalid')
    blocked = []
    for key, label in (('enabled', '서비스 비활성'), ('token_present', '봇 토큰 없음'),
                       ('users_registered', '사용자 미등록'), ('chat_registered', '대화 미등록'),
                       ('pdf_available', 'PDF 의존성 없음'), ('font_available', '한글 글꼴 없음')):
        if not result[key]:
            blocked.append(label)
    if not result['data_ready']:
        blocked.append('유효한 v3 게시본 없음')
    if settings.ai_enabled and (not settings.ai_verified or not cli['ready']):
        blocked.append('Codex 실연결 검증 필요' if not settings.ai_verified else 'Codex: ' + str(cli['reason']))
    if settings.auto_send:
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        if not result.get('data_complete'):
            blocked.append('자동 알림용 자료 검수 미완료')
        if result.get('as_of_date') != str(datetime.now(ZoneInfo('Asia/Seoul')).date() - timedelta(days=1)):
            blocked.append('전일 자료가 최신 게시본이어야 합니다')
    result.update(ready=not blocked, blocked_reasons=blocked)
    conversation_blocked = [label for key, label in (
        ('token_present', '봇 토큰 없음'), ('users_registered', '사용자 미등록'),
        ('chat_registered', '대화 미등록'), ('pdf_available', '런타임 의존성 없음')) if not result[key]]
    if not cli['ready']:
        conversation_blocked.append('Codex 연결 준비 필요')
    if settings.auto_send:
        conversation_blocked.append('대화 전용 배포는 auto_send=false가 필요합니다')
    result.update(conversation_deployable=not conversation_blocked,
                  conversation_blocked_reasons=conversation_blocked,
                  conversation_ready=not conversation_blocked and settings.enabled and settings.ai_enabled and settings.ai_verified)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    wsl = subs.add_parser('configure-wsl')
    wsl.add_argument('--config', type=Path, required=True)
    wsl.add_argument('--distribution', required=True)
    wsl.add_argument('--user', required=True)
    wsl.add_argument('--codex-executable', required=True)
    wsl.add_argument('--dry-run', action='store_true')
    for name in ('doctor', 'service', 'report', 'verify-ai'):
        cmd = subs.add_parser(name)
        cmd.add_argument('--config', type=Path)
        cmd.add_argument('--dry-run', action='store_true')
        cmd.add_argument('--require-ready', action='store_true')
        if name == 'doctor':
            cmd.add_argument('--require-conversation-deployable', action='store_true')
            cmd.add_argument('--require-data-query-ready', action='store_true')
        if name == 'report':
            cmd.add_argument('--snapshot', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == 'configure-wsl':
        from .wsl_config import configure
        return configure(args.config, args.distribution, args.user, args.codex_executable, args.dry_run)
    settings = Settings.load(args.config) if args.config else Settings()
    settings.validate()
    if args.command == 'verify-ai':
        from dataclasses import replace
        from .codex_adapter import CodexAdapter, safe_diagnostic
        import threading
        if args.dry_run:
            return {'ready': False, 'model_called': False, 'files_written': False}
        adapter = CodexAdapter(replace(settings, ai_enabled=True, ai_verified=True))
        probe = adapter.diagnose(refresh=True)
        checks = {}

        def check(name, run, valid, wrong):
            if not run:
                checks[name] = {'executed': False, 'passed': False, 'outcome': 'not_run',
                                'error': 'not_run', 'event_type': None, 'http_status': None,
                                'attempts': 0}
                return None
            adapter.last_error = adapter.last_event_type = adapter.last_http_status = None
            adapter.last_attempts = 0
            try:
                result = run()
            except (ValueError, TypeError, KeyError):
                result = None
                adapter.last_error = wrong
            except Exception:
                result = None
                adapter.last_error = 'model_failed'
            try:
                passed = bool(valid(result))
            except (ValueError, TypeError, KeyError):
                passed = False
            clarified = False
            if name == 'plan' and isinstance(result, dict) and result.get('action') == 'clarify':
                try:
                    validate_plan(result)
                    clarified = True
                except (ValueError, TypeError, KeyError):
                    pass
            error = adapter.last_error or (None if passed or clarified else wrong)
            diagnostic = safe_diagnostic(adapter, name, error)
            checks[name] = {'executed': True, 'passed': passed,
                            'outcome': 'passed' if passed else
                                       'clarify' if clarified else diagnostic['error'],
                            'error': diagnostic['error'], 'event_type': diagnostic['event_type'],
                            'http_status': diagnostic['http_status'],
                            'attempts': adapter.last_attempts or 1}
            return result

        from .grounded import render_answer, validate_plan
        plan = check('plan', (lambda: adapter.plan('이번 달 매출 요약을 보여줘', {
            'snapshot_date': '2026-10-11', 'request_date': '2026-10-12',
            'resolved_period': ['2026-10-01', '2026-10-11'],
            'date_from': '2026-10-01', 'date_to': '2026-10-11', 'history': []},
            threading.Event())) if probe['ready'] else None,
            lambda value: bool(value and validate_plan(value) and value['action'] == 'query'),
            'invalid_plan')
        facts = [{'id': 'F1', 'text': '연결 시험용 가상 자료: 매출 100원, 매입 30원.', 'status': '정상',
                  'data': {'synthetic': True, 'sales_won': 100, 'purchase_won': 30}}]
        def valid_answer(value):
            try:
                render_answer(value, facts, [])
                return True
            except (ValueError, TypeError):
                return False
        answer = check('answer', (lambda: adapter.explain('이 시험 자료에서 확인할 점을 알려줘',
            {'facts': facts, 'notes': []}, threading.Event())) if probe['ready'] else None,
            valid_answer, 'invalid_answer')
        question = '안녕. 오늘 대화의 기억 단어는 해바라기야. 기억해 줘.'
        first = check('conversation', (lambda: adapter.chat(question, {'history': []},
            threading.Event())) if probe['ready'] else None,
            lambda value: isinstance(value, dict) and value.get('kind') == 'reply',
            'invalid_answer')
        second = check('followup', (lambda: adapter.chat('아까 내가 알려준 기억 단어가 뭐였지?',
            {'history': [{'question': question, 'answer': first['text']}]},
            threading.Event())) if checks['conversation']['passed'] else None,
            lambda value: isinstance(value, dict) and value.get('kind') == 'reply'
                          and '해바라기' in value.get('text', ''),
            'invalid_answer')
        ready = all(value['passed'] for value in checks.values())
        return {'ready': ready, 'codex': probe, 'plan_valid': checks['plan']['passed'],
                'answer_valid': checks['answer']['passed'],
                'conversation_valid': checks['conversation']['passed'],
                'followup_valid': checks['followup']['passed'],
                'error': next((v['error'] for v in checks.values() if v['error'] and v['error'] != 'not_run'), None),
                'checks': checks, 'settings_changed': False,
                'additional_spend_controls_verified': False,
                '_exit_code': 2 if args.require_ready and not ready else 0}
    if args.command == 'doctor' or (args.command == 'service' and args.dry_run):
        result = doctor(settings)
        failed = (args.require_ready and not result['ready']) or (getattr(args, 'require_conversation_deployable', False) and not result['conversation_deployable'])
        failed = failed or (getattr(args, 'require_data_query_ready', False) and
                            not (result['conversation_ready'] and result['data_query_ready']))
        result['_exit_code'] = 2 if failed else 0
        return result
    if args.command == 'report':
        from dataclasses import replace
        repository = Repository(replace(settings, snapshot=str(args.snapshot)))
        path, context = repository.current()
        if context.get('format') == 'legacy_v1':
            from .presentation import render_query
            return {'text': render_query(repository.query(path, 'summary')), 'snapshot_id': context['snapshot_id'], 'files_written': False}
        return {'text': briefing(context), 'snapshot_id': context['snapshot_id'], 'files_written': False}
    if args.config is None:
        parser.error('service에는 --config가 필요합니다')
    settings.ready()
    from .runtime import Service
    from .storage import Store
    from .telegram import Telegram
    with instance_lock(settings.state_root):
        store = Store(settings.state_root)
        from logging.handlers import RotatingFileHandler
        log_dir = store.root / 'logs'
        log_dir.mkdir(exist_ok=True)
        handler = RotatingFileHandler(log_dir / 'service.log', maxBytes=2_000_000, backupCount=2, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logging.getLogger().addHandler(handler)
        try:
            asyncio.run(Service(settings, store, Telegram(settings.token)).run())
        finally:
            store.close()
            logging.getLogger().removeHandler(handler)
            handler.close()
    return {'stopped': True}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    result = main()
    code = result.pop('_exit_code', 0)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(code)
