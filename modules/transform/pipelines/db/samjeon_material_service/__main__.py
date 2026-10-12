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


@contextmanager
def instance_lock(root):
    root = local_path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'service.lock').open('a+b') as handle:
        handle.seek(0)
        if not handle.read(1):
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
    result = {'enabled': settings.enabled, 'auto_send': settings.auto_send,
              'token_present': bool(settings.token), 'users_registered': bool(settings.allowed_users),
              'chat_registered': bool(settings.allowed_chats), 'ai_configured': CodexAdapter(settings).available(),
              'ai_live_verified': settings.ai_verified, 'pdf_available': find_spec('reportlab') is not None,
              'font_available': Path(settings.font_path).is_file(), 'files_written': False,
              'state_db_exists': (local_path(settings.state_root) / 'state.sqlite3').is_file()}
    health = local_path(settings.state_root) / 'health.json'
    if health.is_file():
        try:
            result['service_health'] = json.loads(health.read_text(encoding='utf-8'))
        except (ValueError, OSError):
            result['service_health'] = {'status': '진단 파일 확인 필요'}
    try:
        _, context = Repository(settings).current()
        result.update(snapshot_id=context['snapshot_id'], as_of_date=context['as_of_date'],
                      data_status=context['status'], data_complete=context['month_to_date']['complete'])
    except (OSError, ValueError, KeyError) as exc:
        result.update(data_complete=False, data_status='v3 스냅샷 확인 필요', data_error=type(exc).__name__)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    for name in ('doctor', 'service', 'report'):
        cmd = subs.add_parser(name)
        cmd.add_argument('--config', type=Path)
        cmd.add_argument('--dry-run', action='store_true')
        if name == 'report':
            cmd.add_argument('--snapshot', type=Path, required=True)
    args = parser.parse_args(argv)
    settings = Settings.load(args.config) if args.config else Settings()
    settings.validate()
    if args.command == 'doctor' or (args.command == 'service' and args.dry_run):
        return doctor(settings)
    if args.command == 'report':
        from dataclasses import replace
        _, context = Repository(replace(settings, snapshot=str(args.snapshot))).current()
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
    print(json.dumps(main(), ensure_ascii=False, indent=2))
