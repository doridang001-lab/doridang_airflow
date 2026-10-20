"""패키지 명령은 봇·모델을 실행하거나 운영 설정을 활성화하지 않는다."""
from . import assistant_packages as packages
from .config import Settings
import sqlite3


def add_commands(subs):
    command = subs.add_parser('package')
    actions = command.add_subparsers(dest='package_action', required=True)
    for action in ('validate', 'inspect', 'import', 'export', 'export-change'):
        p = actions.add_parser(action)
        p.add_argument('--config')
        if action in ('validate', 'inspect', 'import'):
            p.add_argument('--input', required=True)
        if action == 'import':
            p.add_argument('--dry-run', action='store_true', required=True)
        if action in ('export', 'export-change'):
            p.add_argument('--output', required=True)
            p.add_argument('--user', type=int)
            p.add_argument('--chat', type=int)
        if action == 'export':
            p.add_argument('--id', required=True)
            p.add_argument('--version', required=True)
            p.add_argument('--learning-id', action='append', default=[])
        if action == 'export-change':
            p.add_argument('--learning-id', required=True)


def run(args):
    try:
        settings = Settings.load(args.config) if args.config else None
        if args.package_action == 'export':
            return packages.export_package(settings, args.user, args.chat, args.learning_id,
                                           args.output, args.id, args.version)
        if args.package_action == 'export-change':
            if settings is None:
                raise ValueError('config_required')
            return packages.export_change(settings, args.user, args.chat, args.learning_id, args.output)
        package = packages.read_package(args.input)
        result = packages.validate(package, settings)
        if args.package_action == 'inspect':
            result['manifest'] = package['manifest']
            result['purpose'] = package['files']['purpose.md']
            result['workflow'] = package['files']['workflow.md']
        if args.package_action == 'import':
            result.update(dry_run=True, activated=False)
        return result
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as exc:
        # 파일 경로와 DB 오류 원문은 명령 결과에 넣지 않는다.
        error = str(exc) if isinstance(exc, ValueError) and str(exc).replace('_', '').isalnum() else 'package_operation_failed'
        return {'valid': False, 'error': error, 'files_written': False, '_exit_code': 2}
