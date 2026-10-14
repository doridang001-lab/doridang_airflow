"""중지된 서비스의 실행 설정만 원자적으로 전환한다."""
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile

from .config import Settings, local_path


def configure(filename, distribution, user, executable, dry_run=False):
    filename = local_path(filename)
    original = filename.read_bytes()
    settings = Settings.load(filename)
    changes = dict(codex_backend='wsl', wsl_distribution=distribution, wsl_user=user,
                   wsl_codex_executable=executable, model_reasoning_effort='medium',
                   enabled=False, ai_enabled=False, ai_verified=False, auto_send=False)
    replace(settings, **changes).validate()
    if dry_run:
        return {'settings_changed': False, 'backend': 'wsl', 'activation_required': True}
    from .__main__ import instance_lock
    with instance_lock(settings.state_root):
        if filename.read_bytes() != original:
            raise ValueError('설정이 변경됐습니다. 다시 확인하세요')
        raw = json.loads(original.decode('utf-8'))
        raw.update(changes)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=filename.parent,
                                             prefix='.samjeon-settings-', suffix='.tmp', delete=False) as f:
                temporary = Path(f.name)
                json.dump(raw, f, ensure_ascii=False, indent=2)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, filename)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return {'settings_changed': True, 'backend': 'wsl', 'activation_required': True}
