"""예약 작업과 Telegram 수신 심박을 감시해 숨김 서비스를 복구한다."""
import argparse
from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import subprocess


TASK = 'DoridangSamjeonMaterialAgent'
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0


def age_seconds(value, now):
    try:
        when = datetime.fromisoformat(value)
        if when.tzinfo is None:
            return float('inf')
        return max(0, (now - when.astimezone(timezone.utc)).total_seconds())
    except (TypeError, ValueError, AttributeError):
        return float('inf')


def recovery_reason(task, health, now):
    if task.get('state') != 'Running':
        return 'task_stopped'
    if age_seconds(task.get('last_run'), now) < 120:
        return None
    if age_seconds(health.get('checked_at'), now) > 180:
        return 'health_stale'
    if age_seconds(health.get('telegram_poll_at'), now) > 120:
        return 'telegram_poll_stale'
    return None


def powershell(command):
    return subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden',
                           '-Command', command], capture_output=True, timeout=30,
                          creationflags=NO_WINDOW, check=True).stdout.decode('utf-8-sig').strip()


def task_info():
    output = powershell("$t=Get-ScheduledTask -TaskName '" + TASK + "'; "
        "$i=Get-ScheduledTaskInfo -TaskName '" + TASK + "'; "
        "[pscustomobject]@{state=$t.State.ToString(); "
        "last_run=$i.LastRunTime.ToUniversalTime().ToString('o')} | ConvertTo-Json -Compress")
    return json.loads(output)


def recover(reason):
    if reason == 'task_stopped':
        powershell("Start-ScheduledTask -TaskName '" + TASK + "'")
    else:
        powershell("Stop-ScheduledTask -TaskName '" + TASK + "'; "
                   "Start-Sleep -Seconds 2; Start-ScheduledTask -TaskName '" + TASK + "'")


def main(config):
    raw = json.loads(Path(config).read_text(encoding='utf-8'))
    state = Path(raw['state_root'])
    if not state.is_absolute():
        state = (Path(config).resolve().parent / state).resolve()
    # 진단 기록은 OneDrive가 아닌 기존 서비스 상태 폴더에만 남긴다.
    if 'onedrive' in str(state).casefold():
        raise ValueError('state_root_not_local')
    log_dir = state / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(log_dir / 'watchdog.log', maxBytes=200_000,
                                  backupCount=1, encoding='utf-8')
    logging.basicConfig(handlers=[handler], level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    try:
        health = json.loads((state / 'health.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        health = {}
    try:
        info = task_info()
        reason = recovery_reason(info, health, datetime.now(timezone.utc))
        if reason:
            logging.warning('삼전점 서비스 복구: %s', reason)
            recover(reason)
    except (OSError, ValueError, subprocess.SubprocessError, KeyError):
        logging.exception('삼전점 서비스 감시 실패')
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    arguments = parser.parse_args()
    main(arguments.config)
