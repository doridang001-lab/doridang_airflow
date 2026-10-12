"""Airflow는 확인 요청 파일만 전달한다. 상태 DB와 Telegram은 서비스가 소유한다."""
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import os
import uuid

KST = ZoneInfo('Asia/Seoul')
SLOTS = {'09:30', '10:00', '10:30', '11:00', '11:30', '12:00'}


def valid_job(job, now):
    return (set(job) == {'version', 'kind', 'slot', 'date', 'as_of_date', 'expires'} and
            type(job['version']) is int and job['version'] == 1 and job['kind'] == 'check' and
            job['slot'] in SLOTS and job['date'] == str(now.date()) and
            job['as_of_date'] == str(now.date() - timedelta(days=1)) and
            datetime.fromisoformat(job['expires']) >= now)


def enqueue(root, now=None, *, manual=False, conf=None, dry_run=False):
    from .config import local_path
    now = (now or datetime.now(KST)).astimezone(KST)
    slot = now.strftime('%H:%M')
    if manual or conf or slot not in SLOTS:
        return {'queued': False, 'reason': '정기 허용 슬롯만 작업을 전달합니다'}
    expires = now.replace(hour=12, minute=1, second=0, microsecond=0)
    job = {'version': 1, 'kind': 'check', 'slot': slot, 'date': str(now.date()),
           'as_of_date': str(now.date() - timedelta(days=1)), 'expires': expires.isoformat()}
    target = local_path(root) / 'queue' / f"{now.date()}-{slot.replace(':', '')}.json"
    if dry_run:
        return {'queued': False, 'dry_run': True, 'job': job}
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.' + uuid.uuid4().hex + '.pending')
    try:
        temporary.write_text(json.dumps(job, ensure_ascii=False), encoding='utf-8')
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return {'queued': True, 'job_path': str(target), 'as_of_date': job['as_of_date']}
