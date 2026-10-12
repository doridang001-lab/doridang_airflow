"""마트의 경로 해석. 경로 조회와 파일 생성을 분리한다."""
import os
from pathlib import Path


def root(name: str) -> Path:
    windows = os.name == 'nt'
    value = os.environ.get(name)
    mounts = {'LOCAL_DB': '/opt/airflow/Local_DB', 'MART_DB': '/opt/airflow/onedrive_mart',
              'ANALYTICS_DB': '/opt/airflow/analytics'}
    if value and not (windows and value.rstrip('/') == mounts[name]):
        return Path(value)
    if not windows and Path('/opt/airflow').exists():
        return Path(mounts[name])
    if name == 'LOCAL_DB':
        return Path('C:/Local_DB') if windows else Path.cwd() / 'Local_DB'
    onedrive = next((os.environ[k] for k in ('OneDriveCommercial', 'ONEDRIVECOMMERCIAL', 'OneDrive', 'ONEDRIVE')
                     if os.environ.get(k)), str(Path.home() / 'OneDrive - 주식회사 도리당'))
    return Path(onedrive) / 'data' / ('mart' if name == 'MART_DB' else 'analytics')


LOCAL_DB, MART_DB, ANALYTICS_DB = (root(k) for k in ('LOCAL_DB', 'MART_DB', 'ANALYTICS_DB'))
