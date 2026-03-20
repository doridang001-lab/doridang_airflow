"""
DB 통합 모듈 (조회 + 저장)
기존 extract/extract_db.py, load/load_postgre_db.py의 중앙 허브

사용법:
    from modules.transform.utility.db import read_table, postgre_db_save
"""
import importlib


def __getattr__(name):
    """Lazy import - __init__.py 우회하여 직접 모듈 로드"""
    _EXTRACT_DB = {'read_table', 'db_read_table', 'nb_read_table'}
    _LOAD_DB = {'postgre_db_save'}

    if name in _EXTRACT_DB:
        mod = importlib.import_module('modules.extract.extract_db')
        return getattr(mod, name)
    elif name in _LOAD_DB:
        mod = importlib.import_module('modules.load.load_postgre_db')
        return getattr(mod, name)
    raise AttributeError(f"module 'modules.transform.utility.db' has no attribute '{name}'")


__all__ = [
    'read_table',
    'db_read_table',
    'nb_read_table',
    'postgre_db_save',
]
