"""
Google Sheets 통합 모듈 (추출 + 저장)
기존 extract/extract_gsheet.py, load/load_gsheet.py의 중앙 허브

사용법:
    from modules.transform.utility.gsheet import extract_gsheet, save_to_gsheet
"""
import importlib


def __getattr__(name):
    """Lazy import - __init__.py 우회하여 직접 모듈 로드"""
    _EXTRACT = {'extract_gsheet'}
    _LOAD = {'save_to_gsheet', 'apply_date_format_to_column'}

    if name in _EXTRACT:
        mod = importlib.import_module('modules.extract.extract_gsheet')
        return getattr(mod, name)
    elif name in _LOAD:
        mod = importlib.import_module('modules.load.load_gsheet')
        return getattr(mod, name)
    raise AttributeError(f"module 'modules.transform.utility.gsheet' has no attribute '{name}'")


__all__ = [
    'extract_gsheet',
    'save_to_gsheet',
    'apply_date_format_to_column',
]
