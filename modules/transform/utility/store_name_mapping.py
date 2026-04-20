"""store_name_mapping.py — 하위 호환 래퍼. 실체는 store_normalize.py"""
from modules.transform.utility.store_normalize import (
    STORE_NAME_MAP,
    normalize as normalize_store_names,
    normalize_for_join,
)

__all__ = ['STORE_NAME_MAP', 'normalize_store_names', 'normalize_for_join']
