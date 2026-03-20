# modules/extract/__init__.py
from modules.extract.extract_db import read_table, db_read_table
from modules.extract.extract_local_file import read_local_file
from modules.extract.extract_gsheet import extract_gsheet

# 크롤링 모듈 (선택적 의존성 - Docker 환경에서만 사용)
try:
    from modules.extract.croling_coupang import run_coupang_crawling
except ImportError:
    run_coupang_crawling = None

try:
    from modules.extract.croling_beamin import run_baemin_crawling
except ImportError:
    run_baemin_crawling = None

try:
    from modules.extract.extract_onedrive_file import read_onedrive_file
except ImportError:
    read_onedrive_file = None

try:
    from modules.extract.croling_toorder import run_toorder_crawling
except ImportError:
    run_toorder_crawling = None

__all__ = [
    "read_table",
    "db_read_table",
    "read_local_file",
    "extract_gsheet",
    "run_coupang_crawling",
    "run_baemin_crawling",
    "read_onedrive_file",
    "run_toorder_crawling"
]
