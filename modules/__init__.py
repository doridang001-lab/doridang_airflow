# modules/__init__.py

# ========== 새로운 권장 구조 ==========
# 범용 DB 조회
from modules.extract.extract_db import read_table, db_read_table, nb_read_table

# 설정
from modules.common.config import DB_CONFIG, get_db_url, nb_get_db_url, CHANNELS, STORES
from modules.extract.extract_gsheet import extract_gsheet
from modules.transform.aggregate import agg_df

# ========== load ==========
from modules.load.load_gsheet import save_to_gsheet
from modules.load.load_postgre_db import postgre_db_save
from modules.load.load_onedrive import onedrive_csv_save

from modules.notification.sales_alert import check_threshold
from modules.notification.send_email_graph import send_email_graph
from modules.extract.extract_db import read_table


# ========== transform ==========
from modules.transform.key_generator import add_surrogate_key
from modules.transform.utility.io import read_csv_glob, load_data, preprocess_df
from modules.transform.pipelines.sales_daily_orders import load_baemin_data

# ========== extract ==========
from modules.extract.extract_local_file import read_local_file
from modules.extract.croling_coupang import run_coupang_crawling
from modules.extract.croling_beamin import run_baemin_crawling
from modules.extract.extract_onedrive_file import read_onedrive_file
from modules.transform.aggregate import agg_df  
from modules.extract.croling_toorder import run_toorder_crawling
from modules.load.load_onedrive_csv import load_to_onedrive_csv



# 기존 DAG 호환용 (레거시) - read_table로 대체됨
def db_load_data(schema='public', table=None, columns=None, order_by=None):
    """
    기존 함수 호환 유지 - 내부적으로 read_table 사용
    
    Args:
        schema: 스키마명 (기본값: 'public')
        table: 테이블명 (기본값: 'baemin_sales')
        columns: 선택할 컬럼 리스트 (None이면 전체)
        order_by: 정렬 기준 (str 또는 list)
    """
    if table is None:
        table = 'baemin_sales'
    return read_table(table=table, schema=schema, columns=columns, order_by=order_by)


__all__ = [
    # 새로운 구조
    'read_table',
    'db_read_table',
    'extract_gsheet',
    'DB_CONFIG',
    'get_db_url',
    'CHANNELS',
    'STORES',
    'agg_df',
    # 레거시
    'add_surrogate_key', 
    'save_to_gsheet', 
    'postgre_db_save',
    'check_threshold',
    'send_email_graph',
    'db_load_data',
    'onedrive_csv_save',
    'read_local_file',
    # extract
    'run_baemin_crawling',
    'run_coupang_crawling',
    "read_onedrive_file",
    "run_toorder_crawling",
    # load
    'load_to_onedrive_csv',
    # transform
    'read_csv_glob',
    'load_data',
    'load_baemin_data',
    'preprocess_df'
]