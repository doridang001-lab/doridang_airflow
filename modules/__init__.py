# modules/__init__.py

# ========== 설정 ==========
from modules.common.config import DB_CONFIG, get_db_url, nb_get_db_url, CHANNELS, STORES

# ========== 스케줄 ==========
from modules.transform.utility.schedule import (
    SMD_ORDERS_TIME, SMD_VISIT_LOG,
    SMP_TOORDER_VOC_TIME, SMP_FDAM_CS_TIME,
)

# ========== 이메일 ==========
from modules.transform.utility.mailer import send_email, text_to_html

# ========== transform ==========
from modules.transform.aggregate import agg_df
from modules.transform.key_generator import add_surrogate_key
from modules.transform.utility.io import read_csv_glob, load_data, preprocess_df


# ========== Lazy imports (circular import 방지) ==========
def __getattr__(name):
    """modules 패키지 레벨 lazy import"""

    # DB
    _DB = {'read_table', 'db_read_table', 'nb_read_table'}
    if name in _DB:
        from modules.extract.extract_db import read_table, db_read_table, nb_read_table
        return locals()[name]

    if name == 'postgre_db_save':
        from modules.load.load_postgre_db import postgre_db_save
        return postgre_db_save

    # GSheet
    if name == 'extract_gsheet':
        from modules.extract.extract_gsheet import extract_gsheet
        return extract_gsheet

    if name == 'save_to_gsheet':
        from modules.load.load_gsheet import save_to_gsheet
        return save_to_gsheet

    # load
    if name == 'onedrive_csv_save':
        from modules.load.load_onedrive import onedrive_csv_save
        return onedrive_csv_save

    if name == 'load_to_onedrive_csv':
        from modules.load.load_onedrive_csv import load_to_onedrive_csv
        return load_to_onedrive_csv

    # extract
    _EXTRACT = {
        'read_local_file': ('modules.extract.extract_local_file', 'read_local_file'),
        'run_coupang_crawling': ('modules.extract.croling_coupang', 'run_coupang_crawling'),
        'run_baemin_crawling': ('modules.extract.croling_beamin', 'run_baemin_crawling'),
        'read_onedrive_file': ('modules.extract.extract_onedrive_file', 'read_onedrive_file'),
        'run_toorder_crawling': ('modules.extract.croling_toorder', 'run_toorder_crawling'),
    }
    if name in _EXTRACT:
        import importlib
        mod_path, attr = _EXTRACT[name]
        mod = importlib.import_module(mod_path)
        return getattr(mod, attr)

    # pipelines
    if name == 'load_baemin_data':
        from modules.transform.pipelines.sales.SMD_03_sales_orders_transform import load_baemin_data
        return load_baemin_data

    # 레거시
    if name == 'db_load_data':
        def db_load_data(schema='public', table=None, columns=None, order_by=None):
            from modules.extract.extract_db import read_table
            if table is None:
                table = 'baemin_sales'
            return read_table(table=table, schema=schema, columns=columns, order_by=order_by)
        return db_load_data

    raise AttributeError(f"module 'modules' has no attribute '{name}'")


__all__ = [
    # 설정
    'DB_CONFIG', 'get_db_url', 'CHANNELS', 'STORES',
    # DB
    'read_table', 'db_read_table', 'postgre_db_save',
    # GSheet
    'extract_gsheet', 'save_to_gsheet',
    # 스케줄
    'SMD_ORDERS_TIME', 'SMD_VISIT_LOG', 'SMP_TOORDER_VOC_TIME', 'SMP_FDAM_CS_TIME',
    # 이메일
    'send_email', 'text_to_html',
    # transform
    'agg_df', 'add_surrogate_key', 'read_csv_glob', 'load_data', 'preprocess_df',
    # load
    'onedrive_csv_save', 'load_to_onedrive_csv',
    # extract
    'read_local_file', 'run_baemin_crawling', 'run_coupang_crawling',
    'read_onedrive_file', 'run_toorder_crawling',
    # 레거시
    'db_load_data', 'load_baemin_data',
]
