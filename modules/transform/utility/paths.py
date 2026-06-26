from pathlib import Path
import os
import platform


WINDOWS_ONEDRIVE_FALLBACK = "OneDrive - 주식회사 도리당"
WINDOWS_ONEDRIVE_ENV_KEYS = (
    "OneDriveCommercial",
    "ONEDRIVECOMMERCIAL",
    "OneDrive",
    "ONEDRIVE",
)


def _is_windows() -> bool:
    return platform.system() == "Windows"


def _container_mount(path_str: str) -> Path:
    return Path(path_str)


def _windows_onedrive_root_from_env() -> Path | None:
    for key in WINDOWS_ONEDRIVE_ENV_KEYS:
        value = (os.getenv(key) or "").strip()
        if value:
            return Path(value)
    return None


def get_windows_onedrive_root() -> Path:
    detected = _windows_onedrive_root_from_env()
    if detected:
        return detected
    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK


def _windows_onedrive_path(*parts: str) -> Path:
    return get_windows_onedrive_root().joinpath(*parts)


def _resolve_existing_windows_onedrive_path(*parts: str) -> Path | None:
    candidate = _windows_onedrive_path(*parts)
    if candidate.exists():
        return candidate
    return None


def resolve_onedrive_db() -> Path:
    env_path = os.getenv("ONEDRIVE_DB")
    if env_path:
        p = Path(env_path)
        repo_mount = _container_mount("/opt/airflow/Repository")
        if p.as_posix().endswith("/Doridang_DB") and repo_mount.exists():
            return repo_mount
        return p

    container_mount = _container_mount("/opt/airflow/Repository")
    if container_mount.exists():
        return container_mount

    if _is_windows():
        existing = _resolve_existing_windows_onedrive_path("Repository")
        if existing is not None:
            return existing
        return _windows_onedrive_path("Repository")

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "Repository"


def resolve_collect_db() -> Path:
    env_path = os.getenv("COLLECT_DB")
    if env_path:
        return Path(env_path)

    container_mount = _container_mount("/opt/airflow/Collect_Data")
    if container_mount.exists():
        return container_mount

    if _is_windows():
        existing = _resolve_existing_windows_onedrive_path("Collect_Data")
        if existing is not None:
            return existing
        return _windows_onedrive_path("Collect_Data")

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "Collect_Data"


def resolve_local_db() -> Path:
    env_path = os.getenv("LOCAL_DB")
    if env_path:
        p = Path(env_path)
        p.mkdir(parents=True, exist_ok=True)
        return p

    container_mount = _container_mount("/opt/airflow/Local_DB")
    if container_mount.parent.exists():
        container_mount.mkdir(parents=True, exist_ok=True)
        return container_mount

    if _is_windows():
        local_db = Path("C:/Local_DB")
        local_db.mkdir(parents=True, exist_ok=True)
        return local_db

    fallback = Path.cwd() / "Local_DB"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def resolve_temp_dir() -> Path:
    env_path = os.getenv("TEMP_DIR")
    if env_path:
        p = Path(env_path)
        p.mkdir(parents=True, exist_ok=True)
        return p

    container_default = _container_mount("/opt/airflow/Doridang/temp")
    if container_default.parent.parent.exists():
        container_default.mkdir(parents=True, exist_ok=True)
        return container_default

    if _is_windows():
        win_temp = Path("C:/Local_DB/temp")
        win_temp.mkdir(parents=True, exist_ok=True)
        return win_temp

    fallback = Path.cwd() / "Doridang" / "temp"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def resolve_down_dir() -> Path:
    env_path = os.getenv("DOWN_DIR")
    if env_path:
        p = Path(env_path)
        p.mkdir(parents=True, exist_ok=True)
        return p

    if _is_windows():
        win_down = Path("E:/down")
        win_down.mkdir(parents=True, exist_ok=True)
        return win_down

    container_down = _container_mount("/opt/airflow/download")
    if container_down.parent.exists():
        container_down.mkdir(parents=True, exist_ok=True)
        return container_down

    fallback = Path.cwd() / "download"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def resolve_analytics_db() -> Path:
    env_path = os.getenv("ANALYTICS_DB")
    if env_path:
        return Path(env_path)

    container_mount = _container_mount("/opt/airflow/analytics")
    if container_mount.parent.exists():
        container_mount.mkdir(parents=True, exist_ok=True)
        return container_mount

    if _is_windows():
        return _windows_onedrive_path("data", "analytics")

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "data" / "analytics"


def resolve_report_sales_db() -> Path:
    env_path = os.getenv("REPORT_SALES_DB")
    if env_path:
        return Path(env_path)

    container_mount = _container_mount("/opt/airflow/report/sales")
    if container_mount.parent.parent.exists():
        container_mount.mkdir(parents=True, exist_ok=True)
        return container_mount

    if _is_windows():
        return _windows_onedrive_path("data", "report", "sales")

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "data" / "report" / "sales"


def resolve_mart_db() -> Path:
    env_path = os.getenv("MART_DB")
    if env_path:
        return Path(env_path)

    if _is_windows():
        return _windows_onedrive_path("data", "mart")

    container_mount = _container_mount("/opt/airflow/onedrive_mart")
    if container_mount.exists():
        return container_mount

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "data" / "mart"


def resolve_llm_output_dir() -> Path:
    env_path = os.getenv("LLM_OUTPUT_DIR")
    if env_path:
        return Path(env_path)

    if _is_windows():
        return _windows_onedrive_path("data", "llm")

    container_mount = _container_mount("/opt/airflow/onedrive_llm")
    if container_mount.exists():
        return container_mount

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "data" / "llm"


def resolve_dashboard_db() -> Path:
    env_path = os.getenv("DASHBOARD_DB")
    if env_path:
        return Path(env_path)

    if _is_windows():
        return _windows_onedrive_path("data", "dashboard")

    container_mount = _container_mount("/opt/airflow/dashboard")
    if container_mount.parent.exists():
        return container_mount

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "data" / "dashboard"


def resolve_raw_okpos_sales() -> Path:
    env_path = os.getenv("RAW_OKPOS_SALES")
    if env_path:
        return Path(env_path)
    return resolve_analytics_db() / "okpos_sales_raw"


def resolve_raw_unionpos_sales() -> Path:
    env_path = os.getenv("RAW_UNIONPOS_SALES")
    if env_path:
        return Path(env_path)
    return resolve_analytics_db() / "unionpos_sales_raw"


_cache: dict = {}


def _get(name: str):
    if name not in _cache:
        _cache[name] = _RESOLVERS[name]()
    return _cache[name]


_RESOLVERS = {
    # Primary resolvers
    "ONEDRIVE_DB":    resolve_onedrive_db,
    "COLLECT_DB":     resolve_collect_db,
    "LOCAL_DB":       resolve_local_db,
    "TEMP_DIR":       resolve_temp_dir,
    "DOWN_DIR":       resolve_down_dir,
    "ANALYTICS_DB":   resolve_analytics_db,
    "REPORT_SALES_DB": resolve_report_sales_db,
    "MART_DB":        resolve_mart_db,
    "LLM_OUTPUT_DIR": resolve_llm_output_dir,
    "DASHBOARD_DB":   resolve_dashboard_db,
    "RAW_OKPOS_SALES":    resolve_raw_okpos_sales,
    "RAW_UNIONPOS_SALES": resolve_raw_unionpos_sales,
    # Derived — ANALYTICS_DB 계열
    "BAEMIN_MARKETING_DB":          lambda: _get("ANALYTICS_DB") / "baemin_marketing",
    "BAEMIN_POLICY_CSV_PATH":       lambda: _get("ANALYTICS_DB") / "policy" / "baemin_policy_raw.csv",
    "CHICKEN_PRICE_CSV_PATH":       lambda: _get("ANALYTICS_DB") / "chicken_price" / "chicken_price.csv",
    "COUPANG_POLICY_CSV_PATH":      lambda: _get("ANALYTICS_DB") / "policy" / "coupang_policy_raw.csv",
    "YOGIYO_POLICY_CSV_PATH":       lambda: _get("ANALYTICS_DB") / "policy" / "yogiyo_policy_raw.csv",
    "DDANGYO_POLICY_CSV_PATH":      lambda: _get("ANALYTICS_DB") / "policy" / "ddangyo_policy_raw.csv",
    "BAEDALTTEUK_POLICY_CSV_PATH":  lambda: _get("ANALYTICS_DB") / "policy" / "baedaltteuk_policy_raw.csv",
    "MUKKEBI_POLICY_CSV_PATH":      lambda: _get("ANALYTICS_DB") / "policy" / "mukkebi_policy_raw.csv",
    "BAEDALEUM_POLICY_CSV_PATH":    lambda: _get("ANALYTICS_DB") / "policy" / "baedaleum_policy_raw.csv",
    "NAVER_PLACE_POLICY_CSV_PATH":  lambda: _get("ANALYTICS_DB") / "policy" / "naver_place_policy_raw.csv",
    "POLICY_LOG_PATH":              lambda: _get("ANALYTICS_DB") / "policy" / "log.parquet",
    "POLICY_CONSOLIDATED_CSV":      lambda: _get("ANALYTICS_DB") / "policy" / "policy_consolidated_latest.csv",
    "ITEM_MASTER_CHECKPOINT_DIR":   lambda: _get("ANALYTICS_DB") / "item_master_checkpoints",
    "BAEMIN_ORDERS_DETAIL_DB":      lambda: _get("ANALYTICS_DB") / "baemin_macro",
    "BAEMIN_METRICS_DB":            lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "metrics_now",
    "BAEMIN_OUR_STORE_CLICKS_DB":   lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "metrics_our_store_clicks",
    "BAEMIN_SHOP_CHANGE_DB":        lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "shop_change",
    "BAEMIN_SHOP_OPERATION_DB":     lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "shop_operation",
    "BAEMIN_MONTHLY_OPERATION_DB":  lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "monthly_operation",
    "BAEMIN_ORDERS_DB":             lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "orders",
    "BAEMIN_AD_FUNNEL_DB":          lambda: _get("BAEMIN_ORDERS_DETAIL_DB") / "ad_funnel",
    "COUPANG_ORDERS_DETAIL_DB":     lambda: _get("ANALYTICS_DB") / "coupang_macro",
    "COUPANG_ORDERS_DB":            lambda: _get("COUPANG_ORDERS_DETAIL_DB") / "orders",
    "TOORDER_REVIEW_ANALYTICS_DIR": lambda: _get("ANALYTICS_DB") / "toorder_review",
    # Derived — MART_DB 계열
    "COLLECTION_COMPARE_PATH":      lambda: _get("MART_DB") / "collection_compare" / "collection_compare.parquet",
    "FIN_PRODUCT_CSV_PATH":         lambda: _get("MART_DB") / "fin_product" / "fin_product_grp.csv",
    "FIN_PRODUCT_GRP_TRAIN_JSON_PATH": lambda: _get("MART_DB") / "fin_product" / "fin_product_grp_train.json",
    "FIN_PRODUCT_REVIEW_CSV_PATH":  lambda: _get("MART_DB") / "fin_product" / "fin_product_review.csv",
    "FIN_PRODUCT_ALIAS_CSV_PATH":   lambda: _get("MART_DB") / "fin_product" / "fin_product_alias.csv",
    "FIN_PRODUCT_MAP_CSV_PATH":         lambda: _get("MART_DB") / "fin_product" / "fin_product_map.csv",
    "FIN_PRODUCT_MAP_REVIEW_CSV_PATH":  lambda: _get("MART_DB") / "fin_product" / "fin_product_map_review.csv",
    "FIN_PRODUCT_MAP_TRAIN_JSON_PATH": lambda: _get("MART_DB") / "fin_product" / "fin_product_map_train.json",
    "UNIFIED_REVIEW_MART_DIR":      lambda: _get("MART_DB") / "unified_review",
    # Derived — STORE_SALES 계열
    "STORE_SALES_TARGET_DIR":        lambda: _get("ANALYTICS_DB") / "store_sales_target",
    "STORE_SALES_TARGET_CSV":        lambda: _get("STORE_SALES_TARGET_DIR") / "target.csv",
    "STORE_SALES_DAILY_ACTUALS_CSV": lambda: _get("STORE_SALES_TARGET_DIR") / "daily_actuals.csv",
    "STORE_SALES_ANALYSIS_CSV":      lambda: _get("STORE_SALES_TARGET_DIR") / "sales_analysis.csv",
}


def __getattr__(name: str):
    if name in _RESOLVERS:
        return _get(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
