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


def resolve_extension_dir() -> Path:
    env_path = os.getenv("EXTENSION_DIR")
    if env_path:
        return Path(env_path)

    container_mount = _container_mount("/opt/airflow/Extention")
    if container_mount.exists():
        return container_mount

    if _is_windows():
        existing = _resolve_existing_windows_onedrive_path("Extention")
        if existing is not None:
            return existing
        return _windows_onedrive_path("Extention")

    return Path.home() / WINDOWS_ONEDRIVE_FALLBACK / "Extention"


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


def resolve_manual_down_dir() -> Path:
    env_path = os.getenv("D_DOWN_DIR")
    if env_path:
        return Path(env_path)

    if _is_windows():
        return Path("E:/d_down")

    container_down = _container_mount("/opt/airflow/manual_download")
    if container_down.parent.exists():
        return container_down

    return Path.cwd() / "manual_download"


def resolve_analytics_db() -> Path:
    env_path = os.getenv("ANALYTICS_DB")
    if env_path:
        return Path(env_path)

    if _is_windows():
        return _windows_onedrive_path("data", "analytics")

    container_mount = _container_mount("/opt/airflow/analytics")
    if container_mount.parent.exists():
        container_mount.mkdir(parents=True, exist_ok=True)
        return container_mount

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


def resolve_flow_visit_base_dir() -> Path:
    env_path = os.getenv("FLOW_VISIT_BASE_DIR")
    if env_path:
        return Path(env_path)
    return MART_DB / "Flow_mart" / "Flow_visit"


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


ONEDRIVE_DB = resolve_onedrive_db()
COLLECT_DB = resolve_collect_db()
EXTENSION_DIR = resolve_extension_dir()
COLLECTOR_EXT_DIR = EXTENSION_DIR / "doridang_collector_개발용"
LOCAL_DB = resolve_local_db()
TEMP_DIR = resolve_temp_dir()
DOWN_DIR = resolve_down_dir()
MANUAL_DOWN_DIR = resolve_manual_down_dir()
ANALYTICS_DB = resolve_analytics_db()
BAEMIN_MARKETING_DB = ANALYTICS_DB / "baemin_marketing"
BAEMIN_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "baemin_policy_raw.csv"
CHICKEN_PRICE_CSV_PATH = ANALYTICS_DB / "chicken_price" / "chicken_price.csv"
COUPANG_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "coupang_policy_raw.csv"
YOGIYO_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "yogiyo_policy_raw.csv"
DDANGYO_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "ddangyo_policy_raw.csv"
BAEDALTTEUK_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "baedaltteuk_policy_raw.csv"
MUKKEBI_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "mukkebi_policy_raw.csv"
BAEDALEUM_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "baedaleum_policy_raw.csv"
NAVER_PLACE_POLICY_CSV_PATH = ANALYTICS_DB / "policy" / "naver_place_policy_raw.csv"
POLICY_LOG_PATH = ANALYTICS_DB / "policy" / "log.parquet"
POLICY_CONSOLIDATED_CSV = ANALYTICS_DB / "policy" / "policy_consolidated_latest.csv"
INSTAGRAM_SNAPSHOT_DIR = ANALYTICS_DB / "Instagram"
INSTAGRAM_SNAPSHOT_CSV_PATH = INSTAGRAM_SNAPSHOT_DIR / "instagram_snapshot.csv"
KAKAO_FRIENDS_DIR = ANALYTICS_DB / "Kakao" / "Friends"
KAKAO_FRIENDS_CSV_PATH = KAKAO_FRIENDS_DIR / "kakao_friends.csv"
NAVER_ADS_DIR = ANALYTICS_DB / "naver" / "naver_ad"
NAVER_ADS_FILE_PATTERN = "naverads_adgroups_*.csv"
DAANGN_ADS_CSV_PATH = ANALYTICS_DB / "Daangn_ads" / "daangn_ads.csv"
REPORT_SALES_DB = resolve_report_sales_db()
MART_DB = resolve_mart_db()
LLM_OUTPUT_DIR = resolve_llm_output_dir()
DASHBOARD_DB = resolve_dashboard_db()
FLOW_BASE_DIR = ANALYTICS_DB / "flow"
FLOW_PROJECT_PARQUET = FLOW_BASE_DIR / "flow_project" / "flow_project.parquet"
FLOW_POST_PARQUET = FLOW_BASE_DIR / "flow_post"
FLOW_COMMENT_PARQUET = FLOW_BASE_DIR / "flow_comment"
FLOW_ATTACHMENT_PARQUET = FLOW_BASE_DIR / "flow_attachment"
FLOW_ATTACHMENT_FILES_DIR = FLOW_BASE_DIR / "flow_attachment_files"
DORIDANG_BOT_LOG_MD = FLOW_BASE_DIR / "log.md"
DORIDANG_BOT_KNOWLEDGE_MD = FLOW_BASE_DIR / "bot_knowledge.md"
FLOW_LEGACY_PROJECT_PARQUET = ANALYTICS_DB / "flow" / "flow_project.parquet"
FLOW_LEGACY_POST_PARQUET = ANALYTICS_DB / "flow" / "flow_post.parquet"
FLOW_LEGACY_COMMENT_PARQUET = ANALYTICS_DB / "flow" / "flow_comment.parquet"
FLOW_STATE_JSON = LOCAL_DB / "flow_posts_index.json"
MARKETING_ADS_ALERT_STATE_JSON = LOCAL_DB / "marketing_ads_missing_alert.json"
FLOW_VISIT_BASE_DIR = resolve_flow_visit_base_dir()
FLOW_VISIT_LOG_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_log"
FLOW_VISIT_ISSUE_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_issue"
FLOW_VISIT_FOLLOWUP_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_followup"
FLOW_VISIT_TODO_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_todo"
FLOW_VISIT_SUBTASK_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_subtask"
FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_profile_snapshot"
FLOW_STORE_PROFILE_PARQUET = (
    FLOW_VISIT_BASE_DIR / "flow_store_profile" / "flow_store_profile.parquet"
)
FLOW_VISIT_CORPUS_JSONL = FLOW_VISIT_BASE_DIR / "flow_visit_corpus.jsonl"
FLOW_VISIT_VIZ_PARQUET = FLOW_VISIT_BASE_DIR / "flow_visit_viz.parquet"
FLOW_VISIT_LLM_CACHE = LOCAL_DB / "flow_visit_llm_cache.json"
FLOW_VISIT_PROFILE_CACHE = LOCAL_DB / "flow_visit_profile_cache.json"
COLLECTION_COMPARE_PATH = MART_DB / "collection_compare" / "collection_compare.parquet"
NAVER_CORP_STORE_MKT_CSV_PATH = (
    MART_DB / "naver_corporate_store_marketing" / "naver_corporate_store_marketing.csv"
)
MARKETING_ADS_TRACKING_DIR = MART_DB / "Marketing_Ads_Tracking"
MARKETING_ADS_CAMPAIGN_CSV = MARKETING_ADS_TRACKING_DIR / "marketing_ads_campaign.csv"
MARKETING_ADS_DAILY_CSV = MARKETING_ADS_TRACKING_DIR / "marketing_ads_daily.csv"
MARKETING_ADS_FLOW_COMPARE_CSV = MARKETING_ADS_TRACKING_DIR / "marketing_ads_flow_compare.csv"
MARKETING_ADS_DAILY_FLOW_TASKS_CSV = MARKETING_ADS_TRACKING_DIR / "marketing_ads_daily_flow_tasks.csv"
MARKETING_ADS_LINK_MANUAL_CSV = MARKETING_ADS_TRACKING_DIR / "campaign_link_manual.csv"
DELIVERY_COMMISSION_DIR = MART_DB / "delivery_commission"
DELIVERY_COMMISSION_PATH = DELIVERY_COMMISSION_DIR / "delivery_commission.parquet"
ITEM_MASTER_CHECKPOINT_DIR = ANALYTICS_DB / "item_master_checkpoints"
RAW_OKPOS_SALES = resolve_raw_okpos_sales()
RAW_UNIONPOS_SALES = resolve_raw_unionpos_sales()
FIN_PRODUCT_LEGACY_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp.csv"
FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp_input.csv"
FIN_PRODUCT_REVIEW_CSV_PATH = MART_DB / "fin_product" / "fin_product_review.csv"
FIN_PRODUCT_ALIAS_CSV_PATH = MART_DB / "fin_product" / "fin_product_alias.csv"
FIN_PRODUCT_MART_CSV_PATH = MART_DB / "fin_product" / "fin_product_mart.csv"
POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product" / "fin_product_posfeed_whitelist.csv"
FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"
FIN_PRODUCT_MAP_REVIEW_LEGACY_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_review.csv"
FIN_PRODUCT_MAP_REVIEW_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_review_input.csv"
NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH = MART_DB / "fin_product" / "new_fin_product_map_review_input.csv"
FIN_PRODUCT_MAP_RECENTLY_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_recently.csv"
FIN_PRODUCT_MAP_JOIN_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_join.csv"
FIN_PRODUCT_MAP_TRAIN_JSON_PATH = MART_DB / "fin_product" / "fin_product_map_train.json"
FIN_PRODUCT_RULES_JSON_PATH = MART_DB / "fin_product" / "fin_product_rules.json"
FIN_PRODUCT_RULES_MANUAL_JSON_PATH = MART_DB / "fin_product" / "fin_product_rules_manual.json"
FIN_PRODUCT_RULE_PROPOSAL_DIR = TEMP_DIR / "fin_product_rule_proposals"
ORDER_CROSS_DIR = MART_DB / "order_cross_analysis"

# 입력 엑셀과 통합 산출물을 같은 폴더에 둔다. 입력 파일은 파일명으로 직접 지정하므로
# 산출물이 섞여도 수집 대상에 잡히지 않는다. 별도 마트 폴더를 만들지 않는다.
BSP_KPI_DIR = MART_DB / "brand_strategy_planning_team" / "bsp_kpi"
BSP_KPI_WEEKLY_PARQUET = BSP_KPI_DIR / "bsp_kpi_weekly.parquet"
BSP_KPI_WEEKLY_CSV = BSP_KPI_DIR / "bsp_kpi_weekly.csv"
BSP_MONTHLY_KPI_XLSX = BSP_KPI_DIR / "monthly_kpi.xlsx"


def existing_fin_product_csv_path() -> Path:
    return FIN_PRODUCT_CSV_PATH if FIN_PRODUCT_CSV_PATH.exists() else FIN_PRODUCT_LEGACY_CSV_PATH


def existing_fin_product_map_review_csv_path() -> Path:
    return (
        FIN_PRODUCT_MAP_REVIEW_CSV_PATH
        if FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists()
        else FIN_PRODUCT_MAP_REVIEW_LEGACY_CSV_PATH
    )


def existing_new_fin_product_map_review_csv_path() -> Path:
    return (
        NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH
        if NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists()
        else existing_fin_product_map_review_csv_path()
    )

STORE_SALES_TARGET_DIR = ANALYTICS_DB / "store_sales_target"
STORE_SALES_TARGET_CSV = STORE_SALES_TARGET_DIR / "target.csv"
STORE_SALES_DAILY_ACTUALS_CSV = STORE_SALES_TARGET_DIR / "daily_actuals.csv"
STORE_SALES_ANALYSIS_CSV = STORE_SALES_TARGET_DIR / "sales_analysis.csv"

BAEMIN_ORDERS_DETAIL_DB = ANALYTICS_DB / "baemin_macro"
BAEMIN_METRICS_DB = BAEMIN_ORDERS_DETAIL_DB / "metrics_now"
BAEMIN_OUR_STORE_CLICKS_DB = BAEMIN_ORDERS_DETAIL_DB / "metrics_our_store_clicks"
BAEMIN_SHOP_CHANGE_DB = BAEMIN_ORDERS_DETAIL_DB / "shop_change"
BAEMIN_SHOP_OPERATION_DB = BAEMIN_ORDERS_DETAIL_DB / "shop_operation"
BAEMIN_MONTHLY_OPERATION_DB = BAEMIN_ORDERS_DETAIL_DB / "monthly_operation"
BAEMIN_ORDERS_DB = BAEMIN_ORDERS_DETAIL_DB / "orders"
BAEMIN_AD_FUNNEL_DB = BAEMIN_ORDERS_DETAIL_DB / "ad_funnel"

COUPANG_ORDERS_DETAIL_DB = ANALYTICS_DB / "coupang_macro"
COUPANG_ORDERS_DB = COUPANG_ORDERS_DETAIL_DB / "orders"

UNIFIED_REVIEW_MART_DIR = MART_DB / "unified_review"
TOORDER_REVIEW_ANALYTICS_DIR = ANALYTICS_DB / "toorder_review"
