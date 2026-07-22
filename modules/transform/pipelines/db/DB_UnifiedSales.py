"""
투오더/OKPOS 매출 → mart 일별 unified_sales_YYMMDD.parquet 생성 파이프라인 (Facade).

채널별 구현은 파일로 분리:
- toorder : `DB_UnifiedSales_toorder.py`
- okpos   : `DB_UnifiedSales_okpos.py`
- unionpos: `DB_UnifiedSales_unionpos.py`
- common  : `DB_UnifiedSales_common.py`
"""

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    ADD_TEST_STORES,
    DELIVERY_MANUAL_TEST_STORES,
    EXCLUDE_TEST_STORES,
    TOORDER_MANUAL_STORES,
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    enforce_manual_delivery_sources_for_test_stores,
    filter_manual_delivery_sources_for_test_stores,
    purge_manual_delivery_sources_for_non_test_stores,
    reclassify_hall_platform,
    refresh_store_meta_in_unified_sales,
    resave_existing_unified_sales,
)
from modules.transform.pipelines.db.DB_UnifiedSales_okpos import (
    backfill_okpos,
    run_lookback_okpos,
    run_okpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_toorder import (
    backfill_toorder,
    backfill_toorder_manual_stores,
    run_toorder,
    run_toorder_manual_stores,
    run_lookback_toorder,
    run_lookback_toorder_manual_stores,
)
from modules.transform.pipelines.db.DB_UnifiedSales_unionpos import (
    backfill_unionpos,
    run_lookback_unionpos,
    run_unionpos,
    upsert_fin_product_grp_from_unionpos,
)
from modules.transform.pipelines.db.DB_UnifiedSales_hall_viz import (
    HALL_VIZ_PATH,
    backfill_hall_viz,
    run_hall_viz,
    run_lookback_hall_viz,
)
from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import (
    backfill_posfeed,
    generate_posfeed_whitelist_draft,
    run_lookback_posfeed,
    run_posfeed,
    sync_posfeed_blacklist,
)

__all__ = [
    "UNIFIED_COLUMNS",
    "UNIFIED_ROOT",
    "ADD_TEST_STORES",
    "EXCLUDE_TEST_STORES",
    "DELIVERY_MANUAL_TEST_STORES",
    "TOORDER_MANUAL_STORES",
    "HALL_VIZ_PATH",
    "run_toorder",
    "run_toorder_manual_stores",
    "run_okpos",
    "run_posfeed",
    "run_lookback_toorder",
    "run_lookback_toorder_manual_stores",
    "run_lookback_okpos",
    "run_lookback_posfeed",
    "run_unionpos",
    "run_lookback_unionpos",
    "backfill_toorder",
    "backfill_toorder_manual_stores",
    "backfill_okpos",
    "backfill_unionpos",
    "backfill_posfeed",
    "sync_posfeed_blacklist",
    "generate_posfeed_whitelist_draft",
    "run_hall_viz",
    "run_lookback_hall_viz",
    "backfill_hall_viz",
    "resave_existing_unified_sales",
    "reclassify_hall_platform",
    "refresh_store_meta_in_unified_sales",
    "filter_manual_delivery_sources_for_test_stores",
    "enforce_manual_delivery_sources_for_test_stores",
    "purge_manual_delivery_sources_for_non_test_stores",
    "upsert_fin_product_grp_from_unionpos",
]
