"""비테스트 매장 배달 undercount 진단 (read-only)."""

import pandas as pd

from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, MART_DB, ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    _load_posfeed_blacklist,
    DELIVERY_MANUAL_TEST_STORES,
)

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
TOORDER = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
TARGET_YM = "2026-06"
SAMPLE_STORES = ["부산광안점", "인천간석중앙점", "화성봉담점", "노원점"]


def main():
    # 1. blacklist 규모
    bl = _load_posfeed_blacklist()
    print(f"=== posfeed 블랙리스트 항목 수: {len(bl)} ===")

    grp = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    if {"source", "exclude_check"}.issubset(grp.columns):
        pf = grp[grp["source"].str.strip().str.lower() == "posfeed"]
        ex = pf[pf["exclude_check"].str.strip().str.upper() == "Y"]
        print(f"grp posfeed 행: {len(pf)} / exclude_check=Y: {len(ex)}")
        print("exclude_check=Y 상품명 샘플:")
        print(ex["상품명"].head(30).tolist())

    # 2. unified 로드 (해당 월)
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    parts = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f, columns=["sale_date", "store", "platform", "source", "total_price"]))
        except Exception:
            pass
    u = pd.concat(parts, ignore_index=True)
    u["ym"] = pd.to_datetime(u["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    u = u[u["ym"] == TARGET_YM].copy()
    u["total_price"] = pd.to_numeric(u["total_price"], errors="coerce").fillna(0)

    # 3. toorder 로드
    t = pd.read_parquet(TOORDER, columns=["date", "store", "platform", "price"])
    t["ym"] = pd.to_datetime(t["date"], errors="coerce").dt.strftime("%Y-%m")
    t = t[t["ym"] == TARGET_YM].copy()
    t["price"] = pd.to_numeric(t["price"], errors="coerce").fillna(0)

    print("\n=== toorder platform 분포 (전체) ===")
    print(t.groupby("platform")["price"].sum().sort_values(ascending=False))

    for store in SAMPLE_STORES:
        print(f"\n===== {store} ({TARGET_YM}) =====")
        us = u[u["store"].astype(str).str.strip() == store]
        ts = t[t["store"].astype(str).str.strip() == store]
        print("-- unified by platform/source --")
        if not us.empty:
            print(us.groupby(["platform", "source"])["total_price"].sum().sort_values(ascending=False))
        print(f"unified 합계: {us['total_price'].sum():,.0f}")
        print("-- toorder by platform --")
        if not ts.empty:
            print(ts.groupby("platform")["price"].sum().sort_values(ascending=False))
        print(f"toorder 합계: {ts['price'].sum():,.0f}")


if __name__ == "__main__":
    main()
