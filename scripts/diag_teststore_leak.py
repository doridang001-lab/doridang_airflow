"""테스트 매장 로직이 비테스트 매장에 새는지 진단 (read-only)."""

import pandas as pd
from modules.transform.utility.paths import MART_DB, ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
TOORDER = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
YM = "2026-06"
TEST = set(DELIVERY_MANUAL_TEST_STORES)
MANUAL_SOURCES = {"배민수동", "쿠팡수동"}

# 진단 대상: 테스트(동탄영천점/동두천지행점) + 비테스트(김해구산점/화성봉담점/노원점)
SAMPLE = ["동탄영천점", "동두천지행점", "김해구산점", "화성봉담점", "노원점"]


def main():
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    parts = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f, columns=["sale_date", "store", "platform", "source", "total_price"]))
        except Exception:
            pass
    u = pd.concat(parts, ignore_index=True)
    u["ym"] = pd.to_datetime(u["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    u = u[u["ym"] == YM].copy()
    u["store"] = u["store"].astype(str).str.strip()
    u["total_price"] = pd.to_numeric(u["total_price"], errors="coerce").fillna(0)

    # 1. 비테스트 매장에 manual source(배민수동/쿠팡수동)가 있는가? (있으면 누수)
    nontest = u[~u["store"].isin(TEST)]
    leak = nontest[nontest["source"].astype(str).str.strip().isin(MANUAL_SOURCES)]
    print("=== [검사1] 비테스트 매장의 manual source 행 ===")
    if leak.empty:
        print("  없음 (정상) — 비테스트 매장에 배민수동/쿠팡수동 행 없음")
    else:
        print("  *** 누수 발견 ***")
        print(leak.groupby(["store", "source"])["total_price"].agg(["count", "sum"]).head(30))

    # 2. 토더 로드
    t = pd.read_parquet(TOORDER, columns=["date", "store", "platform", "price"])
    t["ym"] = pd.to_datetime(t["date"], errors="coerce").dt.strftime("%Y-%m")
    t = t[t["ym"] == YM].copy()
    t["store"] = t["store"].astype(str).str.strip()
    t["price"] = pd.to_numeric(t["price"], errors="coerce").fillna(0)

    for s in SAMPLE:
        tag = "TEST" if s in TEST else "non-test"
        print(f"\n===== {s} [{tag}] =====")
        us = u[u["store"] == s]
        ts = t[t["store"] == s]
        print("-- unified platform/source --")
        if not us.empty:
            print(us.groupby(["platform", "source"])["total_price"].sum().sort_values(ascending=False).to_string())
        print(f"unified 합계: {us['total_price'].sum():,.0f}")
        print("-- toorder platform --")
        if not ts.empty:
            print(ts.groupby("platform")["price"].sum().sort_values(ascending=False).to_string())
        print(f"toorder 합계: {ts['price'].sum():,.0f}  | 차이(u-t): {us['total_price'].sum()-ts['price'].sum():,.0f}")


if __name__ == "__main__":
    main()
