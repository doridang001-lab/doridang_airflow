"""동탄영천점 +5.5M 과다집계 정밀 분석 + 비테스트 음수 패턴 (read-only)."""

import pandas as pd
from modules.transform.utility.paths import MART_DB, ANALYTICS_DB

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
TOORDER = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
YM = "2026-06"
STORES = ["동탄영천점", "노원점", "화성봉담점"]


def load_unified():
    parts = []
    for f in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            parts.append(pd.read_parquet(f, columns=["sale_date", "store", "platform", "source", "order_type", "total_price", "order_cnt"]))
        except Exception:
            pass
    u = pd.concat(parts, ignore_index=True)
    u["ym"] = pd.to_datetime(u["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    u = u[u["ym"] == YM].copy()
    u["store"] = u["store"].astype(str).str.strip()
    u["total_price"] = pd.to_numeric(u["total_price"], errors="coerce").fillna(0)
    return u


def main():
    u = load_unified()
    t = pd.read_parquet(TOORDER, columns=["date", "store", "platform", "price"])
    t["ym"] = pd.to_datetime(t["date"], errors="coerce").dt.strftime("%Y-%m")
    t = t[t["ym"] == YM].copy()
    t["store"] = t["store"].astype(str).str.strip()
    t["price"] = pd.to_numeric(t["price"], errors="coerce").fillna(0)

    for s in STORES:
        us = u[u["store"] == s]
        ts = t[t["store"] == s]
        print(f"\n===== {s} =====  unified {us['total_price'].sum():,.0f} vs toorder {ts['price'].sum():,.0f} | 차이 {us['total_price'].sum()-ts['price'].sum():,.0f}")
        print("-- unified platform/source/order_type --")
        print(us.groupby(["platform", "source", "order_type"])["total_price"].agg(["sum", "count"]).sort_values("sum", ascending=False).head(15).to_string())
        print("-- toorder platform --")
        print(ts.groupby("platform")["price"].sum().sort_values(ascending=False).to_string())

    # 동탄영천점 홀 일자별 (이중집계 의심)
    print("\n===== 동탄영천점 홀(unionpos) 일자별 상위 =====")
    hall = u[(u["store"] == "동탄영천점") & (u["platform"] == "홀")]
    by_day = hall.groupby("sale_date")["total_price"].agg(["sum", "count"]).sort_values("sum", ascending=False)
    print(by_day.head(12).to_string())
    print(f"홀 합계: {hall['total_price'].sum():,.0f}, 행수: {len(hall)}")
    # 동탄영천점 unionpos 영수(order_id) 중복 의심: 같은 _pk 여러건?
    th = u[(u["store"] == "동탄영천점")]
    print("\n동탄영천점 source별 합계:")
    print(th.groupby("source")["total_price"].sum().to_string())


if __name__ == "__main__":
    main()
