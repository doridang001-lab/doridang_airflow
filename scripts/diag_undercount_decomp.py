"""비테스트 언더카운트 원인 분해: posfeed(net) vs gross vs 토더, 플랫폼별 (read-only)."""

import glob
import pandas as pd
from modules.transform.utility.paths import ANALYTICS_DB, MART_DB

YM = "2026-06"
STORES = ["노원점", "화성봉담점", "역삼점", "하늘도시점", "김해구산점"]
TOORDER = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"

# 토더 platform → 큰 그룹
GROUP = {
    "배민1": "배민", "배달의민족": "배민", "배민 포장": "배민", "배민 사장": "배민",
    "쿠팡이츠": "쿠팡", "쿠팡 포장": "쿠팡",
    "요기요": "요기요", "요기요 포장": "요기요", "요기배달": "요기요",
    "땡겨요": "땡겨요", "땡겨요 포장": "땡겨요",
    "배달특급": "배달특급",
    "네이버": "네이버", "네이버주문": "네이버",
    "홀": "홀", "홀 포장": "홀", "홀 배달": "홀",
}
PF_PLAT = {  # posfeed 주문경로 → 그룹
    "배민1": "배민", "배달의민족": "배민", "쿠팡이츠": "쿠팡", "요기요": "요기요",
    "땡겨요": "땡겨요", "배달특급": "배달특급", "네이버": "네이버", "기타": "기타",
}


def load_raw_posfeed(store):
    fs = glob.glob(str(ANALYTICS_DB / "posfeed_sales" / "brand=*" / f"store=*{store[:-1]}*" / f"ym={YM}" / "posfeed_orders.csv"))
    parts = []
    for f in fs:
        d = pd.read_csv(f, dtype=str, encoding="utf-8-sig").fillna("")
        parts.append(d)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def main():
    t = pd.read_parquet(TOORDER, columns=["date", "store", "platform", "price"])
    t["ym"] = pd.to_datetime(t["date"], errors="coerce").dt.strftime("%Y-%m")
    t = t[t["ym"] == YM].copy()
    t["store"] = t["store"].astype(str).str.strip()
    t["g"] = t["platform"].astype(str).str.strip().map(GROUP).fillna("기타")
    t["price"] = pd.to_numeric(t["price"], errors="coerce").fillna(0)

    for s in STORES:
        raw = load_raw_posfeed(s)
        print(f"\n===== {s} =====")
        if raw.empty:
            print("  raw posfeed 없음"); continue
        for c in ["총 주문금액", "배달비", "할인", "결제금액"]:
            if c in raw.columns:
                raw[c] = pd.to_numeric(raw[c].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
        raw["g"] = raw["주문경로"].astype(str).str.strip().map(PF_PLAT).fillna("기타")
        # 배달완료만
        raw = raw[raw["주문상태"].astype(str).str.strip() == "배달완료"]
        gross = raw.groupby("g")["결제금액"].sum()
        fee = raw.groupby("g")["배달비"].sum()
        net = gross - fee
        ts = t[t["store"] == s].groupby("g")["price"].sum()
        comp = pd.DataFrame({"posfeed_gross": gross, "배달비합": fee, "posfeed_net": net, "toorder": ts}).fillna(0)
        comp["net-toorder"] = comp["posfeed_net"] - comp["toorder"]
        comp["gross-toorder"] = comp["posfeed_gross"] - comp["toorder"]
        print(comp.round(0).astype("int64", errors="ignore").to_string())
        print(f"  합계: net {net.sum():,.0f} | gross {gross.sum():,.0f} | toorder {ts.sum():,.0f} | 배달비총 {fee.sum():,.0f}")


if __name__ == "__main__":
    main()
