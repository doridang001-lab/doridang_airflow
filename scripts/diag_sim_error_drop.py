"""수정 후 6월 검증 오차가 얼마나 줄지 시뮬레이션 (read-only, DAG 결과 대기 전).

가정:
- 비테스트 매장: 변경 영향 없음(수동行 없음) → 오차 그대로.
- 테스트 매장: 직전 run이 모든 비수동 배달행을 제거했으므로 현재 배달=수동뿐.
  수정 후엔 '수동 없는 (store,date)'에 posfeed 배달이 복구됨.
  복구액 = 6월 posfeed 재산출 배달액 중 해당 매장이 그 날 수동(배민수동/쿠팡수동)이 없는 분.
"""

import pandas as pd
from modules.transform.utility.paths import MART_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_MANUAL_TEST_STORES, DELIVERY_PLATFORM_FAMILIES,
)
import modules.transform.pipelines.db.DB_UnifiedSales_posfeed as PF
from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import (
    _load_orders_for_ym, _load_items_for_ym, _transform_df,
)

# 시뮬레이션: 제외로그 write 부작용 차단(실행 중 DAG와 충돌 방지)
PF._save_exclusion_log = lambda *a, **k: None

YM = "2026-06"
TEST = set(DELIVERY_MANUAL_TEST_STORES)
ERR_CSV = MART_DB / "unified_sales_grp_error_list" / f"unified_sales_monthly_{YM}.csv"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
BAEMIN_FAM = DELIVERY_PLATFORM_FAMILIES["배민수동"]
COUPANG_FAM = DELIVERY_PLATFORM_FAMILIES["쿠팡수동"]


def manual_presence():
    """(store, sale_date) -> set(['배민수동','쿠팡수동']) 현재 저장본 기준."""
    pres = {}
    for f in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(f, columns=["sale_date", "store", "source"])
        except Exception:
            continue
        df = df[df["source"].isin(["배민수동", "쿠팡수동"])]
        for _, r in df.iterrows():
            pres.setdefault((str(r["store"]).strip(), str(r["sale_date"])[:10]), set()).add(r["source"])
    return pres


def rederive_posfeed_delivery():
    """6월 posfeed 재산출 → 테스트 매장 (store,date,family) 배달액."""
    orders = _load_orders_for_ym(YM)
    items = _load_items_for_ym(YM)
    if orders.empty:
        return pd.DataFrame(columns=["store", "date", "fam", "amt"])
    dates = sorted(orders["등록날짜"].astype(str).str.strip().unique())
    rows = []
    for d in dates:
        if not d.startswith(YM):
            continue
        df = _transform_df(orders, items, d)
        if df.empty:
            continue
        df = df[df["store"].isin(TEST)]
        if df.empty:
            continue
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0)
        for fam_name, fam in (("배민수동", BAEMIN_FAM), ("쿠팡수동", COUPANG_FAM)):
            sub = df[df["platform"].isin(fam)]
            if sub.empty:
                continue
            g = sub.groupby("store")["total_price"].sum()
            for store, amt in g.items():
                rows.append({"store": store, "date": d, "fam": fam_name, "amt": float(amt)})
    return pd.DataFrame(rows)


def main():
    err = pd.read_csv(ERR_CSV, dtype=str)
    err["excel_total"] = pd.to_numeric(err["excel_total"], errors="coerce").fillna(0)
    err["unified_total"] = pd.to_numeric(err["unified_total"], errors="coerce").fillna(0)
    err["error_rate"] = pd.to_numeric(err["error_rate"], errors="coerce").fillna(0)
    err["store"] = err["store"].astype(str).str.strip()

    flagged = err[err["error_rate"] >= 2].copy()
    n_test = flagged["store"].isin(TEST).sum()
    print(f"현재 오차 2%↑: {len(flagged)}건 (테스트 {n_test} / 비테스트 {len(flagged)-n_test})")

    pres = manual_presence()
    pf = rederive_posfeed_delivery()
    # 복구액: 그 (store,date,fam)에 수동이 없을 때만 posfeed 복구
    restored = {}
    for _, r in pf.iterrows():
        key = (r["store"], r["date"])
        if r["fam"] in pres.get(key, set()):
            continue  # 수동 있음 → posfeed 대신 수동 사용(복구 안 함)
        restored[r["store"]] = restored.get(r["store"], 0) + r["amt"]

    print("\n=== 테스트 매장 시뮬레이션 (복구 후) ===")
    new_flagged = 0
    for _, r in err[err["store"].isin(TEST)].iterrows():
        add = restored.get(r["store"], 0)
        new_u = r["unified_total"] + add
        new_diff = new_u - r["excel_total"]
        new_rate = round(abs(new_diff) / r["excel_total"] * 100, 2) if r["excel_total"] else 0
        flag = "■2%↑" if new_rate >= 2 else "  OK"
        if new_rate >= 2:
            new_flagged += 1
        print(f"{flag} {r['store']:12s} 오차 {r['error_rate']:.2f}% → {new_rate:.2f}% "
              f"(복구 +{add:,.0f}, 차이 {r['difference']:>12} → {new_diff:,.0f})")

    nontest_flagged = flagged[~flagged["store"].isin(TEST)]
    print(f"\n=== 요약 ===")
    print(f"비테스트(변경 무영향): {len(nontest_flagged)}건 그대로 남음")
    print(f"테스트: 복구 후 2%↑ {new_flagged}건")
    print(f"예상 총 오차 건수: {len(nontest_flagged) + new_flagged}건 (현재 {len(flagged)}건)")


if __name__ == "__main__":
    main()
