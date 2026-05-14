import pandas as pd
from pathlib import Path

home = Path.home()
base = home / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당" / "store=송파삼전점"

dates = ["2026-05-01"]

def signed_sum(df, col, sale_date):
    df2 = df[df["sale_date"].astype(str).str.strip() == sale_date]
    if df2.empty:
        return None, 0, {}
    amt = pd.to_numeric(df2[col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int)
    gbn = df2["구분"].fillna("").astype(str).str.strip().value_counts().to_dict() if "구분" in df2.columns else {}
    neg = int((amt < 0).sum())
    return len(df2), int(amt.sum()), gbn, neg

for sale_date in dates:
    ym = sale_date[:7]
    d = base / f"ym={ym}"
    print(f"\n=== {sale_date} ===")
    for fname, col in [("okpos_daily.csv", "실매출액"), ("okpos_order.csv", "실매출액"), ("okpos_order_item.csv", "실매출액")]:
        csv = d / fname
        nd_txt = d / f".no_data__{fname.replace('.csv', '')}.txt"
        nd = set(nd_txt.read_text(encoding="utf-8").splitlines()) if nd_txt.exists() else set()
        if sale_date in nd:
            print(f"  {fname}: NO_DATA")
            continue
        if not csv.exists():
            print(f"  {fname}: MISSING")
            continue
        df = pd.read_csv(csv, dtype=str)
        rows, total, gbn, neg = signed_sum(df, col, sale_date)
        if rows is None:
            print(f"  {fname}: EMPTY")
        else:
            print(f"  {fname}: {total:,}  (행수={rows}, 구분={gbn}, 음수행={neg})")
