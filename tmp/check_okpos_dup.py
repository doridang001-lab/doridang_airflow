import pandas as pd
from pathlib import Path

home = Path.home()
base = home / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당" / "store=송파삼전점"

dates = ["2026-04-23", "2026-04-29", "2026-05-04"]

for sale_date in dates:
    ym = sale_date[:7]
    d = base / f"ym={ym}"
    print(f"\n=== {sale_date} ===")
    for fname in ["okpos_order.csv", "okpos_order_item.csv"]:
        csv = d / fname
        if not csv.exists():
            print(f"  {fname}: MISSING")
            continue
        df = pd.read_csv(csv, dtype=str)
        df2 = df[df["sale_date"].astype(str).str.strip() == sale_date]
        total_rows = len(df2)
        dup_rows = df2.duplicated(subset=["_pk"]).sum() if "_pk" in df2.columns else "no _pk col"
        collected_vals = df2["collected_at"].value_counts().head(5).to_dict() if "collected_at" in df2.columns else {}
        print(f"  {fname}: 행수={total_rows}, 중복_pk={dup_rows}")
        print(f"    collected_at 분포: {collected_vals}")
