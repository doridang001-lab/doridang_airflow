# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"
ym = "ym=2026-05"
stores = ["store=동두천지행점","store=삼송점","store=송파삼전점","store=평택비전점"]

def read_csv(p):
    for enc in ("utf-8-sig","utf-8","cp949"):
        try: return pd.read_csv(p, dtype=str, encoding=enc)
        except: pass
    return pd.DataFrame()

def net_amt(df, date, col):
    if df.empty or "sale_date" not in df.columns or col not in df.columns: return None
    rows = df[df["sale_date"].str.strip() == date]
    if rows.empty: return None
    vals = pd.to_numeric(rows[col].astype(str).str.replace(",",""), errors="coerce").fillna(0)
    return int(vals.sum())

for s in stores:
    d = base / s / ym
    if not d.exists(): print(f"[{s}] 없음"); continue
    daily = read_csv(d / "okpos_daily.csv")
    order = read_csv(d / "okpos_order.csv")
    item  = read_csv(d / "okpos_order_item.csv")
    o_col = next((c for c in order.columns if "실매출" in c), None)
    i_col = next((c for c in item.columns  if "실매출" in c), None)
    print(f"\n=== {s} ===")
    if daily.empty or "실매출액" not in daily.columns:
        print("  daily 없음"); continue
    for _, r in daily.sort_values("sale_date").iterrows():
        dt = str(r["sale_date"]).strip()
        d_amt = int(pd.to_numeric(str(r["실매출액"]).replace(",",""), errors="coerce") or 0)
        o_amt = net_amt(order, dt, o_col) if o_col else None
        i_amt = net_amt(item,  dt, i_col) if i_col else None
        o_str = f"{o_amt:,}" if o_amt is not None else "MISSING"
        i_str = f"{i_amt:,}" if i_amt is not None else "MISSING"
        ok = "OK" if o_amt == d_amt and i_amt == d_amt else ("NG" if (o_amt is not None and i_amt is not None) else "MISSING")
        print(f"  {dt} | daily={d_amt:,} | today={o_str} | receipt={i_str} | {ok}")
