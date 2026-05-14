# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"
stores = ["store=동두천지행점","store=삼송점","store=송파삼전점","store=평택비전점"]
ym = "ym=2026-05"

for s in stores:
    p = base / s / ym / "okpos_order_item.csv"
    if not p.exists():
        print(f"[{s}] 없음"); continue
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:
            df = pd.read_csv(p, dtype=str, encoding=enc); break
        except: df = pd.DataFrame()
    if df.empty or "sale_date" not in df.columns:
        print(f"[{s}] 읽기실패/컬럼없음"); continue
    
    col = next((c for c in df.columns if "실매출" in c), None)
    grp = df.groupby("sale_date").agg(
        행수=("sale_date","count"),
        실매출합=(col, lambda x: pd.to_numeric(x.str.replace(",",""), errors="coerce").fillna(0).sum()) if col else ("sale_date","count")
    ).reset_index()
    print(f"\n[{s}]")
    for _, r in grp.iterrows():
        print(f"  {r['sale_date']}: {int(r['행수'])}행, 실매출합={int(r['실매출합']):,}")
