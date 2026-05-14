# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"

for s in ["store=동두천지행점","store=삼송점","store=송파삼전점","store=평택비전점"]:
    p = base / s / "ym=2026-05" / "okpos_order_item.csv"
    if not p.exists(): continue
    for enc in ("utf-8-sig","utf-8","cp949"):
        try: df = pd.read_csv(p, dtype=str, encoding=enc); break
        except: df = pd.DataFrame()
    print(f"\n[{s}] 총 {len(df)}행")
    if "collected_at" in df.columns:
        print(df.groupby(["sale_date","collected_at"]).size().to_string())
