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
        print(f"[{s}] okpos_order_item.csv 없음, 스킵")
        continue
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:
            df = pd.read_csv(p, dtype=str, encoding=enc)
            break
        except:
            df = pd.DataFrame()
    if df.empty:
        print(f"[{s}] 읽기 실패")
        continue
    before = len(df)
    if "_pk" in df.columns:
        df = df.drop_duplicates(subset=["_pk"], keep="first")
    else:
        df = df.drop_duplicates(keep="first")
    after = len(df)
    df.to_csv(p, index=False, encoding="utf-8-sig")
    print(f"[{s}] {before}행 -> {after}행 (중복 {before-after}건 제거)")
