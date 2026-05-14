# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"

# 동두천 05-03 receipt 구조 확인
p = base / "store=동두천지행점" / "ym=2026-05" / "okpos_order_item.csv"
for enc in ("utf-8-sig","utf-8","cp949"):
    try: df = pd.read_csv(p, dtype=str, encoding=enc); break
    except: df = pd.DataFrame()

print("컬럼:", list(df.columns))
print(f"총행수: {len(df)}")
print("\n--- 상위 4행 ---")
print(df.head(4).to_string())
print("\n--- 하위 4행 ---")
print(df.tail(4).to_string())

# collected_at 분포 확인
if "collected_at" in df.columns:
    print("\ncollected_at 분포:")
    print(df["collected_at"].value_counts().head(10))
