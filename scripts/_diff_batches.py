# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"
p = base / "store=동두천지행점" / "ym=2026-05" / "okpos_order_item.csv"

for enc in ("utf-8-sig","utf-8","cp949"):
    try: df = pd.read_csv(p, dtype=str, encoding=enc); break
    except: df = pd.DataFrame()

b1 = df[df["collected_at"] == "2026-05-04 02:10:57"].reset_index(drop=True)
b2 = df[df["collected_at"] == "2026-05-04 04:51:35"].reset_index(drop=True)
print(f"배치1: {len(b1)}행, 배치2: {len(b2)}행")

# 같은 인덱스 행끼리 다른 컬럼 찾기
if len(b1) == len(b2):
    for col in df.columns:
        if col in ("collected_at","_pk"): continue
        diff = (b1[col].astype(str) != b2[col].astype(str)).sum()
        if diff > 0:
            print(f"  다른컬럼: {col} ({diff}건)")
            print(f"    배치1 샘플: {b1[col].iloc[:3].tolist()}")
            print(f"    배치2 샘플: {b2[col].iloc[:3].tolist()}")
else:
    print("행수 불일치")
    # 영수증번호 집합 비교
    print("배치1 영수증번호 샘플:", b1["영수증번호"].iloc[:5].tolist())
    print("배치2 영수증번호 샘플:", b2["영수증번호"].iloc[:5].tolist())
