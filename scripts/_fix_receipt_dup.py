# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import pandas as pd
import hashlib
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"
stores = ["store=동두천지행점","store=삼송점","store=송파삼전점","store=평택비전점"]
ym = "ym=2026-05"

DEDUP_COLS = ["포스번호_norm","영수증번호","상품코드","바코드","상품명","수량",
              "총매출액","할인액","실매출액","매장명","sale_date","결제시각"]

for s in stores:
    p = base / s / ym / "okpos_order_item.csv"
    if not p.exists():
        print(f"[{s}] 없음"); continue
    for enc in ("utf-8-sig","utf-8","cp949"):
        try: df = pd.read_csv(p, dtype=str, encoding=enc); break
        except: df = pd.DataFrame()
    if df.empty:
        print(f"[{s}] 읽기실패"); continue

    before = len(df)
    # 포스번호 정규화 (앞 0 제거)
    if "포스번호" in df.columns:
        df["포스번호_norm"] = df["포스번호"].astype(str).str.lstrip("0").str.strip()
    else:
        df["포스번호_norm"] = ""

    # collected_at 내림차순 → 최신 우선 유지 후 dedup
    avail_cols = [c for c in DEDUP_COLS if c in df.columns]
    df = df.sort_values("collected_at", ascending=False)
    df = df.drop_duplicates(subset=avail_cols, keep="first")
    df = df.drop(columns=["포스번호_norm"])

    # _pk 재계산
    pk_cols = ["포스번호","영수증번호","상품코드","sale_date","매장명","결제시각"]
    avail_pk = [c for c in pk_cols if c in df.columns]
    def make_pk(row):
        key = "|".join(str(row.get(c,"")).strip() for c in avail_pk)
        return hashlib.md5(key.encode()).hexdigest()
    df["_pk"] = df.apply(make_pk, axis=1)

    df.to_csv(p, index=False, encoding="utf-8-sig")
    after = len(df)

    # 실매출액 재확인
    col = next((c for c in df.columns if "실매출" in c), None)
    total = int(pd.to_numeric(df[col].str.replace(",",""), errors="coerce").fillna(0).sum()) if col else 0
    print(f"[{s}] {before}행 -> {after}행 (제거 {before-after}건) | 실매출합={total:,}")
