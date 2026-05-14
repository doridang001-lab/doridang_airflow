# -*- coding: utf-8 -*-
"""
okpos_order_item.csv 중복 배치 제거 스크립트
- sale_date별로 가장 최신 collected_at 배치만 유지
- 포스번호/영수증번호 선행 0 정규화
- _pk 재계산: 파이프라인(_transform_okpos_df)과 동일하게 sale_date 그룹별 0-based row_idx 사용
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
import hashlib
import pandas as pd
from pathlib import Path

base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"

DERIVED = {"매장명", "sale_date", "ym", "collected_at", "_pk"}

def make_pk_for_group(group: pd.DataFrame, orig_cols: list) -> pd.Series:
    """파이프라인과 동일: 그룹 내 0-based row_idx를 tie-breaker로 사용"""
    reset = group.reset_index(drop=True)
    content_hash = reset[orig_cols].apply(
        lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
        axis=1,
    )
    row_idx = reset.index.astype(str)
    combined = content_hash.astype(str) + "|" + row_idx
    pks = combined.map(lambda s: hashlib.md5(s.encode()).hexdigest())
    pks.index = group.index
    return pks

total_before = total_after = 0

for item_csv in sorted(base.glob("store=*/ym=*/okpos_order_item.csv")):
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            df = pd.read_csv(item_csv, dtype=str, encoding=enc)
            break
        except Exception:
            df = pd.DataFrame()
    if df.empty:
        print(f"[SKIP] {item_csv.relative_to(base)}")
        continue

    before = len(df)

    # 포스번호/영수증번호 선행 0 제거
    for col in ("포스번호", "영수증번호"):
        if col in df.columns:
            normed = df[col].astype(str).str.lstrip("0")
            df[col] = normed.where(normed != "", "0")

    # sale_date별 최신 collected_at 배치만 유지
    if "sale_date" in df.columns and "collected_at" in df.columns:
        latest_ca = df.groupby("sale_date")["collected_at"].max()
        df = df[df.apply(lambda r: r["collected_at"] == latest_ca.get(r["sale_date"], r["collected_at"]), axis=1)].copy()

    # 원본 컬럼 목록 (파이프라인의 n_orig 컬럼 = DERIVED 제외)
    orig_cols = [c for c in df.columns if c not in DERIVED]

    # sale_date 그룹별로 파이프라인과 동일한 방식으로 정렬 + _pk 재계산
    sort_keys = [c for c in ("영수증번호", "상품코드", "총매출액") if c in df.columns]
    pk_parts = []
    for date, grp in df.groupby("sale_date", sort=True):
        if sort_keys:
            grp = grp.sort_values(sort_keys, kind="stable")
        pk_parts.append(make_pk_for_group(grp, orig_cols))

    if pk_parts:
        df["_pk"] = pd.concat(pk_parts)
    else:
        df["_pk"] = ""

    after = len(df)
    total_before += before
    total_after += after

    df.to_csv(item_csv, index=False, encoding="utf-8-sig")

    net_col = next((c for c in df.columns if "실매출" in c), None)
    by_date = {}
    if net_col and "sale_date" in df.columns:
        for dt, grp in df.groupby("sale_date"):
            by_date[dt] = int(pd.to_numeric(grp[net_col].str.replace(",", ""), errors="coerce").fillna(0).sum())
    date_summary = ", ".join(f"{d}={v:,}" for d, v in sorted(by_date.items()))
    removed = before - after
    flag = " [중복제거]" if removed > 0 else ""
    print(f"{item_csv.relative_to(base)}: {before}->{after}행{flag} | {date_summary}")

print(f"\n전체: {total_before}행 -> {total_after}행 (제거 {total_before - total_after}건)")
