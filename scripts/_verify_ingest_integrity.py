"""xlsx vs CSV 데이터 일관성 검증 및 인제스트 버그 추적"""
import sys
sys.path.insert(0, r"C:\airflow")

import pandas as pd
from pathlib import Path
from modules.transform.utility.paths import RAW_OKPOS_SALES
from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    _read_okpos_excel, _store_open_date, STORES
)

XLSX_PATH = r"E:\down\일자별 종합매출_수동.xlsx"

# 1. xlsx 파싱
raw = _read_okpos_excel(XLSX_PATH)
# Fix 1 적용
if "합매출" in raw.columns and "일자" not in raw.columns:
    unnamed_cols = [c for c in raw.columns if str(c).startswith("Unnamed:")]
    rename_map = {}
    if len(unnamed_cols) >= 1: rename_map[unnamed_cols[0]] = "일자"
    if len(unnamed_cols) >= 2: rename_map[unnamed_cols[1]] = "영업일"
    if len(unnamed_cols) >= 3: rename_map[unnamed_cols[2]] = "영업매장"
    if rename_map:
        raw = raw.rename(columns=rename_map)

dt = pd.to_datetime(raw["일자"].astype(str).str.strip(), errors="coerce")
xlsx_df = raw[dt.notna()].copy()
xlsx_df["sale_date"] = dt[dt.notna()].dt.strftime("%Y-%m-%d")
xlsx_df["영업매장"] = xlsx_df["영업매장"].astype(str).str.strip()
known = {s["name"] for s in STORES}
xlsx_df = xlsx_df[xlsx_df["영업매장"].isin(known)].copy()

print(f"=== xlsx 데이터: {len(xlsx_df)}행 ===")
for store_name in sorted(xlsx_df["영업매장"].unique()):
    sub = xlsx_df[xlsx_df["영업매장"] == store_name]
    dates = sorted(sub["sale_date"].tolist())
    print(f"  {store_name}: {len(dates)}일  {dates[0]} ~ {dates[-1]}")

print()
print("=== CSV vs xlsx 일치 검증 ===")

base = RAW_OKPOS_SALES / "brand=도리당"
for store_info in STORES:
    store_name = store_info["name"]
    store_short = store_name.replace("도리당 ", "", 1)
    open_date = _store_open_date(store_name)

    xlsx_store = xlsx_df[xlsx_df["영업매장"] == store_name]
    xlsx_dates = set(xlsx_store["sale_date"].tolist())

    store_dir = base / f"store={store_short}"
    csv_dates = set()
    if store_dir.exists():
        for ym_dir in store_dir.iterdir():
            csv_path = ym_dir / "okpos_daily.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
                if "sale_date" in df.columns:
                    csv_dates.update(df["sale_date"].dropna().str.strip().tolist())

    # 오픈일 이후만 비교
    if open_date:
        xlsx_valid = {d for d in xlsx_dates if d >= str(open_date)}
        csv_valid = {d for d in csv_dates if d >= str(open_date)}
    else:
        xlsx_valid = xlsx_dates
        csv_valid = csv_dates

    missing_in_csv = xlsx_valid - csv_valid
    extra_in_csv = csv_valid - xlsx_valid

    print(f"\n  [{store_short}] open={open_date}")
    print(f"    xlsx(valid): {len(xlsx_valid)}일  csv(valid): {len(csv_valid)}일")
    if missing_in_csv:
        print(f"    xlsx에는 있지만 CSV에 없음: {sorted(missing_in_csv)}")
    if extra_in_csv:
        print(f"    CSV에는 있지만 xlsx에 없음: {sorted(extra_in_csv)}")
    if not missing_in_csv and not extra_in_csv:
        print(f"    OK - 일치")

print()
print("=== 매출액 샘플 검증 (송파삼전점 Apr) ===")
xlsx_s = xlsx_df[(xlsx_df["영업매장"] == "도리당 송파삼전점") &
                  (xlsx_df["sale_date"] >= "2026-04-17") &
                  (xlsx_df["sale_date"] <= "2026-04-30")]

def _num(v):
    return int(pd.to_numeric(str(v).replace(",", "").strip(), errors="coerce") or 0)

print(f"xlsx 송파삼전점 Apr 17-30:")
for _, r in xlsx_s.sort_values("sale_date").iterrows():
    total = _num(r.get("총매출", r.get("합매출", 0)))
    print(f"  {r['sale_date']}: {total:,}")

csv_path = base / "store=송파삼전점" / "ym=2026-04" / "okpos_daily.csv"
if csv_path.exists():
    csv_df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    print(f"\nCSV 송파삼전점 Apr (open_date 이후):")
    valid = csv_df[csv_df["sale_date"].astype(str) >= "2026-04-17"].sort_values("sale_date")
    for _, r in valid.iterrows():
        print(f"  {r['sale_date']}: {int(r.get('총매출액', 0)):,}")
