"""OKPOS CSV 현황 진단"""
import sys
sys.path.insert(0, r"C:\airflow")
import pandas as pd
from modules.transform.utility.paths import RAW_OKPOS_SALES

base = RAW_OKPOS_SALES / "brand=도리당"
print("RAW_OKPOS_SALES:", RAW_OKPOS_SALES)
print()

stores = ["동두천지행점", "삼송점", "평택비전점", "송파삼전점"]
for store in stores:
    store_dir = base / f"store={store}"
    if not store_dir.exists():
        print(f"{store}: 폴더 없음")
        continue
    for ym_dir in sorted(store_dir.iterdir()):
        csv_path = ym_dir / "okpos_daily.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(csv_path, dtype=str)
        if "sale_date" not in df.columns:
            print(f"  {store}/{ym_dir.name}: 'sale_date' 컬럼 없음, cols={list(df.columns)[:5]}")
            continue
        dates = sorted(df["sale_date"].dropna().tolist())
        first = dates[0] if dates else "?"
        last = dates[-1] if dates else "?"
        print(f"  {store}/{ym_dir.name}: {len(df)}행  {first} ~ {last}")

print()
print("=== 송파삼전점 2026-04 상세 ===")
csv_path = base / "store=송파삼전점" / "ym=2026-04" / "okpos_daily.csv"
if csv_path.exists():
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    print(df[["sale_date", "매장명", "총매출액"]].to_string(index=False))
else:
    print("파일 없음")
