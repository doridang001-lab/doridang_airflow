"""송파삼전점 Apr 14 pre-open row 정리"""
import sys
sys.path.insert(0, r"C:\airflow")
import pandas as pd
from modules.transform.utility.paths import RAW_OKPOS_SALES
from modules.transform.pipelines.db.DB_OKPOS_Sales import _store_open_date

csv_path = RAW_OKPOS_SALES / "brand=도리당" / "store=송파삼전점" / "ym=2026-04" / "okpos_daily.csv"
open_date = _store_open_date("도리당 송파삼전점")
print(f"open_date: {open_date}")

df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
before = len(df)
df_clean = df[df["sale_date"].astype(str) >= str(open_date)].copy()
after = len(df_clean)
print(f"before: {before}행, after: {after}행 (제거: {before-after}행)")
print("제거 대상:", df[df["sale_date"].astype(str) < str(open_date)][["sale_date","총매출액"]].to_string(index=False))

df_clean.to_csv(csv_path, index=False, encoding="utf-8-sig")
print(f"저장 완료: {csv_path}")
