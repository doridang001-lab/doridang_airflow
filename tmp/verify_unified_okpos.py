"""
unified_sales 재처리 결과 검증
daily 실매출액 vs unified total_price 합계 비교
"""
import sys, os
sys.path.insert(0, r"C:\airflow")
os.chdir(r"C:\airflow")

import pandas as pd
from pathlib import Path

home = Path.home()
RAW = home / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당"

# unified_sales parquet 경로 찾기
from modules.transform.utility.paths import MART_DB
MART = MART_DB

dates = ["2026-04-23", "2026-04-29", "2026-05-04"]
store_name = "도리당 송파삼전점"

for sale_date in dates:
    print(f"\n=== {sale_date} ===")

    # daily 기준값
    store_short = "송파삼전점"
    ym = sale_date[:7]
    daily_csv = RAW / f"store={store_short}" / f"ym={ym}" / "okpos_daily.csv"
    daily_sum = 0
    if daily_csv.exists():
        df = pd.read_csv(daily_csv, dtype=str)
        df = df[df["sale_date"].astype(str).str.strip() == sale_date]
        daily_sum = int(pd.to_numeric(df["실매출액"].astype(str).str.replace(",","",regex=False), errors="coerce").fillna(0).astype(int).sum())
    print(f"  daily 기준:      {daily_sum:>12,}")

    # unified_sales parquet 합계
    ymd6 = sale_date.replace("-","")[2:]  # 260423
    parquet_path = MART / "unified_sales_grp" / f"unified_sales_{ymd6}.parquet"
    if not parquet_path.exists():
        print(f"  unified parquet: MISSING ({parquet_path})")
        continue
    udf = pd.read_parquet(parquet_path)
    udf_store = udf[(udf["sale_date"] == sale_date) & (udf["source"] == "okpos") & (udf["store"] == store_short)]
    unified_sum = int(pd.to_numeric(udf_store["total_price"], errors="coerce").fillna(0).astype(int).sum())
    print(f"  unified total_price: {unified_sum:>10,}")
    print(f"  차이: {daily_sum - unified_sum:,}  {'✅ 일치' if daily_sum == unified_sum else '❌ 불일치'}")
