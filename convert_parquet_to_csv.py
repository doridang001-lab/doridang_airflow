"""
Parquet 파일을 CSV로 변환 (Looker Studio 호환)
"""
import pandas as pd
from pathlib import Path
from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB

# Parquet 경로 (OneDrive 또는 임시 저장 위치)
parquet_path = ONEDRIVE_DB / 'sales_daily_orders.parquet'  # 경로 수정 필요
csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts.csv'

print(f"[읽기] {parquet_path}")
df = pd.read_parquet(parquet_path)
print(f"[읽기] 완료: {len(df):,}건")

# 날짜 변환
df['order_daily'] = pd.to_datetime(df['order_daily']).dt.strftime('%Y-%m-%d')

# 모든 컬럼을 문자열로 변환
for col in df.columns:
    if col == 'order_daily':
        continue
    if hasattr(df[col].dtype, 'numpy_dtype'):
        df[col] = df[col].astype('object')
    df[col] = df[col].astype(str)

# NaN 문자열 제거
df = df.replace(['nan', '<NA>', 'NaT', 'None'], '')

# CSV 저장
csv_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(csv_path, index=False, encoding='utf-8-sig')
print(f"[저장] {csv_path}")
print(f"[저장] 완료: {len(df):,}건")
