"""
구글 시트 데이터 디버깅 스크립트
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from modules.transform.utility.paths import LOCAL_DB

csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts.csv'

print(f"[파일] {csv_path}")
print(f"[존재] {csv_path.exists()}")

if csv_path.exists():
    # 원본 읽기 (타입 그대로)
    df_raw = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"\n[원본 데이터]")
    print(f"행: {len(df_raw)}, 열: {len(df_raw.columns)}")
    print(f"\n[컬럼 타입]")
    print(df_raw.dtypes)
    
    print(f"\n[NaN 개수]")
    print(df_raw.isna().sum())
    
    print(f"\n[첫 3행]")
    print(df_raw.head(3))
    
    print(f"\n[order_daily 샘플]")
    if 'order_daily' in df_raw.columns:
        print(df_raw['order_daily'].head(10).tolist())
        print(f"타입: {df_raw['order_daily'].dtype}")
        print(f"고유값 개수: {df_raw['order_daily'].nunique()}")
    
    # 문자열로 읽기
    df_str = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str, keep_default_na=False)
    print(f"\n[문자열 변환 후]")
    print(f"행: {len(df_str)}, 열: {len(df_str.columns)}")
    print(f"\n[빈 문자열 개수]")
    print((df_str == '').sum())
    
    print(f"\n[특수문자 확인]")
    for col in df_str.columns[:5]:  # 첫 5개 컬럼만
        unique_vals = df_str[col].unique()[:3]
        print(f"{col}: {unique_vals}")
    
    print(f"\n[컬럼명 확인]")
    print(df_str.columns.tolist())
    print(f"\n[컬럼명에 특수문자 있는지 확인]")
    for col in df_str.columns:
        if any(ord(c) > 127 for c in col if not c.isalnum() and c not in ' _-'):
            print(f"⚠️ {col}: {[c for c in col]}")
