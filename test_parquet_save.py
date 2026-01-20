import pandas as pd
from pathlib import Path
from modules.transform.utility.paths import ONEDRIVE_DB, LOCAL_DB

# 샘플 데이터로 테스트
local_csv_path = LOCAL_DB / "영업관리부_DB" / "sales_daily_orders.csv"
print(f"CSV 샘플 읽는 중...")

df = pd.read_csv(local_csv_path, encoding='utf-8-sig', nrows=1000)
print(f"데이터 로드 완료: {len(df):,}행")

# OneDrive Parquet으로 저장 시도
onedrive_parquet_path = ONEDRIVE_DB / "영업관리부_DB" / "test_sales.parquet"
onedrive_parquet_path.parent.mkdir(parents=True, exist_ok=True)

print(f"\nParquet 테스트 저장: {onedrive_parquet_path}")
try:
    df.to_parquet(
        onedrive_parquet_path,
        index=False,
        engine='pyarrow',
        compression='snappy'
    )
    print("✅ Parquet 저장 성공!")
    print(f"파일 생성됨: {onedrive_parquet_path.exists()}")
    if onedrive_parquet_path.exists():
        size = onedrive_parquet_path.stat().st_size / 1024
        print(f"파일 크기: {size:.2f} KB")
    
except Exception as e:
    print(f"❌ Parquet 저장 실패: {e}")
    import traceback
    traceback.print_exc()
