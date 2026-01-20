import pandas as pd
from pathlib import Path
from modules.transform.utility.paths import ONEDRIVE_DB, LOCAL_DB

# 전체 CSV 읽기
local_csv_path = LOCAL_DB / "영업관리부_DB" / "sales_daily_orders.csv"
print(f"CSV 읽는 중...")

df = pd.read_csv(local_csv_path, encoding='utf-8-sig')
print(f"데이터 로드 완료: {len(df):,}행")

# Parquet 저장용 DataFrame 복사 및 타입 변환
df_for_parquet = df.copy()

# datetime 컬럼 변환 (Timestamp 객체 처리)
print("\n데이터 타입 변환 중...")
for col in df_for_parquet.columns:
    if df_for_parquet[col].dtype == 'object':
        # datetime으로 변환 시도
        try:
            converted = pd.to_datetime(df_for_parquet[col], errors='ignore')
            if converted.dtype != 'object':
                df_for_parquet[col] = converted
                print(f"  - {col}: datetime으로 변환")
        except:
            pass

# OneDrive Parquet으로 저장
onedrive_parquet_path = ONEDRIVE_DB / "영업관리부_DB" / "sales_daily_orders.parquet"
onedrive_parquet_path.parent.mkdir(parents=True, exist_ok=True)

print(f"\nParquet 저장 중: {onedrive_parquet_path}")
try:
    df_for_parquet.to_parquet(
        onedrive_parquet_path,
        index=False,
        engine='pyarrow',
        compression='snappy'
    )
    print("✅ Parquet 저장 성공!")
    
    # 파일 크기 확인
    csv_size = local_csv_path.stat().st_size / (1024 * 1024)
    parquet_size = onedrive_parquet_path.stat().st_size / (1024 * 1024)
    compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
    print(f"CSV: {csv_size:.2f} MB → Parquet: {parquet_size:.2f} MB ({compression_ratio:.1f}% 압축)")
    
except Exception as e:
    print(f"❌ Parquet 저장 실패: {e}")
    import traceback
    traceback.print_exc()
