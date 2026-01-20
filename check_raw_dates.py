import pandas as pd
from pathlib import Path

temp_dir = Path('/opt/airflow/Local_DB/temp')
parquet_files = list(temp_dir.glob('orders_with_stores_processed_*.parquet'))
if parquet_files:
    latest_parquet = sorted(parquet_files)[-1]
    print(f'파일: {latest_parquet.name}')
    df = pd.read_parquet(latest_parquet)
    print(f'행수: {len(df)}')
    if 'order_date' in df.columns:
        print(f'최소 날짜: {df["order_date"].min()}')
        print(f'최대 날짜: {df["order_date"].max()}')
        dates = sorted(df['order_date'].unique())
        print(f'고유 날짜 개수: {len(dates)}')
        print(f'마지막 15개 날짜:')
        for d in dates[-15:]:
            print(f'  {d}')
else:
    print('Parquet 파일 없음')
