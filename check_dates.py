import pandas as pd

csv_path = '/opt/airflow/Doridang_DB/Doridang_DB/sales_daily_orders.csv'
df = pd.read_csv(csv_path, encoding='utf-8-sig')
print('총 행수:', len(df))
print('최대 날짜:', df['order_date'].max())
print('최소 날짜:', df['order_date'].min())
dates = sorted(df['order_date'].unique())
print('마지막 15개 날짜:')
for d in dates[-15:]:
    print('  ', d)
