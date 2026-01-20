import pandas as pd

df = pd.read_csv('/opt/airflow/Doridang_DB/영업관리부_DB/sales_daily_orders.csv', encoding='utf-8-sig')
print(f'총 행수: {len(df)}')
print(f'최대 날짜: {df["order_date"].max()}')
print(f'최소 날짜: {df["order_date"].min()}')
dates = sorted(df['order_date'].unique())
print(f'마지막 10개 날짜:')
for d in dates[-10:]:
    print(f'  {d}')
