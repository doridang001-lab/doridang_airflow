import pandas as pd

df = pd.read_csv(r'C:\Local_DB\영업관리부_DB\sales_daily_orders.csv', encoding='utf-8-sig')
print('전체 행:', len(df))
print('고유 sub_order_id:', df['sub_order_id'].nunique())

# 중복 확인
dup_counts = df['sub_order_id'].value_counts()
dups = dup_counts[dup_counts > 1]
print(f'\n중복된 sub_order_id 수: {len(dups)}')
if len(dups) > 0:
    print(dups.head(10))

# 교대점 데이터 확인
g = df[df['store_name'].str.contains('교대점', na=False)]
print(f'\n교대점 행: {len(g)}')
print(f'교대점 고유 sub_order_id: {g["sub_order_id"].nunique()}')

# sub_order_id 샘플 확인
print('\n샘플 sub_order_id:')
print(df['sub_order_id'].head(5).tolist())
