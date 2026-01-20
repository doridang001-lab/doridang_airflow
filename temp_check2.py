import pandas as pd

# 원본 파일 읽기
df = pd.read_csv(r'C:\Users\민준\OneDrive - 주식회사 도리당\테스트용\coupangeats_orders_닭도리탕_전문_도리당_교대점_746421_20260113_6.csv')
print(f'원본 행 수: {len(df)}')
print(f'고유 order_id 수: {df["order_id"].nunique()}')

# 중복 제거 테스트 (현재 dedup_key 기준)
dedup_cols = ['store_name', 'order_id', 'order_date', 'menu_name', 'menu_options', 'menu_qty']
before = len(df)
df_dedup = df.drop_duplicates(subset=dedup_cols, keep='first')
after = len(df_dedup)
print(f'중복 제거 후: {after}행 (제거된 행: {before - after})')

# 주문별 행 수 확인
print(f'\n주문별 행 수 (상위 5개):')
print(df['order_id'].value_counts().head(5))

# 샘플 확인
print(f'\n주문 0EGJKR의 모든 행:')
sample = df[df['order_id'] == '0EGJKR'][['order_id', 'menu_name', 'menu_options', 'menu_qty']]
print(sample.to_string())
