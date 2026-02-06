import pandas as pd

master_path = r'c:\Local_DB\영업관리부_DB\visit_sales_log_master.csv'
df = pd.read_csv(master_path, nrows=1)
print("마스터 파일 컬럼:")
print(list(df.columns))
print("\n마스터 데이터 샘플:")
print(df)
