"""원본 파일 중복 확인 스크립트"""
import pandas as pd
import glob

# 파일 읽기
files = glob.glob("/opt/airflow/Collect_Data/영업팀_수집/baemin_orders*.csv")
print(f"파일 개수: {len(files)}")

for file in files:
    print(f"\n{'='*80}")
    print(f"파일: {file}")
    print('='*80)
    
    df = pd.read_csv(file, encoding='utf-8')
    print(f"전체 행: {len(df)}")
    
    # 중복 확인
    dedup_cols = ['주문번호', '주문내역', 'store_name', '주문옵션상세']
    duplicates = df[df.duplicated(subset=dedup_cols, keep=False)]
    
    print(f"중복 행: {len(duplicates)}")
    
    if len(duplicates) > 0:
        print("\n중복된 행:")
        print(duplicates[dedup_cols].sort_values(by='주문번호'))
