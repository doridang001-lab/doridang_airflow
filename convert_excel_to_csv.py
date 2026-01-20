# 엑셀 데이터를 CSV로 변환하는 스크립트
import pandas as pd
from pathlib import Path

# 엑셀 파일 경로 (실제 파일 경로로 수정하세요)
excel_path = r"C:\Users\민준\Downloads\주문데이터_2026-01-19.xlsx"

# 저장 경로
output_dir = Path(r"C:\Users\민준\OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집")

# 엑셀 읽기
df = pd.read_excel(excel_path)

# 플랫폼 구분 (데이터에 platform 컬럼이 있다고 가정)
# 배민 데이터 필터링 (실제 컬럼명에 맞게 수정하세요)
if 'platform' in df.columns or '플랫폼' in df.columns:
    platform_col = 'platform' if 'platform' in df.columns else '플랫폼'
    
    # 배민 데이터
    baemin_df = df[df[platform_col].str.contains('배민|baemin', case=False, na=False)]
    if len(baemin_df) > 0:
        baemin_file = output_dir / f"baemin_orders_20260119.csv"
        baemin_df.to_csv(baemin_file, index=False, encoding='utf-8-sig')
        print(f"배민 데이터 저장: {baemin_file} ({len(baemin_df)}건)")
    
    # 쿠팡 데이터
    coupang_df = df[df[platform_col].str.contains('쿠팡|coupang', case=False, na=False)]
    if len(coupang_df) > 0:
        coupang_file = output_dir / f"coupangeats_orders_20260119.csv"
        coupang_df.to_csv(coupang_file, index=False, encoding='utf-8-sig')
        print(f"쿠팡 데이터 저장: {coupang_file} ({len(coupang_df)}건)")
else:
    # 플랫폼 구분이 없으면 전체를 배민으로 저장
    output_file = output_dir / f"baemin_orders_20260119.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"데이터 저장: {output_file} ({len(df)}건)")

print("완료!")
