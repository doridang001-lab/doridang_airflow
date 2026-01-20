"""
직원 정보를 구글 시트에서 가져오는 DAG

Google Sheet → sales_employee.csv (플랫폼별 행 분리)
"""

import pendulum
import pandas as pd
import os
import re
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)

from modules.transform.utility.paths import LOCAL_DB
from modules.extract.extract_gsheet import extract_gsheet

# 설정
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
EMPLOYEE_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1a6-20U1-FYCQEfbOOVSDG3M0q6G2me5f/edit"
EMPLOYEE_SHEET_NAME = None
EMPLOYEE_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'sales_employee.csv'


def parse_address(address_str):
    """주소를 공백 기준으로 분리하여 광역/시군구/읍면동으로 파싱"""
    if pd.isna(address_str) or str(address_str).strip() == '':
        return '', '', ''
    
    addr = str(address_str).strip()
    
    # 공백 기준으로 split
    parts = addr.split()
    
    sido = parts[0] if len(parts) > 0 else ''
    sigungu = parts[1] if len(parts) > 1 else ''
    dong = parts[2] if len(parts) > 2 else ''
    
    return sido, sigungu, dong


def load_employee_from_gsheet(**context):
    print(f"\n{'='*60}")
    print(f"[구글시트] 직원 정보 로드 시작")
    
    # 1️⃣ Google Sheets 읽기
    try:
        df_raw = extract_gsheet(
            url=EMPLOYEE_GSHEET_URL,
            sheet_name=EMPLOYEE_SHEET_NAME,
            credentials_path=DEFAULT_CREDENTIALS_PATH,
        )
        
        # 헤더 자동 감지
        header_row_idx = None
        for idx, row in df_raw.iterrows():
            if any(str(v).strip() == '호점' for v in row.tolist()):
                header_row_idx = idx
                break

        if header_row_idx is None:
            return "로드 실패: 헤더 탐색 실패"

        raw_header = [str(col).strip().replace('\n', '') if pd.notna(col) else '' for col in df_raw.iloc[header_row_idx].tolist()]
        first_nonempty = next((i for i, h in enumerate(raw_header) if h), 0)
        
        header = raw_header[first_nonempty:]
        data = df_raw.iloc[header_row_idx + 1:, first_nonempty:].copy()
        data.columns = header
        df = data.reset_index(drop=True)
        
        print(f"[로드] 성공: {len(df):,}건")
        
    except Exception as e:
        print(f"[에러] {e}")
        return f"로드 실패: {str(e)}"
    
    # 2️⃣ 컬럼명 정리
    df.columns = [str(col).strip().replace('\n', '') for col in df.columns]
    
    # 3️⃣ 호점 필터링
    if '호점' not in df.columns:
        return "로드 실패: '호점' 컬럼 없음"
    
    df = df[df['호점'].notna()].copy()
    df = df[df['호점'].astype(str).str.strip() != ''].copy()
    df = df[~df['호점'].astype(str).str.contains('~', na=False)].copy()
    
    print(f"[필터링] {len(df):,}건")
    
    # 4️⃣ 컬럼명 매핑
    column_mapping = {
        '오픈순서': '오픈순서', '호점': '호점', '매장명': '매장명',
        '사업자 번호': '사업자번호', '점주명': '점주명',
        '담당 S.V': '담당자', '담당 SV': '담당SV',
        '마케팅비 수령 (110만)': '마케팅비수령',
        '계약체결일': '계약체결일', '실오픈일': '실오픈일',
        '폐업일(양도양수 포함)': '폐업일', '생년월일(880808)': '생년월일',
        '전화번호(mobile)': '전화번호', '주소': '상세주소',
        '배달의 민족ID': '배민ID', '배달의 민족PW': '배민PW',
        '요기요ID': '요기요ID', '요기요PW': '요기요PW',
        '쿠팡이츠ID': '쿠팡ID', '쿠팡이츠PW': '쿠팡PW',
        '땡겨요ID': '땡겨요ID', '땡겨요PW': '땡겨요PW',
        '토더 ID': '토더ID', '토더PW': '토더PW',
        '네이버 ID': '네이버ID', '네이버 pw': '네이버PW'
    }
    
    rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
    if rename_dict:
        df = df.rename(columns=rename_dict)
    
    # 5️⃣ email 매핑
    email_mapping = {
        '김덕기 과장': 'kdk1402@kakao.com',
        '심성준 이사': 'a17019@kakao.com',
        '이병두 과장': 'byoungd201@kakao.com',
        '황대성 대리': 'gjddkemf@kakao.com',
        '김대진 팀장': 'sanbogaja81@kakao.com',
    }

    # 담당자 컬럼 처리
    if '담당자' not in df.columns and '담당SV' in df.columns:
        df['담당자'] = df['담당SV']
    
    if '담당자' in df.columns:
        if '담당SV' in df.columns:
            empty_mask = df['담당자'].astype(str).str.strip() == ''
            df.loc[empty_mask, '담당자'] = df.loc[empty_mask, '담당SV']
        
        df['담당자'] = (df['담당자']
                      .astype(str)
                      .str.replace(r'[\s\u3000\xa0\t\n\r]+', ' ', regex=True)
                      .str.strip()
                      .replace('nan', ''))
        
        print(f"\n[담당자 목록]:")
        for mgr in sorted(df['담당자'].unique()):
            if mgr:
                count = (df['담당자'] == mgr).sum()
                status = "✓" if mgr in email_mapping else "✗"
                print(f"  {status} '{mgr}' ({count}건)")
        
        # email 처리
        base_email = None
        for col in ['email', '이메일']:
            if col in df.columns:
                base_email = df[col].astype(str).fillna('').str.strip()
                print(f"\n[기존 email 컬럼] '{col}' 발견")
                break
        
        if base_email is None:
            base_email = pd.Series([''] * len(df))
            print(f"\n[email 컬럼] 새로 생성")
        
        df['email'] = base_email
        needs_map = (df['email'].str.strip() == '') & (df['담당자'] != '')
        df.loc[needs_map, 'email'] = df.loc[needs_map, '담당자'].map(email_mapping).fillna('')
        
        mapped = (df['email'].str.strip() != '').sum()
        valid = df['email'].str.contains('@', na=False).sum()
        
        print(f"\n[email 결과]")
        print(f"  - 총 채워짐: {mapped}/{len(df)}건")
        print(f"  - @ 포함: {valid}/{len(df)}건")
    
    # 6️⃣ 주소 파싱 (광역/시군구/읍면동)
    if '상세주소' in df.columns:
        print(f"\n[주소 파싱] 시작...")
        df[['광역', '시군구', '읍면동']] = df['상세주소'].apply(
            lambda x: pd.Series(parse_address(x))
        )
        print(f"[주소 파싱] 완료")
    
    # 7️⃣ 플랫폼별로 행 분리 (unpivot)
    print(f"\n[플랫폼 분리] 시작...")
    
    platform_mapping = {
        '배달의 민족': ('배민ID', '배민PW'),
        '요기요': ('요기요ID', '요기요PW'),
        '쿠팡이츠': ('쿠팡ID', '쿠팡PW'),
        '땡겨요': ('땡겨요ID', '땡겨요PW'),
        '토더': ('토더ID', '토더PW'),
    }
    
    # 기본 정보 컬럼 (플랫폼 무관)
    base_columns = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '담당자', 
                    '실오픈일', '상세주소', '광역', '시군구', '읍면동', 'email']
    
    # 존재하는 컬럼만 선택
    base_columns = [col for col in base_columns if col in df.columns]
    
    rows = []
    for _, row in df.iterrows():
        for platform, (id_col, pw_col) in platform_mapping.items():
            # ID와 PW 컬럼이 모두 존재하는 경우만 처리
            if id_col not in df.columns or pw_col not in df.columns:
                continue
            
            account_id = str(row[id_col]).strip() if pd.notna(row[id_col]) else ''
            account_pw = str(row[pw_col]).strip() if pd.notna(row[pw_col]) else ''
            
            # ID가 비어있으면 해당 플랫폼 행 생성 안 함
            if account_id == '' or account_id == 'nan':
                continue
            
            # 담당자 검증: 빈 값이면 해당 행 스킵
            manager = str(row['담당자']).strip() if pd.notna(row['담당자']) else ''
            if manager == '' or manager == 'nan':
                continue
            
            # 기본 정보 복사
            new_row = {col: row[col] for col in base_columns}
            
            # 플랫폼 정보 추가
            new_row['플랫폼'] = platform
            new_row['계정ID'] = account_id
            new_row['계정PW'] = account_pw
            
            rows.append(new_row)
    
    df_final = pd.DataFrame(rows)
    
    print(f"[플랫폼 분리] 완료: {len(df)}개 매장 → {len(df_final)}개 행")
    
    # 8️⃣ collected_at 추가
    df_final['collected_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    
    # 9️⃣ 컬럼 순서 정리
    final_columns = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '담당자', 
                     '실오픈일', '상세주소', '광역', '시군구', '읍면동', 
                     '플랫폼', '계정ID', '계정PW', 'collected_at', 'email']
    
    # 존재하는 컬럼만 선택
    final_columns = [col for col in final_columns if col in df_final.columns]
    df_final = df_final[final_columns]
    
    # 담당자 필터링 (null, 빈 문자열, 'nan' 제외)
    if '담당자' in df_final.columns:
        df_final = df_final[
            (df_final["담당자"].notna()) & 
            (df_final["담당자"].astype(str).str.strip() != '') &
            (df_final["담당자"].astype(str) != 'nan')
        ]
        print(f"[필터링] 담당자 없는 행 제외 후: {len(df_final):,}건")
    
    # 🔟 CSV 저장
    EMPLOYEE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df_final.to_csv(EMPLOYEE_CSV_PATH, index=False, encoding='utf-8-sig')
        file_size = EMPLOYEE_CSV_PATH.stat().st_size / 1024
        
        print(f"\n[저장] ✅ 완료")
        print(f"  - 경로: {EMPLOYEE_CSV_PATH}")
        print(f"  - 행: {len(df_final):,}건 / 열: {len(df_final.columns)}개")
        print(f"  - 크기: {file_size:.2f} KB")
        
        # 플랫폼별 통계
        print(f"\n[플랫폼별 통계]")
        for platform, count in df_final['플랫폼'].value_counts().items():
            print(f"  - {platform}: {count}개")
        
        print(f"{'='*60}\n")
        
        return f"✅ 로드 완료: {len(df_final):,}개 행 (플랫폼별 분리)"
        
    except Exception as e:
        print(f"[에러] 저장 실패: {e}")
        return f"저장 실패: {str(e)}"


with DAG(
    dag_id=filename.replace('.py', ''),
    description='직원/매장 정보를 구글 시트에서 가져와 플랫폼별로 분리하여 CSV로 저장',
    schedule="0 10 * * 1",  # 매주 월 10:00
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['01_employee', 'gsheet', 'load'],
) as dag:
    
    load_employee_task = PythonOperator(
        task_id='load_employee_from_gsheet',
        python_callable=load_employee_from_gsheet,
    )
    
    load_employee_task