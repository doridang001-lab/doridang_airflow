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
        
        # 2️⃣ 헤더 자동 감지 (상단 요약 데이터 건너뛰고 '호점'이 있는 행 찾기)
        header_row_idx = None
        for idx, row in df_raw.iterrows():
            # '호점'이라는 글자가 포함된 행을 진짜 헤더로 인식
            row_list = [str(v).strip() for v in row.tolist()]
            if '호점' in row_list:
                header_row_idx = idx
                print(f"[감지] 헤더 위치: {idx+1}행")
                break

        if header_row_idx is None:
            return "로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다."

        # 헤더 정제 및 데이터 슬라이싱
        raw_header = [str(col).strip().replace('\n', '') if pd.notna(col) else '' for col in df_raw.iloc[header_row_idx].tolist()]
        
        # 실제 데이터는 헤더 다음 줄부터
        df = df_raw.iloc[header_row_idx + 1:].copy()
        df.columns = raw_header
        df = df.reset_index(drop=True)
        
        print(f"[로드] 초기 데이터: {len(df):,}건")
        
    except Exception as e:
        print(f"[에러] 로드 단계 실패: {e}")
        return f"로드 실패: {str(e)}"
    
    # 3️⃣ 컬럼명 표준화 및 매핑
    # 시트의 다양한 컬럼명 표기를 코드 내부 표준명으로 변경
    column_mapping = {
        '오픈순서': '오픈순서', '호점': '호점', '매장명': '매장명',
        '사업자 번호': '사업자번호', '사업자번호': '사업자번호',
        '점주명': '점주명',
        '담당 S.V': '담당자', '담당 SV': '담당자', '담당SV': '담당자',
        '주소': '상세주소',
        '배달의 민족ID': '배민ID', '배달의 민족PW': '배민PW',
        '요기요ID': '요기요ID', '요기요PW': '요기요PW',
        '쿠팡이츠ID': '쿠팡ID', '쿠팡이츠PW': '쿠팡PW',
        '땡겨요ID': '땡겨요ID', '땡겨요PW': '땡겨요PW',
        '토더 ID': '토더ID', '토더PW': '토더PW',
        '실오픈일': '실오픈일'
    }
    
    # 존재하는 컬럼만 변경
    rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
    df = df.rename(columns=rename_dict)
    
    # 4️⃣ 호점 기준 유효 데이터 필터링
    if '호점' not in df.columns:
        return "로드 실패: 매핑 후 '호점' 컬럼을 찾을 수 없습니다."
    
    # 호점이 비어있거나, 헤더 문자가 반복되거나, '~'가 포함된 행 제외
    df = df[df['호점'].notna()].copy()
    df['호점'] = df['호점'].astype(str).str.strip()
    df = df[~df['호점'].isin(['', 'nan', '호점'])].copy()
    df = df[~df['호점'].str.contains('~', na=False)].copy()
    
    print(f"[필터링] 유효 매장: {len(df):,}건")
    
    # 5️⃣ 이메일 매핑 및 담당자 정제
    email_mapping = {
        '김덕기': 'kdk1402@kakao.com',
        '심성준': 'a17019@kakao.com',
        '이병두': 'byoungd201@kakao.com',
        '황대성': 'gjddkemf@kakao.com',
        '김대진': 'sanbogaja81@kakao.com',
    }

    if '담당자' in df.columns:
        # '김덕기 과장' -> '김덕기'로 성함만 추출하여 매핑 확률 극대화
        def extract_name(val):
            val = str(val).strip()
            match = re.match(r'([가-힣]{2,3})', val)
            return match.group(1) if match else val

        df['담당자_정제'] = df['담당자'].apply(extract_name)
        df['email'] = df['담당자_정제'].map(email_mapping).fillna('')
        
        # 로그 출력용
        print(f"\n[담당자별 매칭 현황]:")
        for mgr in sorted(df['담당자_정제'].unique()):
            if mgr and mgr != 'nan':
                count = (df['담당자_정제'] == mgr).sum()
                has_email = "✓" if mgr in email_mapping else "✗ (메일미등록)"
                print(f"  {has_email} {mgr}: {count}건")

    # 6️⃣ 주소 파싱
    if '상세주소' in df.columns:
        df[['광역', '시군구', '읍면동']] = df['상세주소'].apply(lambda x: pd.Series(parse_address(x)))
    
    # 7️⃣ 플랫폼별 행 분리 (Unpivot)
    print(f"\n[플랫폼 분리] 시작...")
    platform_mapping = {
        '배달의 민족': ('배민ID', '배민PW'),
        '요기요': ('요기요ID', '요기요PW'),
        '쿠팡이츠': ('쿠팡ID', '쿠팡PW'),
        '땡겨요': ('땡겨요ID', '땡겨요PW'),
        '토더': ('토더ID', '토더PW'),
    }
    
    base_columns = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '담당자', 
                    '실오픈일', '상세주소', '광역', '시군구', '읍면동', 'email']
    base_columns = [col for col in base_columns if col in df.columns]
    
    rows = []
    for _, row in df.iterrows():
        # 담당자가 없으면 수집에서 제외 (데이터 품질 관리)
        manager = str(row.get('담당자', '')).strip()
        if manager in ['', 'nan', 'None']:
            continue

        for platform, (id_col, pw_col) in platform_mapping.items():
            if id_col in df.columns:
                account_id = str(row[id_col]).strip()
                if account_id and account_id not in ['', 'nan', 'None']:
                    new_row = {col: row[col] for col in base_columns}
                    new_row['플랫폼'] = platform
                    new_row['계정ID'] = account_id
                    new_row['계정PW'] = str(row[pw_col]).strip() if pw_col in df.columns else ''
                    rows.append(new_row)
    
    df_final = pd.DataFrame(rows)
    
    # 8️⃣ 최종 정리 및 저장
    df_final['collected_at'] = pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y-%m-%d %H:%M')
    
    final_columns = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '담당자', 
                     '실오픈일', '상세주소', '광역', '시군구', '읍면동', 
                     '플랫폼', '계정ID', '계정PW', 'collected_at', 'email']
    
    df_final = df_final[[col for col in final_columns if col in df_final.columns]]
    
    # CSV 저장
    EMPLOYEE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        df_final.to_csv(EMPLOYEE_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\n[저장완료] 경로: {EMPLOYEE_CSV_PATH}")
        print(f"  - 최종 행 수: {len(df_final):,}건")
        print(f"  - 플랫폼별: {df_final['플랫폼'].value_counts().to_dict()}")
        return f"✅ 성공: {len(df_final)}행 저장 완료"
    except Exception as e:
        return f"❌ 저장 실패: {str(e)}"


with DAG(
    dag_id=filename.replace('.py', ''),
    description='B3 시작점 대응 및 담당자 기반 플랫폼 분리 수집',
    schedule="0 10 * * 1",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['01_employee', 'gsheet', 'load'],
) as dag:
    
    load_employee_task = PythonOperator(
        task_id='load_employee_from_gsheet',
        python_callable=load_employee_from_gsheet,
    )
    
    load_employee_task