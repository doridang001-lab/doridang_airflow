"""
직원 정보를 구글 시트에서 가져오는 DAG
Google Sheet → sales_employee.csv (플랫폼별 행 분리)
"""

import logging
import os
import re
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)

from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB
from modules.extract.extract_gsheet import extract_gsheet
from modules.transform.utility.store_name_mapping import normalize_store_names
from modules.transform.utility.notifier import on_failure_callback, on_retry_callback, send_telegram_chunks
from modules.transform.utility.mail_recipients import (
    resolve_manager_mail,
)
from modules.transform.pipelines.sales.employee_toder_alert import (
    dispatch_toder_missing_alert,
)
from modules.transform.pipelines.sales.employee_accounts_export import (
    export_accounts_js,
)

from modules.transform.utility.dag_defaults import COLLECT_DAGRUN_TIMEOUT

logger = logging.getLogger(__name__)

# 설정
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
EMPLOYEE_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1a6-20U1-FYCQEfbOOVSDG3M0q6G2me5f/edit"
EMPLOYEE_SHEET_NAME = "가맹점리스트"
EMPLOYEE_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'sales_employee.csv'
AUTOMATION_NOTE_COL = '비고'
AUTOMATION_NOTE_VALUE = '자동화 연결'

# 저장 컬럼 정의 — 새 컬럼 추가 시 이 리스트만 수정
BASE_FIELDS = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '전화번호', '담당자',
               '실오픈일', '상세주소', '광역', '시군구', '읍면동', 'email', AUTOMATION_NOTE_COL]
PHONE_FIELD = '전화번호'
PHONE_FIELD_ALIASES = {'전화번호', '전화번호(mobile)', '핸드폰번호', '휴대폰번호', '연락처'}
RENT_FIELD = '임대료'
PLATFORM_FIELDS = ['플랫폼', '계정ID', '계정PW', RENT_FIELD, 'collected_at']


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


def coalesce_duplicate_columns(df, column_name):
    """중복 표준화된 컬럼은 행별 첫 유효값으로 합친다."""
    matching_positions = [idx for idx, column in enumerate(df.columns) if column == column_name]
    if len(matching_positions) <= 1:
        return df

    coalesced = df.iloc[:, matching_positions].replace(
        to_replace=r'^\s*(?:nan|None)?\s*$',
        value=pd.NA,
        regex=True,
    ).bfill(axis=1).iloc[:, 0]

    result = df.iloc[:, [idx for idx, column in enumerate(df.columns) if column != column_name]].copy()
    insert_at = min(matching_positions)
    result.insert(insert_at, column_name, coalesced)
    return result


def normalize_phone_columns(df):
    """시트 설명이 붙은 전화번호 헤더를 저장 표준명으로 맞춘다."""
    renamed_columns = []
    for column in df.columns:
        normalized = re.sub(r'\s+', '', str(column))
        if normalized in PHONE_FIELD_ALIASES or normalized.lower().startswith('전화번호('):
            renamed_columns.append(PHONE_FIELD)
        else:
            renamed_columns.append(column)

    df = df.copy()
    df.columns = renamed_columns
    return coalesce_duplicate_columns(df, PHONE_FIELD)


def check_toder_null_values(df_original):
    """토더 ID/PW null값 있는 매장 확인 및 텔레그램 알림"""
    result = dispatch_toder_missing_alert(df_original, send_telegram_chunks)
    if result.skipped_reason:
        logger.warning("[토더계정 알림] 스킵: %s", result.skipped_reason)
        return result

    logger.info(
        "[토더계정 알림] 대상=%d 정상=%d 누락=%d 중복매장=%d",
        result.alertable_store_count,
        result.complete_store_count,
        result.missing_store_count,
        result.duplicate_store_count,
    )
    return result



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
        normalize_header = lambda value: re.sub(r'\s+', '', str(value)) if pd.notna(value) else ''
        column_headers = [normalize_header(col) for col in df_raw.columns]
        header_row_idx = None
        if '호점' in column_headers:
            df = df_raw.copy()
            df.columns = column_headers
            print("[감지] 헤더 위치: 컬럼")
        else:
            for idx in range(len(df_raw)):
                row = df_raw.iloc[idx]
                # '호점'이라는 글자가 포함된 행을 진짜 헤더로 인식
                row_list = [normalize_header(v) for v in row.tolist()]
                if '호점' in row_list:
                    header_row_idx = idx
                    print(f"[감지] 헤더 위치: {idx+1}행")
                    break

            if header_row_idx is None:
                raise AirflowException("로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다.")

            # 헤더 정제 및 데이터 슬라이싱
            raw_header = [normalize_header(col) for col in df_raw.iloc[header_row_idx].tolist()]

            # 실제 데이터는 헤더 다음 줄부터
            df = df_raw.iloc[header_row_idx + 1:].copy()
            df.columns = raw_header
            df = df.reset_index(drop=True)
        
        print(f"[로드] 초기 데이터: {len(df):,}건")
    except AirflowException:
        raise
    except Exception as e:
        print(f"[에러] 로드 단계 실패: {e}")
        raise AirflowException(f"로드 실패: {e}") from e
    
    # 3️⃣ 컬럼명 표준화 및 매핑
    # 시트의 다양한 컬럼명 표기를 코드 내부 표준명으로 변경
    column_mapping = {
        '오픈순서': '오픈순서', '호점': '호점', '매장명': '매장명',
        '사업자 번호': '사업자번호', '사업자번호': '사업자번호',
        '점주명': '점주명',
        '전화번호': '전화번호', '핸드폰번호': '전화번호',
        '전화번호(mobile)': '전화번호', '휴대폰번호': '전화번호', '연락처': '전화번호',
        '담당 S.V': '담당자', '담당 SV': '담당자', '담당SV': '담당자',
        '주소': '상세주소',
        '임대료': RENT_FIELD,
        '배달의민족ID': '배민ID', '배달의민족PW': '배민PW',
        '배달의 민족ID': '배민ID', '배달의 민족PW': '배민PW',
        '요기요ID': '요기요ID', '요기요PW': '요기요PW',
        '쿠팡이츠ID': '쿠팡ID', '쿠팡이츠PW': '쿠팡PW',
        '땡겨요ID': '땡겨요ID', '땡겨요PW': '땡겨요PW',
        '토더 ID': '토더ID', '토더PW': '토더PW',
        '실오픈일': '실오픈일',
        '비고': AUTOMATION_NOTE_COL,
    }
    
    # 존재하는 컬럼만 변경
    rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns}
    df = df.rename(columns=rename_dict)
    df = normalize_phone_columns(df)
    
    # 4️⃣ 호점 기준 유효 데이터 필터링
    if '호점' not in df.columns:
        raise AirflowException("로드 실패: 매핑 후 '호점' 컬럼을 찾을 수 없습니다.")
    
    # 호점이 비어있거나, 헤더 문자가 반복되거나, '~'가 포함된 행 제외
    df = df[df['호점'].notna()].copy()
    df['호점'] = df['호점'].astype(str).str.strip()
    df = df[~df['호점'].isin(['', 'nan', '호점'])].copy()
    df = df[~df['호점'].str.contains('~', na=False)].copy()
    
    print(f"[필터링] 유효 매장: {len(df):,}건")

    if AUTOMATION_NOTE_COL in df.columns:
        before_automation_filter = len(df)
        df = df[df[AUTOMATION_NOTE_COL].astype(str).str.strip() == AUTOMATION_NOTE_VALUE].copy()
        print(
            f"[자동화 연결 필터] {before_automation_filter:,}건 -> {len(df):,}건 "
            f"({AUTOMATION_NOTE_COL}='{AUTOMATION_NOTE_VALUE}')"
        )
    else:
        # 이 필터가 폐점 매장 제외의 유일한 스위치다. 컬럼이 없으면 조용히 꺼진 채로 돌기 때문에
        # print가 아니라 warning으로 남겨 실제 시트 헤더까지 보여준다.
        logger.warning(
            "[자동화 연결 필터 스킵] '%s' 컬럼 없음 — 폐점 매장 제외가 동작하지 않습니다. 시트 헤더: %s",
            AUTOMATION_NOTE_COL, list(df.columns),
        )
    
    # 5️⃣ 이메일 매핑 및 담당자 정제
    if '담당자' in df.columns:
        # '김덕기 과장' -> '김덕기'로 성함만 추출하여 매핑 확률 극대화
        def extract_name(val):
            val = str(val).strip()
            match = re.match(r'([가-힣]{2,3})', val)
            return match.group(1) if match else val

        df['담당자_정제'] = df['담당자'].apply(extract_name)
        df['email'] = df['담당자_정제'].apply(lambda name: resolve_manager_mail(name) or '')
        
        # 로그 출력용
        print(f"\n[담당자별 매칭 현황]:")
        for mgr in sorted(df['담당자_정제'].unique()):
            if mgr and mgr != 'nan':
                count = (df['담당자_정제'] == mgr).sum()
                has_email = "✓" if resolve_manager_mail(mgr) else "✗ (메일미등록/비활성)"
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
    
    base_columns = [col for col in BASE_FIELDS if col in df.columns]
    
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
                    rent_value = row.get(RENT_FIELD, '')
                    if pd.isna(rent_value) or str(rent_value).strip() in ['', 'nan', 'None']:
                        new_row[RENT_FIELD] = ''
                    else:
                        new_row[RENT_FIELD] = str(rent_value).strip()
                    rows.append(new_row)
    
    df_final = pd.DataFrame(rows)
    if df_final.empty:
        df_final = pd.DataFrame(columns=BASE_FIELDS + PLATFORM_FIELDS)
    
    # 8️⃣ 최종 정리 및 저장
    df_final['collected_at'] = pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y-%m-%d %H:%M')
    
    
    
    final_column_order = BASE_FIELDS + PLATFORM_FIELDS
    final_columns = [col for col in final_column_order if col in df_final.columns]

    df_final = df_final[final_columns]
    
    # 매장명 정규화 (중앙 매핑: store_name_mapping.py)
    df_final['매장명'] = normalize_store_names(df_final['매장명'])

    
    # 📧 토더 ID/PW 체크 및 메일 알림 (저장 전)
    check_toder_null_values(df)
    
    # CSV 저장 (2개 경로)
    EMPLOYEE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # OneDrive 경로 설정
    ONEDRIVE_PATH = ONEDRIVE_DB / "sales_employee.csv"
    
    try:
        # 1️⃣ 수집/매크로가 읽는 필수 경로에 저장
        df_final.to_csv(EMPLOYEE_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\n[저장완료 1/2] 경로: {EMPLOYEE_CSV_PATH}")
    except Exception as e:
        print(f"[에러] 필수 저장 실패: {e}")
        import traceback
        print(traceback.format_exc())
        raise AirflowException(f"저장 실패: {e}") from e

    mirror_saved = False
    try:
        # 2️⃣ Repository 경로는 환경에 따라 읽기 전용/잠금일 수 있으므로 보조 미러로만 처리
        ONEDRIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(ONEDRIVE_PATH, index=False, encoding='utf-8-sig')
        mirror_saved = True
        print(f"[저장완료 2/2] Repository: {ONEDRIVE_PATH}")
    except PermissionError as e:
        print(f"[Repository 저장 스킵] 권한 없음: {ONEDRIVE_PATH} ({e})")
    except Exception as e:
        print(f"[Repository 저장 스킵] {ONEDRIVE_PATH} ({e})")

    print(f"\n  - 최종 행 수: {len(df_final):,}건")
    print(f"  - 플랫폼별: {df_final['플랫폼'].value_counts().to_dict()}")
    saved_paths = "2개 경로" if mirror_saved else "Local_DB 경로"
    return f"✅ 성공: {len(df_final)}행 저장 완료 ({saved_paths})"


with DAG(
    dag_id=filename.replace('.py', ''),
    description='B3 시작점 대응 및 담당자 기반 플랫폼 분리 수집',
    schedule="30 2 * * *", # 매일 새벽 2시 30분 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=COLLECT_DAGRUN_TIMEOUT,
    tags=['01_employee', 'gsheet', 'load'],
    default_args={
        "retries": 1,
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
    },
) as dag:
    
    load_employee_task = PythonOperator(
        task_id='load_employee_from_gsheet',
        python_callable=load_employee_from_gsheet,
    )

    export_accounts_js_task = PythonOperator(
        task_id='export_accounts_js',
        python_callable=export_accounts_js,
    )

    load_employee_task >> export_accounts_js_task
