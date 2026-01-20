"""
구글시트 가맹점 리스트 ETL DAG

기능:
    1. 구글시트에서 가맹점 리스트 추출
    2. 플랫폼별 계정 정보로 변환 (Wide → Long)
    3. OneDrive CSV에 저장
    4. 처리 결과 이메일 전송

주기: 매일 06:31
"""
from airflow import DAG
import os
import sys
import pendulum
from pathlib import Path
import pandas as pd
import datetime as dt
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

# modules 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 함수 임포트
from modules.extract.extract_gsheet import extract_gsheet
from modules.load.load_onedrive import onedrive_csv_save
from modules.transform.utility.paths import ONEDRIVE_DB

filename = os.path.basename(__file__)

# CSV 저장 경로 (테스트용 - 영업관리부_DB와 충돌 방지)
ONEDRIVE_CSV_PATH = ONEDRIVE_DB / "temp" / "test_sales_employee.csv"
DOWNLOADS_CSV_PATH = ONEDRIVE_DB / "temp" / "test_sales_employee.csv"

# 담당자 → 이메일 매핑(필요시 추가/수정)
MANAGER_EMAIL_MAP = {
    "심성준 이사" : "a17019@kakao.com",
    "김대진 팀장" :  "sanbogaja81@kakao.com",
    "김덕기 과장" : "kdk1402@kakao.com",
    "이병두 과장" : "byoungd201@kakao.com",
    "황대성 대리" : "gjddkemf@kakao.com"

}  

# ============================================================
# Task 1: 구글시트 데이터 추출 및 전처리
# ============================================================

def transform_gsheet_data(df):
    """구글시트 가맹점 데이터를 플랫폼별 계정 정보로 변환"""
    
    # Unnamed 컬럼 제거
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    
    # 헤더 행 설정 (2번째 행을 컬럼명으로 사용)
    header_row_idx = 1
    df.columns = df.iloc[header_row_idx]
    
    # 컬럼명 정리
    df = df.rename(columns={
        '오픈\n순서': '오픈순서',
        '변경 \n담당 SV': '변경담당SV'
    })
    
    # 헤더 이후 데이터만 추출
    df = df[2:].copy()  # .copy() 추가
    
    # 사업자 번호가 있는 행만 필터링 (유효한 매장 데이터)
    print(f"🔍 필터링 전: {len(df)}행, 컬럼: {list(df.columns)[:5]}...")
    print(f"🔍 사업자 번호 컬럼 샘플 (처음 10개):\n{df['사업자 번호'].head(10).tolist()}")
    print(f"🔍 사업자 번호 null 개수: {df['사업자 번호'].isnull().sum()} / {len(df)}")
    print(f"🔍 사업자 번호 공백 문자열 개수: {(df['사업자 번호'].astype(str).str.strip() == '').sum()}")
    
    # null이 아니고 공백이 아닌 행만 필터링
    cond1 = ~df["사업자 번호"].isnull()
    cond2 = df["사업자 번호"].astype(str).str.strip() != ''
    df = df.loc[cond1 & cond2].copy()
    print(f"🔍 필터링 후: {len(df)}행")
    # 주소 정리: 이사후 주소 우선, 없으면 이사전 주소 사용
    df['상세주소'] = (
        df['주소'].astype(str)
        .str.replace('\r', ' ', regex=False)  # 개행 제거
        .str.replace('\n', ' ', regex=False)
        .str.replace('&gt;', '>', regex=False)  # HTML 특수문자
        .str.extract(r'\(이사후 주소\)\s*(.+)', expand=False)  # 이사후 주소 추출
        .fillna(  # 없으면 이사전 주소 사용
            df['주소'].astype(str)
            .str.replace(r'\(이사전 주소\)\s*', '', regex=True)
            .str.replace('\r', ' ', regex=False)
            .str.replace('\n', ' ', regex=False)
            .str.replace('&gt;', '>', regex=False)
        )
        .str.strip()
    )

    # 필요한 컬럼 선택 (복사본 생성으로 SettingWithCopyWarning 방지)
    df_col = [
        '오픈순서', '호점', '매장명', '사업자 번호', '점주명', '변경담당SV', '실오픈일', '상세주소',
        '배달의 민족ID', '배달의 민족PW', '요기요ID', '요기요PW', '쿠팡이츠ID', '쿠팡이츠PW',
        '땡겨요ID', '땡겨요PW', '토더 ID', '토더PW', '네이버 ID', '네이버 pw'
    ]
    gsheet_store_list = df[df_col].copy()  # .copy() 추가
    
    # 주소에서 지역 정보 추출
    gsheet_store_list["광역"] = gsheet_store_list['상세주소'].str.split(' ').str[0].str[:2]
    gsheet_store_list["시군구"] = gsheet_store_list['상세주소'].str.split(' ').str[1]
    gsheet_store_list["읍면동"] = gsheet_store_list['상세주소'].str.split(' ').str[2]
    
    # Wide → Long 형식 변환: 플랫폼별 계정을 각 행으로 분리
    platform_cols = {
        '배달의 민족': ('배달의 민족ID', '배달의 민족PW'),
        '요기요': ('요기요ID', '요기요PW'),
        '쿠팡이츠': ('쿠팡이츠ID', '쿠팡이츠PW'),
        '땡겨요': ('땡겨요ID', '땡겨요PW'),
        '토더': ('토더 ID', '토더PW'),
        '네이버': ('네이버 ID', '네이버 pw')
    }
    
    # 매장 기본 정보 컬럼
    keep_cols = [
        '오픈순서', '호점', '매장명', '사업자 번호', '점주명', '변경담당SV', 
        '실오픈일', '상세주소', '광역', '시군구', '읍면동'
    ]
    
    # 각 매장의 플랫폼별 계정을 별도 행으로 생성
    rows = []
    print(f"🔍 Wide→Long 변환 시작: {len(gsheet_store_list)}개 매장")
    for idx, r in gsheet_store_list.iterrows():
        base = {c: r[c] for c in keep_cols}
        for platform, (id_col, pw_col) in platform_cols.items():
            uid = r.get(id_col)
            pwd = r.get(pw_col)
            # ID나 PW 중 하나라도 있으면 행 추가
            if pd.notna(uid) or pd.notna(pwd):
                rows.append({
                    **base, 
                    '플랫폼': platform, 
                    '계정ID': uid, 
                    '계정PW': pwd
                })
    
    print(f"🔍 변환 결과: {len(rows)}개 행 생성됨 (플랫폼 계정 정보)")
    
    # Long 형식 데이터프레임 생성
    result_df = pd.DataFrame(rows)
    print(f"🔍 result_df 생성: {len(result_df)}행, 컬럼: {list(result_df.columns)}")
    
    # 수집 시간 추가
    result_df["collected_at"] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 컬럼명 변경
    result_df.rename(columns={"변경담당SV": "담당자"}, inplace=True)

    # 담당자 이메일 매핑 (없으면 빈 문자열)
    result_df["email"] = result_df["담당자"].map(MANAGER_EMAIL_MAP).fillna('')
    
    # 컬럼 순서 지정
    column_order = [
        '오픈순서', '호점', '매장명', '사업자 번호', '점주명', '담당자',
        '실오픈일', '상세주소', '광역', '시군구', '읍면동',
        '플랫폼', '계정ID', '계정PW',
        'collected_at', 'email'
    ]
    result_df = result_df[column_order]
    
    # nan 값을 빈 문자열로 변환
    result_df = result_df.fillna('')
    
    return result_df


def fetch_and_transform_gsheet(**context):
    """구글시트에서 가맹점 데이터 추출 및 변환"""
    ti = context['task_instance']
    
    # 1. 구글시트에서 데이터 추출
    print("📥 구글시트에서 데이터 추출 중...")
    raw_df = extract_gsheet(
        url="https://docs.google.com/spreadsheets/d/1a6-20U1-FYCQEfbOOVSDG3M0q6G2me5f/edit?usp=sharing&ouid=116296472165065607173&rtpof=true&sd=true",
        sheet_name="가맹점리스트",
        file_name="001_가맹점리스트_250218"
    )
    print(f"✅ 원본 데이터 추출 완료: {len(raw_df)}행")
    
    # 2. 데이터 변환 (Wide → Long)
    print("🔄 데이터 변환 중...")
    transformed_df = transform_gsheet_data(raw_df)
    print(f"✅ 변환 완료: {len(transformed_df)}행 (플랫폼별 계정)")
    
    # 3. XCom에 결과 저장 (다음 태스크에서 사용)
    # Timestamp 객체를 문자열로 변환하여 직렬화 문제 해결
    transformed_dict = transformed_df.copy()
    
    # datetime/Timestamp 컬럼을 문자열로 변환
    for col in transformed_dict.columns:
        if transformed_dict[col].dtype == 'datetime64[ns]':
            transformed_dict[col] = transformed_dict[col].astype(str)
    
    ti.xcom_push(key='transformed_df', value=transformed_dict.to_dict('records'))
    ti.xcom_push(key='row_count', value=len(transformed_df))
    
    # 4. 이메일용 HTML 생성
    html_preview = transformed_df.head(10).to_html(index=False, border=1)
    ti.xcom_push(key='df_html', value=html_preview)
    
    return f"추출 및 변환 완료: {len(transformed_df)}행"


# ============================================================
# Task 2: CSV 저장
# ============================================================

def save_to_csv(**context):
    """변환된 데이터를 CSV에 저장"""
    ti = context['task_instance']
    
    # 이전 태스크에서 변환된 데이터 가져오기
    records = ti.xcom_pull(task_ids='fetch_and_transform', key='transformed_df')
    
    print(f"🔍 XCom에서 가져온 records: {len(records) if records else 0}개")
    if records:
        print(f"🔍 첫 번째 레코드 샘플: {records[0] if records else None}")
    
    if not records:
        print("❌ 저장할 데이터 없음")
        return "데이터 없음"
    
    df = pd.DataFrame(records)
    print(f"💾 CSV 저장 중... ({len(df)}행)")
    print(f"🔍 DataFrame 컬럼: {list(df.columns)}")
    print(f"🔍 DataFrame 샘플 (첫 2행):\n{df.head(2)}")
    
    # 컬럼 순서 지정
    column_order = [
        '오픈순서', '호점', '매장명', '사업자 번호', '점주명', '담당자',
        '실오픈일', '상세주소', '광역', '시군구', '읍면동',
        '플랫폼', '계정ID', '계정PW',
        'collected_at', 'email'
    ]
    df = df[column_order]
    
    # 담당자가 없는 행 제외 (fillna 전에 필터링)
    df = df[~df["담당자"].isna()]
    df = df[df["담당자"].astype(str).str.strip() != '']
    
    # nan 값 제거 (빈 문자열로 변환)
    df = df.replace('nan', '')
    df = df.fillna('')
    
    # 복합 키 생성: 사업자번호 + 플랫폼 + 수집날짜로 고유 키 만들기
    # collected_at에서 날짜만 추출 (시간 제외)
    df['collection_date'] = pd.to_datetime(df['collected_at']).dt.strftime('%Y-%m-%d')
    df['composite_key'] = df['사업자 번호'].astype(str) + '_' + df['플랫폼'].astype(str) + '_' + df['collection_date'].astype(str)
    
    # ============================================================
    # 1. OneDrive에 저장 (메인 경로) - 안전한 쓰기
    # ============================================================
    print(f"💾 OneDrive에 저장 중... ({len(df)}행)")
    
    # composite_key 컬럼 제거 (저장 시 불필요)
    df_save = df.drop(columns=['composite_key', 'collection_date'])
    
    # OneDrive 안전 저장 (임시 파일 + 동기화 대기)
    from modules.transform.utility.onedrive_sync import save_with_onedrive_sync
    
    save_with_onedrive_sync(
        df_save,
        ONEDRIVE_CSV_PATH,
        index=False,
        encoding='utf-8-sig'
    )
    print(f"✅ OneDrive 저장 완료: {ONEDRIVE_CSV_PATH} ({len(df_save)}건)")
    
    return f"저장 완료: 총 {len(df_save)}건 저장됨"


# ============================================================
# DAG 설정
# ============================================================

# ============================================================
# Task 4: OneDrive 동기화 (Windows로 파일 복사)
# ============================================================

def copy_to_windows_onedrive(**context):
    """Docker 내부의 onedrive_db 파일을 Windows OneDrive로 자동 복사 (양방향 동기화)"""
    import subprocess
    import os
    
    print(f"🔄 Windows OneDrive 동기화 시작...")
    
    # Windows에서 robocopy를 실행하기 위한 명령어
    source = r"C:\Users\민준\doridang_airflow\onedrive_sync"
    target = r"C:\Users\민준\OneDrive - 주식회사 도리당\Doridang_DB"
    
    try:
        # PowerShell을 통해 robocopy 실행
        # Windows 호스트에서만 작동하므로 Docker 내부에서는 실행되지 않음
        cmd = f'powershell -Command "robocopy \\"{source}\\" \\"{target}\\" /MIR /MT:8 /R:3 /W:5"'
        
        print(f"📂 원본 (로컬): {source}")
        print(f"📂 대상 (OneDrive): {target}")
        print(f"⏳ robocopy 실행 중...")
        
        # Docker 내부에서 Windows 호스트 명령을 직접 실행할 수 없으므로
        # 호스트에서 별도로 스크립트를 실행해야 함
        print(f"⚠️  주의: 이 DAG은 Docker 컨테이너에서 실행 중이므로 Windows 명령을 직접 실행할 수 없습니다.")
        print(f"✅ 해결책: 호스트 Windows에서 다음 명령어를 실행하세요:")
        print(f"   robocopy \"{source}\" \"{target}\" /MIR /MT:8")
        
        return "파일 동기화 준비 완료. 호스트에서 robocopy 스크립트 실행 필요"
        
    except Exception as e:
        print(f"❌ 오류: {e}")
        return f"오류 발생: {e}"


# ============================================================
# 이메일 수신자
# ============================================================
TO_MEMBERS = ['a17019@kakao.com']


with DAG(
    dag_id=filename.replace('.py', ''),
    description='구글시트 가맹점 리스트 추출 → 변환 → OneDrive 저장',
    schedule="31 6 * * *",  # 매일 06:31 실행
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['gsheet', 'store', 'etl'],
) as dag:
    
    # Task 1: 구글시트 추출 및 변환
    fetch_task = PythonOperator(
        task_id="fetch_and_transform",
        python_callable=fetch_and_transform_gsheet,
    )
    
    # Task 2: CSV 저장 (downloads 폴더)
    save_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
    )
    
    # Task 3: 이메일 알림 (환경변수 SMTP Connection 사용)
    email_task = EmailOperator(
        task_id='send_email_notification',
        conn_id='conn_smtp_gmail',
        to=TO_MEMBERS,
        subject='[도리당] 가맹점 리스트 업데이트 완료',
        html_content="""<html>
<body>
<h3>📊 가맹점 리스트 업데이트 완료</h3>
<p><strong>처리 행 수:</strong> {{ task_instance.xcom_pull(task_ids='fetch_and_transform', key='row_count') }}행</p>
<p><strong>저장 위치:</strong> OneDrive/Doridang_DB/sales_employee.csv</p>
<p style="color: #666; font-size: 12px;">(백업: /opt/airflow/downloads/sales_employee.csv)</p>

<h4>📋 데이터 미리보기 (상위 10개):</h4>
{{ task_instance.xcom_pull(task_ids='fetch_and_transform', key='df_html') }}

<p style="color: #666; font-size: 12px; margin-top: 20px;">
실행 시간: {{ ts }}<br/>
DAG: {{ dag.dag_id }}
</p>
</body>
</html>""",
    )
    
    # Task 4: 윈도우 OneDrive에 복사
    copy_task = PythonOperator(
        task_id='copy_to_onedrive',
        python_callable=copy_to_windows_onedrive,
    )
    
    # Task 의존성 설정: 추출→변환 → CSV저장 → 윈도우복사 → 이메일
    fetch_task >> save_task >> copy_task >> email_task

