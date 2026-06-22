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
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

filename = os.path.basename(__file__)

from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB
from modules.extract.extract_gsheet import extract_gsheet
from modules.transform.utility.store_name_mapping import normalize_store_names
from modules.transform.utility.notifier import on_failure_callback

# 설정
DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
EMPLOYEE_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1a6-20U1-FYCQEfbOOVSDG3M0q6G2me5f/edit"
EMPLOYEE_SHEET_NAME = None
EMPLOYEE_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'sales_employee.csv'

# 저장 컬럼 정의 — 새 컬럼 추가 시 이 리스트만 수정
BASE_FIELDS = ['오픈순서', '호점', '매장명', '사업자번호', '점주명', '담당자',
               '실오픈일', '상세주소', '광역', '시군구', '읍면동', 'email']
PLATFORM_FIELDS = ['플랫폼', '계정ID', '계정PW', 'collected_at']


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


def send_email_alert(subject, body, to_email):
    """토더 ID/PW null값 감지 시 메일 알림"""
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        sender_email = "airflow.alarm@gmail.com"
        sender_password = "bgtu jxdz grxj xvwu"  # Gmail App Password

        try:
            from airflow.hooks.base import BaseHook

            connection = BaseHook.get_connection("doridang_conn_smtp_gmail")
            if connection.host and connection.port and connection.login and connection.password:
                smtp_server = connection.host
                smtp_port = int(connection.port)
                sender_email = connection.extra_dejson.get("from_email") or connection.login
                sender_password = connection.password
                smtp_login = connection.login
            else:
                smtp_login = sender_email
        except Exception as conn_error:
            print(f"[메일설정] Airflow SMTP 연결 사용 불가, 기본 설정 사용: {conn_error}")
            smtp_login = sender_email
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email
        msg['Subject'] = subject
        
        body_subtype = 'html' if str(body).lstrip().lower().startswith('<html') else 'plain'
        msg.attach(MIMEText(body, body_subtype, 'utf-8'))
        
        if int(smtp_port) == 465:
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(smtp_login, sender_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_login, sender_password)
                server.send_message(msg)
        
        print(f"[메일발송] {to_email} - {subject}")
        return True
    except Exception as e:
        print(f"[메일발송 실패] {to_email}: {e}")
        return False


def check_toder_null_values(df_original):
    """토더 ID/PW null값 있는 매장 확인 및 메일 알림"""
    if '토더ID' not in df_original.columns or '토더PW' not in df_original.columns:
        return
    
    # 토더ID 또는 토더PW가 null이고 매장명이 있는 행 찾기
    null_mask = (df_original['토더ID'].isna() | (df_original['토더ID'].astype(str).str.strip() == '')) | \
                (df_original['토더PW'].isna() | (df_original['토더PW'].astype(str).str.strip() == ''))
    
    null_stores = df_original[null_mask & df_original['매장명'].notna()][['호점', '매장명', '담당자', '토더ID', '토더PW']].copy()
    
    if len(null_stores) > 0:
        print(f"\n[토더계정 알림] null값 감지: {len(null_stores)}건")
        
        # 메일 본문 작성
        html_body = """
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h3 style="color: #d9534f;">⚠ 토더(TORDER) 계정정보 누락 알림</h3>
        <p>다음 매장에서 토더 ID/PW가 등록되지 않았습니다.</p>
        <table border="1" cellpadding="10" style="border-collapse: collapse; margin-top: 20px;">
        <tr style="background-color: #f5f5f5;">
            <th>호점</th>
            <th>매장명</th>
            <th>담당자</th>
            <th>토더ID</th>
            <th>토더PW</th>
        </tr>
        """
        
        for _, row in null_stores.iterrows():
            toder_id = str(row['토더ID']).strip() if pd.notna(row['토더ID']) else '(없음)'
            toder_pw = str(row['토더PW']).strip() if pd.notna(row['토더PW']) else '(없음)'
            html_body += f"""
        <tr>
            <td>{row['호점']}</td>
            <td>{row['매장명']}</td>
            <td>{row['담당자']}</td>
            <td>{toder_id}</td>
            <td>{toder_pw}</td>
        </tr>
            """
        
        html_body += """
        </table>
        <p style="margin-top: 20px; color: #666;">구글시트에서 해당 매장의 토더 ID/PW를 확인하고 등록해주세요.</p>
        </body>
        </html>
        """
        
        subject = f"[도리당 매장관리] 토더(TORDER) 계정정보 누락 - {len(null_stores)}건"
        return send_email_alert(subject, html_body, "a17019@kakao.com")
    return True


def check_new_store_torder_alert(df):
    """실오픈일 임박(7일 이내) 또는 경과 후 토더 계정 미등록 매장 알림"""
    if '실오픈일' not in df.columns:
        return

    today = pd.Timestamp.now(tz='Asia/Seoul').normalize().tz_localize(None)
    alert_rows = []

    for _, row in df.iterrows():
        store_name = str(row.get('매장명', '')).strip()
        if not store_name or store_name in ('nan', 'None'):
            continue

        raw_date = str(row.get('실오픈일', '')).strip()
        if not raw_date or raw_date in ('nan', 'None', ''):
            continue

        try:
            open_date = pd.to_datetime(raw_date, errors='coerce')
            if pd.isna(open_date):
                continue
            open_date = open_date.normalize()
        except Exception:
            continue

        toder_id = str(row.get('토더ID', '')).strip()
        toder_pw = str(row.get('토더PW', '')).strip()
        toder_missing = toder_id in ('', 'nan', 'None') or toder_pw in ('', 'nan', 'None')

        days_until = (open_date - today).days
        is_upcoming = 0 <= days_until <= 7
        is_overdue = days_until < 0 and toder_missing

        if is_upcoming or is_overdue:
            alert_rows.append(row)

    if not alert_rows:
        return

    def _acct(id_val, pw_val):
        """계정 문자열 포맷: ID // PW, 없으면 빈값"""
        i = str(id_val).strip() if pd.notna(id_val) and str(id_val).strip() not in ('nan', 'None', '') else ''
        p = str(pw_val).strip() if pd.notna(pw_val) and str(pw_val).strip() not in ('nan', 'None', '') else ''
        if i and p:
            return f"{i}   // {p}"
        elif i:
            return i
        return ''

    blocks = []
    for row in alert_rows:
        open_date_text = ''
        open_date = pd.to_datetime(row.get('실오픈일', ''), errors='coerce')
        if pd.notna(open_date):
            open_date_text = open_date.strftime('%Y-%m-%d')

        block = f"""[신규 매장]
- 매장명 : {str(row.get('매장명', '')).strip()}
- 사업자명의 : {str(row.get('점주명', '')).strip()}
- 핸드폰번호 : {str(row.get('핸드폰번호', '')).strip()}
- 매장주소 : {str(row.get('상세주소', '')).strip()}
- 발주매장코드 : 
- 배민 계정 : {_acct(row.get('배민ID'), row.get('배민PW'))}
- 요기요 계정 : {_acct(row.get('요기요ID'), row.get('요기요PW'))}
- 쿠팡 계정 : {_acct(row.get('쿠팡ID'), row.get('쿠팡PW'))}
- 땡겨요 계정 : {_acct(row.get('땡겨요ID'), row.get('땡겨요PW'))}
- 오픈일 : {open_date_text}"""
        blocks.append(block)

    body = "\n\n".join(blocks)
    subject = f"[도리당] 토더계정을 만드세요 ({len(alert_rows)}건)"
    sent = send_email_alert(subject, body, "a17019@kakao.com")
    if sent:
        print(f"[토더계정 알림] {len(alert_rows)}건 발송 완료")
    else:
        print(f"[토더계정 알림] {len(alert_rows)}건 발송 실패")
    return sent



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
        for idx in range(len(df_raw)):
            row = df_raw.iloc[idx]
            # '호점'이라는 글자가 포함된 행을 진짜 헤더로 인식
            row_list = [str(v).strip() for v in row.tolist()]
            if '호점' in row_list:
                header_row_idx = idx
                print(f"[감지] 헤더 위치: {idx+1}행")
                break

        if header_row_idx is None:
            return "로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다."

        # 헤더 정제 및 데이터 슬라이싱
        raw_header = [str(col).strip().replace('\r', '').replace('\n', '') if pd.notna(col) else '' for col in df_raw.iloc[header_row_idx].tolist()]
        
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
        '핸드폰번호': '핸드폰번호', '핸드폰': '핸드폰번호',
        '전화번호': '핸드폰번호', '전화번호\n(mobile)': '핸드폰번호',
        '전화번호(mobile)': '핸드폰번호',
        '전화번호 (mobile)': '핸드폰번호', '연락처': '핸드폰번호',
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
        '심성준': 'simjeong01@kakao.com',
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
    
    # 📧 신규매장 토더 계정 알림 (unpivot 전 전체 행 대상)
    check_new_store_torder_alert(df)
    
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
                    rows.append(new_row)
    
    df_final = pd.DataFrame(rows)
    
    # 8️⃣ 최종 정리 및 저장
    df_final['collected_at'] = pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y-%m-%d %H:%M')
    
    
    
    final_columns = [col for col in BASE_FIELDS + PLATFORM_FIELDS if col in df_final.columns]

    df_final = df_final[final_columns]
    
    # 매장명 정규화 (중앙 매핑: store_name_mapping.py)
    df_final['매장명'] = normalize_store_names(df_final['매장명'])

    # CSV 저장 (2개 경로)
    EMPLOYEE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # OneDrive 경로 설정
    ONEDRIVE_PATH = ONEDRIVE_DB / "sales_employee.csv"
    
    try:
        # 1️⃣ 기존 경로에 저장
        df_final.to_csv(EMPLOYEE_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"\n[저장완료 1/2] 경로: {EMPLOYEE_CSV_PATH}")
        
        # 2️⃣ OneDrive 경로에도 저장
        ONEDRIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_final.to_csv(ONEDRIVE_PATH, index=False, encoding='utf-8-sig')
        print(f"[저장완료 2/2] OneDrive: {ONEDRIVE_PATH}")
        
        print(f"\n  - 최종 행 수: {len(df_final):,}건")
        print(f"  - 플랫폼별: {df_final['플랫폼'].value_counts().to_dict()}")
        return f"✅ 성공: {len(df_final)}행 저장 완료 (2개 경로)"
    except Exception as e:
        print(f"[에러] 저장 실패: {e}")
        import traceback
        print(traceback.format_exc())
        return f"❌ 저장 실패: {str(e)}"


with DAG(
    dag_id=filename.replace('.py', ''),
    description='B3 시작점 대응 및 담당자 기반 플랫폼 분리 수집',
    schedule="15 7 * * *", # 매일 오전 7:15에 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['01_employee', 'gsheet', 'load'],
    default_args={
        "retries": 1,
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
) as dag:
    
    load_employee_task = PythonOperator(
        task_id='load_employee_from_gsheet',
        python_callable=load_employee_from_gsheet,
    )
