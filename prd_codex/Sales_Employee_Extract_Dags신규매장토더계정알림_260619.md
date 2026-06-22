# Sales_Employee_Extract_Dags 신규매장 토더계정 알림

## Task
`dags/sales/Sales_Employee_Extract_Dags.py` 에 신규매장 토더계정 생성 알림 기능을 추가한다.
**실오픈일이 7일 이내로 임박했거나, 이미 지났는데 토더ID/PW가 비어 있는 매장**을 감지해
`[신규 매장]` 블록 형태의 이메일을 발송한다.
기존 토더 null 체크 알림은 유지하고, 알림 체크를 unpivot 이전(전체 행 대상)으로 실행한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지 (현재 DAG는 print 사용 중이므로 기존 방식 유지)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 경로 상수: `from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB`
- 스케줄: 기존 하드코딩 `"15 7 * * 2,4,6"` 유지 (schedule.py 상수 미등록)
- 이메일 발송: 기존 `send_email_alert()` (smtplib 직접 사용) 방식 유지

## Files to Create / Modify
- **수정**: `C:\airflow\dags\sales\Sales_Employee_Extract_Dags.py`

## Implementation Steps

### 1. column_mapping에 핸드폰번호 추가 (line ~178)
```python
column_mapping = {
    ...
    '핸드폰번호': '핸드폰번호', '핸드폰': '핸드폰번호',
    '전화번호': '핸드폰번호', '연락처': '핸드폰번호',
    ...
}
```
시트에 해당 컬럼이 없으면 아래 로직에서 `.get('핸드폰번호', '')` 로 빈값 처리.

### 2. 새 함수 `check_new_store_torder_alert(df)` 추가 (기존 `check_toder_null_values` 바로 아래)

```python
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
        is_upcoming = 0 <= days_until <= 7           # 오픈 임박 (오늘~7일 후)
        is_overdue = days_until < 0 and toder_missing  # 오픈 지났는데 계정 없음

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
        block = f"""[신규 매장]
- 매장명 : {str(row.get('매장명', '')).strip()}
- 사업자명의 : {str(row.get('점주명', '')).strip()}
- 핸드폰번호 : {str(row.get('핸드폰번호', '')).strip()}
- 매장주소 : {str(row.get('상세주소', '')).strip()}
- 배민 계정 : {_acct(row.get('배민ID'), row.get('배민PW'))}
- 요기요 계정 : {_acct(row.get('요기요ID'), row.get('요기요PW'))}
- 쿠팡 계정 : {_acct(row.get('쿠팡ID'), row.get('쿠팡PW'))}
- 땡겨요 계정 : {_acct(row.get('땡겨요ID'), row.get('땡겨요PW'))}
- 오픈일 : {str(row.get('실오픈일', '')).strip()}"""
        blocks.append(block)

    body = "\n\n".join(blocks)
    subject = f"[도리당] 토더계정을 만드세요 ({len(alert_rows)}건)"
    send_email_alert(subject, body, "a17019@kakao.com")
    print(f"[토더계정 알림] {len(alert_rows)}건 발송 완료")
```

### 3. 호출 위치 변경 (line ~283)

기존:
```python
# 📧 토더 ID/PW 체크 및 메일 알림 (저장 전)
check_toder_null_values(df)
```

변경 후 — **7단계 unpivot 바로 위**에 두 함수 모두 호출:
```python
# 📧 알림 체크 (unpivot 전 전체 행 대상)
check_toder_null_values(df)
check_new_store_torder_alert(df)

# 7️⃣ 플랫폼별 행 분리 (Unpivot)
```

기존 `check_toder_null_values(df)` 호출 위치(line 284 부근, 저장 직전)는 **삭제**.

## Reference Code
### Sales_Employee_Extract_Dags.py (핵심 부분)
```python
# import 블록
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 기존 send_email_alert (그대로 유지)
def send_email_alert(subject, body, to_email):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "airflow.alarm@gmail.com"
    sender_password = "bgtu jxdz grxj xvwu"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)

# 기존 column_mapping 위치 (line ~178)
column_mapping = {
    '토더 ID': '토더ID', '토더PW': '토더PW',
    '실오픈일': '실오픈일',
    # ← 이 아래에 핸드폰번호 추가
}

# 기존 check_toder_null_values 바로 아래에 새 함수 삽입
```

## Test Cases
1. **import 검증** `python -c "import sys; sys.path.insert(0,'C:/airflow'); from dags.sales.Sales_Employee_Extract_Dags import load_employee_from_gsheet"` → ImportError 없음
2. **알림 함수 단독 테스트** — 아래 스크립트로 확인:
```python
import sys; sys.path.insert(0, 'C:/airflow')
import pandas as pd
from dags.sales.Sales_Employee_Extract_Dags import check_new_store_torder_alert

import pandas as pd
from datetime import datetime, timedelta
today = datetime.now()
df = pd.DataFrame([{
    '매장명': '도리당 테스트점', '점주명': '홍길동', '핸드폰번호': '010-1234-5678',
    '상세주소': '서울시 강남구 역삼동 123', '실오픈일': (today + timedelta(days=3)).strftime('%Y-%m-%d'),
    '토더ID': '', '토더PW': '',
    '배민ID': 'test123', '배민PW': 'pw123',
    '요기요ID': '', '요기요PW': '', '쿠팡ID': '', '쿠팡PW': '', '땡겨요ID': '', '땡겨요PW': '',
}])
check_new_store_torder_alert(df)
# → 콘솔에 "[토더계정 알림] 1건 발송 완료" 출력 + 실제 메일 수신 확인
```
3. **과거 오픈일 + 빈 계정 케이스** — `실오픈일`을 `(today - timedelta(days=10))`으로 바꿔 동일 테스트 → 알림 발송 확인
4. **토더 계정 있는 경우** — `토더ID='torder123'`, `토더PW='pw'` 설정 후 → 알림 발송 안 됨 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~4 순서대로 실행
  2. FAIL → 원인 분석 후 코드 수정
  3. 전체 재실행
종료 조건: 전체 PASS
```

## Constraints
- `send_email_alert()` 시그니처 변경 금지 (plain text 방식 유지)
- 기존 `check_toder_null_values(df)` 함수 삭제 금지 (별도로 남겨둠)
- unpivot 로직(`rows` 리스트 생성 부분) 변경 금지
- `BASE_FIELDS`, `PLATFORM_FIELDS` 리스트 변경 금지
- 핸드폰번호 컬럼이 시트에 없어도 KeyError 나지 않도록 `.get()` 사용

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- 핸드폰번호 컬럼이 시트에 없으면: 빈값(`''`)으로 표시
- 타입 힌트 여부: 기존 파일과 동일하게 (없음)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- `_acct()` 헬퍼 함수는 `check_new_store_torder_alert` 내부 중첩 함수로 정의
