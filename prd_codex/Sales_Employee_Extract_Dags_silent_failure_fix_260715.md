# Sales_Employee_Extract_Dags 조용한 실패 + 헤더 감지 수정

## Task
`Sales_Employee_Extract_Dags`의 `load_employee_from_gsheet` 태스크가 2026-06-24까지 정상, 이후 실패 중.
로그는 `Returned value was: 로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다.` 인데 **알림이 안 뜬다**.
두 버그가 겹침: (1) 실패 시 예외를 던지지 않고 문자열을 `return`해서 태스크가 SUCCESS로 마킹됨 → `on_failure_callback`(이메일/텔레그램/heal_queue) 미발동, (2) 시트 상단 편집으로 `호점` 헤더 감지가 깨짐.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지 (단, 이번 최소수정 범위에서 기존 print 전면 교체는 하지 않음)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- DAG 파일 위치: `dags/{domain}/`
- 실패는 반드시 예외로: `raise AirflowException(...)` — 문자열 return 금지
- 스케줄/경로 상수: `from modules.transform.utility.schedule / paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `dags/sales/Sales_Employee_Extract_Dags.py` (유일한 코드 수정 대상)
- **조사용 임시**: scratchpad 진단 스크립트 (커밋 금지)

## Implementation Steps

### Step 0 — 라이브 시트 덤프로 근본원인 확정 (코드 수정 전 필수)
`extract_gsheet`를 실제 자격증명으로 호출해 `df_raw.columns` 와 `head(12)` 출력하는 임시 스크립트를 scratchpad에 만들어 1회 실행.
확인 항목:
- `호점`이 **컬럼**으로 올라갔는지 vs **데이터 행**에 있는지
- 셀 텍스트에 `\n`·중간 공백이 섞였는지
- `호점` 열이 `extract_gsheet`의 `unnamed_N` 드롭으로 사라졌는지

주의: `extract_gsheet._read_worksheet_as_dataframe`는 **시트 0번 행을 컬럼으로 소비**하고, 0번 행이 빈 칸인 열을 `unnamed_N`으로 드롭한다(extract_gsheet.py:120-146). 상단 요약행 편집으로 `호점` 열 0번 행이 비면 그 열 전체가 사라짐.

### Step 1 — 실패를 예외로 승격 (알림 복구, 확정 수정)
`dags/sales/Sales_Employee_Extract_Dags.py`
- 상단에 `from airflow.exceptions import AirflowException` 추가.
- 아래 실패 `return` 5곳을 `raise AirflowException(...)`로 교체:
  - 171행 `return "로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다."`
  - 185행 `return f"로드 실패: {str(e)}"` (except 블록) → `raise AirflowException(f"로드 실패: {e}") from e`
  - 210행 `return "로드 실패: 매핑 후 '호점' 컬럼을 찾을 수 없습니다."`
  - 221행 `return f"로드 실패: '{AUTOMATION_NOTE_COL}' 컬럼을 찾을 수 없습니다."`
  - 325행 저장 except `return f"❌ 저장 실패: {str(e)}"` → `raise AirflowException(f"저장 실패: {e}") from e`
- 성공 return(320행 `"✅ 성공..."`)은 유지.

효과: 실패 시 `default_args.on_failure_callback=on_failure_callback` 발동(이메일+텔레그램+heal_queue). `retries:1`로 1회 재시도 후 최종 실패 시 알림.

### Step 2 — 헤더 감지 견고화 (Step 0 실측 반영)
159-171행 감지 블록 수정:
- 정규화 통일: 감지·정제 모두 공백/줄바꿈 제거 후 비교. `re`는 이미 import됨.
  예: `norm = lambda v: re.sub(r'\s+', '', str(v)) if pd.notna(v) else ''`
- `호점`을 **데이터 행뿐 아니라 컬럼(`df_raw.columns`)에서도** 탐색.
  extract_gsheet가 0번 행을 컬럼으로 올린 경우, 컬럼에서 `호점`이 발견되면 `df_raw` 자체를 헤더 확정 df로 사용.
- Step 0에서 `호점` 열이 `unnamed_N` 드롭으로 사라진 게 원인이면: `extract_gsheet`는 공유 유틸이라 시그니처/동작 변경 지양. DAG 측에서 방어 처리하거나 드롭 이슈면 별도 협의 후 진행.

## Reference Code

### dags/sales/Sales_Employee_Extract_Dags.py (수정 대상 핵심 블록)
```python
from airflow.operators.python import PythonOperator
# (추가 필요) from airflow.exceptions import AirflowException

def load_employee_from_gsheet(**context):
    try:
        df_raw = extract_gsheet(url=EMPLOYEE_GSHEET_URL, sheet_name=EMPLOYEE_SHEET_NAME,
                                credentials_path=DEFAULT_CREDENTIALS_PATH)
        # 헤더 자동 감지: '호점' 있는 행 찾기
        header_row_idx = None
        for idx in range(len(df_raw)):
            row = df_raw.iloc[idx]
            row_list = [str(v).strip() for v in row.tolist()]   # ← strip만 함 (정제는 \n 제거 → 불일치)
            if '호점' in row_list:
                header_row_idx = idx
                break
        if header_row_idx is None:
            return "로드 실패: '호점' 컬럼이 포함된 헤더 행을 찾을 수 없습니다."  # ← raise로 교체
        raw_header = [str(col).strip().replace('\n', '') if pd.notna(col) else '' for col in df_raw.iloc[header_row_idx].tolist()]
        df = df_raw.iloc[header_row_idx + 1:].copy()
        df.columns = raw_header
        df = df.reset_index(drop=True)
    except Exception as e:
        return f"로드 실패: {str(e)}"   # ← raise ... from e 로 교체
    # ... 매핑/필터 후: return "로드 실패: 매핑 후 ..."(210), return "로드 실패: '비고' ..."(221) 도 raise
    # 저장 except: return f"❌ 저장 실패: {str(e)}"(325) 도 raise ... from e

with DAG(..., default_args={"retries": 1, "email_on_failure": False,
                            "on_failure_callback": on_failure_callback}) as dag:
    ...
```

### modules/extract/extract_gsheet.py (0번 행=컬럼, 빈 헤더 열 드롭)
```python
def _read_worksheet_as_dataframe(spreadsheet, sheet_name=None):
    ...
    all_values = worksheet.get_all_values()       # 0번 행을 헤더로 사용
    raw_headers = all_values[0]
    headers = []
    for i, h in enumerate(raw_headers):
        h = str(h).strip()
        headers.append(h if h else f'unnamed_{i}')   # 빈 헤더 → unnamed_N
    rows = all_values[1:]
    df = pd.DataFrame(padded, columns=headers)
    unnamed_cols = [c for c in df.columns if c.startswith('unnamed_')]
    if unnamed_cols:
        df.drop(columns=unnamed_cols, inplace=True)  # ← 0번 행 빈 칸 열 전체 삭제
    return df
```

### modules/transform/utility/notifier.py (알림 발동 조건)
```python
def on_failure_callback(context) -> None:
    # 태스크가 raise로 failed 될 때만 호출됨. 문자열 return이면 SUCCESS라 미호출.
    ...
    _send_email_alert(subject, body)
    send_telegram(f"{body}\n{_TELEGRAM_TRIGGER}")
    enqueue_heal_task(context)   # heal_queue.jsonl 기록
# classify_failure가 "unknown"이어도 알림은 정상 전송됨
```

## Test Cases
1. [DAG import] `python -c "from dags.sales.Sales_Employee_Extract_Dags import dag"` → 기대: ImportError 없음, `AirflowException` import 정상
2. [실패→알림] `호점` 미발견 상황 재현(임시 잘못된 시트/행) 후 태스크 트리거 → 기대: 태스크 상태 `failed`, 이메일/텔레그램 수신, `heal_queue.jsonl`에 라인 1건 추가
3. [정상 경로] 실제 시트로 DAG 트리거 → 기대: `[감지] 헤더 위치` 로그 출력, `sales_employee.csv` 2경로 저장, 플랫폼별 건수 dict 출력
4. [Step 0 실측] 진단 스크립트 출력에서 확인한 실제 시트 구조로 헤더 감지가 통과하는지 재확인

Airflow 트리거/로그 조회 절차: `docs/codex/airflow-workflow.md`

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→4 순서 실행
  2. FAIL 항목 원인 분석
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `extract_gsheet`는 여러 DAG가 공유하는 유틸 → 시그니처/기본 동작 변경 지양. 변경이 불가피하면 별도 협의.
- 성공 return 값(XCom 메시지)은 유지.
- 이번 최소수정 범위: 실패 경로의 `return`→`raise` 승격 + 헤더 감지 정규화. print 전면 교체는 스코프 밖.
- 실패 문자열이 텔레그램 알림에 그대로 노출되므로 자격증명 등 민감정보 미포함.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code 패턴 따라가기
- 예외 타입: `AirflowException` (from `airflow.exceptions`)
- 변수명·함수명: snake_case, 기존 파일과 동일
- 주석: 최소화 (WHY만)
