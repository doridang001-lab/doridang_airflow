# VOC Transform DAG XCom None 오류 반복 수정

## Task
`업로드_temp` 폴더에 처리할 파일이 없을 때 `upload_store_summary_to_gsheet` / `upload_topic_summary_to_gsheet` 태스크가 `FileNotFoundError`를 던져 retries=3 × 재시도로 텔레그램 알림이 반복 발송되고 있다. upstream(`load_files`, `preprocess_df`)이 이미 "파일 없음 = None push + SUCCESS"로 설계된 것과 일관되게, 업로드 함수도 None XCom 수신 시 graceful skip으로 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지 (단, 이 파일은 기존에 print를 쓰고 있으므로 기존 스타일 유지)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- **수정**: `modules/transform/pipelines/strategy/SMP_crawling_toorder_voc_02_trans.py`
  - 함수: `_upload_to_gsheet()` (line ~1059)

## Implementation Steps

1. `_upload_to_gsheet()` 함수 내 XCom None 처리 로직 변경

   **현재 코드 (line 1063~1068):**
   ```python
   file_path_value = ti.xcom_pull(task_ids=cfg['xcom_task_id'], key=cfg['xcom_key'])
   if not file_path_value:
       raise FileNotFoundError(
           f"[{cfg['label']}] XCom 경로 없음 "
           f"(task_id={cfg['xcom_task_id']}, key={cfg['xcom_key']})"
       )
   ```

   **수정 후:**
   ```python
   file_path_value = ti.xcom_pull(task_ids=cfg['xcom_task_id'], key=cfg['xcom_key'])
   if not file_path_value:
       print(f"[{cfg['label']}] 업로드 스킵: 입력 데이터 없음 (XCom None)")
       return f"[{cfg['label']}] 스킵: 데이터 없음"
   ```

   파일이 없음을 에러로 처리하지 않고 조용히 종료. 실제 파일 경로는 있는데 파일이 없는 경우는 아래 `file_path.exists()` 체크(line ~1072)가 여전히 잡아줌.

## Reference Code

### SMP_crawling_toorder_voc_02_trans.py (수정 대상 함수)
```python
def _upload_to_gsheet(config_key: str, **context):
    cfg = GSHEET_CONFIG[config_key]
    ti = context['ti']

    file_path_value = ti.xcom_pull(task_ids=cfg['xcom_task_id'], key=cfg['xcom_key'])
    if not file_path_value:
        raise FileNotFoundError(          # ← 이 블록을 교체
            f"[{cfg['label']}] XCom 경로 없음 "
            f"(task_id={cfg['xcom_task_id']}, key={cfg['xcom_key']})"
        )

    file_path = Path(file_path_value)
    if not file_path.exists():
        raise FileNotFoundError(f"[{cfg['label']}] 파일 없음: {file_path}")
    ...

def upload_store_summary_to_gsheet(**context):
    _upload_to_gsheet('store_summary', **context)

def upload_topic_summary_to_gsheet(**context):
    _upload_to_gsheet('topic_summary', **context)

def upload_review_summary_to_gsheet(**context):
    _upload_to_gsheet('review_summary', **context)
```

### io.py (graceful skip 참조 패턴)
```python
# load_files() — 파일 없을 때 None push + 정상 종료
if not all_files:
    print(f"[❌] 파일 없음 (패턴: {patterns})")
    ti.xcom_push(key=xcom_key, value=None)
    return "0건 (파일 없음)"

# preprocess_df() — 입력 None일 때 None push + 정상 종료
if not parquet_path:
    print(f"[경고] 입력 데이터 없음")
    ti.xcom_push(key=output_xcom_key, value=None)
    return "0건 (입력 없음)"
```

## Test Cases

1. **모듈 import 확인**
   ```
   python -c "from modules.transform.pipelines.strategy.SMP_crawling_toorder_voc_02_trans import _upload_to_gsheet; print('ok')"
   ```
   → 기대: `ok` (ImportError 없음)

2. **Airflow DAG parse 확인**
   ```
   python -c "from dags.strategy.Strategy_ToOrderVoc_02_Transform_Dags import dag; print('ok')"
   ```
   → 기대: `ok`

3. **수동 트리거 검증** (Airflow UI에서)
   - `업로드_temp` 폴더가 비어있는 상태에서 DAG 수동 실행
   - `upload_store_summary_to_gsheet` → 기대: **success**, 로그에 "업로드 스킵: 입력 데이터 없음 (XCom None)"
   - `upload_topic_summary_to_gsheet` → 기대: **success**
   - 텔레그램 알림 미발송 확인

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1, 2 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `_upload_to_gsheet()` 내 `file_path.exists()` 체크(None 이후 실제 파일 존재 확인)는 **건드리지 말 것**
- `raise FileNotFoundError` 한 줄만 `print + return`으로 교체. 그 외 로직 변경 금지
- `upload_review_summary_to_gsheet`도 동일 함수를 공유하므로 자동 적용됨 (별도 수정 불필요)
- 파일 상단 print 스타일 그대로 유지 (이 파일은 logging 미사용)

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
