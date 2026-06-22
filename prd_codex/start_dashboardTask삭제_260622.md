# start_dashboard Task 삭제

## Task
`DB_Beamin_Macro_Dags.py`에서 `start_dashboard` task를 완전히 제거한다.
대시보드 서버 기동·브라우저 자동 열기는 운영 DAG 흐름과 무관한 부수 기능이므로 삭제.
DAG 첫 노드는 `precheck_manual_baemin_orders`가 된다.

## Files to Modify
- `dags/db/DB_Beamin_Macro_Dags.py`

## Implementation Steps

1. **`start_dashboard` 함수 삭제** (line 1394–1446)
   - `def start_dashboard(**context) -> str:` 부터 `return dashboard_url` 까지 전체 블록 제거
   - 함수 내 `import subprocess`, `import urllib.request`, `import time`은 로컬 import라 top-level 영향 없음

2. **`t_dash` PythonOperator 정의 삭제** (line 1459–1463)
   ```python
   # 아래 블록 전체 삭제
   t_dash = PythonOperator(
       task_id="start_dashboard",
       python_callable=start_dashboard,
       execution_timeout=timedelta(minutes=2),
   )
   ```

3. **task chain에서 `t_dash >>` 제거** (line 1520)
   ```python
   # 변경 전
   t_dash >> t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8

   # 변경 후
   t0 >> t1 >> t2 >> t3 >> [t4, t5, t6] >> t7 >> t8
   ```

## Test Cases

1. **Import 검증**
   `python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print('OK')"` → 기대: `OK` (ImportError 없음)

2. **start_dashboard 미존재 확인**
   `python -c "import dags.db.DB_Beamin_Macro_Dags as m; print(hasattr(m, 'start_dashboard'))"` → 기대: `False`

3. **task 목록 확인**
   `python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print([t.task_id for t in dag.tasks])"` → 기대: `start_dashboard`가 목록에 없음

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `webbrowser`, `subprocess`, `urllib.request`는 `start_dashboard` 함수 내에서만 사용 — 다른 함수에서 쓰이는지 먼저 grep 확인 후 top-level import가 없으면 그냥 함수만 삭제
- 나머지 task(`t0`~`t8`) 및 함수는 **절대 변경하지 않는다**
- 빈 줄·공백 정리 외 추가 리팩터링 금지

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
