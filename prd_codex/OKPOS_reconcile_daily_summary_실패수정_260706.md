# OKPOS reconcile_against_daily_summary 실패 수정

## Task
`DB_OKPOS_Sales_Dags`의 `reconcile_against_daily_summary`(t6c) task가 실패하고 알림이 발송됨.
원인은 OKPOS "일자별 종합매출(daily)" 롤업 집계 지연으로 daily=0인데, t6c가 이를 mismatch로 보고
`today` 페이지를 무의미하게 재다운로드(2회×4매장 Selenium)하다 worker가 kill되어 실패하는 것.
daily=0 케이스는 이미 앞 task `reconcile_zero_daily_against_sales_detail`(t5b)이 전담하므로,
t6c에서 daily=0일 때 재다운로드 대상에서 제외한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — `reconcile_against_daily_summary` 함수 (핵심)
- 수정(방어, 선택): `dags/db/DB_OKPOS_Sales_Dags.py` — t6c(및 t5b) PythonOperator에 `execution_timeout` 추가

## Background (로그·데이터로 확정된 근본 원인)
- OKPOS daily 페이지가 2026-07-05에 대해 **실제로 0원 반환** (재수집해도 Excel이 `영업일수합: 0 일`, 전 컬럼 0).
  → 서버측 daily 롤업 집계 지연(lag). `today`(okpos_order)·`receipt`(okpos_order_item)는 서로 일치하는 정상 매출.
- 실행 순서: `... t5 >> t5b >> t6c >> t6a >> t6b >> write_log >> trigger`
  - t5b(`reconcile_zero_daily_against_sales_detail`): daily=0 감지 → daily 페이지 2회 재수집 → 여전히 0 → 정상 포기 → SUCCESS. (정상 동작)
  - t6c(`reconcile_against_daily_summary`): daily(0) vs order(정상)를 mismatch로 판정 → **today 페이지 재다운로드** 시도.
    - order는 이미 정상(receipt와 일치)이라 today 재수집해도 값 동일 → daily=0은 절대 해소 불가(futile).
    - t5b가 방금 ~7.5분 Selenium 후 t6c가 또 Selenium 시작 → worker kill(로그가 traceback 없이 중단) → 실패·알림.
- daily=0 행은 이후 OKPOS가 집계를 게시하면 t5b가 lookback(120일) 재점검으로 자동 치유 → 데이터 수동 보정 불필요.

## Implementation Steps

1. **핵심 수정** — `DB_OKPOS_Sales.py::reconcile_against_daily_summary`, `remaining` 루프(약 3530~3533행).
   `expected`를 구한 직후 daily 합계가 0 이하이면 mismatch 대상에서 제외하는 가드 추가.

   변경 전:
   ```python
   for sale_date, store in remaining:
       expected, reason = _load_daily_expected(sale_date, store)
       if reason or expected is None:
           continue
       ym = sale_date[:7]
   ```

   변경 후:
   ```python
   for sale_date, store in remaining:
       expected, reason = _load_daily_expected(sale_date, store)
       if reason or expected is None:
           continue
       # daily 합계가 0이면 OKPOS 일자별 종합매출이 아직 미집계(서버측 lag)된 상태이며,
       # today 재다운로드로는 절대 해소되지 않는다. zero-daily 보정
       # (reconcile_zero_daily_against_sales_detail, t5b)이 daily 페이지 재수집을 전담하므로 여기서는 스킵.
       if expected <= 0:
           continue
       ym = sale_date[:7]
   ```
   - 효과: daily=0 케이스는 `mism_today`에 담기지 않음 → Selenium 재다운로드 0회 → `daily 기준 reconcile 완료: mismatch 없음` 반환 → SUCCESS.
   - daily가 정상 게시된(>0) 뒤 today와 다른 진짜 불일치(today 누락수집 등)는 기존대로 재수집 보호막 유지.

2. **방어(선택)** — `dags/db/DB_OKPOS_Sales_Dags.py`의 t6c(`reconcile_against_daily_summary`) PythonOperator에
   `execution_timeout=timedelta(minutes=15)` 추가(`from datetime import timedelta` 이미 import됨). t5b도 동일 권장.
   향후 Selenium 지연 시 zombie kill(무-traceback 실패) 대신 깔끔한 timeout 실패로 전환.

## Reference Code
### modules/transform/pipelines/db/DB_OKPOS_Sales.py (reconcile_against_daily_summary 판정 루프, 3524~3556)
```python
    remaining: list[tuple[str, dict]] = [(d, s) for d in sale_dates for s in STORES if _should_collect(s["name"], d)]
    for attempt in range(1, max_attempts + 1):
        mism_today: list[tuple[str, dict]] = []
        mism_receipt: list[tuple[str, dict]] = []
        details: list[str] = []

        for sale_date, store in remaining:
            expected, reason = _load_daily_expected(sale_date, store)
            if reason or expected is None:
                continue
            ym = sale_date[:7]
            store_short = store["name"].replace("도리당 ", "", 1)
            order_csv = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_order.csv"

            order_sum, oreason = _sum_csv(order_csv, sale_date, "실매출액")

            # order vs daily 비교 (order CSV는 반품 행이 음수로 저장되어 직접 합산 = 순매출)
            if order_sum is not None and abs(expected - order_sum) > tol:
                mism_today.append((sale_date, store))
                details.append(f"{sale_date}__{store['name']}:daily={expected}, order={order_sum}, diff={expected-order_sum}")

        if not mism_today and not mism_receipt:
            msg = f"daily 기준 reconcile 완료: mismatch 없음 (tol={tol}) | attempts={attempt}"
            logger.info(msg)
            return msg

        logger.warning("daily 기준 mismatch 발견(attempt=%d): today=%d, receipt=%d | 예: %s", attempt, len(mism_today), len(mism_receipt), details[:10])
        saved_t = _redownload_page("today", mism_today, attempt_tag=str(attempt))
```

## Test Cases
1. [문법] `python -c "import ast; ast.parse(open('modules/transform/pipelines/db/DB_OKPOS_Sales.py',encoding='utf-8').read()); print('OK')"` → 기대: `OK`
2. [가드 존재] `python -c "import re,io; s=open('modules/transform/pipelines/db/DB_OKPOS_Sales.py',encoding='utf-8').read(); assert 'if expected <= 0:' in s; print('guard OK')"` → 기대: `guard OK`
3. [DAG import] `python -c "import ast; ast.parse(open('dags/db/DB_OKPOS_Sales_Dags.py',encoding='utf-8').read()); print('DAG OK')"` → 기대: `DAG OK` (execution_timeout 추가 시)
4. [실데이터 재현] 2026-07-05 4개 매장(동두천지행점/삼송점/평택비전점/송파삼전점) daily=0, order>0 상태에서
   수정 후 판정 루프가 `mism_today`를 **빈 리스트**로 만들어야 함.
   확인 경로: `RAW_OKPOS_SALES/brand=도리당/store=*/ym=2026-07/okpos_daily.csv`(2026-07-05 실매출액=0) & `okpos_order.csv`(2026-07-05 행 존재).
5. [컨테이너 재실행] 실패 run의 t6c만 clear 후 재실행(`_reload_and_call`이 모듈 reload):
   `docker compose exec airflow-scheduler airflow tasks run DB_OKPOS_Sales_Dags reconcile_against_daily_summary scheduled__2026-07-05T21:10:00+00:00`
   → 기대: 로그에 `reconcile 완료: mismatch 없음` + `Marking task as SUCCESS`, **브라우저 실행 로그 없음**.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `reconcile_zero_daily_against_sales_detail`(t5b) 로직은 **수정 금지** — daily=0 재수집을 이미 올바르게 전담함.
- daily>0 인 정상 mismatch의 today 재다운로드 경로는 **제거하지 말 것** — 진짜 today 누락수집 보호막.
- daily=0 stale 행을 코드로 강제 삭제/보정하지 말 것 — OKPOS 집계 게시 후 t5b가 자동 치유.
- DAG 파일에는 오케스트레이션(execution_timeout 등)만 추가, 비즈니스 로직 금지.
- 기존 로그 메시지 포맷/한글 유지, print 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트/주석: 기존 파일과 동일하게 (주석은 WHY만 최소화)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- execution_timeout 추가 여부 판단 애매 시: t6c에 15분 추가(권장), t5b는 선택
