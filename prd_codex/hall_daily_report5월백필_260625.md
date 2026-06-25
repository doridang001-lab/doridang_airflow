# hall_daily_report 5월 백필 — build_daily_tracking_csv ym 파라미터 추가

## Task

`DB_Hall_Daily_CSV.py`가 2026-06-24에 처음 생성된 신규 파일이라 5월에 한 번도 실행된 적 없어 `hall_daily_report.csv`에 5월 데이터가 없다. `build_daily_tracking_csv`에 선택적 `ym` 파라미터를 추가해 특정 월 백필을 지원하고, DAG conf로 `{"ym": "2026-05"}` 트리거 시 5월을 처리할 수 있게 한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify

- `modules/transform/pipelines/db/DB_Hall_Daily_CSV.py` — `build_daily_tracking_csv` 함수 시그니처 및 내부 `ym` 파생 로직 수정
- `dags/db/DB_Hall_Sales_Target_Dags.py` — `t_daily_csv` 태스크 `op_kwargs`에 `ym` conf 지원 추가

## Implementation Steps

### 1. `build_daily_tracking_csv` 함수 시그니처 변경 (DB_Hall_Daily_CSV.py:423)

현재 코드:
```python
def build_daily_tracking_csv(
    monthly_targets: dict,
    marketing_monthly_targets: dict,
    daily_target: dict,
) -> str:
    today = date.today()
    ym = today.strftime("%Y-%m")
    year, month = today.year, today.month
    days_in_month = calendar.monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
```

변경 후:
```python
def build_daily_tracking_csv(
    monthly_targets: dict,
    marketing_monthly_targets: dict,
    daily_target: dict,
    ym: str | None = None,
) -> str:
    today = date.today()
    if not ym:
        ym = today.strftime("%Y-%m")
    year, month = int(ym[:4]), int(ym[5:7])
    days_in_month = calendar.monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
```

> `today`는 스냅샷 파일명(`hall_daily_report_{today.strftime('%y%m%d')}.csv`)에 여전히 사용됨 — 삭제 금지.
> `ym` 파생 로직만 분리하면 됨.

### 2. DAG `t_daily_csv` 태스크에 conf 지원 추가 (DB_Hall_Sales_Target_Dags.py)

현재 op_kwargs:
```python
op_kwargs={
    "monthly_targets":           MONTHLY_TARGETS,
    "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
    "daily_target":              DAILY_TRACKING_TARGET,
},
```

변경 후:
```python
op_kwargs={
    "monthly_targets":           MONTHLY_TARGETS,
    "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
    "daily_target":              DAILY_TRACKING_TARGET,
    "ym": "{{ dag_run.conf.get('ym') or '' }}",
},
```

> 빈 문자열 `""`은 함수 내 `if not ym` 분기에서 `date.today()` 당월로 처리됨.
> `None`이 아닌 `""`로 처리하는 이유: Jinja 템플릿은 항상 문자열 반환.

## Reference Code

### DB_Hall_Daily_CSV.py — 현재 build_daily_tracking_csv (line 423~456)

```python
def build_daily_tracking_csv(
    monthly_targets: dict,
    marketing_monthly_targets: dict,
    daily_target: dict,
) -> str:
    today = date.today()
    ym = today.strftime("%Y-%m")
    year, month = today.year, today.month
    days_in_month = calendar.monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    actual_df = _load_sales(ym)
    mkt_df = _load_marketing(ym)
    df = _fill_predictions(actual_df, mkt_df, all_dates, daily_target)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df = _add_cumulative(df)
    df = _add_targets(df, monthly_targets, marketing_monthly_targets, daily_target)
    df["ym"] = ym.replace("-", "_")

    df = _merge_with_existing_report(df)

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = 0

    df = df[OUTPUT_COLUMNS]
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    snap = OUTPUT_CSV.parent / f"hall_daily_report_{today.strftime('%y%m%d')}.csv"
    df.to_csv(snap, index=False, encoding="utf-8-sig")

    logger.info("hall_daily_report.csv 저장 완료: %s", OUTPUT_CSV)
    return f"hall_daily_report.csv 저장 완료: {OUTPUT_CSV}"
```

### DB_Hall_Sales_Target_Dags.py — 현재 t_daily_csv 태스크 (line 66~74)

```python
t_daily_csv = PythonOperator(
    task_id="build_daily_tracking_csv",
    python_callable=build_daily_tracking_csv,
    op_kwargs={
        "monthly_targets":           MONTHLY_TARGETS,
        "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
        "daily_target":              DAILY_TRACKING_TARGET,
    },
)
```

## Test Cases

1. **함수 직접 백필 테스트**
   ```python
   python -c "
   from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
   from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv
   mt, mmt, dt = build_targets()
   result = build_daily_tracking_csv(mt, mmt, dt, ym='2026-05')
   print(result)
   "
   ```
   → 기대: `hall_daily_report.csv 저장 완료: ...` 출력, 에러 없음

2. **CSV 5월 행 확인**
   ```python
   python -c "
   import pandas as pd
   from modules.transform.utility.paths import MART_DB
   df = pd.read_csv(MART_DB / 'hall_sales_target' / 'hall_daily_report.csv')
   print(df[df['ym']=='2026_05'][['sale_date','ym','total_amt']].head())
   print('5월 행 수:', len(df[df['ym']=='2026_05']))
   "
   ```
   → 기대: `ym='2026_05'` 행 31개 출력

3. **6월 재실행 후 5월 데이터 보존 확인**
   ```python
   python -c "
   from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
   from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv
   mt, mmt, dt = build_targets()
   build_daily_tracking_csv(mt, mmt, dt)   # ym=None → 당월(6월)
   import pandas as pd
   from modules.transform.utility.paths import MART_DB
   df = pd.read_csv(MART_DB / 'hall_sales_target' / 'hall_daily_report.csv')
   print('월별 행수:', df.groupby('ym').size().to_dict())
   "
   ```
   → 기대: `{'2026_05': 31, '2026_06': 30}` 형태로 5월 데이터 보존 확인

4. **DAG import 오류 없음 확인**
   ```bash
   python -c "from dags.db.DB_Hall_Sales_Target_Dags import dag; print('OK')"
   ```
   → 기대: `OK` 출력, ImportError 없음

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~4 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `today` 변수는 스냅샷 파일명(`hall_daily_report_{today.strftime('%y%m%d')}.csv`)에 사용 중이므로 **삭제 금지**
- `ym` 파라미터 기본값은 `None` (또는 `""`), 명시적으로 `date.today()` 당월로 폴백
- `_merge_with_existing_report`는 수정하지 않음 — 기존 merge 로직 그대로 유지
- Jinja 템플릿에서 `None`은 문자열 `"None"`으로 렌더링되므로 `""`(빈 문자열)로 처리
- `monthly_targets`에 백필 월(2026-05)이 포함되어 있는지 먼저 확인 (`build_targets()`는 2026-04부터 생성하므로 포함됨 — 별도 수정 불필요)

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
