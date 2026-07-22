# DB_DeliveryCommission 수집 커버리지/누락 진단 리포트

## Task
`DB_DeliveryCommission` 마트는 **주문이 걷힌 (날짜×매장)만** 행으로 만드는 sparse 구조라 정상이다.
(조사 결과: 송파삼전점은 마트에 정상 존재, 코드 버그 아님. "누락"의 실체는 상류 배민/쿠팡 수집 결측 —
7월 배민이 68개 매장 중 일 15~20개만 수집됨.)
배민 정산액은 주문서의 `입금예정금액`에 날짜×매장 기준 우리가게클릭 `광고지출`을 결합해 계산한다.
우리가게클릭 행 또는 값이 없으면 별도 누락 상태로 구분하지 않고 해당 주문 집계 행의 `광고지출=0`으로 처리한다.
정산 마트를 zero-fill 완전 그리드로 만들면 "미수집"과 "진짜 0매출"이 구분 불가해져 커미션 분석이 오염된다.
따라서 마트는 그대로 두고, **어떤 매장이 어느 날 안 걷혔는지**를 별도 커버리지/누락 진단 리포트(parquet)로 노출한다.
이 리포트로 매일 어떤 플랫폼·매장이 미수집인지 한눈에 파악한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/transform/utility/paths.py`
  - `DELIVERY_COVERAGE_PATH = MART_DB / "delivery_commission" / "delivery_coverage.parquet"` 추가
    (기존 `DELIVERY_COMMISSION_DIR` 옆, 269~270행 근처)
- 수정: `modules/transform/pipelines/db/DB_DeliveryCommission.py`
  - 신규 public 함수 `build_delivery_coverage_report()` 추가 (기존 `build_delivery_commission()`는 절대 변경 금지)
  - 기존 private 로더(`_load_baemin_orders_agg`, `_load_coupang_orders_agg`) **재사용**
- 수정: `dags/db/DB_DeliveryCommission_Dags.py`
  - 기존 `build_commission` task 뒤에 `build_coverage` PythonOperator 추가하고 `build_commission >> build_coverage`로 연결
    (기존 task/DAG 파라미터는 그대로)

## Implementation Steps

1. **경로 상수 추가** (`paths.py`)
   - `DELIVERY_COMMISSION_PATH` 정의 직후에 아래 추가:
     ```python
     DELIVERY_COVERAGE_PATH = DELIVERY_COMMISSION_DIR / "delivery_coverage.parquet"
     ```

2. **커버리지 집계 함수** (`DB_DeliveryCommission.py`)
   - `from modules.transform.utility.paths import ...` 에 `DELIVERY_COVERAGE_PATH` 추가 import.
   - 아래 함수 추가. 배민/쿠팡 각각 `_load_*_orders_agg()`가 반환하는 `(date, store, ...)` 집계를 사용해
     **플랫폼×날짜별 수집 매장 수 + 전체 매장(양 플랫폼 store 합집합) + 누락 매장 목록**을 만든다.
     ```python
     COVERAGE_COLUMNS = [
         "sale_date", "platform", "collected_stores", "total_stores",
         "missing_count", "missing_stores",
     ]

     def _coverage_from(agg: pd.DataFrame, platform: str, all_stores: list[str]) -> pd.DataFrame:
         total = len(all_stores)
         rows = []
         for date, grp in agg.groupby("date"):
             collected = sorted(set(grp["store"]))
             missing = [s for s in all_stores if s not in set(collected)]
             rows.append({
                 "sale_date": date,
                 "platform": platform,
                 "collected_stores": len(collected),
                 "total_stores": total,
                 "missing_count": len(missing),
                 "missing_stores": ", ".join(missing),
             })
         return pd.DataFrame(rows, columns=COVERAGE_COLUMNS)

     def build_delivery_coverage_report() -> str:
         baemin = _load_baemin_orders_agg()      # columns: date, store, total_amt, baemin_deposit_amt
         coupang = _load_coupang_orders_agg()     # columns: date, store, total_amt, coupang_settlement_amt
         all_stores = sorted(set(baemin["store"]) | set(coupang["store"]))
         result = pd.concat([
             _coverage_from(baemin, "배달의민족", all_stores),
             _coverage_from(coupang, "쿠팡이츠", all_stores),
         ], ignore_index=True)
         if result.empty:
             raise RuntimeError("delivery_coverage 결과 없음; 기존 리포트 보존")
         result = result.sort_values(["sale_date", "platform"]).reset_index(drop=True)
         _write_parquet_atomic_generic(result, DELIVERY_COVERAGE_PATH, COVERAGE_COLUMNS)
         logger.info("delivery_coverage 저장 완료: %s rows=%s", DELIVERY_COVERAGE_PATH, len(result))
         return f"delivery_coverage {len(result)}행 -> {DELIVERY_COVERAGE_PATH}"
     ```
   - 기존 `_write_parquet_atomic`는 `OUTPUT_COLUMNS` 하드코딩이라 재사용 불가.
     컬럼 인자를 받는 범용 버전 `_write_parquet_atomic_generic(result, path, columns)`를 추가하고
     (기존 `_write_parquet_atomic` 본문을 복사해 `OUTPUT_COLUMNS` → 인자 `columns`로 치환),
     기존 `_write_parquet_atomic`는 그대로 두거나 새 함수에 위임하도록만 변경(동작 불변).

3. **DAG에 task 추가** (`DB_DeliveryCommission_Dags.py`)
   - import에 `build_delivery_coverage_report` 추가.
   - 기존 `build_commission` 아래에:
     ```python
     build_coverage = PythonOperator(
         task_id="build_delivery_coverage",
         python_callable=build_delivery_coverage_report,
     )
     build_commission >> build_coverage
     ```

## Reference Code
### modules/transform/pipelines/db/DB_DeliveryCommission.py (기존, 재사용 대상)
```python
from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB, BAEMIN_OUR_STORE_CLICKS_DB, COUPANG_ORDERS_DB, DELIVERY_COMMISSION_PATH,
)
OUTPUT_COLUMNS = ["sale_date","store","platform","total_amt","settlement_amount","diff_amt"]

def _load_baemin_orders_agg() -> pd.DataFrame:
    # 반환 columns = ["date","store","total_amt","baemin_deposit_amt"]
    ...

def _load_coupang_orders_agg() -> pd.DataFrame:
    # 반환 columns = ["date","store","total_amt","coupang_settlement_amt"]
    ...

def _write_parquet_atomic(result: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        result.to_parquet(temp_path, index=False, engine="pyarrow")
        verified = pd.read_parquet(temp_path)
        if list(verified.columns) != OUTPUT_COLUMNS or len(verified) != len(result):
            raise RuntimeError("delivery_commission 임시 parquet 검증 실패")
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

def build_delivery_commission() -> str:   # <- 변경 금지
    ...
```

### dags/db/DB_DeliveryCommission_Dags.py (기존)
```python
from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission
from modules.transform.utility.schedule import DB_DELIVERY_COMMISSION_TIME
...
    build_commission = PythonOperator(
        task_id="build_delivery_commission",
        python_callable=build_delivery_commission,
    )
```

## Test Cases
1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_coverage_report, build_delivery_commission"` → 기대: ImportError 없음
2. [경로 상수] `python -c "from modules.transform.utility.paths import DELIVERY_COVERAGE_PATH; print(DELIVERY_COVERAGE_PATH)"` → 기대: `.../mart/delivery_commission/delivery_coverage.parquet`
3. [DAG import] `python -c "from dags.db.DB_DeliveryCommission_Dags import dag; print([t.task_id for t in dag.tasks])"` → 기대: `['build_delivery_commission', 'build_delivery_coverage']` (순서 무관, 2개)
4. [기능 실행] `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_coverage_report as f; print(f())"` →
   기대: `delivery_coverage N행 -> ...` 출력, N>0
5. [결과 검증] 아래로 리포트 정합성 확인 → 기대: 각 플랫폼×날짜의 `collected_stores + missing_count == total_stores`,
   `missing_count > 0`인 행 존재(수집 결측이 실제 있으므로), 컬럼이 `COVERAGE_COLUMNS`와 일치
   ```python
   import pandas as pd
   from modules.transform.utility.paths import DELIVERY_COVERAGE_PATH
   d = pd.read_parquet(DELIVERY_COVERAGE_PATH)
   assert list(d.columns) == ["sale_date","platform","collected_stores","total_stores","missing_count","missing_stores"]
   assert (d["collected_stores"] + d["missing_count"] == d["total_stores"]).all()
   print(d[d["sale_date"]>="2026-07-18"].to_string())
   ```
6. [기존 마트 불변 회귀] `build_delivery_commission()` 실행 후 `delivery_commission.parquet` 컬럼이
   기존 `OUTPUT_COLUMNS`와 동일하고 송파삼전점 행이 그대로 존재하는지 확인 → 기대: 변화 없음

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
- `build_delivery_commission()`과 기존 `delivery_commission.parquet` 스키마/동작은 **절대 변경 금지** (정산 마트).
- 배민 우리가게클릭은 주문서 날짜×매장 행에 left join하고, 매칭 행 또는 `광고지출` 값이 없으면 구분 없이 `0`으로 채워 `입금예정금액-광고지출`을 계산한다.
- 우리가게클릭 미매칭을 별도 누락 유형·상태·행으로 만들지 않는다.
- 마트에 zero-fill/전체 그리드 확장 금지 — 결측은 **별도 커버리지 리포트로만** 표현한다.
- `_write_parquet_atomic` 기존 동작(=`OUTPUT_COLUMNS` 검증)은 유지. 범용 버전은 새 함수로 추가.
- 새 재시도/폴백/타임아웃 로직 추가 금지. 범위는 "커버리지 리포트 생성"에 한정.
- 전체 매장 기준선은 배민·쿠팡 원천 파티션 store 합집합으로 산출(별도 매장 마스터 파일 도입 금지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
