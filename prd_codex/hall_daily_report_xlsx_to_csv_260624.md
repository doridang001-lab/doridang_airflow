# hall_daily_report xlsx → CSV (매출+마케팅 통합, 회귀예측)

## Task
`build_daily_tracking_excel`이 만들던 `hall_daily_report_*.xlsx`를 CSV로 전환한다.
매출+마케팅 지표를 **1개 wide-format 테이블**에 통합하고,
당월 실적이 없는 남은 날짜는 `sklearn.LinearRegression`으로 예측값을 채워 `구분` 컬럼으로 구분한다.

---

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import MART_DB` (하드코딩 금지)

---

## Files to Create / Modify

- **신규** `modules/transform/pipelines/db/DB_Hall_Daily_CSV.py`
- **수정** `dags/db/DB_Hall_Sales_Target_Dags.py` — `t_daily_excel` → `t_daily_csv` 교체

---

## Implementation Steps

### 1. `DB_Hall_Daily_CSV.py` 신규 작성

#### 상수 및 import

```python
import calendar
import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from modules.transform.pipelines.db.DB_Hall_Sales_Target import classify_hall_time_slots
from modules.transform.utility.paths import MART_DB

logger = logging.getLogger(__name__)

STORE_NAME   = "송파삼전점"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
MKT_CSV      = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"
OUTPUT_CSV   = MART_DB / "hall_sales_target" / "hall_daily_report.csv"
```

---

#### `_load_sales(ym: str) -> pd.DataFrame`

```python
# unified_sales_grp/*.parquet → store='송파삼전점', source='okpos', platform='홀'
# classify_hall_time_slots() 로 점심/저녁 분류
# sale_date 기준 일별 집계 반환

# 반환 컬럼:
# sale_date (date), total_amt (int), tot_order_cnt (int),
# 점심_매출 (int), 점심_주문건수 (int),
# 저녁_매출 (int), 저녁_주문건수 (int)
```

구현 포인트:
- `files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))`
- `pd.read_parquet()` → store / source / platform 필터
- `classify_hall_time_slots(df)` 호출 (기존 함수 재사용)
- `sale_date` 기준 groupby:
  - `total_price` 합 → `total_amt`
  - `order_cnt` 합 → `tot_order_cnt`
  - `time_slot == '점심'` 필터 후 → `점심_매출`, `점심_주문건수`
  - `time_slot == '저녁'` 필터 후 → `저녁_매출`, `저녁_주문건수`
- `sale_date` 컬럼을 `datetime.date` 형으로 변환 후 반환

---

#### `_load_marketing(ym: str) -> pd.DataFrame`

```python
# hall_marketing_target.csv → 해당 월(ym) 필터
# 반환 컬럼:
# sale_date (date), 플레이스_유입, 홍보물_배포,
# 쿠폰_회수건수, 인스타_노출, 당근_노출, 네이버_오더 (모두 int)
```

구현 포인트:
- CSV가 없으면 빈 DataFrame 반환 (logger.warning)
- `기준일자` 컬럼 우선, 없으면 `입력날짜` 사용
- 숫자 컬럼 쉼표 제거 후 `pd.to_numeric(errors='coerce').fillna(0).astype(int)`
- 컬럼명 오타 처리: `인스타_노출_traget` → `인스타_노출`

---

#### `_fill_predictions(actual_df, mkt_df, all_dates, daily_target) -> pd.DataFrame`

입력:
- `actual_df`: `_load_sales()` 결과 (실적 있는 날만)
- `mkt_df`: `_load_marketing()` 결과
- `all_dates`: 당월 전체 날짜 리스트 (`date` 객체)
- `daily_target`: `DAILY_TRACKING_TARGET` dict

반환: `sale_date` + `구분` + 모든 수치 컬럼을 가진 full DataFrame (월 전체 날짜)

구현 포인트:
```python
# 예측 대상 컬럼 (마케팅 포함)
PRED_METRICS = [
    "total_amt", "tot_order_cnt",
    "점심_매출", "점심_주문건수",
    "저녁_매출", "저녁_주문건수",
    "플레이스_유입", "홍보물_배포", "쿠폰_회수건수",
    "인스타_노출", "당근_노출", "네이버_오더",
]

# 실적 날짜 = actual_df + mkt_df 교집합 (둘 다 있는 날만 실제값)
# 남은 날 = all_dates - 실적 날짜

# LinearRegression 적용 per metric:
X_train = [[d.day] for d in actual_dates]  # day_of_month
if len(actual_dates) < 3:
    pred_val = daily_target.get(metric_key, 0)  # fallback
else:
    model = LinearRegression().fit(X_train, y_train)
    pred_val = max(0, round(model.predict([[future_day]])[0]))

# 쿠폰_회수율은 직접 예측하지 않음 (누적 건수로 재계산)
```

---

#### `_add_cumulative(df: pd.DataFrame) -> pd.DataFrame`

`sale_date` 오름차순 정렬 후 cumsum/파생 컬럼 추가:

| 추가 컬럼 | 계산식 |
|-----------|--------|
| 누적매출 | `total_amt.cumsum()` |
| 누적주문수 | `tot_order_cnt.cumsum()` |
| 객단가 | `total_amt / tot_order_cnt` (0 방지) |
| 누적_객단가 | `누적매출 / 누적주문수` |
| 일영수건수(일평균) | `누적매출 / 경과일수` (경과일 = row index+1) |
| 점심_객단가 | `점심_매출 / 점심_주문건수` |
| 누적_점심_매출 | `점심_매출.cumsum()` |
| 누적_점심_주문건수 | `점심_주문건수.cumsum()` |
| 누적_점심_객단가 | `누적_점심_매출 / 누적_점심_주문건수` |
| 저녁_객단가 | `저녁_매출 / 저녁_주문건수` |
| 누적_저녁_매출 | `저녁_매출.cumsum()` |
| 누적_저녁_주문건수 | `저녁_주문건수.cumsum()` |
| 누적_저녁_객단가 | `누적_저녁_매출 / 누적_저녁_주문건수` |
| 플레이스_유입_누적 | `플레이스_유입.cumsum()` |
| 플레이스_유입_일평균 | `플레이스_유입_누적 / 경과일수` |
| 홍보물_배포_누적 | `홍보물_배포.cumsum()` |
| 쿠폰_회수건수_누적 | `쿠폰_회수건수.cumsum()` |
| 쿠폰_회수율 | `쿠폰_회수건수_누적 / 홍보물_배포_누적 * 100` (%, 소수 1자리) |
| 인스타_노출_누적 | `인스타_노출.cumsum()` |
| 당근_노출_누적 | `당근_노출.cumsum()` |
| 네이버_오더_누적 | `네이버_오더.cumsum()` |
| 네이버_오더_일평균 | `네이버_오더_누적 / 경과일수` (소수 1자리) |

---

#### `_add_targets(df, monthly_targets, marketing_monthly_targets, daily_target) -> pd.DataFrame`

`ym = df['sale_date'].dt.strftime('%Y-%m').iloc[0]` 로 해당 월 목표 조회

추가 컬럼 (모든 행에 동일 값):

| 컬럼 | 소스 |
|------|------|
| 월매출_목표 | `monthly_targets[ym]['sale']` |
| 객단가_목표 | `monthly_targets[ym]['aov']` |
| 일영수건수_목표 | `monthly_targets[ym]['orders']` |
| 점심_일매출목표 | `monthly_targets[ym]['lunch_sale']` |
| 저녁_일매출목표 | `monthly_targets[ym]['dinner_sale']` |
| 플레이스_유입_목표 | `marketing_monthly_targets[ym]['플레이스_유입']` |
| 플레이스_유입_일평균_목표 | `800` (MARKETING_DAILY_TARGET 값) |
| 홍보물_배포_목표 | `marketing_monthly_targets[ym]['홍보물_배포']` |
| 쿠폰_회수율_목표 | `0.7` (%) |
| 인스타_노출_목표 | `marketing_monthly_targets[ym]['인스타_노출']` |
| 당근_노출_목표 | `marketing_monthly_targets[ym]['당근_노출']` |
| 네이버_오더_일평균_목표 | `15` |

---

#### `_add_achievement(df) -> pd.DataFrame`

| 컬럼 | 계산식 |
|------|--------|
| 플레이스_유입_달성률 | `플레이스_유입_누적 / 플레이스_유입_목표 * 100` (%) |
| 홍보물_배포_달성률 | `홍보물_배포_누적 / 홍보물_배포_목표 * 100` |
| 쿠폰_회수율_달성률 | `쿠폰_회수율 / 쿠폰_회수율_목표 * 100` |
| 인스타_노출_달성률 | `인스타_노출_누적 / 인스타_노출_목표 * 100` |
| 당근_노출_달성률 | `당근_노출_누적 / 당근_노출_목표 * 100` |
| 네이버_오더_달성률 | `네이버_오더_일평균 / 네이버_오더_일평균_목표 * 100` |

달성률은 소수 1자리 반올림. 분모가 0이면 `None`/`NaN`.

---

#### 메인 함수 `build_daily_tracking_csv`

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

    actual_df  = _load_sales(ym)
    mkt_df     = _load_marketing(ym)
    df = _fill_predictions(actual_df, mkt_df, all_dates, daily_target)
    df = _add_cumulative(df)
    df = _add_targets(df, monthly_targets, marketing_monthly_targets, daily_target)
    df = _add_achievement(df)

    # 컬럼 순서 고정 (최종 CSV 컬럼 순서표 참조)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    # 날짜별 스냅샷
    snap = OUTPUT_CSV.parent / f"hall_daily_report_{today.strftime('%y%m%d')}.csv"
    df.to_csv(snap, index=False, encoding="utf-8-sig")

    logger.info("hall_daily_report.csv 저장 완료: %s", OUTPUT_CSV)
    return f"hall_daily_report.csv 저장 완료: {OUTPUT_CSV}"
```

---

#### 최종 CSV 컬럼 순서 (wide format, 1개 테이블)

```
sale_date, 구분,
total_amt, tot_order_cnt, 객단가,
점심_매출, 점심_주문건수, 점심_객단가,
저녁_매출, 저녁_주문건수, 저녁_객단가,
누적매출, 누적주문수, 누적_객단가, 일영수건수(일평균),
누적_점심_매출, 누적_점심_주문건수, 누적_점심_객단가,
누적_저녁_매출, 누적_저녁_주문건수, 누적_저녁_객단가,
월매출_목표, 객단가_목표, 일영수건수_목표, 점심_일매출목표, 저녁_일매출목표,
플레이스_유입, 홍보물_배포, 쿠폰_회수건수, 인스타_노출, 당근_노출, 네이버_오더,
플레이스_유입_누적, 플레이스_유입_일평균,
홍보물_배포_누적,
쿠폰_회수건수_누적, 쿠폰_회수율,
인스타_노출_누적,
당근_노출_누적,
네이버_오더_누적, 네이버_오더_일평균,
플레이스_유입_목표, 플레이스_유입_일평균_목표,
홍보물_배포_목표,
쿠폰_회수율_목표,
인스타_노출_목표,
당근_노출_목표,
네이버_오더_일평균_목표,
플레이스_유입_달성률, 홍보물_배포_달성률, 쿠폰_회수율_달성률,
인스타_노출_달성률, 당근_노출_달성률, 네이버_오더_달성률
```

---

### 2. `dags/db/DB_Hall_Sales_Target_Dags.py` 수정

기존 import + task 교체:

```python
# 제거
from modules.transform.pipelines.db.DB_Hall_Daily_Excel import build_daily_tracking_excel

# 추가
from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv
```

task 교체:

```python
# 기존 t_daily_excel 삭제, 아래로 대체
t_daily_csv = PythonOperator(
    task_id="build_daily_tracking_csv",
    python_callable=build_daily_tracking_csv,
    op_kwargs={
        "monthly_targets":           MONTHLY_TARGETS,
        "marketing_monthly_targets": MARKETING_MONTHLY_TARGETS,
        "daily_target":              DAILY_TRACKING_TARGET,
    },
)

# 의존성 (기존 t_daily_excel 자리 대체)
t_csv >> [t_excel, t_daily_csv, t_check_mkt]
t_excel >> t_llm_log
```

---

## Reference Code

### DB_Hall_Daily_Excel.py (경로 상수 및 import 패턴)

```python
from modules.transform.pipelines.db.DB_Hall_Sales_Target import classify_hall_time_slots
from modules.transform.utility.paths import MART_DB

STORE_NAME   = "송파삼전점"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"
MKT_CSV      = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"
OUTPUT_XLSX  = MART_DB / "hall_sales_target" / "hall_daily_report.xlsx"
```

### DB_Hall_Sales_Target.py (classify_hall_time_slots 시그니처)

```python
def classify_hall_time_slots(df: pd.DataFrame) -> pd.DataFrame:
    """점심(06~16시) / 저녁(16~24시) time_slot 컬럼 부여.
    order_time=00:00:00 취소행은 order_id 매칭으로 원 시간대 복원.
    """
    # ... (기존 구현 재사용, 수정 불필요)
```

### DB_Hall_Sales_Target_config.py (daily_target 키 구조)

```python
DAILY_TARGET = {
    "sale": 2_356_000, "aov": 38_000, "orders": 62,
    "lunch_sale": 812_000, "lunch_orders": 29, "lunch_aov": 28_000,
    "dinner_sale": 1_544_000, "dinner_orders": 33, "dinner_aov": 47_000,
}
MARKETING_DAILY_TARGET = {
    "플레이스_유입": 800, "홍보물_배포": 300, "쿠폰_회수": 7,
    "인스타_노출": 3_334, "당근_노출": 3_334, "네이버_오더": 15,
}
# daily_tracking_target 키: "sale","lunch_sale","lunch_orders","lunch_aov",
#   "dinner_sale","dinner_orders","dinner_aov","place","홍보물_배포",
#   "coupon","insta","karrot","naver"
```

---

## Test Cases

1. **모듈 import 확인**
   ```
   python -c "from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv"
   ```
   → ImportError 없음

2. **직접 실행 (로컬)**
   ```python
   from modules.transform.pipelines.db.DB_Hall_Daily_CSV import build_daily_tracking_csv
   from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets
   t, m, d = build_targets()
   print(build_daily_tracking_csv(t, m, d))
   ```
   → `hall_daily_report.csv 저장 완료: ...` 출력

3. **CSV 구조 검증**
   ```python
   import pandas as pd
   df = pd.read_csv("C:/Users/민준/OneDrive - 주식회사 도리당/data/mart/hall_sales_target/hall_daily_report.csv")
   assert "구분" in df.columns
   assert set(df["구분"].unique()) <= {"실제값", "예측값"}
   assert df["total_amt"].min() >= 0
   assert len(df) == 30  # 6월은 30일
   print(df[["sale_date","구분","total_amt","쿠폰_회수율","플레이스_유입_달성률"]].to_string())
   ```

4. **쿠폰 회수율 수작업 대조 (마지막 실제값 행)**
   ```python
   row = df[df["구분"]=="실제값"].iloc[-1]
   expected = round(row["쿠폰_회수건수_누적"] / row["홍보물_배포_누적"] * 100, 1)
   assert abs(row["쿠폰_회수율"] - expected) < 0.01
   ```

5. **DAG import 확인**
   ```
   python -c "from dags.db.DB_Hall_Sales_Target_Dags import dag"
   ```
   → ImportError 없음

---

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL → 원인 분석 (컬럼명 불일치, cumsum 순서, 날짜 타입 등)
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

---

## Constraints

- `DB_Hall_Daily_Excel.py` 파일 자체는 삭제하지 않는다 (기존 task_id 로그 보존)
- 예측값 행에서도 누적 컬럼은 연속적으로 cumsum이 이어져야 함 (실제값+예측값 합산)
- 쿠폰_회수율은 예측값 행에서도 직접 회귀예측하지 말고, 예측된 누적 건수로 재계산
- 음수 예측값은 0으로 clamp (`max(0, ...)`)
- 학습 데이터가 3일 미만이면 `daily_target` 값으로 fallback (회귀 불안정 방지)
- 달성률 분모 0이면 `None` (ZeroDivisionError 방지)
- CSV 인코딩: `utf-8-sig` (Excel에서 한글 깨짐 방지)
- 출력 컬럼 순서는 위 "최종 CSV 컬럼 순서" 표를 반드시 준수

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
