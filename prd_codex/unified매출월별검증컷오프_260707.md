# unified매출 월별검증 부분수집일 컷오프

## Task
`unified_sales` 월별 검증 알림이 2026-07 대상에서 오차율 2% 이상 56건을 발화했는데, 실제 원인의 대부분(~38건)은 **검증 실행일(KST 전일=최신 미완성일)의 posfeed 배달 부분수집**이다. 최신 미완성일을 현재월 비교 창에서 제외해 오탐을 제거한다. (구조적 잔여 ~18건은 이번 범위 밖)

## Root Cause (근거 데이터)
- 검증 비교: `unified_total`(MART unified_sales_grp/*.parquet `total_price` 합, **toorder 소스 제외**) vs `excel_total`(기준값 = ANALYTICS toorder_store_platform_daily.parquet `price` 합), `error_rate = |unified-excel|/|excel|*100`, 2%↑ → error.
- `_resolve_target_date`는 항상 **KST 전일** 반환 → 7/7 실행 시 `target_date=2026-07-06`. 2026-07은 6일치(7/1~7/6)뿐이고 최신일 7/6이 미완성.
- 일별 전사 합계: 7/1~7/5는 toorder와 ±1% 일치. **7/6만 unified 8,957,702 vs toorder 50,393,921**(약 41M 누락), unified 7/6에 음수행 3,101개(합 −20,975,191, 취소/환불만 적재 = 배달 정산 지연).
- 7/6 제외 재계산 시 오차 2%↑ 매장 **56 → 18건**. 거의 모든 매장이 unified < toorder(전액 배달)로 쏠린 이유가 이것. → 데이터 오류 아님, 부분수집일 조기 발화.
- 잔여 18건(범위 밖): 다수가 unified > toorder(예: 광명철산 +112% = toorder에 배민 미릴레이 → toorder 기준값 결함), 일부 수동수집 테스트매장 소액차.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 타임존: 파일 상단 기존 `KST = ZoneInfo("Asia/Seoul")` 재사용 (신규 정의 금지)

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_validate.py`
  - `_load_parquet_monthly_totals` (line 299): `max_date` 인자 추가
  - `_load_excel_monthly_totals` (line 335): `max_date` 인자 추가
  - `validate_monthly_sales` (line 401): current_ym에만 컷오프 전달

## Implementation Steps

1. **`_load_parquet_monthly_totals`에 날짜 상한 인자 추가** (line 299)
   - 시그니처: `def _load_parquet_monthly_totals(target_ym: str, max_date: str | None = None) -> pd.DataFrame:`
   - 기존 `df["sale_date"]`는 이미 `%Y-%m-%d` 문자열로 변환됨(line 318). ym 필터(line 320) 직후 다음 추가:
     ```python
     if max_date is not None:
         df = df[df["sale_date"] <= max_date].copy()
         if df.empty:
             return pd.DataFrame(columns=["ym", "store", "unified_total"])
     ```

2. **`_load_excel_monthly_totals`에 날짜 상한 인자 추가** (line 335)
   - 시그니처: `def _load_excel_monthly_totals(target_ym: str, max_date: str | None = None) -> pd.DataFrame:`
   - toorder `date` 컬럼은 문자열(`YYYY-MM-DD`). ym 필터(line 344) 직후 다음 추가:
     ```python
     if max_date is not None:
         df = df[df["date"].astype(str).str[:10] <= max_date].copy()
         if df.empty:
             logger.warning("토더 parquet 데이터 없음 (월별, 컷오프): %s <= %s", target_ym, max_date)
             return pd.DataFrame(columns=["ym", "store", "excel_total"])
     ```
   - 주의: `date` 컬럼 타입이 datetime일 수 있으므로 `.astype(str).str[:10]`로 안전 비교.

3. **`validate_monthly_sales`에서 current_ym에만 컷오프 적용** (line 401)
   - `target_date = _resolve_target_date(**context)` 직후, 컷오프 상한(= target_date 전일) 계산:
     ```python
     cutoff_date = (
         datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=1)
     ).strftime("%Y-%m-%d")
     ```
   - 루프 내 `for ym in all_ym:` 에서 로드 호출을 다음처럼 분기:
     ```python
     max_date = cutoff_date if ym == current_ym else None
     parquet = _load_parquet_monthly_totals(ym, max_date=max_date)
     excel = _load_excel_monthly_totals(ym, max_date=max_date)
     ```
   - 과거월(`ym != current_ym`)은 `max_date=None` → 전체 비교 유지(회귀 없음).
   - 로그 1줄 추가 권장: `logger.info("월별 검증 컷오프: current_ym=%s max_date=%s", current_ym, cutoff_date)`

## Reference Code
### DB_UnifiedSales_validate.py (상단 import + 상수)
```python
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import (
    ANALYTICS_DB, COLLECT_DB, LLM_OUTPUT_DIR, LOCAL_DB, MART_DB, ONEDRIVE_DB,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

VALIDATION_DIR = MART_DB / "unified_sales_grp_error_list"
MONTHLY_FILE_PREFIX = "unified_sales_monthly_"
```

### 수정 대상 함수 현재 형태 (핵심부)
```python
def _resolve_target_date(**context) -> str:
    """ToOrder 검증 기준일은 DAG conf와 무관하게 항상 KST 기준 전일로 고정한다."""
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def _load_parquet_monthly_totals(target_ym: str) -> pd.DataFrame:
    ...
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["ym"] = df["sale_date"].str[:7]
    df = df[df["ym"] == target_ym].copy()          # ← 이 직후 max_date 필터 삽입
    ...
    grouped = df.groupby(["ym", "store"], as_index=False)["total_price"].sum() \
        .rename(columns={"total_price": "unified_total"})
    return grouped

def _load_excel_monthly_totals(target_ym: str) -> pd.DataFrame:
    path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
    df = pd.read_parquet(path, columns=["date", "store", "price"])
    df["ym"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
    df = df[df["ym"] == target_ym].copy()           # ← 이 직후 max_date 필터 삽입
    ...

def validate_monthly_sales(**context) -> str:
    target_date = _resolve_target_date(**context)   # ← 직후 cutoff_date 계산
    current_year = target_date[:4]
    current_ym = target_date[:7]
    all_ym = _get_parquet_year_months(current_year)
    saved, alert_sent = [], False
    for ym in all_ym:
        parquet = _load_parquet_monthly_totals(ym)  # ← max_date 전달로 수정
        excel = _load_excel_monthly_totals(ym)      # ← max_date 전달로 수정
        diff = _compute_monthly_diff(excel=excel, parquet=parquet)
        csv_path = _save_monthly_comparison_csv(diff=diff, target_ym=ym)
        saved.append(ym)
        if ym == current_ym:
            error_rows = diff[diff["error_rate"] >= 2].copy()
            if not error_rows.empty:
                _send_monthly_alert(ym, error_rows, csv_path, **context)
                alert_sent = True
    ...
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import validate_monthly_sales, _load_parquet_monthly_totals, _load_excel_monthly_totals"` → 기대: 에러 없음
2. [컷오프 동작 - unified] 
   `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _load_parquet_monthly_totals as f; a=f('2026-07'); b=f('2026-07', max_date='2026-07-05'); print(int(a['unified_total'].sum()), int(b['unified_total'].sum()))"`
   → 기대: 두 번째(컷오프)가 첫 번째보다 작음 (7/6 제외분만큼)
3. [현재월 오차 감소] `validate_monthly_sales`를 로컬 실행(또는 helper로 diff 재현) 후 생성된 `MART_DB/unified_sales_grp_error_list/unified_sales_monthly_2026-07.csv`의 `status=="error"` 행 수 → 기대: 56 → 약 18건
4. [구조적 잔존 확인] 위 CSV에서 `광명철산점` 행 → 기대: 여전히 error(오차율 ~110%대). 이는 toorder 기준값 결함(범위 밖), 정상 잔존
5. [과거월 회귀] 과거월(예: 2026-06) CSV 값이 수정 전과 동일 → 기대: 컷오프 미적용으로 변화 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 date 컬럼 타입/문자열 비교, current_ym 분기)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: TC1~3,5 PASS + TC4는 광명철산 잔존 확인 + Constraints 위반 없음
```

## Constraints
- `_resolve_target_date`는 수정하지 말 것 (일별 검증도 공유). 컷오프는 `validate_monthly_sales` 내부에서만 계산.
- 과거월(`ym != current_ym`)에는 절대 컷오프 적용 금지 — 회귀 발생.
- 일별 검증(`validate_sales`, `_load_parquet_totals`, `_load_excel_totals`)은 건드리지 말 것. 이번 조치는 **월별 함수 3개만**.
- 원인 2/3(toorder 미릴레이·수동수집 매장)은 이번 태스크 범위 밖 — 코드로 손대지 말 것.
- `KST` 신규 정의 금지, 기존 상단 상수 재사용.
- 날짜 비교는 문자열(`YYYY-MM-DD`) 사전순 비교로 통일 (타입 혼선 방지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`str | None` 사용)
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
