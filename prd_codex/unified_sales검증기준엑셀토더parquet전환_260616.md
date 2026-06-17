# unified_sales 검증 기준을 엑셀 → 토더 parquet로 전환

## Task
`DB_UnifiedSales` DAG의 일별/월별 검증은 현재 unified_sales DB 합계를 **AI daily collection(엑셀 일별매출보고서)** 와 비교한다. 비교 기준을 **ToOrder 일별 store/platform parquet**(`toorder_store_platform_daily.parquet`, `sum(price)`)로 교체하고, 메일 표시 라벨을 "엑셀합계" → "토더합계"로 변경한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `C:\airflow\modules\transform\pipelines\db\DB_UnifiedSales_validate.py`
- DAG 파일 `dags/db/DB_UnifiedSales.py` 는 **변경 없음** (task/콜러블 시그니처 동일)

토더 parquet 경로 (기존 `ANALYTICS_DB` 상수 재사용):
`ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"`
컬럼: `date, store, platform, price, receipts_num`

## Implementation Steps

1. **메일 라벨 변경** (`_COL_LABELS`, 약 line 71-80)
   - `"excel_total": "엑셀합계"` → `"excel_total": "토더합계"`
   - 내부 컬럼 키 `excel_total` 은 그대로 유지 (merge/diff/CSV/HTML 전반에서 사용 — 라벨 텍스트만 교체)

2. **일별 baseline 로더 교체** (`_load_excel_totals(target_date)`, 약 line 235-250)
   - `load_ai_daily_collection_detail_df(...)` 호출 제거
   - 토더 parquet 읽기 → `date == target_date` 필터 → `groupby(["date","store"])["price"].sum()`
   - 기존 merge 키에 맞게 rename: `date → sale_date`, `price → excel_total`
   - 반환 컬럼: `["sale_date", "store", "excel_total"]` (기존과 동일, 정수 round)
   - 파일 없음/빈 데이터 시 빈 DataFrame(`columns=["sale_date","store","excel_total"]`) 반환
   ```python
   def _load_excel_totals(target_date: str) -> pd.DataFrame:
       path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
       if not path.exists():
           logger.warning("토더 parquet 없음: %s", path)
           return pd.DataFrame(columns=["sale_date", "store", "excel_total"])
       df = pd.read_parquet(path, columns=["date", "store", "price"])
       df["sale_date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
       df = df[df["sale_date"] == target_date].copy()
       if df.empty:
           return pd.DataFrame(columns=["sale_date", "store", "excel_total"])
       df["store"] = df["store"].astype(str).str.strip()
       df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
       grouped = (
           df.groupby(["sale_date", "store"], as_index=False)["price"]
           .sum()
           .rename(columns={"price": "excel_total"})
       )
       grouped["excel_total"] = grouped["excel_total"].round().astype(int)
       return grouped
   ```

3. **월별 baseline 로더 교체** (`_load_excel_monthly_totals(target_ym)`, 약 line 382-405)
   - 토더 parquet 전체 로드 → `ym = sale_date[:7]` 파생 → `ym == target_ym` 필터
   - `groupby(["ym","store"])["price"].sum()` → rename `price → excel_total`
   - 반환 컬럼: `["ym", "store", "excel_total"]` (기존과 동일)
   ```python
   def _load_excel_monthly_totals(target_ym: str) -> pd.DataFrame:
       path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
       if not path.exists():
           logger.warning("토더 parquet 없음 (월별): %s", path)
           return pd.DataFrame(columns=["ym", "store", "excel_total"])
       df = pd.read_parquet(path, columns=["date", "store", "price"])
       df["ym"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m")
       df = df[df["ym"] == target_ym].copy()
       if df.empty:
           return pd.DataFrame(columns=["ym", "store", "excel_total"])
       df["store"] = df["store"].astype(str).str.strip()
       df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
       grouped = (
           df.groupby(["ym", "store"], as_index=False)["price"]
           .sum()
           .rename(columns={"price": "excel_total"})
       )
       grouped["excel_total"] = grouped["excel_total"].round().astype(int)
       return grouped
   ```

4. **법흥리점 토더/홀 제외 로직 제거** (사용자 확정)
   - `_load_parquet_totals` (약 line 217-223): 법흥리점 `beopheung_mask`/`exclude_mask` 블록 + 관련 logger 제거
   - `_load_parquet_monthly_totals` (약 line 365-371): 동일 블록 제거
   - 제거 후 `source`/`platform` 미사용 → 두 로더의 `read_parquet(columns=[...])` 에서 `source`, `platform` 제거하고 `["sale_date","store","total_price"]` 만 로드. 관련 `df["source"]`/`df["platform"]` 정규화 라인도 제거.

5. **미사용 import 정리**
   - `from modules.transform.ai_daily_collection_integrator import load_ai_daily_collection_detail_df` (약 line 22) 제거 (파일 내 다른 사용처 없음)

## Reference Code
### DB_UnifiedSales_validate.py (수정 대상 — 현재 상태 핵심부)
```python
from modules.transform.ai_daily_collection_integrator import load_ai_daily_collection_detail_df  # ← 제거
from modules.transform.utility.paths import (
    ANALYTICS_DB, COLLECT_DB, LLM_OUTPUT_DIR, LOCAL_DB, MART_DB, ONEDRIVE_DB,
)
UNIFIED_ROOT = MART_DB / "unified_sales_grp"

_COL_LABELS = {
    "sale_date": "날짜", "ym": "월", "store": "매장",
    "excel_total": "엑셀합계",   # ← "토더합계"
    "unified_total": "DB합계", "difference": "차이",
    "error_rate": "오차율(%)", "status": "상태",
}

# _load_parquet_totals / _load_parquet_monthly_totals 내부 (제거 대상 블록):
#   beopheung_mask = df["store"] == "법흥리점"
#   exclude_mask = beopheung_mask & ((df["source"] == "toorder") | (df["platform"] == "홀"))
#   df = df[~exclude_mask].copy()

# 기존 _load_excel_totals: load_ai_daily_collection_detail_df(base_dir=ANALYTICS_DB/"ai_daily_collection", ...)
#   → groupby(["sale_date","store"])["sales"].sum().rename(sales→excel_total)
```

### toorder parquet 생성부 (DB_Toorder_store_platform_daily.py — 스키마 확인용)
```python
_DEFAULT_DEST = ANALYTICS_DB / "toorder_daily_store_platform"
_PARQUET_NAME = "toorder_store_platform_daily.parquet"
_OUTPUT_COLUMNS = ["date", "store", "platform", "price", "receipts_num"]
# 키: ["date","store","platform"], price/receipts_num 은 int
```

## Test Cases
1. [import] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_validate"` → 기대: ImportError 없음 (load_ai_daily_collection_detail_df 제거 후에도 정상)
2. [DAG import] `python -c "from dags.db.DB_UnifiedSales import dag"` → 기대: 오류 없음
3. [일별 로더] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _load_excel_totals as f; print(f('2026-06-15').to_string())"` → 기대: `sale_date, store, excel_total` 컬럼, DuckDB `SELECT store, sum(price) FROM read_parquet('.../toorder_store_platform_daily.parquet') WHERE date='2026-06-15' GROUP BY store` 와 store별 합계 일치
4. [월별 로더] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _load_excel_monthly_totals as f; print(len(f('2026-06')))"` → 기대: ym='2026-06' 행 수 > 0, `ym,store,excel_total` 컬럼
5. [잔존 참조 검사] `grep -n "load_ai_daily_collection_detail_df\|법흥리점\|beopheung" modules/transform/pipelines/db/DB_UnifiedSales_validate.py` → 기대: 매치 없음

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
- 내부 컬럼 키 `excel_total` 은 변경 금지 (merge/diff/CSV/HTML 전반 사용). 표시 라벨만 "토더합계".
- DAG 파일, task 구성, 콜러블 시그니처(`validate_sales`, `validate_monthly_sales`) 변경 금지.
- 오차율 임계(2%), 일별 ±10만원 알림 조건, CSV 저장 경로/파일명(`unified_sales_error_*.csv`, `unified_sales_monthly_*.csv`) 변경 금지.
- 메일 제목/본문 타이틀 변경 금지 (라벨만 변경 요청).
- 경로는 `ANALYTICS_DB` 상수로만 구성 (절대경로 하드코딩 금지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
