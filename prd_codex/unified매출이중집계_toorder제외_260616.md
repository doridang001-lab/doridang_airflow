# unified_sales toorder 중복 제외 (okpos 매장)

## Task
unified_sales가 okpos 매장(삼송/평택비전/송파삼전/동두천지행)의 매출을 `okpos`와 `toorder` 두 소스로
**이중 집계**한다. 실데이터 검증 결과 toorder의 홀 매출은 okpos 홀과 정확히 일치(송파삼전 1,014,100=1,014,100,
평택비전 106,600=106,600)하고 배달은 posfeed와 중복이다. **`toorder` 변환에서 okpos 매장 행을 제외**해
홀=okpos, 배달=posfeed(또는 okpos POS)로 단일화한다. 비-okpos 매장의 toorder는 유지.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- selenium 의존 모듈(`DB_OKPOS_Sales`) 직접 import 금지 — 무거운 의존성(undetected_chromedriver) 유입

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py`
  - import에 `RAW_OKPOS_SALES`, `functools.lru_cache` 추가
  - okpos 매장명 동적 도출 헬퍼 추가
  - `_transform_df()`에 okpos 매장 행 제외 필터 추가

## Implementation Steps

1. **import 보강** (현재 line 13~17 부근)
   - `from functools import lru_cache` 추가
   - `from modules.transform.utility.paths import ANALYTICS_DB` → `from modules.transform.utility.paths import ANALYTICS_DB, RAW_OKPOS_SALES`

2. **okpos 매장명 집합 헬퍼 추가** (모듈 레벨, 하드코딩 금지 — raw 파티션에서 도출)
   ```python
   @lru_cache(maxsize=1)
   def _okpos_store_names() -> frozenset:
       names = set()
       base = RAW_OKPOS_SALES
       if base.exists():
           for p in base.glob("brand=*/store=*"):
               if p.is_dir():
                   names.add(p.name.split("store=", 1)[-1].strip())
       return frozenset(n for n in names if n)
   ```
   - `RAW_OKPOS_SALES` = `analytics/okpos_sales_raw`. 파티션 `brand=도리당/store=송파삼전점/...` 에서
     `store=` 뒤 short 매장명(`송파삼전점` 등)을 모은다.

3. **`_transform_df()`에 okpos 매장 제외 필터 추가**
   - 위치: store 정규화 직후 — 현재 `df["store"] = _strip_brand(df["_store_norm"])` 와
     `df = df.drop(columns=["_store_norm"])` (현재 line 91~92) **바로 다음**.
   - toorder의 `df["store"]`는 이미 `_strip_brand` 적용된 short명이라 okpos raw 매장명과 같은 그레인으로 비교 가능.
   ```python
   okpos_stores = _okpos_store_names()
   if okpos_stores:
       _before = len(df)
       df = df[~df["store"].astype(str).str.strip().isin(okpos_stores)].copy()
       _dropped = _before - len(df)
       if _dropped:
           logger.info("toorder: okpos 매장 중복 제외 %d행 (stores=%s)", _dropped, sorted(okpos_stores))
   if df.empty:
       return pd.DataFrame(columns=UNIFIED_COLUMNS)
   ```

4. **정정(기존 중복 제거)**: 코드 반영 후 `DB_UnifiedSales`를 `conf {"backfill": true}` 또는
   영향 기간 `conf {"sale_date": "YYYY-MM-DD"}`로 재빌드. `_save_unified_daily`의 source-aware replace가
   기존 toorder 행을 교체(okpos 매장 toorder 행 0건이 됨).

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_toorder.py (현재 import + _transform_df 앞부분)
```python
import logging
from datetime import datetime, timedelta
import pandas as pd
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.store_normalize import (
    normalize as _normalize_store_names,
    strip_brand as _strip_brand,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, _load_store_map, _lookup_store_meta,
    _make_unified_pk, _save_unified_daily, _to_int_series,
)
TOORDER_SOURCE = "toorder"

def _transform_df(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df = df[df["date"].astype(str).str.strip() == date_str].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    df["price"] = _to_int_series(df["price"])
    df = df[df["price"] > 0].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    df["_store_norm"] = _normalize_store_names(df["store"].fillna("").astype(str).str.strip())
    df["brand"] = df["_store_norm"].map(_extract_brand)
    df["store"] = _strip_brand(df["_store_norm"])
    df = df.drop(columns=["_store_norm"])
    # ← (여기에 Step 3 okpos 매장 제외 필터 삽입)
    df["platform"] = df["platform"].fillna("").astype(str).str.strip().map(lambda v: _PLATFORM_MAP.get(v, v))
    df = df.groupby(["brand", "store", "platform"], as_index=False).agg(price=("price", "sum"))
    # ... (이하 store_map/region/order_type/order_id/total_price 등 기존 로직 유지)
```

### modules/transform/utility/paths.py (재사용 상수)
```python
def resolve_raw_okpos_sales() -> Path:
    env_path = os.getenv("RAW_OKPOS_SALES")
    if env_path:
        return Path(env_path)
    return resolve_analytics_db() / "okpos_sales_raw"

RAW_OKPOS_SALES = resolve_raw_okpos_sales()   # analytics/okpos_sales_raw
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import _transform_df, _okpos_store_names"` → ImportError 없음
2. [헬퍼] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import _okpos_store_names; print(sorted(_okpos_store_names()))"` → 기대: okpos 매장 short명 집합(예: ['동두천지행점','삼송점','송파삼전점','평택비전점']) 출력 (raw 존재 시)
3. [필터 동작] toorder 입력에 okpos 매장이 있어도 `_transform_df` 결과 `store`에 okpos 매장 0건:
   ```python
   python -X utf8 -c "import pandas as pd; from modules.transform.pipelines.db.DB_UnifiedSales_toorder import _transform_df,_okpos_store_names; \
   ok=sorted(_okpos_store_names())[:1] or ['송파삼전점']; \
   df=pd.DataFrame({'date':['2026-06-15']*2,'store':['도리당 '+ok[0],'도리당 비okpos가상점'],'platform':['배민1','배민1'],'price':[1000,2000]}); \
   out=_transform_df(df,'2026-06-15'); print('okpos rows:', out['store'].isin(_okpos_store_names()).sum())"
   ```
   → 기대: `okpos rows: 0`
4. [정정 후 데이터] `DB_UnifiedSales` 재빌드 후 `MART_DB/unified_sales_grp/unified_sales_260615.parquet`에서
   `source=toorder` & okpos 매장 행 = 0건, 송파삼전 홀(okpos)=1,014,100 유지.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행 (4는 재빌드 후)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 비-okpos 매장 toorder 행은 반드시 유지 (전체 toorder 제거 금지).
- okpos 매장 목록 하드코딩 금지 — `RAW_OKPOS_SALES` 파티션에서 동적 도출.
- `DB_OKPOS_Sales`(selenium 의존) import 금지.
- `_transform_df`의 반환 스키마(`UNIFIED_COLUMNS`)·이후 로직 변경 금지, 필터만 추가.
- okpos 매장 중 posfeed 미수집 매장(예: 평택비전점)은 배달이 okpos POS에 포함되는지 Test Case 4에서 확인.
  누락 매장 발견 시 해당 매장만 예외 처리(드롭 제외).

## Do Not Ask — Decide Yourself
- 파일 존재 시: 덮어쓰기(수정)
- import 모호하면: Reference Code 패턴 따라가기
- 타입 힌트/주석: 기존 파일과 동일, 주석은 WHY만 최소화
- 변수명·함수명: snake_case, 기존 파일과 동일
