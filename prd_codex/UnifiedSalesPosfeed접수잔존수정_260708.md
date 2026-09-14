# UnifiedSales posfeed 잔존 / 접수 과대계상 수정

## Task
`DB_UnifiedSales` 파이프라인에서 과거일(어제 이후) posfeed 배달 매출이 잔존하며 "접수"(진행중) 주문을 정상 매출로 과대계상하는 문제를 수정한다. (1) 과거일 posfeed는 "배달완료"만 계상(접수 제외), today는 접수 포함 현행 유지. (2) 배민/쿠팡 reconcile가 수집월≠판매월(크롤 지연으로 월말 판매분이 다음 수집월 파일에 존재) 데이터를 못 찾아 posfeed가 수동으로 안 덮이는 문제를 수정.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 타입 힌트/주석 스타일: 기존 파일과 동일하게(주석은 WHY만 최소화)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` (Fix 1: 과거일 접수 제외)
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py` (Fix 2: 수집월 파티션 보정)
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` (Fix 2: 수집월 파티션 보정)

## Background — 근본 원인 (실데이터 확인 완료)
- posfeed 주문상태는 딱 3종: `배달완료`(348,360) / `접수`(1,652) / `취소`(1,538).
  현재 `_CANCEL_STATUSES = {"취소"}`만 취소 처리 → "접수"가 정상 매출로 계상됨.
  과거일에 posfeed가 잔존하면 접수분이 그대로 과대계상 (예: 송파삼전점 07-07 접수 11건 289,700).
- 배민/쿠팡 원천은 **수집(collection)월**로 파티션됨. 크롤 지연으로 월말 판매분이 **다음 달 수집 파일**에 들어감.
  예) 06-30 판매분 → `orders_2026-07.parquet`(sale_date 06-26~07-06)에 존재.
  하지만 reconcile는 date 06-30에 대해 `ym=2026-06` 파일만 조회 → 그 파일은 06-01~06-29만 있어 0건 → posfeed 유지.

## Implementation Steps

### Fix 1 — 과거일 posfeed 접수 제외 (`DB_UnifiedSales_posfeed.py`)
1. common import 블록(`from modules.transform.pipelines.db.DB_UnifiedSales_common import (...)`)에 **`_kst_today_str`** 추가.
   (필터가 이미 `_kst_today_str`로 KST today를 판정하므로 동일 기준 재사용.)
2. `_transform_df(orders, items, date_str)`의 **날짜 필터 직후**(`orders = orders[orders["등록날짜"]...==date_str].copy()` 및 empty 반환 블록 다음, 수치형 변환 `for col in ["총 주문금액", ...]` 전)에 아래 추가:
   ```python
   # 과거일(어제 이후)에는 미완료(접수 등) 주문을 유령 매출로 보고 제외한다.
   # today는 아직 진행 중인 주문을 라이브 프록시로 유지(당일 대량 음수 왜곡 방지).
   if date_str != _kst_today_str():
       before = len(orders)
       status = orders["주문상태"].fillna("").astype(str).str.strip()
       orders = orders[status.isin({"배달완료", "취소"})].copy()
       dropped = before - len(orders)
       if dropped:
           logger.info("posfeed %s: 과거일 미완료(접수) 주문 %d건 제외", date_str, dropped)
       if orders.empty:
           return pd.DataFrame(columns=UNIFIED_COLUMNS)
   ```
3. `_CANCEL_STATUSES`, today_mode, source-aware replace, 취소(음수) 로직은 **변경 금지**.
   → 자가 치유: `run_lookback_posfeed`(기본 14일)가 재도출 시 기존 접수 잔존행을 완료-only로 교체.

### Fix 2 — reconcile 수집월 파티션 보정 (`DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py`)
4. 공통 헬퍼 추가(각 파일 또는 한 곳): `ym`("YYYY-MM") 다음 달 문자열 반환.
   ```python
   def _next_ym(ym: str) -> str:
       y, m = map(int, ym.split("-"))
       return f"{y + 1}-01" if m == 12 else f"{y}-{m + 1:02d}"
   ```
5. **baemin**: reconcile 루프에서 판매월 `ym`뿐 아니라 `_next_ym(ym)` 파일도 함께 읽어 합친다.
   - `_find_baemin_files(store, ym)` 호출을 `_find_baemin_files(store, ym) + _find_baemin_files(store, _next_ym(ym))`로 확장(중복 경로 dedup).
   - 이후 기존 로직대로 `sale_date == date`로 정확 필터하므로 다른 날짜 오염 없음. 합친 원천 행은 dedup.
6. **coupang**: `_read_coupang_month(store, ym)` 호출을 `ym`+`_next_ym(ym)` 두 달로 확장.
   - `_read_coupang_month`가 반환한 프레임들을 concat 후 기존 `_deduplicate_raw`로 중복 제거(다음 달 파일에 겹치는 sale_date 있어도 안전).
7. `_upsert_daily`의 "수동 없으면 posfeed 유지"(빈 입력 early return `if df_new is None or df_new.empty: return 0, 0`) 동작은 **유지**한다(엄격 제거 아님). 파일 탐색만 넓혀 수동 원천이 있는 날짜가 정상 교체되게 함.

## Reference Code

### DB_UnifiedSales_posfeed.py (수정 대상 함수)
```python
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    _apply_posfeed_blacklist,
    _make_unified_pk,
    _normalize_time,
    _save_unified_daily,
    _to_int_series,
    iter_unified_sales_files,
    # ADD: _kst_today_str
)

_CANCEL_STATUSES = {"취소"}  # 변경 금지

def _transform_df(orders, items, date_str):
    if orders.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    orders = orders[orders["등록날짜"].astype(str).str.strip() == date_str].copy()
    if orders.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    # <-- Fix 1 삽입 위치 (여기)
    for col in ["총 주문금액", "배달비", "할인", "결제금액"]:
        if col in orders.columns:
            orders[col] = _to_int_series(orders[col])
    orders["포스피드_매출"] = _derive_posfeed_revenue(orders)
    ...
    orders["sale_type"] = orders["주문상태"].fillna("").astype(str).map(
        lambda v: "취소" if v in _CANCEL_STATUSES else "정상")
```

### DB_UnifiedSales_common.py (재사용 함수)
```python
def _kst_today_str() -> str:
    return pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")
```

### DB_UnifiedSales_baemin.py (수정 대상)
```python
def _find_baemin_files(store: str, ym: str) -> list[str]:
    base = ANALYTICS_DB / "baemin_macro" / "orders"
    for ext in ("parquet", "csv"):
        pattern = str(base / "brand=*" / f"store={store}" / f"ym={ym}" / f"orders_{ym}.{ext}")
        files = sorted(glob(pattern))
        if files:
            return files
    return []

# reconcile_baemin_for_test_stores 루프 내부:
for store in stores:
    for ym in ym_list:
        baemin_files = _find_baemin_files(store, ym)   # <-- ym + _next_ym(ym) 로 확장
        ...
        df_raw = _read_baemin_files(baemin_files)
        ...
        for date in dates:
            if date[:7] != ym: continue
            day = df_raw[df_raw["sale_date"] == date].copy()   # 정확 필터 유지
```

### DB_UnifiedSales_coupang.py (수정 대상)
```python
def _read_coupang_month(store: str, ym: str) -> pd.DataFrame:
    frames = []
    for path in sorted(COUPANG_ORDERS_DB.glob(f"brand=*/store={store}/ym={ym}/orders_{ym}.parquet")):
        ...
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# reconcile_coupang_for_test_stores 루프 내부:
for store in stores:
    for ym in sorted({d[:7] for d in dates}):
        raw = _read_coupang_month(store, ym)   # <-- ym + _next_ym(ym) concat 으로 확장
        ...
        raw = _deduplicate_raw(raw, store, ym)
        for date in dates:
            if date[:7] != ym: continue
            day = raw[raw["sale_date"] == date].copy()   # 정확 필터 유지
```

## Test Cases
1. [posfeed 과거일 접수 제외] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import run_posfeed; print(run_posfeed('2026-07-07', overwrite=True))"` → 기대: 에러 없이 저장. 이후 아래 검증 스니펫으로 송파삼전점 07-07 posfeed 합이 배달완료만(≈110,500)으로 감소.
2. [today 회귀] `python -c "import pendulum; from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import run_posfeed; d=pendulum.now('Asia/Seoul').strftime('%Y-%m-%d'); print(run_posfeed(d, overwrite=True))"` → 기대: 접수 주문이 여전히 포함(제외 로그 없음).
3. [baemin reconcile 06-30] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_baemin import reconcile_baemin_for_test_stores; print(reconcile_baemin_for_test_stores(['법흥리점'], sale_date='2026-06-30'))"` → 기대: 제거>0, 추가>0. 06-30 파일에서 posfeed 배달의민족 → 배민수동 교체.
4. [_next_ym 단위] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_baemin import _next_ym; print(_next_ym('2026-06'), _next_ym('2026-12'))"` → 기대: `2026-07 2027-01`.
5. [import 무결성] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_posfeed, modules.transform.pipelines.db.DB_UnifiedSales_baemin, modules.transform.pipelines.db.DB_UnifiedSales_coupang; print('ok')"` → 기대: `ok`.
6. [검증 스니펫] 아래 실행 → 법흥리점 06-30·07-07 posfeed 잔존 사라짐(수동 도착일은 배민수동, 미도착일은 posfeed 배달완료만).
   ```python
   import pandas as pd
   from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files
   for f in iter_unified_sales_files():
       d=pd.read_parquet(f, columns=['sale_date','store','source','platform','total_price'])
       d['skey']=d['store'].astype(str).str.strip().str.split().str[-1]
       s=d[d['skey'].isin(['법흥리점','송파삼전점'])]
       if len(s): print(f.name, s.groupby(['skey','source'])['total_price'].sum().to_dict())
   ```

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- today_mode / `_save_unified_daily` source-aware replace / `filter_manual_delivery_sources_for_test_stores` 게이팅 로직은 **변경 금지**.
- `_upsert_daily` 빈-입력 early-return **유지**(엄격 제거 아님).
- 취소(음수 차감) 로직 유지. `_CANCEL_STATUSES` 값 변경 금지.
- reconcile 정확 날짜 필터(`sale_date == date`) 유지 — 파일 집합만 넓힘.
- print 금지, 절대 import 경로 유지, 파일/함수명 보존.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(덮어쓰기)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `_next_ym` 배치 위치: baemin·coupang 각 파일에 각각 두거나 한 파일에 두고 import — 기존 파일 구조와 충돌 없는 쪽 선택
