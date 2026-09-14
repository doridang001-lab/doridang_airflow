# unified_sales 테스트매장 수동배달(배민수동/쿠팡수동) "오늘 제외"

## Task
테스트 매장(`DELIVERY_MANUAL_TEST_STORES` 10곳)의 배달 매출을 배민수동/쿠팡수동 직수집으로
교체하는 로직이 현재 **오늘 날짜까지** 포함해 돌아, `today_dags`가 오늘 수집한 자동 POS 배달
데이터를 수동으로 덮어버린다. 수동 교체를 **"어제 이하"에만** 적용하고 **오늘은 자동수집
(POS/posfeed)만** 사용하도록 바꾼다. 하루 지나 오늘이 어제가 되면 다음 날 lookback 실행이
그 날짜를 reconcile 대상에 넣어 자동으로 수동 교체된다.

핵심 불변식:
- `sale_date == 오늘(KST)` → 테스트 매장 배달 = 자동만, 수동 행 없음
- `sale_date <= 어제` → 기존대로 수동 우선(수동 있으면 같은 플랫폼 패밀리 자동행 제거)

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.pipelines.db.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 타임존: KST 기준일은 `pendulum.now("Asia/Seoul")` 사용 (baemin/coupang 기존 패턴과 동일)
- 폴더 구조/파일명 변경 금지, 기존 함수 시그니처 보존

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_common.py`
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py`
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py`
- 생성 파일 없음

## Implementation Steps

### 1) DB_UnifiedSales_common.py
1-a. 상단 import에 `import pendulum` 추가 (현재 `from datetime import datetime`만 있음).

1-b. KST 오늘 헬퍼 추가 (파일 아무 유틸 영역, 예: `_unified_daily_path` 근처):
```python
def _kst_today_str() -> str:
    return pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")
```

1-c. `filter_manual_delivery_sources_for_test_stores()` 를 "오늘 인지"하도록 수정.
이 함수는 `_save_unified_daily`(저장 시 2회) + `enforce_manual_delivery_sources_for_test_stores`
(전 파일 sweep)에서 모두 호출되므로 여기 한 곳만 고치면 저장·sweep 양쪽에 적용된다.
`store/platform/source/date/keys` 계산부는 그대로 두고, 패밀리 루프를 아래로 교체:
```python
    today = _kst_today_str()

    remove_mask = pd.Series(False, index=out.index)
    for manual_src, family in DELIVERY_PLATFORM_FAMILIES.items():
        in_family = store.isin(store_set) & platform.isin(family)
        if not in_family.any():
            continue
        is_manual = in_family & source.eq(manual_src)
        # 오늘: 테스트 매장도 자동수집(POS/posfeed)만 사용 → 수동 행 제거
        remove_mask |= is_manual & date.eq(today)
        # 과거(어제 이하): 수동 있으면 같은 패밀리 비수동 행 제거. 오늘 POS는 절대 건드리지 않음
        manual_keys = set(keys[is_manual & date.ne(today)])
        if not manual_keys:
            continue
        remove_mask |= in_family & keys.isin(manual_keys) & source.ne(manual_src) & date.ne(today)
```

### 2) DB_UnifiedSales_baemin.py — `_resolve_baemin_target_dates()`
오늘 날짜를 대상에서 제외. 3개 분기 모두 처리:
```python
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")

    if sale_date:
        return [] if str(sale_date) == today else [str(sale_date)]

    if lookback_days is not None:
        kst_now = pendulum.now("Asia/Seoul")
        return [
            (kst_now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, lookback_days + 1)
        ]

    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_baemin_source_dates(store))
    dates.update(_collect_existing_unified_baemin_manual_dates(stores))
    dates.update(_collect_existing_baemin_duplicate_dates(stores))
    return sorted(
        d for d in dates
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and d != today
    )
```

### 3) DB_UnifiedSales_coupang.py — `_resolve_target_dates()`
동일 패턴 적용:
```python
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")

    if sale_date:
        return [] if str(sale_date) == today else [str(sale_date)]
    if lookback_days is not None:
        now = pendulum.now("Asia/Seoul")
        return [
            (now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, lookback_days + 1)
        ]

    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_source_dates(store))
    dates.update(_collect_existing_dates(stores))
    return sorted(
        d for d in dates
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) and d != today
    )
```

## Reference Code

### DB_UnifiedSales_common.py (현재 filter 함수 — 이 본문을 교체)
```python
def filter_manual_delivery_sources_for_test_stores(
    df: pd.DataFrame,
    stores: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty or not {"store", "platform", "source"}.issubset(df.columns):
        return df
    store_set = {
        str(store).strip()
        for store in (stores or DELIVERY_MANUAL_TEST_STORES)
        if str(store).strip()
    }
    if not store_set:
        return df
    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    platform = out["platform"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    date = (
        out["sale_date"].fillna("").astype(str).str.strip()
        if "sale_date" in out.columns else pd.Series("", index=out.index)
    )
    keys = pd.Series(list(zip(store, date)), index=out.index)
    remove_mask = pd.Series(False, index=out.index)
    for manual_src, family in DELIVERY_PLATFORM_FAMILIES.items():
        in_family = store.isin(store_set) & platform.isin(family)
        if not in_family.any():
            continue
        is_manual = in_family & source.eq(manual_src)
        manual_keys = set(keys[is_manual])
        if not manual_keys:
            continue
        remove_mask |= in_family & keys.isin(manual_keys) & source.ne(manual_src)
    removed = int(remove_mask.sum())
    if removed:
        logger.warning("테스트 매장 수동 우선 배달 정리: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)
```

### 참고: DELIVERY 상수 (common.py, 수정 불필요)
```python
DELIVERY_MANUAL_TEST_STORES = ["해운대중동점","법흥리점","송파삼전점","동탄영천점","중랑면목점",
    "시흥배곧점","강원영월점","평택비전점","부산장림점","경북상주점"]
DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡수동": {"쿠팡이츠", "쿠팡 포장"},
}
```

## Test Cases
1. 파싱 확인:
   `python -c "import ast; [ast.parse(open(p,encoding='utf-8').read()) for p in ['modules/transform/pipelines/db/DB_UnifiedSales_common.py','modules/transform/pipelines/db/DB_UnifiedSales_baemin.py','modules/transform/pipelines/db/DB_UnifiedSales_coupang.py']]; print('OK')"`
   → 기대: `OK`

2. filter 오늘=수동제거 / 어제=POS제거 시뮬 (pandas 필요):
```python
python - <<'PY'
import pandas as pd, pendulum
from datetime import timedelta
from modules.transform.pipelines.db.DB_UnifiedSales_common import filter_manual_delivery_sources_for_test_stores as f
today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")
yday = (pendulum.now("Asia/Seoul") - timedelta(days=1)).strftime("%Y-%m-%d")
rows = [
  {"store":"법흥리점","platform":"배달의민족","source":"배민수동","sale_date":today},
  {"store":"법흥리점","platform":"배달의민족","source":"posfeed","sale_date":today},
  {"store":"법흥리점","platform":"배달의민족","source":"배민수동","sale_date":yday},
  {"store":"법흥리점","platform":"배달의민족","source":"posfeed","sale_date":yday},
  {"store":"법흥리점","platform":"배달의민족","source":"posfeed","sale_date":yday,"order_id":"x"},
]
df = pd.DataFrame(rows)
out = f(df)
# 오늘: posfeed만 남고 배민수동 제거 / 어제: 배민수동만 남고 posfeed 제거
t = out[out.sale_date==today]["source"].tolist()
y = out[out.sale_date==yday]["source"].tolist()
assert t==["posfeed"], t
assert set(y)=={"배민수동"}, y
print("PASS", t, y)
PY
```
   → 기대: `PASS ['posfeed'] ...`

3. 대상 날짜에서 오늘 제외 확인:
```python
python -c "import pendulum; from modules.transform.pipelines.db.DB_UnifiedSales_baemin import _resolve_baemin_target_dates as b; from modules.transform.pipelines.db.DB_UnifiedSales_coupang import _resolve_target_dates as c; t=pendulum.now('Asia/Seoul').strftime('%Y-%m-%d'); db=b([],None,7); dc=c([],None,7); assert t not in db and t not in dc, (t,db,dc); assert len(db)==7 and len(dc)==7; print('PASS',db[0],db[-1])"
```
   → 기대: `PASS` + 첫 날짜=어제, 마지막=7일 전 (오늘 미포함)

4. import 무결성:
   `python -c "from modules.transform.pipelines.db import DB_UnifiedSales_common, DB_UnifiedSales_baemin, DB_UnifiedSales_coupang; print('OK')"`
   → 기대: `OK`

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
- `reconcile_*._upsert_daily`는 filter를 안 거치고 parquet에 직접 쓰므로, 오늘 수동 생성을 막으려면
  `_resolve_*_target_dates`에서 오늘 제외가 **필수**. filter 수정은 저장·sweep 시 잔여 수동행 청소
  + 오늘 POS 보존을 위한 이중 방어. → 둘 다 반영해야 함.
- `today_mode` 실행은 이미 reconcile/enforce를 스킵. 단 POS 채널 build는 `_save_unified_daily`→filter를
  타므로 filter 수정이 오늘 수동 잔여(전환기 stale)까지 정리한다.
- `filter_manual_delivery_sources_for_non_test_stores`, `purge_manual_delivery_sources_for_non_test_stores`
  등 비테스트 정리 로직은 **수정 금지**.
- `DELIVERY_MANUAL_TEST_STORES`, `DELIVERY_PLATFORM_FAMILIES` 값 변경 금지.
- lookback 윈도우는 `range(1, lookback_days + 1)`로 어제~N일 전(7일치 유지). 오늘만 빠진다.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code / 기존 파일 패턴 따라가기
- 타입 힌트 여부: 기존 함수와 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
