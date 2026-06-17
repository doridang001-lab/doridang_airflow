# 어제자 OKPOS unified_sales 누락 (송파삼전점 등)

## Task
매일 DAG가 돌면 **어제자 OKPOS 수집건이 `unified_sales`에 적재되지 않는다**(송파삼전점 등 전 매장 공통). 원인은 unified 변환 로직 결함이 아니라 **수집-적재 타이밍 레이스**다: (1) 수집측이 "어제"를 거짓 NO_DATA로 마킹해 정시(07:35)에 안 받아오고 자정 이후에야 늦게 적재됨, (2) unified okpos 월 로더가 캐시 mtime 검증이 없어 늦게 들어온 데이터를 못 보는 잠재 버그. 이 두 지점을 고쳐 어제자가 정시에 반영되도록 한다.

## 진단 근거 (데이터로 확인됨, 구현 전 참고용)
- raw `송파삼전점/ym=2026-06/okpos_order.csv` 의 06-14 행 `collected_at = 2026-06-15 00:31:38` (자정 이후 적재)
- `unified_sales_260614.parquet` 마지막 기록 = `2026-06-14 23:45:40`, okpos 0행 → unified가 06-14를 만든 시점에 06-14 okpos가 raw에 아직 없었음
- `.no_data__okpos_order.txt` 에 06-09~06-14가 `today:download_no_data`로 거짓 마킹됨
- `_transform_okpos_df('2026-06-14')` 는 지금 실행하면 송파삼전점 포함 575행 정상 생성 → 변환 코드는 정상

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 기존 파일 스타일 유지: snake_case, 타입힌트 기존과 동일, 주석은 WHY만 최소화

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_OKPOS_Sales.py`
  - `_filter_missing_keys` (line ~611): 최근 N일(기본 2일) 이내 날짜는 NO_DATA 마커가 있어도 skip하지 않고 항상 재시도
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_okpos.py`
  - `_load_okpos_month` (line ~358): 월 캐시에 mtime 검증 추가 (장수명 워커에서 stale 캐시로 신규 수집분 누락 방지)
  - `run_lookback_okpos` (line ~857): okpos CSV 없어 skip 시 `logger.info` → `logger.warning` 승격
- **생성/수정 없음**: DAG 파일 변경 불필요

## Implementation Steps

### 1) `_filter_missing_keys` 최근 N일 NO_DATA 재시도 가드 (근본 원인)
모듈 상단(상수 영역)에 추가:
```python
# "어제/그제"는 OKPOS 마감이 지연되어 빈 결과(거짓 NO_DATA)가 찍히기 쉽다.
# 최근 N일 이내 날짜는 마커가 있어도 항상 재수집하여 영구 미수집을 방지한다.
_NO_DATA_RETRY_RECENT_DAYS = 2
```
`_filter_missing_keys` 내부의 NO_DATA 마커 skip 분기 수정. 기존:
```python
if sale_date in marker_cache[marker_path]:
    continue
```
→ 변경 (최근 N일 이내면 마커 무시하고 계속 진행, 즉 missing 후보로 남겨 재수집):
```python
if sale_date in marker_cache[marker_path]:
    try:
        _d = datetime.strptime(sale_date, "%Y-%m-%d").date()
        _recent = (_kst_now().date() - _d).days <= _NO_DATA_RETRY_RECENT_DAYS
    except Exception:
        _recent = False
    if not _recent:
        continue
    # 최근 N일 이내 → 마커 무시하고 아래 존재 여부 체크로 진행
```
- `datetime`, `_kst_now`는 이미 모듈에 import/정의됨 → 추가 import 불필요.
- 과거(N일 초과) 진짜 휴무 마킹은 그대로 skip 유지(오탐 방지).

### 2) `_load_okpos_month` mtime 캐시 무효화 (안전망)
현재 `_OKPOS_MONTH_CACHE[ym]`는 mtime 검증 없이 무한 재사용 → 워커가 살아있으면 데이터 없던 시점 캐시가 남는다. 캐시 저장/조회를 (df, df, mtime) 형태로 바꾼다.
```python
def _okpos_month_mtime(ym: str) -> float:
    paths = list(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order.csv")) \
          + list(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order_item.csv"))
    return max((p.stat().st_mtime for p in paths), default=0.0)
```
`_load_okpos_month` 진입부:
```python
    global _OKPOS_MONTH_CACHE
    cur_mtime = _okpos_month_mtime(ym)
    cached = _OKPOS_MONTH_CACHE.get(ym)
    if cached is not None and cached[2] == cur_mtime:
        return cached[0], cached[1]
```
저장부(기존 `_OKPOS_MONTH_CACHE[ym] = (order_df, item_df)`):
```python
    _OKPOS_MONTH_CACHE[ym] = (order_df, item_df, cur_mtime)
    return order_df, item_df
```
- 캐시 타입 주석/힌트도 `tuple[pd.DataFrame, pd.DataFrame, float]`로 갱신.

### 3) `run_lookback_okpos` skip 로그 승격
```python
        except FileNotFoundError:
            logger.warning("okpos CSV 없음, 스킵: %s", date_str)
            continue
```

## Reference Code

### modules/transform/pipelines/db/DB_OKPOS_Sales.py (마커/필터 관련)
```python
def _kst_now() -> datetime:                       # line 39
    return datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)

def _no_data_marker_path(store_short, ym, csv_stem) -> Path:   # line 96
    return (RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}"
            / f"ym={ym}" / f".no_data__{csv_stem}.txt")

def _read_no_data_dates(marker_path) -> set[str]:   # line 111

def _filter_missing_keys(sale_dates, csv_name) -> list:   # line 611  ← 수정 대상
    missing = []
    marker_cache: dict[Path, set[str]] = {}
    for sale_date in sale_dates:
        ym = sale_date[:7]
        for store in STORES:
            if not _should_collect(store["name"], sale_date):
                continue
            store_short = store["name"].replace("도리당 ", "", 1)
            csv_path = (RAW_OKPOS_SALES / "brand=도리당"
                        / f"store={store_short}" / f"ym={ym}" / f"{csv_name}.csv")
            marker_path = _no_data_marker_path(store_short, ym, csv_name)
            if marker_path not in marker_cache:
                marker_cache[marker_path] = _read_no_data_dates(marker_path)
            if sale_date in marker_cache[marker_path]:   # ← 이 분기 수정
                continue
            if not csv_path.exists():
                missing.append((sale_date, store)); continue
            try:
                df = pd.read_csv(csv_path, dtype=str, usecols=["sale_date"])
                if df[df["sale_date"].str.strip() == sale_date].empty:
                    missing.append((sale_date, store))
            except Exception:
                missing.append((sale_date, store))
    return missing
```

### modules/transform/pipelines/db/DB_UnifiedSales_okpos.py (월 로더)
```python
_OKPOS_MONTH_CACHE: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}   # line 35  ← (df,df,mtime)로 변경

def _load_okpos_month(ym: str) -> tuple[pd.DataFrame, pd.DataFrame]:   # line 358  ← 수정 대상
    global _OKPOS_MONTH_CACHE
    if ym in _OKPOS_MONTH_CACHE:           # ← mtime 검증으로 교체
        return _OKPOS_MONTH_CACHE[ym]
    order_dfs, item_dfs = [], []
    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order.csv")):
        ...
    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order_item.csv")):
        ...
    order_df = pd.concat(order_dfs, ignore_index=True) if order_dfs else pd.DataFrame()
    item_df  = pd.concat(item_dfs, ignore_index=True) if item_dfs else pd.DataFrame()
    ...
    _OKPOS_MONTH_CACHE[ym] = (order_df, item_df)   # ← (order_df, item_df, cur_mtime)
    return order_df, item_df

def run_lookback_okpos(days: int = 7) -> str:   # line 837
    ...
        try:
            order_df, item_df = _load_okpos_by_date(date_str)
        except FileNotFoundError:
            logger.info("okpos CSV 없음, 스킵: %s", date_str)   # ← warning 승격
            continue
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import _filter_missing_keys, _NO_DATA_RETRY_RECENT_DAYS; print('ok')"` → 기대: `ok`
2. [import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _load_okpos_month, _okpos_month_mtime; print('ok')"` → 기대: `ok`
3. [최근 N일 마커 무시] 06-14는 송파삼전점 raw CSV에 존재하고 NO_DATA 마커에도 있음. 오늘(06-15) 기준 1일 전:
   ```
   python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import _filter_missing_keys; m=_filter_missing_keys(['2026-06-14'],'okpos_order'); print('missing has 송파삼전점:', any(s['name'].endswith('송파삼전점') for _,s in m))"
   ```
   → 기대: CSV에 06-14 데이터가 이미 있으므로 missing 후보에서 **빠짐**(False). 핵심은 마커 때문에 무조건 skip되지 않는 것(예외/에러 없이 동작).
4. [월 캐시 mtime] 
   ```
   python -c "
   from modules.transform.pipelines.db import DB_UnifiedSales_okpos as o
   o._OKPOS_MONTH_CACHE.clear()
   a=o._load_okpos_month('2026-06'); b=o._load_okpos_month('2026-06')
   import pandas as pd; print('cached ok:', len(a[0])==len(b[0]) and len(a[0])>0)
   "
   ```
   → 기대: `cached ok: True` (2회 호출 동일, 에러 없음)
5. [변환 회귀] 
   ```
   python -c "
   from modules.transform.pipelines.db import DB_UnifiedSales_okpos as o
   o._OKPOS_MONTH_CACHE.clear()
   od,it=o._load_okpos_by_date('2026-06-14'); df=o._transform_okpos_df(od,it)
   print('rows', len(df), 'has 송파삼전점', '송파삼전점' in set(df['store']))
   "
   ```
   → 기대: `rows 575 has 송파삼전점 True` (행수는 데이터 변동 가능, 송파삼전점 포함이 핵심)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 → 코드 수정
  3. 전체 재실행
종료 조건: TC1~5 전체 PASS + Constraints 위반 없음
```

## 운영 조치 (코드 PR 후 사용자/운영자가 WSL에서 실행 — 구현 범위 아님)
```bash
# 06-14 강제 재수집(NO_DATA 무시) 후 unified 재빌드
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_OKPOS_Sales_Dags \
  -c '{"sale_date_from":"2026-06-14","sale_date_to":"2026-06-14","force_redownload":true,"force_redownload_today":true,"force_redownload_receipt":true}'
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_UnifiedSales -c '{"sale_date":"2026-06-14"}'
```

## Constraints
- `_filter_missing_keys` 시그니처/반환 형식((sale_date, store) 튜플 리스트) 유지.
- 과거(N일 초과) 진짜 휴무 NO_DATA skip 동작 보존 — 가드는 최근 N일에만 적용.
- `_mark_no_data` 기존 가드(당일/미래 차단)는 건드리지 않는다.
- `_load_okpos_month` 외부 반환 시그니처(`tuple[df, df]`) 유지 — 캐시 내부만 3-튜플.
- 날짜 파싱 실패 시 예외로 파이프라인 중단 금지 → try/except로 감싸 기존 동작 유지.
- print 금지, logger 사용. 신규 import 최소화(기존 `datetime`, `_kst_now` 재사용).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 해당 함수만 수정(전체 덮어쓰기 금지)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트/주석 스타일: 기존 파일과 동일, 주석은 WHY만 최소화
- 변수명: snake_case, 기존과 동일
