# 해운대중동점 toorder 채널 보충 (배민·쿠팡 제외)

## Task
`해운대중동점`은 POS 원천(okpos/unionpos/easypos/posfeed)이 전혀 없는 배달수동 테스트매장이라, 현재 `unified_sales`에 **배민수동(배달의민족)·쿠팡수동(쿠팡이츠)** 행만 생성되고 나머지 채널(요기요·땡겨요·요기배달·홀 포장 등)이 통째로 누락된다. 원인은 `toorder` 채널이 okpos 이중집계 사고 때문에 전면 비활성화(SKIP 스텁)되어 있기 때문. 전역 toorder는 계속 끈 채, `해운대중동점`에 한해 **배민·쿠팡 계열 플랫폼만 제외**하고 나머지 채널을 toorder에서 한 줄(store×platform 집계 1행)씩 적재한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_common.py` — 상수 `TOORDER_MANUAL_STORES` 추가
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py` — scoped 적재 함수 3종 추가 (기존 SKIP 스텁은 유지)
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales.py` — facade import·`__all__`에 신규 함수/상수 추가
- **수정** `dags/db/DB_UnifiedSales_Dags.py` — `build_toorder` 콜러블 + `PythonOperator` + 체인 삽입

## Implementation Steps

### 1. `DB_UnifiedSales_common.py` — 대상 매장 상수
테스트매장 목록(`_BASE_DELIVERY_MANUAL_TEST_STORES` / `ADD_TEST_STORES`) 근처에 추가:
```python
# POS 원천이 없어 나머지 채널을 toorder로 보충하는 매장.
TOORDER_MANUAL_STORES = ["해운대중동점"]
```

### 2. `DB_UnifiedSales_toorder.py` — scoped 적재
기존 `_load_parquet` / `_transform_df`(store 정규화 + okpos 매장 drop + 플랫폼 매핑 + 메타 + `_make_unified_pk`)를 **그대로 재사용**한다. 기존 `run_toorder`/`run_lookback_toorder`/`backfill_toorder` SKIP 스텁은 **건드리지 않는다**(전역 toorder 비활성 유지).

상단 import·상수 (파일 상단 공통 import 블록 근처):
```python
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    DELIVERY_PLATFORM_FAMILIES,
    TOORDER_MANUAL_STORES,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _save_unified_daily,
    _to_int_series,
)

# 배민수동/쿠팡수동이 담당하는 배달 플랫폼 → toorder 적재에서 제외.
# _transform_df가 배민1→배달의민족으로 매핑하므로 매핑 후 값 기준으로 걸러진다.
_MANUAL_DELIVERY_PLATFORMS = set().union(*DELIVERY_PLATFORM_FAMILIES.values())
```

신규 함수(파일 하단 SKIP 스텁 아래에 추가):
```python
def _resolve_manual_stores(stores=None) -> list[str]:
    base = {str(s).strip() for s in TOORDER_MANUAL_STORES if str(s).strip()}
    if stores is None:
        return sorted(base)
    req = {str(s).strip() for s in stores if str(s).strip()}
    return sorted(base & req)


def run_toorder_manual_stores(date_str: str, stores=None, overwrite: bool = False) -> str:
    """POS 없는 수동매장(해운대중동점 등)의 배민·쿠팡 제외 채널을 toorder로 적재."""
    target = _resolve_manual_stores(stores)
    if not target:
        return f"SKIP: toorder 수동매장 대상 없음 ({date_str})"

    df = _transform_df(_load_parquet(), date_str)
    if df.empty:
        return f"SKIP: toorder {date_str} 해당일 데이터 없음"

    store = df["store"].fillna("").astype(str).str.strip()
    platform = df["platform"].fillna("").astype(str).str.strip()
    df = df[store.isin(target) & ~platform.isin(_MANUAL_DELIVERY_PLATFORMS)].copy()
    if df.empty:
        return f"SKIP: toorder {date_str} 대상 매장 비배민·쿠팡 채널 없음"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite, replace_stores=target)
    return f"toorder(수동매장) {date_str}: {saved}행 저장 (stores={target})"


def run_lookback_toorder_manual_stores(days: int = 14, stores=None) -> str:
    from datetime import datetime, timedelta
    today = datetime.now()
    total = 0
    for i in range(days):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            result = run_toorder_manual_stores(d, stores=stores, overwrite=False)
            logger.info(result)
            if result.startswith("toorder(수동매장)"):
                try:
                    total += int(result.split(":")[1].strip().split("행")[0].strip())
                except Exception:
                    pass
        except Exception as e:
            logger.warning("toorder(수동매장) lookback error: %s | %s", d, e)
    return f"toorder(수동매장) lookback({days}일): {total}행 저장"


def backfill_toorder_manual_stores(stores=None) -> str:
    target = _resolve_manual_stores(stores)
    if not target:
        return "SKIP: toorder 수동매장 대상 없음"
    df_all = _load_parquet()
    if df_all.empty:
        return "SKIP: toorder parquet 없음"
    store_col = df_all["store"].fillna("").astype(str)
    sub = df_all[store_col.str.contains("|".join(target))]
    dates = sorted({str(d).strip() for d in sub["date"].dropna().unique() if str(d).strip()})
    if not dates:
        return "SKIP: 대상 매장 유효 date 없음"
    total = 0
    for date_str in dates:
        try:
            result = run_toorder_manual_stores(date_str, stores=stores, overwrite=True)
            logger.info(result)
            if result.startswith("toorder(수동매장)"):
                try:
                    total += int(result.split(":")[1].strip().split("행")[0].strip())
                except Exception:
                    pass
        except Exception as e:
            logger.warning("toorder(수동매장) backfill 실패: %s | %s", date_str, e)
    return f"toorder(수동매장) backfill 완료: {len(dates)}일 / {total}행 저장"
```
주의: `_transform_df`는 store 정규화 직후 okpos 매장 행을 drop한다. `해운대중동점`은 okpos 매장이 아니므로 그대로 통과한다. `_transform_df` 내부의 `date` 필터/groupby(store×platform sum)를 재사용하므로 한 채널당 1행이 생성된다.

### 3. `DB_UnifiedSales.py` (facade)
`DB_UnifiedSales_toorder` import 블록에 신규 3함수를, `DB_UnifiedSales_common` import 블록에 `TOORDER_MANUAL_STORES`를 추가하고 `__all__`에도 4개 이름을 추가한다.

### 4. `dags/db/DB_UnifiedSales_Dags.py`
import 추가(기존 `from modules.transform.pipelines.db.DB_UnifiedSales import (...)` 블록에):
```python
    TOORDER_MANUAL_STORES,
    backfill_toorder_manual_stores as pipeline_backfill_toorder,
    run_toorder_manual_stores as pipeline_run_toorder,
    run_lookback_toorder_manual_stores as pipeline_lookback_toorder,
```
콜러블 추가(`build_posfeed` 패턴 준용, `build_posfeed` 함수 근처):
```python
def build_toorder(**context) -> str:
    """POS 없는 수동매장의 배민·쿠팡 제외 채널을 toorder에서 적재."""
    dag_run = context.get("dag_run")
    conf    = (getattr(dag_run, "conf", None) or {}) if dag_run else {}

    if _truthy(conf.get("backfill")):
        return pipeline_backfill_toorder()

    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    if sale_date:
        if _is_partial_store_mode(context):
            stores = context["ti"].xcom_pull(task_ids="resolve_date", key="stores") or []
            return pipeline_run_toorder(sale_date, stores=stores, overwrite=False)
        return pipeline_run_toorder(sale_date, overwrite=True)
    lookback_days = _require_lookback_days()
    if lookback_days is None:
        return pipeline_backfill_toorder()
    return pipeline_lookback_toorder(lookback_days)
```
Operator 추가(`t5a = build_posfeed` 근처):
```python
    t_toorder = PythonOperator(
        task_id="build_toorder",
        python_callable=build_toorder,
    )
```
체인 삽입: 기존 `>> t5a >> t5a3 >>` 부분을 `>> t5a >> t_toorder >> t5a3 >>` 로 변경.

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_toorder.py (기존)
```python
from modules.transform.utility.paths import ANALYTICS_DB, RAW_OKPOS_SALES
from modules.transform.utility.store_normalize import (
    normalize as _normalize_store_names, strip_brand as _strip_brand)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, _load_store_map, _lookup_store_meta,
    _make_unified_pk, _save_unified_daily, _to_int_series)

TOORDER_PARQUET = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
TOORDER_SOURCE = "toorder"

def _load_parquet() -> pd.DataFrame:
    if not TOORDER_PARQUET.exists(): return pd.DataFrame()
    df = pd.read_parquet(TOORDER_PARQUET).fillna("")
    if not {"date","store","platform","price"}.issubset(df.columns): return pd.DataFrame()
    return df

def _transform_df(df, date_str) -> pd.DataFrame:  # date 필터 → price>0 → store 정규화
    # → okpos 매장 drop → 플랫폼 매핑 → groupby(brand,store,platform) sum
    # → 메타(region/담당자/오픈일) → source=toorder, order_id=store_platform, _pk
    ...  # 반환: UNIFIED_COLUMNS 재인덱스

# 현재 전역 비활성(그대로 유지):
def run_toorder(date_str, overwrite=False) -> str:
    return f"SKIP: toorder 채널 비활성화 (unified_sales 제외) {date_str}"
```

### modules/transform/pipelines/db/DB_UnifiedSales_common.py (기존 핵심)
```python
DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡수동": {"쿠팡이츠", "쿠팡 포장"},
}
def _save_unified_daily(df, date_str, overwrite=False, replace_stores=None) -> int:
    # overwrite=False: 동일 source 행 교체(source-aware). replace_stores 지정 시 해당 매장만.
    # 저장 전 filter_manual_delivery_sources_for_test_stores 로 과거일 비수동 배달행 방어.
    ...
def _unified_daily_path(date_str):  # UNIFIED_ROOT / f"unified_sales_{YYMMDD}.parquet"
    ...
```

## Test Cases
1. [단건 적재] `set PYTHONIOENCODING=utf-8 && python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import run_toorder_manual_stores; print(run_toorder_manual_stores('2026-07-12', overwrite=True))"` → 기대: `toorder(수동매장) 2026-07-12: N행 저장` (N>0)
2. [채널 검증] 아래 실행 → 기대: `source=toorder`에 요기요·땡겨요·요기배달·홀 포장 등 존재, 배달의민족/배민1/배민 포장/쿠팡이츠/쿠팡 포장 **없음**, 배민수동/쿠팡수동 행은 그대로 유지
   ```
   python -c "import pandas as pd; from modules.transform.pipelines.db.DB_UnifiedSales_common import _unified_daily_path; df=pd.read_parquet(_unified_daily_path('2026-07-12')); m=df[df['store']=='해운대중동점']; print(m.groupby(['source','platform'])['total_price'].sum())"
   ```
3. [멱등성] Test Case 1을 2회 실행 후 Test Case 2 재확인 → 기대: `_pk` 중복 없음, 행 수/합계 동일
4. [lookback] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import run_lookback_toorder_manual_stores; print(run_lookback_toorder_manual_stores(14))"` → 기대: `toorder(수동매장) lookback(14일): N행 저장` 오류 없음
5. [facade] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales import run_toorder_manual_stores, TOORDER_MANUAL_STORES; print(TOORDER_MANUAL_STORES)"` → 기대: `['해운대중동점']`
6. [DAG import] `python -c "import dags.db.DB_UnifiedSales_Dags as d; print([t.task_id for t in d.dag.tasks if t.task_id=='build_toorder'])"` → 기대: `['build_toorder']`, ImportError 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석 (인코딩은 PYTHONIOENCODING=utf-8 로 회피)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 기존 `run_toorder`/`run_lookback_toorder`/`backfill_toorder` SKIP 스텁은 **절대 수정 금지**(전역 toorder 비활성 유지 = okpos 이중집계 방지).
- `_transform_df`의 okpos 매장 drop 로직 유지 → 다른 POS 매장 toorder 재유입 금지.
- 대상 매장은 `TOORDER_MANUAL_STORES` 상수로만 확장. 코드에 매장명 하드코딩 금지(상수 참조).
- 배민·쿠팡 계열 제외는 `DELIVERY_PLATFORM_FAMILIES` 파생 집합(`_MANUAL_DELIVERY_PLATFORMS`)으로만 판정.
- 저장은 반드시 `_save_unified_daily(..., replace_stores=target)` 사용(source-aware + store 스코프로 안전 교체).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트/주석: 기존 파일과 동일 스타일, 주석은 WHY만 최소화
- 변수명·함수명: snake_case, 기존 파일과 동일
