# TEST_STORES posfeed 쿠팡이츠 중복 제거

## Task

`TEST_STORES` 매장은 배민·쿠팡 직수집을 모두 적용하는 대상이다.
현재 `enforce_coupang_manual_only_for_test_stores`가 "해당 날짜에 쿠팡수동 행이 있을 때만" posfeed 쿠팡이츠를 제거하는 조건부 로직이라, 쿠팡 수집이 아직 없는 날짜에 posfeed 쿠팡이츠가 잔류하다가 나중에 쿠팡수동이 추가되면 이중 집계가 발생한다.
이 조건 제거 → `store_set` 전체에 대해 non-쿠팡수동 쿠팡이츠를 무조건 제거하도록 수정한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify

- `dags/db/DB_UnifiedSales_Dags.py` — 주석 1줄 수정 (line 88)
- `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` — `enforce_coupang_manual_only_for_test_stores` 함수 수정

## Implementation Steps

### 1. `dags/db/DB_UnifiedSales_Dags.py` line 88 주석 수정

```python
# Before
TEST_STORES: list[str] = ["해운대중동점", "법흥리점", "송파삼전점", "동탄영천점", "중랑면목점", "시흥배곧점", "동두천지행점", "평택비전점"]  # 배민 직수집 교정 대상 테스트 매장 (검증 완료 후 확대)

# After
TEST_STORES: list[str] = ["해운대중동점", "법흥리점", "송파삼전점", "동탄영천점", "중랑면목점", "시흥배곧점", "동두천지행점", "평택비전점"]  # 배민·쿠팡 직수집 교정 대상 테스트 매장 (검증 완료 후 확대)
```

### 2. `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` — `enforce_coupang_manual_only_for_test_stores` 수정

현재 코드 (line 102~161):

```python
def enforce_coupang_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 최종 방어막."""
    if not stores:
        return "TEST_STORES 없음 - 쿠팡수동 최종 정리 스킵"

    if sale_date:
        dates = [str(sale_date)]
    else:
        kst_now = pendulum.now("Asia/Seoul")
        dates = [(kst_now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]

    store_set = {str(store).strip() for store in stores if str(store).strip()}
    total_removed = 0
    changed_files = 0

    for date in dates:
        daily_path = _unified_daily_path(date)
        if not daily_path.exists():
            logger.info("쿠팡수동 최종 정리 스킵, 파일 없음: %s", daily_path)
            continue

        df = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        manual_store_set = set(
            df.loc[
                df["store"].fillna("").astype(str).str.strip().isin(store_set)
                & df["platform"].fillna("").astype(str).str.strip().eq(COUPANG_PLATFORM)
                & df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE),
                "store",
            ].fillna("").astype(str).str.strip()
        )
        if not manual_store_set:
            logger.info("쿠팡수동 최종 정리 스킵, 수동 행 없음: %s", daily_path.name)
            continue

        remove_mask = (
            df["store"].fillna("").astype(str).str.strip().isin(manual_store_set)
            & df["platform"].fillna("").astype(str).str.strip().eq(COUPANG_PLATFORM)
            & ~df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE)
        )
        removed = int(remove_mask.sum())
        if removed == 0:
            logger.info("쿠팡수동 최종 정리 변경 없음: %s", daily_path.name)
            continue

        df_out = df[~remove_mask].reset_index(drop=True)
        _write_unified_daily(df_out, daily_path)
        total_removed += removed
        changed_files += 1
        logger.warning(
            "쿠팡수동 최종 정리: %s | 제거=%d | stores=%s",
            daily_path.name,
            removed,
            sorted(df.loc[remove_mask, "store"].dropna().astype(str).unique().tolist()),
        )

    return f"쿠팡수동 최종 정리 완료 | 파일={changed_files} 제거={total_removed}행"
```

수정 후 코드 — `manual_store_set` 블록 전체 제거, `remove_mask`를 `store_set` 기준으로 직접 작성:

```python
def enforce_coupang_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 최종 방어막.

    쿠팡수동 존재 여부와 무관하게 store_set의 non-쿠팡수동 쿠팡이츠 행을 제거한다.
    """
    if not stores:
        return "TEST_STORES 없음 - 쿠팡수동 최종 정리 스킵"

    if sale_date:
        dates = [str(sale_date)]
    else:
        kst_now = pendulum.now("Asia/Seoul")
        dates = [(kst_now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]

    store_set = {str(store).strip() for store in stores if str(store).strip()}
    total_removed = 0
    changed_files = 0

    for date in dates:
        daily_path = _unified_daily_path(date)
        if not daily_path.exists():
            logger.info("쿠팡수동 최종 정리 스킵, 파일 없음: %s", daily_path)
            continue

        df = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        remove_mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().eq(COUPANG_PLATFORM)
            & ~df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE)
        )
        removed = int(remove_mask.sum())
        if removed == 0:
            logger.info("쿠팡수동 최종 정리 변경 없음: %s", daily_path.name)
            continue

        df_out = df[~remove_mask].reset_index(drop=True)
        _write_unified_daily(df_out, daily_path)
        total_removed += removed
        changed_files += 1
        logger.warning(
            "쿠팡수동 최종 정리: %s | 제거=%d | stores=%s",
            daily_path.name,
            removed,
            sorted(df.loc[remove_mask, "store"].dropna().astype(str).unique().tolist()),
        )

    return f"쿠팡수동 최종 정리 완료 | 파일={changed_files} 제거={total_removed}행"
```

**변경 요약:** `manual_store_set` 계산 블록(10줄)과 `if not manual_store_set: continue` 조건 삭제. `remove_mask`의 `isin(manual_store_set)` → `isin(store_set)` 교체.

## Reference Code

### DB_UnifiedSales_coupang.py (관련 상수 및 함수 시그니처)

```python
COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"

def _upsert_daily(df_new: pd.DataFrame, date: str, store: str) -> tuple[int, int]:
    # store + platform="쿠팡이츠" 전체를 제거 후 쿠팡수동 삽입 (reconcile_coupang에서 호출)
    ...

def _write_unified_daily(df: pd.DataFrame, daily_path) -> None:
    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df.to_parquet(daily_path, index=False, engine="pyarrow")
```

### DB_UnifiedSales_common.py (관련 함수)

```python
UNIFIED_COLUMNS = [
    "sale_date", "ym", "source", "brand", "store", "region", "담당자", "실오픈일",
    "platform", "order_type", "order_id", "order_time", "menu_name", "item_seq",
    "item_id", "item_name", "qty", "unit_price", "total_price", "discount_amount",
    "sale_type", "_pk", "collected_at", "order_cnt",
]

def _unified_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"
```

## Test Cases

1. **구문 검사**
   ```
   python -c "import ast; ast.parse(open('modules/transform/pipelines/db/DB_UnifiedSales_coupang.py').read()); print('ok')"
   ```
   → 기대: `ok`

2. **DAG import 검사**
   ```
   python -c "from dags.db.DB_UnifiedSales_Dags import dag; print('ok')"
   ```
   → 기대: `ok`

3. **함수 import 검사**
   ```
   python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import enforce_coupang_manual_only_for_test_stores; print('ok')"
   ```
   → 기대: `ok`

4. **동작 검증 (manual_store_set 조건 제거 확인)**
   수정된 함수 본문에 `manual_store_set` 문자열이 없어야 함:
   ```
   python -c "
   import inspect
   from modules.transform.pipelines.db.DB_UnifiedSales_coupang import enforce_coupang_manual_only_for_test_stores
   src = inspect.getsource(enforce_coupang_manual_only_for_test_stores)
   assert 'manual_store_set' not in src, 'manual_store_set 잔류'
   print('ok')
   "
   ```
   → 기대: `ok`

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~4 순서대로 실행
  2. FAIL 항목 → 원인 분석 후 코드 수정
  3. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `manual_store_set` 관련 코드(계산 + `if not manual_store_set: continue`) 완전 삭제
- `remove_mask` 기준은 반드시 `store_set` (입력 그대로) 사용 — 새로운 집합 변수 도입 금지
- DAG 파일(`DB_UnifiedSales_Dags.py`)에서는 **주석 1줄만** 변경, 다른 줄 건드리지 말 것
- 기존 로그 메시지 스타일 유지 (`logger.warning`, `logger.info` 혼용 패턴 그대로)
- `_write_unified_daily` 호출 방식 변경 없음

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
