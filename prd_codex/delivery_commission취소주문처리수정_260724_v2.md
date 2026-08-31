# delivery_commission 취소주문 처리 수정 + unified_sales 전기간 백필

> v1(`delivery_commission취소주문처리수정_260724.md`)은 마트 코드 수정만 다뤘다.
> v2는 여기에 **Part B(unified_sales 전 매장 전 기간 백필)** 를 추가한 전체 범위다.
> v1을 이미 실행했다면 Part A는 건너뛰고 Part B만 수행한다.

## Task

`delivery_commission.parquet` 마트가 취소주문을 잘못 집계해 unified_sales와 불일치한다.
배민은 `주문상태` 필터가 없어 `주문취소` 1,159행(181주문 / 총결제 4,801,600원, 입금예정 0원)을
매출로 계상하고, 쿠팡은 주문단위 `max()` 집계라 환불 음수행(`0`, `-40,000` → `max=0`)이 소거된다.
동시에 unified_sales는 lookback 7일 밖의 원천 재수집분을 반영하지 못해 스테일 상태다
(실측: 2026-05-29 송파삼전점 쿠팡 commission 367,700 / unified 264,100 — 원인이 양쪽에 각각 있음).
Part A에서 마트 집계를 고치고, Part B에서 unified를 전 기간 재적재해 양쪽을 일치시킨다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 필수 컬럼 누락은 `raise RuntimeError(...)` fail-fast (기존 파일 패턴 유지)

## Files to Create / Modify

- 수정: `modules/transform/pipelines/db/DB_DeliveryCommission.py` (Part A)
- 수정: `tests/test_delivery_commission.py` (Part A)
- 코드 변경 없음 / 실행만: `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py` (Part B)
- 생성 파일 없음

---

# Part A — delivery_commission 취소주문 처리 수정

## Implementation Steps

### A-1. 배민 — 배달완료만 집계 (`_load_baemin_orders_agg`)

1. `required` 집합에 `"주문상태"` 추가.
   현재: `required = {"주문번호", "주문시각", "brand", "store", "총결제금액", "입금예정금액"}`
2. `df.groupby(["brand", "주문번호"])` **직전**에 배달완료 필터 적용:

```python
status = df["주문상태"].fillna("").astype(str).str.strip()
cancelled = ~status.eq("배달완료")
if cancelled.any():
    logger.info("배민 취소/미완료 주문 제외: %d행", int(cancelled.sum()))
    df = df[~cancelled]
if df.empty:
    raise RuntimeError("baemin orders 배달완료 데이터 없음; 기존 마트 보존")
```

3. **순서 주의**: 미정산 제외 로직(`unsettled = grouped["총결제금액"].gt(0) & grouped["입금예정금액"].isna()`)은
   반드시 필터 **이후**에 그대로 남긴다. 취소주문은 입금예정이 비어 있어, 필터가 뒤로 가면
   멀쩡한 날짜×매장이 통째로 제외된다.
4. `총결제금액`/`입금예정금액`의 주문단위 `max()` 집계는 그대로 유지.

### A-2. 쿠팡 — 행 dedup 후 `sum()` 집계 (`_load_coupang_orders_agg`)

1. `required` 집합에 `"is_cancelled"` 추가.
2. `parts.append(df)` 전에 `df["_src_path"] = str(path)` 추가.
3. `pd.concat(parts)` **직후** 원천 행 중복 제거를 먼저 수행한다.
   `sum()`은 중복 적재에 취약하므로 이 단계가 없으면 금액이 부풀 수 있다.

```python
_COUPANG_DEDUP_COLS = [
    "_src_path", "order_date", "order_id", "delivery_type", "order_status",
    "order_summary", "total_price", "is_cancelled", "menu_name", "menu_qty",
    "menu_price", "menu_options",
]

def _deduplicate_coupang_raw(df: pd.DataFrame) -> pd.DataFrame:
    cols = [col for col in _COUPANG_DEDUP_COLS if col in df.columns]
    before = len(df)
    out = df.drop_duplicates(subset=cols, keep="last").copy()
    if before - len(out):
        logger.warning("쿠팡 원천 중복 제거: %d행", before - len(out))
    return out
```

4. 주문단위 집계를 `max` → `sum`으로 변경:

```python
grouped = df.groupby(["brand", "order_id"], as_index=False).agg(
    order_date=("order_date", "max"),
    store=("store", "max"),
    매출액=("매출액", "sum"),
    정산_예정_금액=("정산_예정_금액", "sum"),
)
```

- `매출액`/`정산_예정_금액`은 주문의 첫 행에만 값이 있고 나머지는 NaN이다.
  `_money()`가 NaN→0으로 채우므로 정상 주문 금액은 `sum()` 후에도 동일하고,
  취소 환불 음수행만 추가 반영된다.
- 취소주문을 **제외하지 않는다**. 순액(음수 포함)을 그대로 반영해 unified와 기준을 맞춘다.

### A-3. 테스트 보강 (`tests/test_delivery_commission.py`)

1. `_write_baemin_order()`에 `status: str = "배달완료"` 추가, dict에 `"주문상태": status` 포함.
2. `_write_coupang_order()`에 `is_cancelled: str = "N"` 추가, dict에 포함.
   기존 3개 테스트는 기본값으로 그대로 통과해야 한다.
3. `_write_coupang_order()`는 현재 단일 행만 쓴다. 취소 케이스 검증을 위해
   여러 행(`매출액` 리스트)을 쓸 수 있게 하거나 별도 helper를 추가한다.
4. 신규 테스트 2건:
   - `test_baemin_excludes_cancelled_orders` — `주문상태="주문취소"` 주문이 집계에서 빠지는지.
     동일 날짜×매장에 배달완료 주문 1건을 함께 넣어 날짜 자체가 사라지지 않는지도 확인.
   - `test_coupang_reflects_cancel_refund` — 같은 `order_id`에 `매출액` `0` / `-40_000` 두 행을
     쓰고 `total_amt`가 `-40_000`인지.

---

# Part B — unified_sales 전 매장 전 기간 백필 (코드 변경 없음)

## 배경

`DB_UnifiedSales_Dags.py`는 `backfill=true` + `AUTO_ENFORCE_TEST_STORES`가 있을 때
`lookback_days=None`(전기간)으로 동작하지만, **동시에 대상 매장을 `ADD_TEST_STORES`로 좁힌다.**
현재 `ADD_TEST_STORES`에는 `삼송점` 등 10개만 있어 DAG 트리거만으로는 전 매장이 돌지 않는다.

```python
def _manual_delivery_target_stores(context) -> list[str]:
    if _is_backfill_mode(context) and AUTO_ENFORCE_TEST_STORES:
        return list(AUTO_ENFORCE_TEST_STORES)   # ← 여기서 좁혀짐
    return list(TEST_STORES)

def _manual_delivery_lookback_days(context) -> int | None:
    if _is_backfill_mode(context) and AUTO_ENFORCE_TEST_STORES:
        return None                              # ← 전기간
    return _require_lookback_days()
```

따라서 DAG 트리거 대신 **파이프라인 함수를 직접 호출**한다(부수효과 최소).

## Implementation Steps

### B-1. 백업

```python
python -c "
import shutil, pendulum
from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT
dst = UNIFIED_ROOT.parent / ('unified_sales_bak_' + pendulum.now('Asia/Seoul').format('YYMMDD_HHmm'))
shutil.copytree(UNIFIED_ROOT, dst)
print('backup ->', dst)"
```

### B-2. 대상 매장·규모 확인 (실행 전 필수)

```python
python -c "
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES as S
print(len(S), S)"
```

### B-3. 배민 전기간 재적재

```python
python -c "
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES as S
from modules.transform.pipelines.db.DB_UnifiedSales_baemin import reconcile_baemin_for_test_stores
print(reconcile_baemin_for_test_stores(stores=S, sale_date=None, lookback_days=None))"
```

- `lookback_days=None` → 원천 날짜 + 기존 unified 배민수동/중복 날짜 전체가 대상
  (`_resolve_baemin_target_dates`). 당일 날짜는 자동 제외된다.

### B-4. 쿠팡 전기간 재적재

```python
python -c "
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES as S
from modules.transform.pipelines.db.DB_UnifiedSales_coupang import reconcile_coupang_for_test_stores
print(reconcile_coupang_for_test_stores(stores=S, sale_date=None, lookback_days=None))"
```

### B-5. 배민 최종 정리 (POS 잔존행 제거)

```python
python -c "
from modules.transform.pipelines.db.DB_UnifiedSales_common import DELIVERY_MANUAL_TEST_STORES as S
from modules.transform.pipelines.db.DB_UnifiedSales_baemin import enforce_baemin_manual_only_for_test_stores
print(enforce_baemin_manual_only_for_test_stores(stores=S, sale_date=None, lookback_days=None))"
```

## Reference Code

### modules/transform/pipelines/db/DB_DeliveryCommission.py (현재 상태, 발췌)

```python
def _money(series: pd.Series) -> pd.Series:
    return _nullable_money(series).fillna(0)

def _load_baemin_orders_agg() -> pd.DataFrame:
    ...
    df = pd.concat(parts, ignore_index=True)
    required = {"주문번호", "주문시각", "brand", "store", "총결제금액", "입금예정금액"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"baemin 필수 컬럼 없음: {sorted(missing)}")

    df["총결제금액"] = _money(df["총결제금액"])
    df["입금예정금액"] = _nullable_money(df["입금예정금액"])
    grouped = df.groupby(["brand", "주문번호"], as_index=False).agg(
        주문시각=("주문시각", "max"), store=("store", "max"),
        총결제금액=("총결제금액", "max"), 입금예정금액=("입금예정금액", "max"),
    )
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["brand"], grouped["store"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    unsettled = grouped["총결제금액"].gt(0) & grouped["입금예정금액"].isna()
    ...  # 미정산 날짜×매장 전체 제외 (필터보다 뒤에 유지할 것)

def _load_coupang_orders_agg() -> pd.DataFrame:
    ...
    required = {"order_id", "order_date", "brand", "store", "매출액", "정산_예정_금액"}
    df["매출액"] = _money(df["매출액"])
    df["정산_예정_금액"] = _money(df["정산_예정_금액"])
    grouped = df.groupby(["brand", "order_id"], as_index=False).agg(
        order_date=("order_date", "max"), store=("store", "max"),
        매출액=("매출액", "max"), 정산_예정_금액=("정산_예정_금액", "max"),
    )
```

### modules/transform/pipelines/db/DB_UnifiedSales_coupang.py — 중복 제거 기준 / 백필 진입점

```python
def reconcile_coupang_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    """TEST_STORES의 쿠팡이츠 행을 coupang_macro 직수집 기준으로 교정."""
    dates = _resolve_target_dates(stores, sale_date, lookback_days)

def _deduplicate_raw(df: pd.DataFrame, store: str, ym: str) -> pd.DataFrame:
    cols = [
        "_src_path", "order_date", "order_id", "delivery_type", "order_status",
        "order_summary", "total_price", "is_cancelled", "menu_name", "menu_qty",
        "menu_price", "menu_options",
    ]
    cols = [col for col in cols if col in df.columns]
    before = len(df)
    out = df.drop_duplicates(subset=cols, keep="last").copy()
    dropped = before - len(out)
    if dropped:
        logger.warning("쿠팡 원천 중복 제거: store=%s ym=%s 제거=%d행", store, ym, dropped)
    return out
```

### modules/transform/pipelines/db/DB_UnifiedSales_baemin.py — 배달완료 기준(정답 기준) / 백필 진입점

```python
def reconcile_baemin_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,
) -> str:
    dates = _resolve_baemin_target_dates(stores, sale_date, lookback_days)

# _transform_to_unified 내부 (line 564)
df = df[df["주문상태"].fillna("").astype(str).str.strip().eq("배달완료")].copy()
```

### tests/test_delivery_commission.py — 기존 helper (수정 대상)

```python
def _write_baemin_order(
    root: Path, brand: str, store: str, total: int, deposit: int | str, *,
    order_id: str = "공통주문번호",
    order_time: str = "2026. 07. 19. (일) 오후 12:00:00",
) -> None:
    pd.DataFrame([{
        "주문번호": order_id, "주문시각": order_time,
        "총결제금액": total, "입금예정금액": deposit,
    }]).to_parquet(_partition(root, brand, store) / "orders_2026-07.parquet", index=False)


def _write_coupang_order(root: Path, brand: str, store: str, total: int, settlement: int) -> None:
    pd.DataFrame([{
        "order_id": "공통주문번호", "order_date": "2026.07.19 12:00:00",
        "매출액": total, "정산_예정_금액": settlement,
    }]).to_parquet(_partition(root, brand, store) / "orders_2026-07.parquet", index=False)
```

## Test Cases

### Part A

1. [유닛 테스트] `python -m pytest tests/test_delivery_commission.py -q`
   → 기대: 기존 3건 + 신규 2건 전부 PASS

2. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission; print('ok')"`
   → 기대: `ok`

3. [DAG import] `python -c "from dags.db.DB_DeliveryCommission_Dags import dag; print(dag.dag_id)"`
   → 기대: `DB_DeliveryCommission_Dags`

4. [마트 재생성 — 백업 후 실행]
   ```
   python -c "import shutil; from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH as p; shutil.copy(p, str(p)+'.bak'); print('backup ok')"
   python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission; print(build_delivery_commission())"
   ```
   → 기대: `delivery_commission N행 -> ...` 출력, 예외 없음

5. [배민 취소 제외 확인]
   ```
   python -c "
   import pandas as pd
   from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH as p
   new = pd.read_parquet(p); old = pd.read_parquet(str(p)+'.bak')
   b = lambda d: d[d.platform=='배달의민족'].total_amt.sum()
   print('배민 매출 감소분:', b(old)-b(new))"
   ```
   → 기대: 감소분 ≈ 4,801,600원 (취소 181주문분)

6. [쿠팡 환불 반영 확인]
   ```
   python -c "
   import pandas as pd
   from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH as p
   d = pd.read_parquet(p)
   print(d[(d.sale_date=='2026-05-29')&(d.store=='송파삼전점')&(d.platform=='쿠팡이츠')].to_string())"
   ```
   → 기대: `total_amt == 327700` (수정 전 367,700에서 환불 -40,000 반영)

### Part B

7. [백필 실행 결과] B-3 / B-4 / B-5 각 명령의 반환 문자열
   → 기대: `제거=N행 추가=M행` 형태로 정상 종료, 예외 없음

8. [스테일 해소 — 송파삼전점 개별 검증]
   ```
   python -c "
   import pandas as pd
   from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT
   u = pd.read_parquet(UNIFIED_ROOT/'unified_sales_260628.parquet')
   u = u[(u.store=='송파삼전점')&(u.platform=='배달의민족')]
   print('orders:', sorted(u.order_id.unique()))
   print('sum:', pd.to_numeric(u.total_price, errors='coerce').sum())"
   ```
   → 기대: 취소된 `T2E40000D883` 사라지고 8주문 / 합계 234,500 (±10원)

9. [최종 대조 — 송파삼전점 전 기간]
   ```
   python -c "
   import pandas as pd
   from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT
   from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH
   dc = pd.read_parquet(DELIVERY_COMMISSION_PATH)
   dc = dc[dc.store=='송파삼전점']
   parts = []
   for p in sorted(UNIFIED_ROOT.glob('unified_sales_*.parquet')):
       try: d = pd.read_parquet(p, columns=['sale_date','store','platform','total_price'])
       except Exception: continue
       d = d[(d.store.astype(str).str.strip()=='송파삼전점') & d.platform.isin(['배달의민족','쿠팡이츠'])]
       if len(d): parts.append(d)
   u = pd.concat(parts)
   u['total_price'] = pd.to_numeric(u.total_price, errors='coerce').fillna(0)
   ug = u.groupby(['sale_date','platform'], as_index=False).total_price.sum()
   m = dc.merge(ug, on=['sale_date','platform'], how='outer')
   m['gap'] = m.total_amt.fillna(0) - m.total_price.fillna(0)
   big = m[m.gap.abs() > 10]
   print('키:', len(m), '| |gap|>10:', len(big))
   print(big[['sale_date','platform','total_amt','total_price','gap']].to_string())"
   ```
   → 기대: `|gap|>10` 이 0건. 단 마트 미생성 당일(최근 1일)은 unified만 존재할 수 있으므로 제외 판단

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~9 순서대로 실행 (Part A → Part B 순서 고정)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정 또는 백필 재실행
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

### Part A
- 배민 미정산 제외 로직(`unsettled`)의 **위치를 바꾸지 말 것**. 배달완료 필터가 반드시 앞이다.
- 쿠팡 `sum()` 전환은 반드시 행 dedup과 세트로 적용한다. dedup 없이 `sum()`만 바꾸면 금액이 부풀 수 있다.
- 쿠팡 취소주문은 **제외하지 않고** 순액(음수 포함)으로 반영한다. 배민만 상태 필터로 제외한다.
- 배민 쪽 `max()` 집계는 유지한다(`sum()`으로 바꾸지 말 것).
- `_write_parquet_atomic`, `_finalize`, `OUTPUT_COLUMNS`는 수정하지 않는다.
- 마트 실행 전 반드시 백업본(`.bak`)을 만든다. 이 DAG는 파일 전체를 원자적 교체한다.

### Part B
- **Part A → Part B 순서를 지킨다.** 반대로 하면 Test Case 9 대조가 무의미해진다.
- Part B는 코드 수정이 아니다. `DB_UnifiedSales_baemin.py` / `_coupang.py` / `_common.py`를 고치지 말 것.
- `ADD_TEST_STORES` 목록을 **영구 변경하지 말 것**. 직접 호출 방식이므로 건드릴 필요가 없다.
- unified 일별 parquet 다수가 재작성된다. B-1 백업(`unified_sales_bak_*`)을 반드시 먼저 만든다.
- 백필 도중 실패하면 부분 적용 상태가 된다. 백업에서 복원 후 재시도한다.
- 배민 ±1~8원 반올림 차이는 정상이다. 수정 대상 아님(unified의 아이템 비례배분 반올림).
  대조 허용오차는 ±10원.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
