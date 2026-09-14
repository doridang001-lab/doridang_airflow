# delivery_commission 취소주문 처리 수정

## Task

`delivery_commission.parquet` 마트가 취소주문을 잘못 집계한다. 배민은 `주문상태` 필터가 없어
`주문취소` 1,159행(181주문 / 총결제 4,801,600원, 입금예정 0원)을 매출로 계상하고,
쿠팡은 주문단위 `max()` 집계라 환불 음수행(예: `0`, `-40,000` → `max=0`)이 소거된다.
결과적으로 매출(`total_amt`)이 부풀고 수수료(`diff_amt`)가 과대계산되어 unified_sales와
불일치한다(실측: 2026-05-29 송파삼전점 쿠팡 commission 367,700 / unified 264,100).
원천 parquet 자체는 정상이며 마트 집계 코드만 수정한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 필수 컬럼 누락은 `raise RuntimeError(...)` fail-fast (기존 파일 패턴 유지)

## Files to Create / Modify

- 수정: `modules/transform/pipelines/db/DB_DeliveryCommission.py`
- 수정: `tests/test_delivery_commission.py`
- 생성 파일 없음

## Implementation Steps

### 1. 배민 — 배달완료만 집계 (`_load_baemin_orders_agg`)

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
4. `총결제금액`/`입금예정금액`의 주문단위 `max()` 집계는 그대로 유지(배달완료 필터 후 중복 안전, 환불 음수 없음).

### 2. 쿠팡 — 행 dedup 후 `sum()` 집계 (`_load_coupang_orders_agg`)

1. `required` 집합에 `"is_cancelled"` 추가.
2. `pd.concat(parts)` **직후** 원천 행 중복 제거를 먼저 수행한다.
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

- 현재 `_load_coupang_orders_agg`는 `_src_path` 컬럼을 만들지 않는다.
  `parts.append(df)` 하기 전에 `df["_src_path"] = str(path)`를 추가해 파일 간 동일 행이
  서로 다른 원천으로 보존되도록 한다(`DB_UnifiedSales_coupang._deduplicate_raw`와 동일 기준).

3. 주문단위 집계를 `max` → `sum`으로 변경:

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

### 3. 테스트 보강 (`tests/test_delivery_commission.py`)

1. `_write_baemin_order()` 시그니처에 `status: str = "배달완료"` 추가하고 dict에 `"주문상태": status` 포함.
2. `_write_coupang_order()` 시그니처에 `is_cancelled: str = "N"` 추가하고 dict에 포함.
   기존 3개 테스트는 기본값으로 그대로 통과해야 한다.
3. `_write_coupang_order()`는 현재 단일 행만 쓴다. 취소 케이스 검증을 위해
   여러 행(`매출액` 리스트)을 쓸 수 있게 하거나 별도 helper를 추가한다.
4. 신규 테스트 2건 추가:
   - `test_baemin_excludes_cancelled_orders`:
     `주문상태="주문취소"` 주문이 `total_amt`/`settlement_amount`에 포함되지 않는지.
     동일 날짜×매장에 배달완료 주문 1건을 함께 넣어 날짜 자체가 사라지지 않는지도 확인.
   - `test_coupang_reflects_cancel_refund`:
     같은 `order_id`에 `매출액` `0` / `-40_000` 두 행을 쓰고 `total_amt`가 `-40_000`인지.

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

### modules/transform/pipelines/db/DB_UnifiedSales_coupang.py — 중복 제거 기준

```python
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

### modules/transform/pipelines/db/DB_UnifiedSales_baemin.py:564 — 배달완료 기준(정답 기준)

```python
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

7. [키 중복·플랫폼 가드 회귀] Test Case 4가 예외 없이 완료되면 `_finalize()`의
   중복키/플랫폼 누락 가드 통과로 간주

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- 배민 미정산 제외 로직(`unsettled`)의 **위치를 바꾸지 말 것**. 배달완료 필터가 반드시 앞이다.
- 쿠팡 `sum()` 전환은 반드시 행 dedup과 세트로 적용한다. dedup 없이 `sum()`만 바꾸면 금액이 부풀 수 있다.
- 쿠팡 취소주문은 **제외하지 않고** 순액(음수 포함)으로 반영한다. 배민만 상태 필터로 제외한다.
- 배민 쪽 `max()` 집계는 유지한다(`sum()`으로 바꾸지 말 것).
- `_write_parquet_atomic`, `_finalize`, `OUTPUT_COLUMNS`는 수정하지 않는다.
- 마트 실행 전 반드시 백업본(`.bak`)을 만든다. 이 DAG는 파일 전체를 원자적 교체한다.
- unified_sales 백필(스테일 해소)은 이 태스크 범위 밖이다. 별도 작업으로 처리한다.
- 배민 ±1~8원 반올림 차이는 정상이다. 수정 대상 아님(unified의 아이템 비례배분 반올림).

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
