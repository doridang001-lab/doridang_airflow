# delivery_commission 마트 우가클 지표 3종, 배민 즉시할인, 쿠팡 CMG 추가

## Task

`delivery_commission.parquet`(PowerBI 배달수수료 마트)는 현재 배민 우리가게클릭 CSV에서 **광고지출만** 읽어 정산금액 차감에 사용한다. 같은 CSV에 이미 존재하는 노출수/클릭수/주문수를 활용해 광고 효율 파생 컬럼 3종을 마트에 추가한다. 또한 배민 주문 parquet의 `즉시할인_파트너부담`을 일자×매장×브랜드 단위로 합산해 `배민_즉시할인` 컬럼으로 추가하고, 쿠팡 CMG CSV의 광고비율/비용/신규고객/노출/클릭 지표를 쿠팡이츠 행에 추가한다.

| 신규 컬럼 | 계산식 |
|---|---|
| `배민_즉시할인` | 주문번호 단위 대표 `즉시할인_파트너부담` 합계 |
| `우가클_평균비용` | 광고지출 / 클릭수 (CPC) |
| `우가클_주문수` | 주문수 합계 |
| `우가클_클릭율` | 클릭수 / 노출수 |
| `쿠팡_신규비율` | CMG `광고비율`의 신규 비율 |
| `쿠팡_재주문비율` | CMG `광고비율`의 재주문 비율 |
| `쿠팡_광고비용` | sum(`광고비용`) |
| `쿠팡_신규고객` | sum(`신규고객`) |
| `쿠팡_광고노출수` | sum(`광고노출수`) |
| `쿠팡_광고클릭수` | sum(`광고클릭수`) |

**빈 값 정책(확정)**: 쿠팡이츠 행의 `배민_즉시할인`과 우가클 3컬럼은 **NULL**. 배민 행의 쿠팡 CMG 컬럼도 **NULL**. 배민 중 우가클 미수집 날짜·매장은 우가클 3컬럼만 **NULL**(0 아님). 쿠팡 중 CMG 미수집 날짜·매장은 쿠팡 CMG 컬럼만 **NULL**. 기존 `ad_spend`의 `fillna(0)`은 정산금액 계산용이므로 **그대로 유지**.

## 쿠팡 CMG 추가 명세

- 경로: `COUPANG_ORDERS_DETAIL_DB / "cmg"` = `{ANALYTICS_DB}/coupang_macro/cmg`
- 글롭 패턴: `brand=*/store=*/ym=*/cmg.csv`
- 필수 컬럼: `조회일자`, `광고비율`, `광고비용`, `신규고객`, `광고노출수`, `광고클릭수`
- join key: CSV 내부 `매장명`이 아니라 파티션 `brand/store`와 `조회일자`
- store key: `lookup_store_key(brand, partition_store)` 후 공백 제거
- 비율 저장: `15%`는 parquet에 `0.15` float로 저장
- `광고비율` 파싱:
  - `전체 15%` → 신규 `0.15`, 재주문 `0.15`
  - `신규 12%, 재주문 7%` → 신규 `0.12`, 재주문 `0.07`
  - `신규 10%` → 신규 `0.10`, 재주문 `NULL`
- 같은 `date/store/brand`에 서로 다른 비율이 섞이면 fail-fast.
- 전매장명은 CSV 내부 표기(`닭도리탕 전문 도리당 ...`, `나홀로 1인 곱도리탕 ...`)가 미묘하게 달라 join에 쓰지 않는다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 이 모듈은 fail-fast 스타일: 원천 이상 시 `RuntimeError`를 던져 기존 마트를 보존한다. 예외를 삼키지 말 것.

## Files to Create / Modify

**수정**
- `modules/transform/pipelines/db/DB_DeliveryCommission.py` (핵심 변경 전부)
- `tests/test_delivery_commission.py` (헬퍼 확장 + 신규 테스트 3개 이상)

**변경 없음**
- `dags/db/DB_DeliveryCommission_Dags.py` — 오케스트레이션만 담당, 로직은 전부 파이프라인 모듈에 있음. 건드리지 말 것.

## 원천 데이터 명세

경로 상수: `BAEMIN_OUR_STORE_CLICKS_DB` = `{ANALYTICS_DB}/baemin_macro/metrics_our_store_clicks`
글롭 패턴: `BAEMIN_OUR_STORE_CLICKS_DB.rglob("*.csv")`
실제 파일 예: `brand=도리당/store=광명철산점/ym=2026-06/woori_shop_click.csv`

헤더 (209개 파일 전수 확인 결과 100% 동일):
```
collected_at,store_id,store_name,날짜,광고지출,노출수,클릭수,주문수,주문금액,광고효과
```

샘플 행:
```
2026-07-26T03:53:50.817635+09:00,14827091,도리당 광명철산점,2026-06-01,31200,1392,48,2,85200,2.73
2026-07-26T03:53:50.817635+09:00,14827091,도리당 광명철산점,2026-06-02,24700,1172,38,13,368700,14.93
```

## Implementation Steps

### 1. `OUTPUT_COLUMNS` 확장

`DB_DeliveryCommission.py` 18행. `brand` 뒤에 4개 추가 (우가클 3컬럼은 마지막 순서 고정):

```python
OUTPUT_COLUMNS = [
    "sale_date",
    "store",
    "platform",
    "total_amt",
    "settlement_amount",
    "diff_amt",
    "brand",
    "배민_즉시할인",
    "우가클_평균비용",
    "우가클_주문수",
    "우가클_클릭율",
]
```

### 2. `_load_baemin_orders_agg()` 확장

- 반환 `columns` 리스트에 `baemin_partner_instant_discount` 추가
- 필수 컬럼 체크에 `즉시할인_파트너부담` 추가
- `_money()`로 숫자화 후 주문번호 단위 groupby에서 `max()`로 대표값을 잡는다. 상세 옵션행 중복으로 같은 주문의 즉시할인이 과대집계되지 않게 한다.
- 일자×매장×브랜드 groupby에서 `즉시할인_파트너부담`을 `sum()`하고, 최종 컬럼명을 `baemin_partner_instant_discount`로 변경한다.
- dtype은 금액 합계이므로 `int` 유지. 배민 주문 원천에 없는 경우는 fail-fast로 기존 마트 보존.

### 3. `_load_baemin_ad_spend_agg()` 확장 (230행~)

- 반환 `columns` 리스트: `["date", "store", "brand", "ad_spend", "wgc_avg_cost", "wgc_orders", "wgc_ctr"]`
- 파일별 필수 컬럼 체크에 `노출수`, `클릭수`, `주문수` 추가 → `{"날짜", "store_name", "광고지출", "노출수", "클릭수", "주문수"}`
  (전 파일 존재 확인 완료. 기존 fail-fast 스타일 유지)
- 파일별 파싱 시 기존 `_money()` 재사용해 3개 컬럼 숫자화 → `out["impressions"]`, `out["clicks"]`, `out["wgc_orders"]`
- groupby(`date`,`store`,`brand`)에서 `ad_spend`와 함께 `impressions`/`clicks`/`wgc_orders`도 `sum()`

```python
result = result.groupby(["date", "store", "brand"], as_index=False)[
    ["ad_spend", "impressions", "clicks", "wgc_orders"]
].sum()
```

- **비율은 반드시 합계 이후에 계산** (일자별 비율의 평균이 아님). 분모 0이면 `pd.NA`:

```python
clicks = result["clicks"]
impressions = result["impressions"]
result["wgc_avg_cost"] = (result["ad_spend"] / clicks.where(clicks.ne(0))).round(0)
result["wgc_ctr"] = (clicks / impressions.where(impressions.ne(0))).round(4)
```

- dtype: `ad_spend`는 기존대로 `int`, `wgc_orders`는 `Int64`(nullable), `wgc_avg_cost`/`wgc_ctr`은 `float64`
  → **`wgc_avg_cost`를 int 캐스팅하지 말 것** (NULL 보존 불가)
- 중간 컬럼 `impressions`/`clicks`는 반환하지 않는다 (`return result[columns]`가 자동 처리)

### 4. `build_delivery_commission()` 수정 (388행~)

```python
WGC_COLUMNS = {
    "wgc_avg_cost": "우가클_평균비용",
    "wgc_orders": "우가클_주문수",
    "wgc_ctr": "우가클_클릭율",
}
```

- 배민 merge 후 `ad_spend`만 `fillna(0)` 유지. **우가클 3컬럼은 fillna 금지** (미수집 날짜는 NULL 유지)
- 배민 프레임 select + rename:

```python
baemin = baemin.rename(columns={"baemin_partner_instant_discount": "배민_즉시할인", **WGC_COLUMNS})
baemin = baemin[
    ["date", "store", "platform", "total_amt", "settlement_amount", "brand", "배민_즉시할인", *WGC_COLUMNS.values()]
]
```

- 쿠팡 프레임에는 `배민_즉시할인`과 우가클 3컬럼을 명시적으로 생성 (concat 암묵 정렬에 의존 금지). dtype을 배민과 일치시켜 object 승격 방지:

```python
coupang["배민_즉시할인"] = pd.Series(pd.NA, index=coupang.index, dtype="Int64")
coupang["우가클_평균비용"] = pd.Series(float("nan"), index=coupang.index, dtype="float64")
coupang["우가클_주문수"] = pd.Series(pd.NA, index=coupang.index, dtype="Int64")
coupang["우가클_클릭율"] = pd.Series(float("nan"), index=coupang.index, dtype="float64")
```

### 5. `_finalize()` / `_write_parquet_atomic()`

구조 변경 불필요.
- `_finalize`의 빈 DataFrame fallback `columns` 리스트에도 신규 4컬럼을 추가해 스키마 일관성 유지
- `result = result[OUTPUT_COLUMNS]` 가 순서를 보장
- `_write_parquet_atomic`의 `list(verified.columns) != OUTPUT_COLUMNS` 검증이 신규 컬럼까지 자동 커버
- 기존 중복키/플랫폼/브랜드 검증 로직은 손대지 말 것

### 6. 테스트 수정 (`tests/test_delivery_commission.py`)

- 44행 `_write_baemin_ad` 헬퍼에 `impressions`/`clicks`/`orders` 인자 추가. **기본값을 주어 기존 호출부 6곳이 무수정으로 통과**하게 할 것:

```python
def _write_baemin_ad(
    root: Path,
    brand: str,
    store_name: str,
    spend: int,
    *,
    impressions: int = 1_000,
    clicks: int = 50,
    orders: int = 5,
) -> None:
    path = _partition(root, brand, "광고") / "metrics.csv"
    pd.DataFrame([{
        "날짜": "2026-07-19",
        "store_name": store_name,
        "광고지출": spend,
        "노출수": impressions,
        "클릭수": clicks,
        "주문수": orders,
    }]).to_csv(path, index=False, encoding="utf-8-sig")
```

- 105행 `test_build_combines_both_brands_and_brand_aware_store_aliases`의 `assert list(result.columns) == delivery.OUTPUT_COLUMNS` 는 그대로 통과해야 한다

- 신규 테스트 3개 추가:
  1. `test_wgc_metrics_are_computed_from_summed_totals`
     — 같은 매장에 서로 다른 날짜 CSV 2건(예: 광고 30,000/노출 1,000/클릭 50/주문 3, 광고 10,000/노출 1,000/클릭 10/주문 1)을 넣고, 합계 기준 평균비용 = 40,000/60 ≈ 667, 클릭율 = 60/2000 = 0.03, 주문수 = 4 임을 검증. **일별 비율의 단순 평균과 값이 달라지는 케이스로 구성할 것**
  2. `test_wgc_metrics_are_null_for_coupang_and_zero_clicks`
     — 쿠팡 행 3컬럼이 전부 `isna()`, 그리고 클릭수 0인 배민 행의 `우가클_평균비용`이 `isna()`임을 검증
  3. `test_baemin_partner_instant_discount_is_not_double_counted_by_detail_rows`
     — 같은 주문번호 상세행이 2행 이상 있어도 `즉시할인_파트너부담`은 주문 대표값만 더해지는지 검증

## Reference Code

### modules/transform/pipelines/db/DB_DeliveryCommission.py (현재 상태 발췌)

```python
import logging
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB,
    BAEMIN_OUR_STORE_CLICKS_DB,
    COUPANG_ORDERS_DB,
    DELIVERY_COMMISSION_PATH,
)
from modules.transform.utility.store_normalize import lookup_store_key, strip_brand

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "sale_date", "store", "platform", "total_amt",
    "settlement_amount", "diff_amt", "brand",
]
REQUIRED_BRANDS = {"도리당", "나홀로"}


def _money(series: pd.Series) -> pd.Series:
    return _nullable_money(series).fillna(0)


def _load_baemin_ad_spend_agg() -> pd.DataFrame:
    columns = ["date", "store", "brand", "ad_spend"]
    files = sorted(BAEMIN_OUR_STORE_CLICKS_DB.rglob("*.csv"))
    if not files:
        raise RuntimeError(f"baemin 광고 csv 없음: {BAEMIN_OUR_STORE_CLICKS_DB}")
    _validate_source_brands(files, "baemin ads")

    parts = []
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = _read_csv_with_fallback(path)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        missing = {"날짜", "store_name", "광고지출"} - set(df.columns)
        if missing:
            read_errors.append((path, ValueError(f"필수 컬럼 없음: {sorted(missing)}")))
            continue
        out = pd.DataFrame()
        out["date"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
        brands = pd.Series(_partition_value(path, "brand"), index=df.index, dtype=str)
        out["brand"] = brands
        out["store"] = _normalize_store_name(brands, df["store_name"])
        out["ad_spend"] = _money(df["광고지출"])
        parts.append(out)
    _raise_read_errors("baemin ads", read_errors)

    if not parts:
        raise RuntimeError("baemin ads 유효 데이터 없음; 기존 마트 보존")

    result = pd.concat(parts, ignore_index=True)
    result = result.dropna(subset=["date"])
    result = result[result["store"] != ""]
    if result.empty:
        raise RuntimeError("baemin ads 집계 결과 없음; 기존 마트 보존")
    result = result.groupby(["date", "store", "brand"], as_index=False)["ad_spend"].sum()
    result["ad_spend"] = result["ad_spend"].round().astype(int)
    return result[columns]


def build_delivery_commission() -> str:
    baemin_orders = _load_baemin_orders_agg()
    baemin_ads = _load_baemin_ad_spend_agg()
    baemin = baemin_orders.merge(baemin_ads, on=["date", "store", "brand"], how="left")
    if not baemin.empty:
        baemin["ad_spend"] = baemin["ad_spend"].fillna(0)
        baemin["settlement_amount"] = baemin["baemin_deposit_amt"] - baemin["ad_spend"]
        baemin["platform"] = "배달의민족"
        baemin = baemin[
            ["date", "store", "platform", "total_amt", "settlement_amount", "brand"]
        ]

    coupang = _load_coupang_orders_agg()
    if not coupang.empty:
        coupang["settlement_amount"] = coupang["coupang_settlement_amt"]
        coupang["platform"] = "쿠팡이츠"
        coupang = coupang[
            ["date", "store", "platform", "total_amt", "settlement_amount", "brand"]
        ]

    result = _finalize([df for df in (baemin, coupang) if not df.empty])
    _write_parquet_atomic(result, DELIVERY_COMMISSION_PATH)
    logger.info("delivery_commission mart 저장 완료: %s rows=%s", DELIVERY_COMMISSION_PATH, len(result))
    return f"delivery_commission {len(result)}행 -> {DELIVERY_COMMISSION_PATH}"
```

### tests/test_delivery_commission.py (수정 대상 헬퍼)

```python
def _write_baemin_ad(root: Path, brand: str, store_name: str, spend: int) -> None:
    path = _partition(root, brand, "광고") / "metrics.csv"
    pd.DataFrame([{"날짜": "2026-07-19", "store_name": store_name, "광고지출": spend}]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )


def _configure_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    output = tmp_path / "mart" / "delivery_commission.parquet"
    monkeypatch.setattr(delivery, "BAEMIN_ORDERS_DB", tmp_path / "baemin_orders")
    monkeypatch.setattr(delivery, "BAEMIN_OUR_STORE_CLICKS_DB", tmp_path / "baemin_ads")
    monkeypatch.setattr(delivery, "COUPANG_ORDERS_DB", tmp_path / "coupang_orders")
    monkeypatch.setattr(delivery, "DELIVERY_COMMISSION_PATH", output)
    return output
```

## Test Cases

1. **기존 회귀** `python -m pytest tests/test_delivery_commission.py -q`
   → 기대: 기존 테스트 전부 PASS (신규 컬럼 추가로 깨지지 않음)

2. **신규 테스트** `python -m pytest tests/test_delivery_commission.py -q -k "wgc or instant_discount"`
   → 기대: 신규 우가클/즉시할인 테스트 PASS

3. **DAG import** `python -c "import dags.db.DB_DeliveryCommission_Dags"`
   → 기대: ImportError 없음

4. **실데이터 실행** `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission; print(build_delivery_commission())"`
   → 기대: `delivery_commission N행 -> ...` 출력, RuntimeError 없음

5. **산출물 스키마/NULL 정책 확인**
```bash
python -c "
import pandas as pd
from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH
df = pd.read_parquet(DELIVERY_COMMISSION_PATH)
print(df.dtypes)
assert list(df.columns)[-4:] == ['배민_즉시할인','우가클_평균비용','우가클_주문수','우가클_클릭율']
cp = df[df.platform=='쿠팡이츠'][['배민_즉시할인','우가클_평균비용','우가클_주문수','우가클_클릭율']]
assert cp.isna().all().all(), '쿠팡 행은 전부 NULL이어야 함'
bm = df[df.platform=='배달의민족']
print(bm[['sale_date','store','brand','배민_즉시할인','우가클_평균비용','우가클_주문수','우가클_클릭율']].head(10))
print('배민 NULL 비율:', bm['우가클_클릭율'].isna().mean())
"
```
   → 기대: assert 전부 통과. `배민_즉시할인`/`우가클_주문수` Int64, `우가클_평균비용`/`우가클_클릭율` float64

6. **원천 교차 검증** — 광명철산점 2026-06-01 원천(광고지출 31,200 / 노출 1,392 / 클릭 48 / 주문 2) 대비
```bash
python -c "
import pandas as pd
from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH
df = pd.read_parquet(DELIVERY_COMMISSION_PATH)
row = df[(df.sale_date=='2026-06-01') & (df.store.str.contains('광명철산')) & (df.platform=='배달의민족')]
print(row[['store','brand','우가클_평균비용','우가클_주문수','우가클_클릭율']])
"
```
   → 기대: 평균비용 650.0, 주문수 2, 클릭율 0.0345
   (단 해당 매장·날짜에 우가클 CSV가 1건뿐인 경우. 여러 건이면 합계 기준으로 재계산해 대조할 것)

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- **`ad_spend`의 `fillna(0)`은 절대 제거하지 말 것** — 정산금액(`settlement_amount = baemin_deposit_amt - ad_spend`) 계산에 쓰인다. 배민 우가클 3컬럼만 NULL 유지.
- `배민_즉시할인`은 정산금액 계산에 반영하지 말고, 쿠팡 행에만 NULL을 유지한다.
- **비율을 파일별/일별로 계산한 뒤 평균내지 말 것** — 반드시 groupby sum 이후 합계끼리 나눈다.
- `우가클_평균비용`을 `astype(int)`로 캐스팅하지 말 것 (NULL 소실).
- `_finalize()`의 중복키/플랫폼/브랜드 검증, `_write_parquet_atomic()`의 원자적 교체 로직은 수정 금지.
- 마트는 원자적 교체이므로 실패 시 기존 파일이 보존된다. 별도 백업 로직 추가 불필요.
- `dags/db/DB_DeliveryCommission_Dags.py`는 수정하지 않는다.
- 기존 함수 시그니처(`build_delivery_commission()` → `str`) 변경 금지.
- print 금지, `logger` 사용.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (전부 명시)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게 (private helper는 `_` 접두)
