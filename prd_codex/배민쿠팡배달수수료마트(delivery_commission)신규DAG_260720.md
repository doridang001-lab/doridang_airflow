# 배민·쿠팡 배달 수수료 마트 (delivery_commission) 신규 DAG

## Task
`OneDrive/data/mart/delivery_commission` 폴더가 이미 예약돼 있지만 비어있다. 배민(baemin_macro)·쿠팡(coupang_macro) 원천의 매출/정산/광고비 데이터를 합쳐 **일자×매장×플랫폼** 단위 수수료(정산 차이) 요약 parquet을 만들어 이 마트에 적재하는 신규 DAG를 만든다. 매일 전체 재계산 후 overwrite 방식(기존 `DB_CollectionCompare.py`와 동일한 패턴)이며, 증분/upsert는 필요 없다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG는 orchestration만, 비즈니스 로직은 전부 `modules/`에 둔다 (dags/CLAUDE.md 규칙)

## Files to Create / Modify
- 수정: `modules/transform/utility/paths.py` — `DELIVERY_COMMISSION_DIR` 상수 추가
- 수정: `modules/transform/utility/schedule.py` — `DB_DELIVERY_COMMISSION_TIME` 상수 추가
- 생성: `modules/transform/pipelines/db/DB_DeliveryCommission.py` — 핵심 계산 로직
- 생성: `dags/db/DB_DeliveryCommission_Dags.py` — DAG orchestration

## Implementation Steps

### 1. `modules/transform/utility/paths.py`
`COLLECTION_COMPARE_PATH = MART_DB / "collection_compare" / "collection_compare.parquet"` 라인(268번째 줄) 근처에 추가:
```python
DELIVERY_COMMISSION_DIR = MART_DB / "delivery_commission"
DELIVERY_COMMISSION_PATH = DELIVERY_COMMISSION_DIR / "delivery_commission.parquet"
```
기존 `BAEMIN_ORDERS_DB`, `BAEMIN_OUR_STORE_CLICKS_DB`, `COUPANG_ORDERS_DB`는 이미 존재하므로 그대로 import해서 쓴다 (아래 Reference Code 참고).

### 2. `modules/transform/utility/schedule.py`
`DB_COLLECTION_COMPARE_TIME` 라인(42번째 줄) 근처에 추가:
```python
DB_DELIVERY_COMMISSION_TIME = "15 9 * * *"  # 매일 09:15 실행 (DB_UNIFIED_SALES_GUARD 09:05 이후)
```

### 3. `modules/transform/pipelines/db/DB_DeliveryCommission.py` (신규)

`DB_CollectionCompare.py`의 `load_baemin_macro()` / `load_coupang_macro()` / `_partition_value()` / `_normalize_store_name()` 패턴을 그대로 재사용/확장한다 (Reference Code 섹션 참고). 작성할 함수:

**`_partition_value(path, prefix)`** — Reference Code의 것을 그대로 복사 (brand=/store= 파티션 폴더명에서 값 추출).

**`_normalize_store_name(series)`** — Reference Code의 것을 그대로 복사 (`normalize_for_join` 기반).

**`_load_baemin_orders_agg() -> pd.DataFrame`**
- `BAEMIN_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet")` 전체 로드
- `store` 컬럼 없으면 `_partition_value(path, "store")`로 채움 (Reference Code의 `load_baemin_macro` 패턴)
- 필수 컬럼 체크: `{"주문번호", "주문시각", "store", "총결제금액", "입금예정금액"}` — 없으면 경고 로그 후 빈 DF 반환
- `총결제금액`, `입금예정금액` 모두 콤마 제거 후 `pd.to_numeric(..., errors="coerce").fillna(0)`
- **주문번호 기준 dedup**: `groupby("주문번호", as_index=False).agg(주문시각=("주문시각","max"), store=("store","max"), 총결제금액=("총결제금액","max"), 입금예정금액=("입금예정금액","max"))` (Reference Code의 `load_baemin_macro`와 동일한 dedup 이유 — 원본이 주문 내 메뉴 행마다 주문 전체 금액이 반복 기재됨)
- `date = _baemin_order_date(주문시각)` (Reference Code의 `_baemin_order_date` 정규식 재사용)
- `store = _normalize_store_name(store)`
- `groupby(["date","store"], as_index=False)[["총결제금액","입금예정금액"]].sum()` → 컬럼명 `total_amt`, `baemin_deposit_amt`로 rename
- 반환 컬럼: `["date", "store", "total_amt", "baemin_deposit_amt"]`

**`_load_baemin_ad_spend_agg() -> pd.DataFrame`**
- `BAEMIN_OUR_STORE_CLICKS_DB.glob("*.csv")` (또는 하위 디렉토리 있으면 `rglob`) 전체 로드, 인코딩 폴백 `("utf-8-sig","utf-8","cp949")` (Reference Code의 `DB_OrderCrossAnalysis._read_csv_utf8` 패턴 참고)
- 컬럼: `날짜, store_name, 광고지출`
- `date = pd.to_datetime(날짜, errors="coerce").dt.strftime("%Y-%m-%d")`
- `store = _normalize_store_name(store_name)`
- `광고지출 = pd.to_numeric(콤마제거, errors="coerce").fillna(0)`
- `groupby(["date","store"], as_index=False)["광고지출"].sum()` → 컬럼명 `ad_spend`
- 반환 컬럼: `["date", "store", "ad_spend"]`

**`_load_coupang_orders_agg() -> pd.DataFrame`**
- `COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet")` 전체 로드
- `store` 컬럼 파티션값으로 채움 (Reference Code의 `load_coupang_macro` 패턴)
- 필수 컬럼: `{"order_id", "order_date", "store", "매출액", "취소금액", "정산_예정_금액"}`
- 세 금액 컬럼 모두 콤마 제거 후 숫자 변환
- **order_id 기준 dedup**: `groupby("order_id", as_index=False).agg(order_date=("order_date","max"), store=("store","max"), 매출액=("매출액","max"), 취소금액=("취소금액","max"), 정산_예정_금액=("정산_예정_금액","max"))`
- `date = _coupang_order_date(order_date)` (Reference Code의 `_coupang_order_date` 재사용)
- `store = _normalize_store_name(store)`
- `groupby(["date","store"], as_index=False)[["매출액","취소금액","정산_예정_금액"]].sum()`
- `total_amt = 매출액 - 취소금액`
- 반환 컬럼: `["date", "store", "total_amt", "정산_예정_금액"]` → `정산_예정_금액`을 `coupang_settlement_amt`로 rename

**`build_delivery_commission() -> str`** (DAG가 호출하는 진입점)
```python
def build_delivery_commission() -> str:
    # 배민
    baemin_orders = _load_baemin_orders_agg()
    baemin_ads = _load_baemin_ad_spend_agg()
    baemin = baemin_orders.merge(baemin_ads, on=["date", "store"], how="left")
    baemin["ad_spend"] = baemin["ad_spend"].fillna(0)
    baemin["settlement_amount"] = baemin["baemin_deposit_amt"] - baemin["ad_spend"]
    baemin["platform"] = "배달의민족"

    # 쿠팡
    coupang = _load_coupang_orders_agg()
    coupang["settlement_amount"] = coupang["coupang_settlement_amt"]
    coupang["platform"] = "쿠팡이츠"

    frames = []
    for df in (baemin, coupang):
        if df.empty:
            continue
        out = df[["date", "store", "platform", "total_amt", "settlement_amount"]].copy()
        out["diff_amt"] = out["total_amt"] - out["settlement_amount"]
        frames.append(out)

    result = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "store", "platform", "total_amt", "settlement_amount", "diff_amt"])
    )
    result = result.rename(columns={"date": "sale_date"})
    result = result.sort_values(["sale_date", "store", "platform"]).reset_index(drop=True)

    DELIVERY_COMMISSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(DELIVERY_COMMISSION_PATH, index=False, engine="pyarrow")
    return f"delivery_commission {len(result)}행 -> {DELIVERY_COMMISSION_PATH}"
```
- import: `from modules.transform.utility.paths import BAEMIN_ORDERS_DB, BAEMIN_OUR_STORE_CLICKS_DB, COUPANG_ORDERS_DB, DELIVERY_COMMISSION_PATH`
- import: `from modules.transform.utility.store_normalize import normalize_for_join`

### 4. `dags/db/DB_DeliveryCommission_Dags.py` (신규)
`DB_CollectionCompare_Dags.py`를 템플릿으로 그대로 따라간다 (Reference Code 참고). 차이점만 적용:
- `dag_id=Path(__file__).stem`
- `schedule=DB_DELIVERY_COMMISSION_TIME`
- 단일 태스크: `PythonOperator(task_id="build_delivery_commission", python_callable=build_delivery_commission)`
- `tags=["db", "dashboard", "delivery_commission", "powerbi"]`
- 업스트림 ingest 태스크 불필요 (원천은 이미 다른 DAG가 채워둔 baemin_macro/coupang_macro 파티션을 읽기만 함)

## Reference Code

### modules/transform/pipelines/db/DB_CollectionCompare.py (관련 발췌)
```python
from modules.transform.utility.paths import (
    ANALYTICS_DB, BAEMIN_ORDERS_DB, COLLECTION_COMPARE_PATH, COUPANG_ORDERS_DB, MART_DB,
)
from modules.transform.utility.store_normalize import normalize_for_join

def _normalize_store_name(series: pd.Series) -> pd.Series:
    stripped = series.astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    return normalize_for_join(stripped)

def _partition_value(path: Path, prefix: str) -> str:
    token = f"{prefix}="
    for part in path.parts:
        if part.startswith(token):
            return part[len(token):].strip()
    return ""

def _baemin_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.)", expand=False)
    return pd.to_datetime(extracted, format="%Y. %m. %d.", errors="coerce").dt.strftime("%Y-%m-%d")

def _coupang_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\d{2}\.\d{2})", expand=False)
    return pd.to_datetime(extracted, format="%Y.%m.%d", errors="coerce").dt.strftime("%Y-%m-%d")

def load_baemin_macro() -> pd.DataFrame:
    files = sorted(BAEMIN_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    parts = []
    for path in files:
        df = pd.read_parquet(path)
        if df.empty:
            continue
        if "store" not in df.columns:
            df = df.copy()
            df["store"] = _partition_value(path, "store") or df.get("store_name", "")
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    df["총결제금액"] = pd.to_numeric(
        df["총결제금액"].astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce"
    ).fillna(0)
    grouped = df.groupby("주문번호", as_index=False).agg(
        주문시각=("주문시각", "max"), store=("store", "max"), 총결제금액=("총결제금액", "max"),
    )
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["store"])
    grouped = grouped.groupby(["date", "store"], as_index=False)["총결제금액"].sum()
    return grouped
```

### modules/transform/utility/store_normalize.py (전체)
```python
import re
import pandas as pd

STORE_NAME_MAP: dict[str, str] = { ... }  # 구버전 매장명 -> 현재명

_PREFIX_RE = re.compile(r"^도리당\s*")
_SPACE_RE = re.compile(r"\s+")

def normalize(series: pd.Series) -> pd.Series:
    """매장명 Series를 현재 공식명으로 치환."""
    return series.replace(_MAP)

def normalize_for_join(series: pd.Series) -> pd.Series:
    """JOIN 키 생성: 정규화 -> '도리당' 접두어/공백 제거."""
    return normalize(series).map(
        lambda s: _SPACE_RE.sub("", _PREFIX_RE.sub("", str(s)).strip())
    )
```

### dags/db/DB_CollectionCompare_Dags.py (전체, 템플릿)
```python
"""Collection comparison mart DAG."""
from pathlib import Path
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.transform.pipelines.db.DB_CollectionCompare import build_collection_compare
from modules.transform.utility.schedule import DB_COLLECTION_COMPARE_TIME

dag_id = Path(__file__).stem
default_args = {
    "retries": 1, "retry_delay": timedelta(minutes=5),
    "depends_on_past": False, "email_on_failure": False, "email_on_retry": False,
}

with DAG(
    dag_id=dag_id, schedule=DB_COLLECTION_COMPARE_TIME,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False, max_active_runs=1, default_args=default_args,
    tags=["db", "dashboard", "collection_compare", "powerbi"],
) as dag:
    build_compare = PythonOperator(
        task_id="build_collection_compare", python_callable=build_collection_compare,
    )
```

### 실제 원천 컬럼 (실측 확인됨)
- `baemin_macro/orders`: `..., 주문시각, ..., 결제금액, ..., 총결제금액, ..., 입금예정금액` (파티션: `brand=*/store=*/ym=*/orders_*.parquet`)
- `baemin_macro/metrics_our_store_clicks`: `collected_at, store_id, store_name, 날짜, 광고지출, 노출수, 클릭수, 주문수, 주문금액, 광고효과`
- `coupang_macro/orders`: `..., order_id, order_date, ..., 매출액, ..., 정산_예정_금액, 취소금액, ...` (파티션: `brand=*/store=*/ym=*/orders_*.parquet`)

## Test Cases
1. paths import 확인: `python -c "from modules.transform.utility.paths import DELIVERY_COMMISSION_DIR, DELIVERY_COMMISSION_PATH; print(DELIVERY_COMMISSION_PATH)"` → 기대: 에러 없이 경로 출력
2. schedule import 확인: `python -c "from modules.transform.utility.schedule import DB_DELIVERY_COMMISSION_TIME; print(DB_DELIVERY_COMMISSION_TIME)"` → 기대: `10 8 * * *` 출력
3. 파이프라인 실행: `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission; print(build_delivery_commission())"` → 기대: `delivery_commission {N}행 -> ...parquet` 문자열 출력, 예외 없음
4. 결과 parquet 검증: `python -c "import pandas as pd; from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH; df = pd.read_parquet(DELIVERY_COMMISSION_PATH); print(df.shape); print(df['platform'].value_counts()); print(df[['total_amt','settlement_amount','diff_amt']].describe()); print(df.columns.tolist())"` → 기대: 두 platform 값(`배달의민족`,`쿠팡이츠`) 모두 존재, 출력 컬럼은 `sale_date, store, platform, total_amt, settlement_amount, diff_amt`
5. DAG import 확인: `python -c "from dags.db.DB_DeliveryCommission_Dags import dag; print(dag.dag_id)"` → 기대: ImportError 없이 dag_id 출력

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (컬럼명 오타, 파티션 glob 패턴 불일치, dedup 키 누락 등)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `dags/*.py`에는 오케스트레이션만 둔다. 집계/변환 로직은 반드시 `modules/transform/pipelines/db/DB_DeliveryCommission.py`에 둔다.
- 경로는 반드시 `paths.py` 상수를 통해서만 참조한다 (하드코딩 금지).
- 스케줄은 반드시 `schedule.py` 상수를 통해서만 참조한다 (하드코딩 금지).
- `print()` 금지, `logger = logging.getLogger(__name__)` + `logger.warning/info` 사용.
- 매장명은 반드시 `modules/transform/utility/store_normalize.py`의 `normalize_for_join`(또는 동일 패턴의 `_normalize_store_name` 래퍼)을 통해 정규화한다. 새로운 정규화 테이블을 별도로 만들지 않는다.
- 배민/쿠팡 원천은 **주문 내 메뉴 행마다 주문 전체 금액이 반복 기재**되므로, 반드시 `주문번호`/`order_id` 기준으로 dedup(max)한 뒤 날짜×매장 합계를 낸다. dedup을 생략하면 매출이 수 배로 뻥튀기된다.
- 출력은 매 실행마다 전체 재계산 후 단일 parquet(`DELIVERY_COMMISSION_PATH`)을 overwrite한다. 증분 upsert 로직을 만들지 않는다 (원천이 이미 전체 히스토리 파티션이라 불필요).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (함수 시그니처에만 최소 적용)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `metrics_our_store_clicks`가 CSV만 있는지 하위 폴더 파티션이 있는지 불확실하면 `glob("**/*.csv")`로 재귀 탐색해 안전하게 처리
