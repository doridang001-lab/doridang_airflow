# unified_sales 쿠팡 이중집계 수정 (2026-06 월별검증 오차)

## Task
2026-06 월별 검증(`validate_monthly_sales`)에서 일부 TEST_STORES(동탄영천/송파삼전/시흥배곧/중랑면목)의
unified 매출이 toorder보다 크게 과대 집계됨. 원인은 `쿠팡이츠` platform에 `source=쿠팡수동`과
`source=posfeed` 행이 같은 날짜에 동시 존재하는 **이중집계**. 쿠팡 교정/방어 로직이 "최근 7일"만
처리하는 반면 배민은 "전체 기간"을 처리하는 비대칭이 근본 원인. 쿠팡을 배민과 동일하게 전체 기간
처리하도록 통일한다.

## 진단 근거 (이미 확정된 사실 — 재조사 불필요)
- 이중집계 발생 날짜가 전부 **06-19 이하** → DAG 06-26 실행 시 쿠팡 정리범위(최근 7일=06-20~06-26) 밖.
- 동탄영천점: 쿠팡 posfeed 이중집계 +13,952,000 (06-02~06-19, 16일) → 오차율 64.44%의 핵심.
- 송파삼전 +4,560,905 / 시흥배곧 +1,341,624 / 중랑면목 +439,400 동일 패턴.
- 동두천지행점/평택비전점도 테스트 매장 전환 후 `source=okpos`와 `source=쿠팡수동`이 같은 날짜에 동시 존재한다.
  이는 TEST_STORES 수동 source 기준에서 기존 POS 쿠팡 행이 함께 남은 상태이므로 이번 정리 대상에 포함한다.
- 추가 확인: TEST_STORES 쿠팡 platform 이중존재(`쿠팡수동` + 비수동 source)는 **2026-05-27~2026-06-19**에도 존재.
  즉 2026-06 월별 오차 수정이 주목적이지만, 전체기간 분기는 2026-05 말 잔존 중복도 함께 방어해야 한다.
- 같은 매장 배민은 `posfeed 배민 = 0`, 이중집계 0건 → 쿠팡 전용 문제.
- 매 실행 `build_posfeed`가 백필(`LOOKBACK_DAYS=None`)로 전체 날짜 posfeed 쿠팡이츠를 재주입하지만,
  `enforce_coupang_manual_only`는 최근 7일만 청소 → 7일 밖 과거 날짜에 posfeed 쿠팡이 영구 잔존.
- 부산장림(非 TEST)/해운대중동의 잔여 오차는 posfeed↔toorder 소스 정의 차이(버그 아님) → 이번 수정 대상 아님.
- 기준: `TEST_STORES`에 포함된 매장은 해당 플랫폼을 수동 수집 소스로 대체하는 대상이다.
  따라서 배민 platform은 `배민수동`, 쿠팡 platform은 `쿠팡수동`이 들어가야 하며, 같은 날짜의 기존 POS/posfeed 쿠팡 행은 최종 정리 대상이다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.pipelines.db.XXX import ...` / `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `dags/db/DB_UnifiedSales_Dags.py` — `reconcile_coupang`, `enforce_coupang_manual_only`의 lookback 인자
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` — 전체 기간(None) 날짜 해석 추가

## Implementation Steps

### 1. DAG: 쿠팡 lookback을 배민과 동일하게 LOOKBACK_DAYS로 통일
`dags/db/DB_UnifiedSales_Dags.py`

`reconcile_coupang` (현재 `lookback_days=LOOKBACK_DAYS or 7`):
```python
def reconcile_coupang(**context) -> str:
    if not TEST_STORES:
        return "쿠팡수동 대상 TEST_STORES 없음 - 스킵"
    from modules.transform.pipelines.db.DB_UnifiedSales_coupang import (
        reconcile_coupang_for_test_stores,
    )
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return reconcile_coupang_for_test_stores(
        stores=TEST_STORES,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,   # was: LOOKBACK_DAYS or 7
    )
```

`enforce_coupang_manual_only` (현재 `lookback_days=LOOKBACK_DAYS or 7`):
```python
    return enforce_coupang_manual_only_for_test_stores(
        stores=TEST_STORES,
        sale_date=sale_date,
        lookback_days=LOOKBACK_DAYS,   # was: LOOKBACK_DAYS or 7
    )
```

### 2. 쿠팡 파이프라인: lookback_days=None → 전체 기간 날짜 해석
`modules/transform/pipelines/db/DB_UnifiedSales_coupang.py`

배민(`DB_UnifiedSales_baemin.py`)의 `_resolve_baemin_target_dates` +
`_collect_baemin_source_dates` + `_collect_existing_unified_baemin_dates` 패턴을 쿠팡용으로 미러링한다.
쿠팡 상수/경로/파서를 사용: `COUPANG_REPLACED_PLATFORMS`, `coupang_macro/orders`, `_parse_coupang_datetime`,
원천 날짜 컬럼은 `order_date` (parquet only).

2-1. 함수 시그니처 변경 (두 곳):
```python
def reconcile_coupang_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,   # int -> int | None
) -> str:
```
```python
def enforce_coupang_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int | None = 7,   # int -> int | None
) -> str:
```

2-2. 두 함수의 날짜 산출부를 공통 헬퍼로 교체.
현재 두 함수 모두 아래처럼 되어 있음 → `_resolve_coupang_target_dates(...)` 호출로 교체:
```python
    if sale_date:
        dates = [str(sale_date)]
    else:
        kst_now = pendulum.now("Asia/Seoul")
        dates = [(kst_now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
```
교체 후:
```python
    dates = _resolve_coupang_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 교정 스킵 | 대상 날짜 없음"  # enforce 쪽은 메시지 문구만 맞게
```

2-3. 신규 헬퍼 추가 (배민 미러링):
```python
def _resolve_coupang_target_dates(
    stores: list[str],
    sale_date: str | None,
    lookback_days: int | None,
) -> list[str]:
    if sale_date:
        return [str(sale_date)]
    if lookback_days is not None:
        kst_now = pendulum.now("Asia/Seoul")
        return [(kst_now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
    dates: set[str] = set()
    for store in stores:
        dates.update(_collect_coupang_source_dates(store))
    dates.update(_collect_existing_unified_coupang_manual_dates(stores))
    dates.update(_collect_existing_coupang_duplicate_dates(stores))
    return sorted(d for d in dates if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d))


def _find_all_coupang_files(store: str) -> list[str]:
    base = ANALYTICS_DB / "coupang_macro" / "orders"
    pattern = str(base / "brand=*" / f"store={store}" / "ym=*" / "orders_*.parquet")
    return sorted(glob(pattern))


def _collect_coupang_source_dates(store: str) -> set[str]:
    dates: set[str] = set()
    for file_path in _find_all_coupang_files(store):
        try:
            df = pd.read_parquet(file_path, columns=["order_date"])
        except Exception as exc:
            logger.warning("쿠팡 날짜 수집 실패: %s | %s", file_path, exc)
            continue
        parsed = df["order_date"].map(_parse_coupang_datetime)
        dates.update(date for date, _ in parsed if date)
    return dates


def _collect_existing_unified_coupang_manual_dates(stores: list[str]) -> set[str]:
    store_set = {str(store).strip() for store in stores if str(store).strip()}
    if not store_set or not UNIFIED_ROOT.exists():
        return set()
    dates: set[str] = set()
    for path in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "source"])
        except Exception as exc:
            logger.warning("unified 쿠팡수동 날짜 수집 실패: %s | %s", path, exc)
            continue
        mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().isin(COUPANG_REPLACED_PLATFORMS)
            & df["source"].fillna("").astype(str).str.strip().eq(COUPANG_SOURCE)
        )
        if mask.any():
            dates.update(df.loc[mask, "sale_date"].fillna("").astype(str).str.strip().tolist())
    return dates


def _collect_existing_coupang_duplicate_dates(stores: list[str]) -> set[str]:
    store_set = {str(store).strip() for store in stores if str(store).strip()}
    if not store_set or not UNIFIED_ROOT.exists():
        return set()
    dates: set[str] = set()
    for path in sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(path, columns=["sale_date", "store", "platform", "source"])
        except Exception as exc:
            logger.warning("unified 쿠팡 중복 날짜 수집 실패: %s | %s", path, exc)
            continue
        df = df.copy()
        df["store"] = df["store"].fillna("").astype(str).str.strip()
        df["platform"] = df["platform"].fillna("").astype(str).str.strip()
        df["source"] = df["source"].fillna("").astype(str).str.strip()
        sub = df[
            df["store"].isin(store_set)
            & df["platform"].isin(COUPANG_REPLACED_PLATFORMS)
        ]
        if sub.empty:
            continue
        source_sets = sub.groupby(["sale_date", "store", "platform"])["source"].agg(set)
        for (sale_date, _, _), sources in source_sets.items():
            if COUPANG_SOURCE in sources and any(source != COUPANG_SOURCE for source in sources):
                dates.add(str(sale_date).strip())
    return dates
```
주의: 여기서 "중복범위"는 같은 `sale_date + store + platform`에 `쿠팡수동`과 비수동 source가 동시에 존재하는 날짜다.
다만 운영 기준은 더 단순하다. `TEST_STORES`에 포함된 매장의 쿠팡 platform은 `쿠팡수동`이 정답 source이므로,
쿠팡수동 원천이 있는 날짜에 남아 있는 `okpos`/`posfeed` 쿠팡 행은 source 종류와 무관하게 정리한다.

### 3. enforce 함수의 빈-날짜 반환 메시지 정리
`enforce_coupang_manual_only_for_test_stores`에서 dates 빈 경우:
```python
    dates = _resolve_coupang_target_dates(stores, sale_date, lookback_days)
    if not dates:
        return "쿠팡수동 최종 정리 스킵 | 대상 날짜 없음"
```

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_baemin.py (미러링 원본 패턴)
```python
from datetime import timedelta
from glob import glob
import re
import pandas as pd
import pendulum
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, UNIFIED_ROOT, _unified_daily_path, ...
)
BAEMIN_REPLACED_PLATFORMS = {"배달의민족", "배민1", "배민 포장"}

def _resolve_baemin_target_dates(stores, sale_date, lookback_days):
    if sale_date:
        return [str(sale_date)]
    if lookback_days is not None:
        kst_now = pendulum.now("Asia/Seoul")
        return [(kst_now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
    dates = set()
    for store in stores:
        dates.update(_collect_baemin_source_dates(store))
    dates.update(_collect_existing_unified_baemin_dates(stores))
    return sorted(d for d in dates if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d))
# _collect_baemin_source_dates / _collect_existing_unified_baemin_dates 구조 동일
```

### modules/transform/pipelines/db/DB_UnifiedSales_coupang.py (수정 대상 상단부)
```python
import re
from datetime import timedelta
from glob import glob
import pandas as pd
import pendulum
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, UNIFIED_ROOT, _load_store_map, _lookup_store_meta,
    _make_unified_pk, _to_int_series, _unified_daily_path,
)
from modules.transform.utility.paths import ANALYTICS_DB

COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"
COUPANG_REPLACED_PLATFORMS = {COUPANG_PLATFORM, "쿠팡 포장"}

def _parse_coupang_datetime(value) -> tuple[str, str]:
    if not isinstance(value, str):
        return "", ""
    match = re.match(r"\s*(\d{4})\.(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})", str(value).strip())
    if not match:
        return "", ""
    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
    hour, minute = int(match.group(4)), int(match.group(5))
    return f"{year:04d}-{month:02d}-{day:02d}", f"{hour:02d}:{minute:02d}:00"
```
주의: `re`, `glob`, `timedelta`, `pendulum`은 이미 import 되어 있음(추가 import 불필요).

## Test Cases
1. [모듈 import] PowerShell: `$env:PYTHONPATH='C:\airflow'; python -c "import modules.transform.pipelines.db.DB_UnifiedSales_coupang as m; print(hasattr(m,'_resolve_coupang_target_dates'))"` → 기대: `True`
2. [DAG import] PowerShell: `$env:PYTHONPATH='C:\airflow'; python -c "from dags.db.DB_UnifiedSales_Dags import dag; print('ok')"` → 기대: `ok`, ImportError 없음
3. [전체기간 분기] `$env:PYTHONPATH='C:\airflow'; python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import _resolve_coupang_target_dates as f; print(len(f(['동탄영천점'], None, None))>0)"` → 기대: `True` (전체 기간 날짜 산출)
4. [7일 분기 회귀] `$env:PYTHONPATH='C:\airflow'; python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import _resolve_coupang_target_dates as f; print(len(f(['x'], None, 7))==7)"` → 기대: `True`
5. [sale_date 분기] `$env:PYTHONPATH='C:\airflow'; python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import _resolve_coupang_target_dates as f; print(f(['x'],'2026-06-10',7)==['2026-06-10'])"` → 기대: `True`
6. [이중집계 해소 — 정정 실행 후 재검증] 전체 백필 1회 실행 후, `unified_sales_grp`에서 store별 `쿠팡이츠`/`쿠팡 포장` platform의 `source=쿠팡수동`과 비수동 source 동시존재 날짜 = 0 확인. → 기대: `2026-05-27~2026-06-19` 포함, 동탄영천/송파삼전/시흥배곧/중랑면목 및 전환 후 okpos 중복이 있는 동두천지행/평택비전 모두 0건
7. [TEST_STORES 수동 source 기준 확인] TEST_STORES에 포함된 매장의 배민 platform은 `배민수동`, 쿠팡 platform은 `쿠팡수동`만 남는지 확인한다. → 기대: 쿠팡수동 원천이 있는 날짜의 `okpos`/`posfeed` 쿠팡 행은 모두 제거
8. [읽기 실패 방어] OneDrive parquet 일부 `PermissionError`/읽기 실패가 발생해도 warning 후 해당 파일만 skip하며, 날짜 수집 실패만으로 platform 전체 비수동 행을 삭제하지 않는지 확인. → 기대: 삭제 범위 확대 없음

(검증 환경: PowerShell 기준. 모든 명령은 repo 루트 `C:\airflow`에서 실행하고 `PYTHONPATH=C:\airflow`를 명시)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행 (정적/단위)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~5 전체 PASS + Constraints 위반 없음
(Test Case 6~8은 실제 parquet 정정 실행/전후 비교가 필요하므로 사용자/운영 단계에서 확인)
```

## Constraints
- 배민 로직(`DB_UnifiedSales_baemin.py`)은 이미 정상 → **건드리지 말 것**. 쿠팡만 배민 패턴에 맞춘다.
- `_upsert_daily` / `_transform_to_unified` / `_calc_order_total_price` 등 금액 계산 로직은 변경 금지 (이중집계는 날짜 범위 문제이지 금액 계산 문제가 아님).
- posfeed↔toorder 소스 정의 차이(부산장림/평택비전/해운대중동)는 이번 수정 범위 아님 — 손대지 말 것.
- 이번 수정은 TEST_STORES 쿠팡 platform을 `쿠팡수동` 기준으로 정리하는 것이 목적이다. 쿠팡수동 원천이 있는 날짜에는 기존 POS/posfeed 쿠팡 행을 남기지 않는다.
- `lookback_days=None`에서 기존 unified 날짜를 수집할 때는 쿠팡수동 원천 날짜, 기존 `source=쿠팡수동` 날짜, 실제 이중존재 날짜를 우선한다.
- 신규 import 추가 금지(`re`/`glob`/`timedelta`/`pendulum`/`pd` 모두 기존 존재).
- DAG의 `reconcile_baemin`/`enforce_baemin_manual_only` 인자는 그대로 둔다.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(Edit)
- import가 모호하면: Reference Code의 배민 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`int | None`, `list[str]`)
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 배민 대응 함수명과 1:1 매칭(`_collect_coupang_source_dates` 등)
