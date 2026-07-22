# UnifiedSales 배민/쿠팡 수동 — 부분취소 파싱버그 + POS 폴백(가역) + 재수집 알림

## Task
`DB_UnifiedSales` DAG는 TEST_STORES(배달수동 대상 14개 매장)의 배달의민족/쿠팡이츠 매출을
배민수동/쿠팡수동 직수집으로 교정한다. 세 가지를 고친다: ① 배민 `결제금액`이
`"부분취소30800"`처럼 상태접두어+금액이면 0으로 깨져 주문 매출이 통째 누락되는 파싱버그(+주문별
반올림 잔액 귀속으로 정확 일치), ② 수동 원데이터가 없는 날 배달 매출이 0으로 떨어지는 문제를
POS(posfeed) 폴백으로 대체하고 수동이 걷히면 자동 복원(가역), ③ 폴백 발동일에 재수집 알림/마커.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 알림: `from modules.transform.utility.notifier import send_telegram`
- 응답/주석 한글, UTF-8(BOM 미포함) 유지

## Files to Create / Modify
- `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py` — (수정) 파싱 헬퍼 적용 + 잔액귀속 + reconcile/enforce 조건부 + 폴백 이벤트 emit
- `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` — (수정) reconcile/enforce 조건부 + 폴백 이벤트 emit (파싱/잔액귀속은 불필요 — 이미 정확)
- `modules/transform/pipelines/db/DB_UnifiedSales_common.py` — (수정) 폴백 마커/알림 공용 헬퍼 3종 + POS 요약 헬퍼 추가
- (미수정, 이미 올바른 조건부 폴백) `filter_manual_delivery_sources_for_test_stores`(DAG t6b2)

## 배경 실측(확정 사실)
- 송파삼전점 07-15 배민: 현재 705,495 → 파싱수정+잔액귀속 후 **정확히 736,300**(order_cnt=27, 전 주문 Σtotal_price==주문금액).
- 전 데이터 스캔 결과 배민 `결제금액` 비숫자 패턴은 `부분취소`뿐(전 매장 18건). `nan`은 빈 행.
- 송파삼전 07-15 쿠팡수동 원데이터 없음 → 쿠팡이츠 0원. posfeed 원천엔 쿠팡이츠 199,100/6건 존재.
- posfeed는 테스트 매장 배달을 사전 제외하지 않고 배달행 emit(비테스트 41개 매장 배달의민족 6,043·쿠팡이츠 5,084행 확인). posfeed가 `배민1→배달의민족` 정규화하므로 플랫폼 표기 정상.
- 쿠팡 `_transform_to_unified`는 대표행에 주문금액 전액을 넣고 나머지 0 → 주문별 합 이미 정확(반올림 불필요).

## Implementation Steps

### 1. 배민 결제금액 파싱 헬퍼 적용 (DB_UnifiedSales_baemin.py)
- 헬퍼 `_parse_baemin_amount_series(s)`는 이미 파일에 존재. 없으면 아래로 추가:
  ```python
  def _parse_baemin_amount_series(s: pd.Series) -> pd.Series:
      """'부분취소30800' 같은 상태접두어+금액에서도 숫자만 추출."""
      if s is None:
          return pd.Series(0)
      v = s.astype(str).str.replace(",", "", regex=False).str.strip()
      extracted = v.str.extract(r"(-?\d+)", expand=False)
      return pd.to_numeric(extracted, errors="coerce").fillna(0).astype(int)
  ```
- **[필수]** `_transform_to_unified` 내 order_amount 산정부에서
  `_to_int_series(df["결제금액"])` → `_parse_baemin_amount_series(df["결제금액"])` 로 교체.
  (헬퍼만 추가하고 호출부를 안 바꾸면 효과 없음 — 현재 죽은 코드 상태.)
- 범위 한정: `결제금액`에만 적용. `주문옵션금액`/`주문수량` 등은 기존 `_to_int_series` 유지.

### 2. 배민 주문별 반올림 잔액 귀속 (DB_UnifiedSales_baemin.py)
- `_transform_to_unified`에서 `out["total_price"] = (... * order_amount).round().astype(int)`
  **직후, `item_total` 컬럼 drop 이전**에 잔액을 주문의 `item_total` 최대 행에 귀속:
  ```python
  order_sum = out.groupby("order_id")["total_price"].transform("sum")
  residual = order_amount - order_sum  # 주문 내 동일값
  idx_max = out.groupby("order_id")["item_total"].idxmax()
  out.loc[idx_max, "total_price"] = out.loc[idx_max, "total_price"] + residual.loc[idx_max]
  ```
  결과: 주문별 `Σtotal_price == 주문금액` 정확 일치. (order_amount는 out.index에 정렬된 Series.)

### 3. reconcile: 수동 결측일 POS 미삭제 (baemin & coupang `_upsert_daily`)
- 현재 `_upsert_daily`는 `if df_new.empty` **이전에** remove_mask(플랫폼군 전체)를 무조건 적용한다.
  remove_mask 계산 자체를 df_new 유무로 분기하도록 수정한다.
- 배민 `_upsert_daily`(daily_path.exists() 블록):
  ```python
  df_existing = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
  store_s  = df_existing["store"].fillna("").astype(str).str.strip()
  plat_s   = df_existing["platform"].fillna("").astype(str).str.strip()
  source_s = df_existing["source"].fillna("").astype(str).str.strip()
  remove_mask = store_s.eq(store) & plat_s.isin(BAEMIN_PLATFORMS)
  if df_new is None or df_new.empty:
      # 수동 결측일: 수동 잔존행만 제거하고 posfeed/okpos 배달행은 fallback으로 유지
      remove_mask = remove_mask & source_s.eq(BAEMIN_SOURCE)
  removed_count = int(remove_mask.sum())
  df_existing = df_existing[~remove_mask]
  ```
- 쿠팡 `_upsert_daily`도 동일 패턴(`COUPANG_PLATFORMS`, `COUPANG_SOURCE`).
- 수동이 **있을** 때(df_new 비어있지 않음)는 현행대로 플랫폼군 전체 제거 후 수동 재적재 → 복원 자동.

### 4. enforce_*_manual_only: 수동 있는 날만 POS 제거 (baemin & coupang)
- `enforce_baemin_manual_only_for_test_stores`의 remove_mask를
  `filter_manual_delivery_sources_for_test_stores`와 동일한 조건부로 변경:
  같은 매장/일자/플랫폼군에 수동 행이 실제 존재하는 날만 비수동 제거, 없는 날은 POS 유지.
  ```python
  store_s = df["store"].fillna("").astype(str).str.strip()
  plat_s  = df["platform"].fillna("").astype(str).str.strip()
  src_s   = df["source"].fillna("").astype(str).str.strip()
  date_s  = df["sale_date"].fillna("").astype(str).str.strip()
  in_family = store_s.isin(store_set) & plat_s.isin(BAEMIN_PLATFORMS)
  is_manual = src_s.eq(BAEMIN_SOURCE)
  manual_keys = set(zip(store_s[in_family & is_manual], date_s[in_family & is_manual]))
  row_keys = pd.Series(list(zip(store_s, date_s)), index=df.index)
  has_manual = row_keys.isin(manual_keys)
  remove_mask = in_family & ~is_manual & has_manual
  ```
- 쿠팡 `enforce_coupang_manual_only_for_test_stores`도 동일(COUPANG_*).

### 5. 폴백 마커/알림 공용 헬퍼 (DB_UnifiedSales_common.py)
- 상단: `from modules.transform.utility.paths import LOCAL_DB` + `from modules.transform.utility.notifier import send_telegram`(순환 import 주의 시 함수 내부 지연 import).
- `MANUAL_FALLBACK_MARKER_ROOT = LOCAL_DB / "manual_fallback_markers"`.
- 헬퍼:
  - `pos_delivery_summary(date, store, platforms, manual_source) -> tuple[int, int, int]`:
    해당 일별 unified parquet에서 `store==store & platform in platforms & source != manual_source`
    행의 (total_price합, order_cnt합, 행수) 반환. POS 폴백 대상 존재 판정+알림 값에 사용.
  - `record_manual_fallback_marker(source, store, date, meta: dict) -> bool`:
    `MANUAL_FALLBACK_MARKER_ROOT/{source}/{store}/{date}.json`이 없으면 생성하고 True(신규),
    있으면 갱신 후 False. 부모 폴더 mkdir(parents, exist_ok). 실패해도 예외 전파 금지(warning).
  - `clear_manual_fallback_marker(source, store, date) -> bool`: 마커 파일 있으면 삭제 True.
  - `notify_manual_fallback(source_label: str, events: list[dict]) -> None`:
    신규 이벤트만 모아 1건 텔레그램. 메시지 예:
    `[도리당] 배달 수동 결측→POS 대체({source_label})\n- 송파삼전점 2026-07-15 쿠팡이츠 199,100/6건\n재수집 요망`.

### 6. reconcile에서 이벤트 emit + 복원 시 마커 해제 (baemin & coupang)
- reconcile 루프에서 "수동 결측(빈 upsert)" 분기에 도달하면:
  `amt, cnt, rows = pos_delivery_summary(date, store, PLATFORMS, MANUAL_SOURCE)`.
  `rows > 0`이면(POS 배달행 존재) 이벤트 후보로 수집하고 `record_manual_fallback_marker(...)`가
  True(신규)면 알림 이벤트 목록에 추가. `rows == 0`이면 알림/마커 없음(휴무·양쪽 결측 추정, 0 유지).
- 수동 데이터가 있어 정상 교정(df_new 비어있지 않음)된 (store, date)에 대해서는
  `clear_manual_fallback_marker(MANUAL_SOURCE, store, date)` 호출(복원 시 마커 해제).
- 루프 종료 후 신규 이벤트가 있으면 `notify_manual_fallback("배민수동"|"쿠팡수동", events)` 1회 호출.
- 반환 문자열 메시지에 `폴백 매장/날짜 N건` 요약 append(디버깅용).

## Reference Code

### DB_UnifiedSales_baemin.py (핵심부)
```python
import logging, re
from datetime import timedelta
from glob import glob
import pandas as pd
import pendulum
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES, UNIFIED_COLUMNS, UNIFIED_ROOT,
    _load_store_map, _lookup_store_meta, _make_unified_pk, _to_int_series,
    _unified_daily_path, iter_unified_sales_files,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import allocate_manual_item_ids

BAEMIN_SOURCE = "배민수동"; BAEMIN_PLATFORM = "배달의민족"
BAEMIN_PLATFORMS = DELIVERY_PLATFORM_FAMILIES[BAEMIN_SOURCE]  # {'배민 사장','배달의민족','배민 포장','배민1'}

# _transform_to_unified 내 (교체 대상):
#   out["item_total"] = out["unit_price"] * out["qty"]
#   order_item_sum = out.groupby("order_id")["item_total"].transform("sum")
#   order_amount = _first_nonzero_by_order(out["order_id"], _to_int_series(df["결제금액"]))  # ← 교체
#   out["total_price"] = ((out["item_total"]/order_item_sum.replace(0,1))*order_amount).round().astype(int)
#   ... out = out.drop(columns=["item_total"], errors="ignore")  # ← 잔액귀속은 이 drop 이전에

def _first_nonzero_by_order(order_id: pd.Series, values: pd.Series) -> pd.Series: ...  # 기존 유지
def _upsert_daily(df_new, date, store) -> tuple[int,int]: ...  # remove_mask 조건부화 대상
def reconcile_baemin_for_test_stores(stores, sale_date=None, lookback_days=7) -> str: ...  # 이벤트 emit 추가
def enforce_baemin_manual_only_for_test_stores(stores, sale_date=None, lookback_days=7) -> str: ...  # 조건부화
```

### DB_UnifiedSales_coupang.py (핵심부)
```python
from modules.transform.utility.paths import COUPANG_ORDERS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES, UNIFIED_COLUMNS, UNIFIED_ROOT,
    _load_store_map, _lookup_store_meta, _make_unified_pk, _unified_daily_path, iter_unified_sales_files,
)
COUPANG_SOURCE = "쿠팡수동"; COUPANG_PLATFORM = "쿠팡이츠"
COUPANG_PLATFORMS = DELIVERY_PLATFORM_FAMILIES[COUPANG_SOURCE]  # {'쿠팡 포장','쿠팡이츠'}
def _upsert_daily(df_new, date, store) -> tuple[int,int]: ...  # remove_mask 조건부화 대상
def reconcile_coupang_for_test_stores(...): ...   # 이벤트 emit 추가
def enforce_coupang_manual_only_for_test_stores(...): ...  # 조건부화
# 주의: _transform_to_unified는 대표행에 주문금액 전액 → 반올림 수정 불필요
```

### DB_UnifiedSales_common.py (참조/재사용)
```python
DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배민 사장", "배달의민족", "배민 포장", "배민1"},
    "쿠팡수동": {"쿠팡 포장", "쿠팡이츠"},
}
UNIFIED_COLUMNS = [...]; UNIFIED_ROOT = MART_DB / "unified_sales_grp"
def _unified_daily_path(date) -> Path: ...       # unified_sales_YYMMDD.parquet
def iter_unified_sales_files(): ...
# 이미 올바른 조건부 폴백(참고, 미수정):
def filter_manual_delivery_sources_for_test_stores(df, stores=None) -> pd.DataFrame:
    # 과거일: 같은 매장/일자/플랫폼군에 수동행 있을 때만 비수동 제거, 없으면 POS 유지 / 오늘: POS만
```

## Test Cases
1. [파싱+잔액귀속] 07-15 송파삼전 배민 재계산 합계 = **736,300**, order_cnt=27, 전 주문 Σtotal_price==주문금액:
   `python -X utf8 scripts/_verify_baemin_songpa_0715.py`(신규 진단 스크립트, `scripts/`에 작성) → 기대: `736300 / 27 / all-match`
2. [부분취소 회귀] 전 매장 `결제금액` 비숫자 18건이 매출에 반영(0 아님) 스팟체크 → 기대: 누락 0건
3. [폴백-쿠팡] 송파삼전 07-15 쿠팡이츠 = posfeed 값(199,100/6건) 유지 → 기대: 0 아님, source=posfeed 잔존
4. [복원] 쿠팡수동 07-15 원천 존재 상황 재실행 시 쿠팡이츠가 수동값으로 교체·posfeed 제거 → 기대: source=쿠팡수동만
5. [회귀-정상일] 07-14처럼 수동 정상 수집일은 수동만 남음(POS 중복 없음) → 기대: 배달행 source 단일
6. [알림/마커] 폴백 발동 시 `LOCAL_DB/manual_fallback_markers/...` 생성 + 텔레그램 1건, 재실행 시 재알림 없음(멱등), 복원 시 마커 삭제
7. [import 무결성] `python -X utf8 -c "from dags.db.DB_UnifiedSales_Dags import dag; print(dag.dag_id)"` → 기대: `DB_UnifiedSales`, ImportError 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- OneDrive(원천/mart) 데이터 실적재 재실행은 사용자 승인 후. 폴백 마커는 `LOCAL_DB`(비-OneDrive)에만 기록.
- 비즈니스 로직은 `modules/transform/pipelines/db/`에, DAG는 오케스트레이션만.
- 파싱 수정은 배민 `결제금액`에만. 쿠팡은 반올림/파싱 수정 대상 아님.
- 가역성은 DAG의 `LOOKBACK_DAYS=14` 재처리에 의존(14일 내 수동 걷히면 자동 복원). 초과분은 conf `sale_date` 정정으로 커버.
- 진단/임시 스크립트는 저장소 루트 금지 → `scripts/` 또는 `.tmp/`. UTF-8(BOM 미포함).
- reconcile은 `today`(당일)를 처리하지 않음(`_resolve_*_target_dates`가 today 제외). 당일 배달은 t6b2가 POS만 사용. 이 경계 유지.
- 마커/알림 실패가 파이프라인 본류를 막지 않도록 try/except로 감싸고 warning 로깅.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(단, 기존 함수 시그니처·호출부 호환 유지)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
