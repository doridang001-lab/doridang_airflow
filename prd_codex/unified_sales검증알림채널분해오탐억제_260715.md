# unified_sales 검증알림 채널분해 + 오탐억제

## Task
`[도리당] unified_sales 일별 검증 알림`이 매일 오차율 2%+ 매장 목록으로 반복 발생한다. 진단 결과 원인은 대부분 **수집 타이밍 오탐**(검증이 08:37에 D-1을 보는데 ToOrder/배민매크로가 그날치를 아직 못 채움)이고, 일부는 진짜 누락이다. `DB_UnifiedSales_validate.py`를 고쳐 **① 채널별로 어디가 차이나는지 + 어느 쪽(ToOrder 누락/DB 누락/금액상이)인지 표시**하고 **② D-1→D-2 이동으로 타이밍 오탐을 억제**한다.

## 진단 근거 (구현 전 이해용)
- 검증은 `unified_sales`(DB합계) vs `ToOrder store_platform_daily + POS홀`(토더합계/원본)을 **매장 단위 total만** 비교 → 어느 채널이 왜 차이나는지 안 보임.
- `배민1`은 버그 아님: 검증이 ToOrder `배달의민족+배민1`을 합산해 unified 단일 `배달의민족`과 맞춘다(정상).
- (+)오차: 송파삼전/평택비전 = ToOrder 07-14 쿠팡 **하루 지연**(07-13까지 정상), 광명철산 = ToOrder 배민·땡겨요 누락.
- (−)오차: 시흥배곧/전주전북대/경북상주 = 배민 매크로 07-14 수집 실패/부분(unified 쪽 빠짐).
- 중랑면목 = 이미 자동 해소(타이밍 오탐 증거). → D-2 시점엔 타이밍 갭 대부분 해소됨.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_validate.py` (유일한 대상)
- 참고(수정 X): `dags/db/DB_UnifiedSales_Dags.py`(validate task 호출부), `modules/transform/pipelines/db/DB_UnifiedSales_common.py`(상수/헬퍼)

## Implementation Steps

### 1) 채널(platform-family) 분해 헬퍼
- 신규 `_platform_family(platform: str) -> str` 추가. 반환: `배민 / 쿠팡 / 요기 / 땡겨요 / 먹깨비 / 홀 / 기타`.
- 기존 상수 재사용: `HALL_PLATFORMS`(이미 모듈 상단 정의), `DELIVERY_PLATFORM_FAMILIES`(common). 규칙 예시:
  ```python
  def _platform_family(platform: str) -> str:
      p = str(platform).strip()
      if p in HALL_PLATFORMS: return "홀"
      if "배민" in p or p == "배달의민족": return "배민"
      if "쿠팡" in p: return "쿠팡"
      if "요기" in p: return "요기"
      if "땡겨요" in p: return "땡겨요"
      if "먹깨비" in p: return "먹깨비"
      return "기타"
  ```

### 2) 집계에 channel 차원 추가
- `_load_parquet_totals(target_date)`: 로드 컬럼에 `platform` 추가 → `channel=_platform_family(platform)` 파생 → groupby `["sale_date","store","channel"]` 합산해 `unified_total` 반환.
- `_load_excel_totals(target_date, unified_platform_keys)`: 기존 제외 로직(`_exclude_manual_delivery_test_store_platforms`, `_exclude_pos_hall_platforms`) **그대로 먼저 적용**(groupby 이전 단계라 무관) 후, `channel` 파생 → groupby `["sale_date","store","channel"]`.
- `_load_unified_pos_hall_totals(target_date=...)`: 홀 원천만 반환하므로 결과에 `channel="홀"` 컬럼 고정 추가.
- `_sum_validation_baseline(parts, key_cols)`: 호출 시 `key_cols=["sale_date","store","channel"]` 로 전달.

### 3) 차이 위치 + 방향 라벨 (요구 ②)
- `_compute_diff(excel, parquet)`: merge on `["sale_date","store","channel"]`, how="outer". 채널별 `difference = unified_total - excel_total`, `error_rate` 동일 공식.
- 신규 `reason` 컬럼:
  ```python
  def _reason(u, e):
      if u > 0 and e == 0: return "ToOrder 누락"
      if u == 0 and e > 0: return "DB 누락"
      return "금액상이"
  ```
- 매장 롤업(store-level total)을 별도 산출해 **임계값 판정**(오차율≥2% & |차이|≥100000)은 매장 단위 유지. 상세는 channel 라인으로 보관.

### 4) 타이밍 오탐 억제 (요구 ①)
- 일별 알림 대상일을 **D-1 → D-2** 로 이동. `_resolve_target_date`는 그대로 두고 `validate_sales`에서만 알림용 오프셋 -1일 추가하거나, `_resolve_target_date`의 기본 fallback을 -2일로 조정(단, conf.sale_date/XCom 우선순위는 유지).
- CSV(`_save_validation_csv`)는 채널 상세 포함해 계속 생성(기록은 숨기지 않음). 알림만 D-2 기준.
- 월별(`validate_monthly_sales`)은 이미 cutoff 로직 있으므로 채널 분해/라벨만 동일 적용, 대상일 오프셋은 건드리지 않음.

### 5) 표시 반영
- `_COL_LABELS` 에 `"channel": "채널"`, `"reason": "사유"` 추가.
- `_compute_diff` 반환 컬럼 순서에 `channel`, `reason` 포함.
- `_format_telegram_rows` / `_send_alert`: 매장별로 차이 채널을 `채널 / DB합계 / 토더합계 / 차이 / 사유` 로 나열.
- `_build_html_table`: `channel`, `reason` 컬럼 렌더. `reason=="ToOrder 누락"`/`"DB 누락"` 은 색상 강조.
- 월별 함수들(`_compute_monthly_diff` 등)도 동일 channel/reason 반영.

## Reference Code
### modules/transform/pipelines/db/DB_UnifiedSales_validate.py (현재 시그니처)
```python
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import (ANALYTICS_DB, COLLECT_DB, MART_DB, ONEDRIVE_DB, RAW_OKPOS_SALES, RAW_UNIONPOS_SALES, ...)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_MANUAL_TEST_STORES, DELIVERY_PLATFORM_FAMILIES, PLATFORM_TO_MANUAL_SOURCE, iter_unified_sales_files)

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
VALIDATION_DIR = MART_DB / "unified_sales_grp_error_list"
HALL_PLATFORMS = {"홀", "홀 포장", "홀 배달"}
POS_HALL_SOURCES = {"okpos", "unionpos", "easypos"}
_COL_LABELS = {"sale_date":"날짜","ym":"월","store":"매장","excel_total":"토더합계","unified_total":"DB합계","difference":"차이","error_rate":"오차율(%)","status":"상태"}

def _exclude_manual_delivery_test_store_platforms(df, *, date_col, unified_platform_keys=None) -> pd.DataFrame: ...
def _exclude_pos_hall_platforms(df) -> pd.DataFrame: ...
def _load_unified_pos_hall_totals(*, target_date=None, target_ym=None, max_date=None) -> pd.DataFrame: ...
def _sum_validation_baseline(parts, key_cols) -> pd.DataFrame: ...
def _resolve_target_date(**context) -> str: ...   # conf.sale_date > XCom resolve_date > D-1 fallback
def _load_parquet_totals(target_date) -> pd.DataFrame:      # groupby [sale_date, store] -> unified_total
def _load_excel_totals(target_date, unified_platform_keys=None) -> pd.DataFrame:  # toorder(price)+pos홀 -> excel_total
def _compute_diff(excel, parquet) -> pd.DataFrame:         # merge [sale_date, store], difference/error_rate/status
def _save_validation_csv(diff, target_date) -> Path: ...
def _send_alert(target_date, error_rows, csv_path, **context) -> None: ...
def validate_sales(**context) -> str:                     # error: error_rate>=2 & difference.abs()>=100000
def validate_monthly_sales(**context) -> str: ...
```
### toorder 기준 parquet 경로
`ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"` (컬럼: date, store, platform, price)

## Test Cases
1. [헬퍼] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _platform_family as f; print(f('배민1'),f('배달의민족'),f('쿠팡이츠'),f('홀 포장'),f('요기배달'),f('땡겨요'))"` → 기대: `배민 배민 쿠팡 홀 요기 땡겨요`
2. [일별 검증 실행/채널·사유 출력] conf로 2026-07-14 지정 실행:
   ```
   python -c "from modules.transform.pipelines.db import DB_UnifiedSales_validate as V; import pandas as pd; pq=V._load_parquet_totals('2026-07-14'); k=V._load_unified_platform_keys('2026-07',max_date='2026-07-14'); ex=V._load_excel_totals('2026-07-14',k); d=V._compute_diff(ex,pq); print([c for c in ['channel','reason'] if c in d.columns]); print(d[d['store']=='송파삼전점'][['store','channel','excel_total','unified_total','difference','reason']].to_string())"
   ```
   → 기대: `['channel','reason']` 출력, 송파삼전점 쿠팡 행이 `unified>0, excel==0, reason=ToOrder 누락` 로 표기.
3. [DB 누락 라벨] 위 `d`에서 `d[d['store']=='시흥배곧점']` → 배민 채널이 `unified==0, excel>0, reason=DB 누락`.
4. [오탐 억제=D-2] `validate_sales` 를 conf 없이(fallback) 실행했을 때 대상일이 D-2 인지 로그/반환 문자열 확인. 07-13 대상으로 재실행 시 송파삼전/평택비전 쿠팡 차이가 사라지는지(ToOrder 07-13은 정상) 확인.
5. [모듈 import] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_validate"` → 기대: 에러 없음.
6. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음.
7. [회귀] `python -m pytest tests/test_unified_sales_manual_delivery_cleanup.py -q` → 기대: 전부 PASS.

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
- `DB_UnifiedSales_validate.py` **한 파일만** 수정. DAG/공통 모듈 시그니처 변경 금지.
- 기존 제외 로직(`_exclude_manual_delivery_test_store_platforms`, `_exclude_pos_hall_platforms`, POS홀 치환)은 **동작·순서 그대로** 유지 — channel groupby는 그 이후 단계에만 추가.
- `배민1`은 이미 정상 처리되므로 배민 매핑 로직 훼손 금지(배달의민족+배민1이 같은 `배민` 채널로 합산돼야 함).
- 임계값(오차율≥2% & |차이|≥100000)은 **매장 롤업 기준** 유지. 채널 라인은 상세 표시용.
- conf.sale_date / XCom resolve_date 우선순위는 D-2 오프셋보다 우선(수동 지정일은 그대로 검증).
- 월별 검증의 cutoff 로직/대상일은 변경하지 말 것(채널·사유 표시만 추가).
- print 금지, 하드코딩 경로/스케줄 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(덮어쓰기)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- D-2 오프셋 구현 위치(validate_sales vs _resolve_target_date): 기존 우선순위(conf>XCom)를 깨지 않는 쪽으로 자유 결정
