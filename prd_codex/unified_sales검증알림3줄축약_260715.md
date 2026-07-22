# unified_sales 검증 알림 매장별 총합/배민/쿠팡 3줄 축약

## Task
`[도리당] unified_sales 월별/일별 검증 알림`이 오차 매장마다 **모든 플랫폼 채널**
(배민·쿠팡·요기·땡겨요·먹깨비·홀·기타)을 한 줄씩 나열해 `오차율 2% 이상: 34건`처럼
지나치게 길고 `광명철산점 / 기타 / 1,000` 같은 사소한 라인이 노이즈가 된다.
**매장마다 `총합`(DB 전체합 vs 토더 전체합) + `배민` + `쿠팡` 정확히 3줄만** 알림에
표시하도록 축약한다. 임계값 판정(오차율 2%)은 **매장 총합 기준 그대로 유지**하고,
표시 행 수만 줄인다. CSV 산출물은 전체 채널을 그대로 저장한다(기록 숨김 금지).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정 (유일한 대상)**: `modules/transform/pipelines/db/DB_UnifiedSales_validate.py`
- 참고(수정 X): `dags/db/DB_UnifiedSales_Dags.py`(validate/monthly task 호출부),
  `modules/transform/pipelines/db/DB_UnifiedSales_common.py`(상수/헬퍼)

## Implementation Steps

### 1) `_store_threshold_detail_rows` → `_alert_summary_rows` 로 재작성 (라인 609)
현재 이 헬퍼는 오차 매장의 **모든 채널 상세 행**을 반환한다. 매장별
`총합/배민/쿠팡` **정확히 3줄**만 반환하도록 재작성한다. 반환 컬럼·시그니처는 유지:
`(diff: pd.DataFrame, *, date_col: str) -> pd.DataFrame`,
반환 컬럼 `[date_col, "store", "channel", "excel_total", "unified_total", "difference", "error_rate", "reason", "status"]`.

로직:
1. `diff`(채널 단위)를 `[date_col, "store"]`로 롤업 → 매장 총합 `excel_total/unified_total` 합산,
   `difference = unified_total - excel_total`, `error_rate` = 기존 공식
   (`abs(diff)/abs(excel)*100`, excel==0이면 0.0).
2. **임계값(오차율 ≥ 2% AND |차이| ≥ 100000)** 통과 매장 키 `error_keys`(`[date_col,"store"]`) 추출.
   비면 `diff.iloc[0:0].copy()` 반환(기존 빈-프레임 동작 유지).
3. **총합 줄**: 롤업 결과를 `error_keys`로 inner-merge → `channel="총합"`,
   `reason=_reason(unified_total, excel_total)`, `status = "error" if error_rate>=2 else "ok"`.
4. **배민/쿠팡 줄**: `error_keys × ["배민","쿠팡"]` 카테시안 생성 후 채널 diff를
   `[date_col,"store","channel"]`로 **left-merge** → 없는 조합은
   `excel_total/unified_total = 0` (fillna). 각 줄 `difference/error_rate/reason/status` 재계산.
   (해당 매장에 배민/쿠팡 데이터가 없어도 **항상 0/0 줄 표시** — 매장당 3줄 고정.)
5. 총합+배민+쿠팡 `concat` 후 `channel`을 `["총합","배민","쿠팡"]` 순서 Categorical로 지정하고
   `[date_col,"store"(오름차순), "channel"(카테고리순)]` 정렬 → `reset_index(drop=True)`.
6. 위 반환 컬럼 순서로 슬라이싱해 반환.

기존 `_reason`(라인 111), `error_rate` 공식, `_platform_family`(라인 94 — 채널명 "배민"/"쿠팡" 산출)를 그대로 재사용한다. 새 상수 예: `_ALERT_CHANNELS = ["총합", "배민", "쿠팡"]`.

### 2) 호출부 2곳 함수명 교체
- `validate_sales` (라인 662): `_store_threshold_detail_rows(diff, date_col="sale_date")`
  → `_alert_summary_rows(diff, date_col="sale_date")`
- `validate_monthly_sales` (라인 863): 동일하게 `date_col="ym"`으로 교체.

호출 시그니처·반환 컬럼이 동일하므로 `_send_alert` / `_send_monthly_alert` /
`_format_telegram_rows` / `_build_html_table` 는 **수정 불필요**(channel/reason 컬럼 이미 렌더).

### 3) 변경 금지 (그대로 유지)
- 제외 로직 `_exclude_manual_delivery_test_store_platforms`, `_exclude_pos_hall_platforms`, POS홀 치환.
- 월별 cutoff/대상일 오프셋(`validate_monthly_sales`의 max_date, D-2 등).
- CSV 저장 `_save_validation_csv` / `_save_monthly_comparison_csv` → **전체 채널 diff 저장 유지**.
- DAG, 공통 모듈 시그니처.

## Reference Code
### modules/transform/pipelines/db/DB_UnifiedSales_validate.py (현재 시그니처)
```python
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import (ANALYTICS_DB, COLLECT_DB, LLM_OUTPUT_DIR,
    LOCAL_DB, MART_DB, ONEDRIVE_DB, RAW_OKPOS_SALES, RAW_UNIONPOS_SALES)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_MANUAL_TEST_STORES, DELIVERY_PLATFORM_FAMILIES,
    PLATFORM_TO_MANUAL_SOURCE, iter_unified_sales_files)

HALL_PLATFORMS = {"홀", "홀 포장", "홀 배달"}
_COL_LABELS = {"sale_date":"날짜","ym":"월","store":"매장","channel":"채널",
    "excel_total":"토더합계","unified_total":"DB합계","difference":"차이",
    "error_rate":"오차율(%)","reason":"사유","status":"상태"}

def _platform_family(platform: str) -> str:
    p = str(platform).strip()
    if p in HALL_PLATFORMS: return "홀"
    if "배민" in p or p == "배달의민족": return "배민"
    if "쿠팡" in p: return "쿠팡"
    if "요기" in p: return "요기"
    if "땡겨요" in p: return "땡겨요"
    if "먹깨비" in p: return "먹깨비"
    return "기타"

def _reason(unified_total: int, excel_total: int) -> str:
    if unified_total > 0 and excel_total == 0: return "ToOrder 누락"
    if unified_total == 0 and excel_total > 0: return "DB 누락"
    return "금액상이"

# 현재 헬퍼 (재작성 대상) — 매장 롤업으로 임계값 판정 후 '모든 채널' 상세 반환
def _store_threshold_detail_rows(diff: pd.DataFrame, *, date_col: str) -> pd.DataFrame:
    if diff.empty: return diff.copy()
    grouped = diff.groupby([date_col, "store"], as_index=False)[["excel_total","unified_total"]].sum()
    grouped["difference"] = grouped["unified_total"] - grouped["excel_total"]
    grouped["error_rate"] = grouped.apply(lambda r: round(abs(r["difference"])/abs(r["excel_total"])*100, 2)
        if r["excel_total"] != 0 else 0.0, axis=1)
    error_keys = grouped[(grouped["error_rate"] >= 2) & (grouped["difference"].abs() >= 100000)][[date_col, "store"]]
    if error_keys.empty: return diff.iloc[0:0].copy()
    details = diff.merge(error_keys, on=[date_col, "store"], how="inner")
    details = details[details["difference"].ne(0)].copy()
    return details.sort_values([date_col, "store", "channel"]).reset_index(drop=True)

# 호출부
def validate_sales(**context) -> str:
    ...
    error_rows = _store_threshold_detail_rows(diff, date_col="sale_date")   # ← 라인 662
    ...
def validate_monthly_sales(**context) -> str:
    ...
    error_rows = _store_threshold_detail_rows(diff, date_col="ym")          # ← 라인 863
    ...
```
### toorder 기준 parquet 경로
`ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"` (컬럼: date, store, platform, price)

## Test Cases
1. [import] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_validate"` → 기대: 에러 없음.
2. [월별 3줄 축약] 2026-07 실측:
   ```
   python -c "from modules.transform.pipelines.db import DB_UnifiedSales_validate as V; \
   pq=V._load_parquet_monthly_totals('2026-07',max_date='2026-07-14'); \
   k=V._load_unified_platform_keys('2026-07',max_date='2026-07-14'); \
   ex=V._load_excel_monthly_totals('2026-07',max_date='2026-07-14',unified_platform_keys=k); \
   d=V._compute_monthly_diff(ex,pq); r=V._alert_summary_rows(d,date_col='ym'); \
   print(sorted(r['channel'].unique())); print('per_store_max=',int(r.groupby('store').size().max())); \
   print(r[r['store']=='강원영월점'][['store','channel','excel_total','unified_total','difference','reason']].to_string())"
   ```
   → 기대: `channel ⊆ {총합,배민,쿠팡}`, `per_store_max=3`, 강원영월점 `총합/배민/쿠팡` 3줄만 출력.
3. [매장 목록 불변] `_alert_summary_rows`가 뽑은 `store` 집합 == 기존 임계값(매장 롤업 오차율≥2% & |차이|≥10만) 통과 매장 집합과 동일한지 비교(총합 줄의 store 목록으로 확인).
4. [일별에도 적용] conf 2026-07-14 실측:
   ```
   python -c "from modules.transform.pipelines.db import DB_UnifiedSales_validate as V; \
   pq=V._load_parquet_totals('2026-07-14'); k=V._load_unified_platform_keys('2026-07',max_date='2026-07-14'); \
   ex=V._load_excel_totals('2026-07-14',k); d=V._compute_diff(ex,pq); r=V._alert_summary_rows(d,date_col='sale_date'); \
   print(sorted(r['channel'].unique())); print('per_store_max=',int(r.groupby('store').size().max()) if not r.empty else 0)"
   ```
   → 기대: `channel ⊆ {총합,배민,쿠팡}`, 매장당 3줄.
5. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음.
6. [회귀] `python -m pytest tests/test_unified_sales_manual_delivery_cleanup.py -q` → 기대: 전부 PASS.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `DB_UnifiedSales_validate.py` **한 파일만** 수정. DAG/공통 모듈 시그니처 변경 금지.
- **임계값(오차율 ≥ 2% & |차이| ≥ 100000)은 매장 총합 롤업 기준 그대로 유지** — 어떤 매장을
  알림에 올릴지는 바뀌면 안 됨. 표시 행만 3줄로 축약.
- 알림 표시는 매장당 **총합 + 배민 + 쿠팡 3줄 고정**(배민/쿠팡 데이터 없어도 0/0 줄 표시).
- CSV(`_save_*`)는 **전체 채널 diff 그대로 저장** — 기록 숨김 금지.
- 제외 로직(`_exclude_manual_delivery_test_store_platforms`, `_exclude_pos_hall_platforms`, POS홀 치환),
  월별 cutoff/대상일 오프셋은 **동작·순서 그대로** 유지.
- `배민1`은 이미 정상(`배달의민족+배민1`이 같은 `배민` 채널로 합산) — 매핑 훼손 금지.
- print 금지, 하드코딩 경로/스케줄 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 수정(덮어쓰기)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 기존 `_store_threshold_detail_rows` 를 rename 할지 신규 함수 추가할지: rename(`_alert_summary_rows`) 권장, 호출부 2곳만 교체
