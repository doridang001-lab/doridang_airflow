# unified_sales 월별검증 오탐제거 (ToOrder 기준값 최신일 컷오프)

## Task
`unified_sales` 월별 검증 알림(2026-07-30 09:50 발송)이 오차율 2% 이상 **45곳**을 발화했으나, 실제 원인은 기준값인 ToOrder 수집이 하루 밀린 것이다. 검증이 "기준값이 실제로 존재하는 날짜"까지만 비교하도록 컷오프에 상한을 걸어 오탐을 제거한다. 부수로, 기준값이 통째로 없는 날 일별 검증이 오차 0으로 무음 통과하는 갭도 막는다.

## Root Cause (근거 데이터)
- 검증 비교: `unified_total`(MART `unified_sales_grp/*.parquet` `total_price` 합) vs `excel_total`(기준값 = ANALYTICS `toorder_store_platform_daily.parquet` `price` 합 + POS홀은 unified 자체값), `difference = unified_total - excel_total`, `error_rate = |difference|/|excel_total|*100`, 2%↑ → error.
- 알림 비교범위는 `2026-07-01 ~ 2026-07-29`. 그러나 **ToOrder parquet 최신일은 2026-07-28**(mtime 2026-07-29 09:39), **07-29 행수 0**.
- unified 2026-07-29는 정상 존재: **47,747,708원** (posfeed 35.8M / 배민수기 4.4M / 쿠팡수기 4.9M / okpos 2.5M, 47매장). → **POS(posfeed) 미수집 아님**.
- 45곳 중 **43곳이 + 방향**, diff 합계 **+44,037,903**. 이 45곳의 **07-29 unified 합계가 40,363,036** → diff의 92%가 "07-29 하루치를 기준값 0과 비교"한 결과.
- 컷오프를 `2026-07-28`로 재계산 시 매장단위 오차(2%↑ & ±10만↑) **45곳 → 10곳**, diff 합계 `+44,037,903 → -1,716,808`.
- ToOrder가 밀린 이유: `DB_Toorder_store_platform_daily_Dags`는 `10 7 * * *` + `catchup=False`인데 scheduler 로그 시작시각이 07-29·07-30 모두 09:00 → 07:10 슬롯 미실행. 07-29는 `recovery__scheduler_heartbeat__2026-07-28T22:10:00Z` 런이 09:29~09:39에 돌아 07-28까지만 수집, **07-30은 런 자체가 없음**.
- 검증 DAG(`DB_UnifiedSales`, 08:37 / guard 09:05)는 ToOrder보다 먼저 끝날 수 있는데 **기준값 신선도 가드가 없다** — 이것이 구조적 원인.
- `_compute_diff`는 `excel_total == 0`이면 `error_rate = 0.0`으로 처리 → 기준값이 통째로 없는 날 일별 검증은 오차 0으로 **무음 통과**.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 타임존: 파일 상단 기존 `KST = ZoneInfo("Asia/Seoul")` 재사용 (신규 정의 금지)
- 날짜 비교는 문자열 `YYYY-MM-DD` 사전순 비교로 통일 (타입 혼선 방지)

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_UnifiedSales_validate.py`
  - 파일 상단 상수 블록(line 74~80 부근): `TOORDER_DAILY_PARQUET` 상수 추가
  - 신규 헬퍼 `_toorder_baseline_max_date()` 추가
  - `_load_excel_totals` (line 568) / `_load_excel_monthly_totals` (line 782): 하드코딩 parquet 경로를 새 상수로 교체
  - `validate_monthly_sales` (line 879): 컷오프에 기준값 상한 적용 + `baseline_lagged` 전달
  - `_send_monthly_alert` (line 856): `baseline_lagged` 인자 추가, 알림에 지연 사실 1줄 표기
  - `validate_sales` (line 702): 기준값 없는 날 조기 반환 + 텔레그램 경고
- 생성 파일 없음

## Implementation Steps

1. **`TOORDER_DAILY_PARQUET` 상수 추가** (line 74~80 상수 블록)
   ```python
   TOORDER_DAILY_PARQUET = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
   ```
   - `ANALYTICS_DB`는 이미 상단에서 import되어 있음(line 24).
   - `_load_excel_totals`(line 572) / `_load_excel_monthly_totals`(line 788)의 지역변수 `path = ANALYTICS_DB / ...` 를 `path = TOORDER_DAILY_PARQUET` 로 교체. 이후 `path` 사용부는 그대로 둔다.

2. **`_toorder_baseline_max_date()` 헬퍼 추가** (`_load_excel_monthly_totals` 바로 위 또는 상수 직후)
   ```python
   def _toorder_baseline_max_date() -> str | None:
       """검증 기준값(ToOrder)이 실제로 존재하는 마지막 날짜를 반환한다."""
       if not TOORDER_DAILY_PARQUET.exists():
           logger.warning("토더 기준 parquet 없음: %s", TOORDER_DAILY_PARQUET)
           return None
       try:
           df = pd.read_parquet(TOORDER_DAILY_PARQUET, columns=["date"])
       except Exception as exc:
           logger.warning("토더 기준 최신일 조회 실패: %s | %s", TOORDER_DAILY_PARQUET, exc)
           return None
       dates = df["date"].astype(str).str[:10]
       dates = dates[dates.str.match(r"\d{4}-\d{2}-\d{2}")]
       if dates.empty:
           return None
       return str(dates.max())
   ```
   - `date` 컬럼 타입이 datetime일 수 있으므로 반드시 `.astype(str).str[:10]` 경유.

3. **`validate_monthly_sales`에 기준값 상한 적용** (line 879)
   - 기존 `cutoff_date` 확정 블록(아래 Reference Code 참고) **직후**에 삽입:
     ```python
     baseline_max = _toorder_baseline_max_date()
     baseline_lagged = bool(baseline_max) and baseline_max < cutoff_date
     if baseline_lagged:
         logger.warning(
             "기준값(ToOrder) 지연으로 월별 검증 컷오프 축소: %s → %s",
             cutoff_date,
             baseline_max,
         )
         cutoff_date = baseline_max
     ```
   - `baseline_max`가 `None`이거나 `cutoff_date` 이상이면 기존 동작 그대로(회귀 없음).
   - 과거월(`ym != current_ym`)은 지금처럼 `max_date=None` 유지 — **절대 컷오프 적용 금지**.
   - 알림 호출부를 `_send_monthly_alert(ym, error_rows, csv_path, max_date=max_date, baseline_lagged=baseline_lagged, **context)` 로 수정.

4. **`_send_monthly_alert`에 지연 표기 추가** (line 856)
   - 시그니처에 키워드 인자 추가: `baseline_lagged: bool = False`
   - `target_label` 조립부 수정:
     ```python
     target_label = f"대상월: {target_ym}"
     if max_date:
         target_label += f"\n비교범위: {target_ym}-01 ~ {max_date}"
     if baseline_lagged and max_date:
         target_label += f"\n※ ToOrder 기준값이 {max_date}까지만 수집됨 (이후 일자 비교 제외)"
     ```
   - 목적: 수신자가 "왜 어제까지가 아니라 그저께까지 비교했나"를 알림만 보고 판단 가능하게.

5. **일별 검증 무음 통과 가드** (`validate_sales`, line 702)
   - `excel = _load_excel_totals(...)` 직후, `_compute_diff` 호출 **전에** 삽입:
     ```python
     if excel.empty:
         msg = f"[도리당] unified_sales 일별 검증 보류: {target_date} ToOrder 기준값 없음(수집 지연)"
         logger.warning(msg)
         send_telegram(msg)
         return msg
     ```
   - `send_telegram`은 이미 상단 import되어 있음(line 22).
   - CSV 저장 전에 반환하므로, 기준값 0짜리 CSV로 기존 파일을 덮어쓰는 사고도 같이 막힌다.
   - `_load_excel_totals`는 해당 날짜 toorder 행이 0이면 POS홀 합산 전에 빈 프레임을 즉시 반환하므로(line 586~588) `excel.empty` 판정이 정확하다.

## Reference Code

### DB_UnifiedSales_validate.py — 상단 import + 상수 (line 14~80)
```python
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.utility.notifier import send_telegram, send_telegram_chunks
from modules.transform.utility.paths import (
    ANALYTICS_DB, COLLECT_DB, LLM_OUTPUT_DIR, LOCAL_DB, MART_DB, ONEDRIVE_DB,
    RAW_OKPOS_SALES, RAW_UNIONPOS_SALES,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_MANUAL_TEST_STORES, DELIVERY_PLATFORM_FAMILIES,
    PLATFORM_TO_MANUAL_SOURCE, iter_unified_sales_files, save_unified_parquet,
)

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
VALIDATION_DIR = MART_DB / "unified_sales_grp_error_list"
VALIDATION_FILE_PREFIX = "unified_sales_error_"
MONTHLY_FILE_PREFIX = "unified_sales_monthly_"
HALL_PLATFORMS = {"홀", "홀 포장", "홀 배달"}
POS_HALL_SOURCES = {"okpos", "unionpos", "easypos"}
_ALERT_CHANNELS = ["총합", "쿠팡", "배민", "기타"]
```

### DB_UnifiedSales_validate.py — `_send_monthly_alert` 현재 형태 (line 856)
```python
def _send_monthly_alert(
    target_ym: str,
    error_rows: pd.DataFrame,
    csv_path: Path,
    *,
    max_date: str | None = None,
    **context,
) -> None:
    display = error_rows[["ym", "store", "channel", "excel_total", "unified_total", "difference", "error_rate", "reason", "status"]]
    target_label = f"대상월: {target_ym}"
    if max_date:
        target_label += f"\n비교범위: {target_ym}-01 ~ {max_date}"
    message = _build_telegram_message(
        title="[도리당] unified_sales 월별 검증 알림",
        target_label=target_label,
        error_rows=display,
        csv_path=csv_path,
        date_col="ym",
        include_details=False,
    )
    send_telegram_chunks(message)
```

### DB_UnifiedSales_validate.py — `validate_monthly_sales` 현재 형태 (line 879)
```python
def validate_monthly_sales(**context) -> str:
    """올해 데이터가 있는 모든 ym에 대해 월별 CSV를 생성하고, 현재월은 최신 미완성일을 제외한다."""
    target_date = _resolve_target_date(**context)
    today = datetime.now(KST).strftime("%Y-%m-%d")
    if target_date >= today:
        cutoff_date = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        cutoff_date = target_date
    # ← 이 직후에 baseline_max / baseline_lagged 블록 삽입
    current_year = target_date[:4]
    current_ym = target_date[:7]
    logger.info("unified_sales 월별 검증 시작: year=%s, alert_ym=%s", current_year, current_ym)
    logger.info("월별 검증 컷오프: current_ym=%s max_date=%s", current_ym, cutoff_date)

    all_ym = _get_parquet_year_months(current_year)
    if not all_ym:
        return f"월별 검증: {current_year} 데이터 없음"

    saved = []
    alert_sent = False
    for ym in all_ym:
        max_date = cutoff_date if ym == current_ym else None
        parquet = _load_parquet_monthly_totals(ym, max_date=max_date)
        unified_platform_keys = _load_unified_platform_keys(ym, max_date=max_date)
        excel = _load_excel_monthly_totals(ym, max_date=max_date, unified_platform_keys=unified_platform_keys)
        diff = _compute_monthly_diff(excel=excel, parquet=parquet)
        csv_path = _save_monthly_comparison_csv(diff=diff, target_ym=ym)
        saved.append(ym)
        logger.info("월별 검증 저장: %s | rows=%d", csv_path, len(diff))

        if ym == current_ym:
            error_rows = _alert_summary_rows(diff, date_col="ym")
            if not error_rows.empty:
                _send_monthly_alert(ym, error_rows, csv_path, max_date=max_date, **context)  # ← baseline_lagged 추가
                alert_sent = True
    ...
```

### DB_UnifiedSales_validate.py — `validate_sales` 현재 형태 (line 702)
```python
def validate_sales(**context) -> str:
    """대상일 1일 기준 unified_sales 와 일별매출보고서를 비교한다."""
    target_date = _resolve_daily_validation_target_date(**context)
    logger.info("unified_sales 검증 대상일: %s", target_date)

    parquet = _load_parquet_totals(target_date=target_date)
    unified_platform_keys = _load_unified_platform_keys(target_date[:7], max_date=target_date)
    excel = _load_excel_totals(target_date=target_date, unified_platform_keys=unified_platform_keys)
    # ← 이 직후에 excel.empty 가드 삽입
    diff = _compute_diff(excel=excel, parquet=parquet)
    csv_path = _save_validation_csv(diff=diff, target_date=target_date)
    logger.info("검증 결과 저장: %s | rows=%d", csv_path, len(diff))

    error_rows = _alert_summary_rows(diff, date_col="sale_date")
    if error_rows.empty:
        return f"검증 완료: {target_date} | 오차율 2% 이상(±10만원↑) 없음 | CSV: {csv_path}"

    _send_alert(target_date=target_date, error_rows=error_rows, csv_path=csv_path, **context)
    return f"검증 경고: {target_date} | 오차율 2% 이상 {len(error_rows)}건 | CSV: {csv_path}"
```

### `_alert_summary_rows` 오차 판정 기준 (line 634, 참고용 — 수정 금지)
```python
error_keys = grouped[
    (grouped["error_rate"] >= 2)
    & (grouped["difference"].abs() >= 100000)
][[date_col, "store"]]
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import validate_monthly_sales, validate_sales, _toorder_baseline_max_date, TOORDER_DAILY_PARQUET"` → 기대: 에러 없음
2. [기준값 최신일] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _toorder_baseline_max_date as f; print(f())"` → 기대: `2026-07-28` (ToOrder 백필 전 현재 상태)
3. [컷오프 축소 로그] `validate_monthly_sales()` 로컬 실행 → 기대: 로그에 `기준값(ToOrder) 지연으로 월별 검증 컷오프 축소: 2026-07-29 → 2026-07-28` 1줄
4. [오탐 제거 — 핵심]
   ```
   python -c "import pandas as pd; from modules.transform.utility.paths import MART_DB; d=pd.read_csv(MART_DB/'unified_sales_grp_error_list'/'unified_sales_monthly_2026-07.csv',encoding='utf-8-sig'); g=d.groupby('store')[['excel_total','unified_total']].sum(); g['diff']=g['unified_total']-g['excel_total']; g['rate']=(g['diff'].abs()/g['excel_total'].abs()*100); e=g[(g['rate']>=2)&(g['diff'].abs()>=100000)]; print(len(e), int(e['diff'].sum()))"
   ```
   → 기대: `10 -1716808` (수정 전은 `45 44037903`)
5. [알림 문구] Test 3 실행 시 텔레그램 본문 → 기대: `비교범위: 2026-07-01 ~ 2026-07-28` + `※ ToOrder 기준값이 2026-07-28까지만 수집됨 (이후 일자 비교 제외)`
6. [일별 무음 가드] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import _load_excel_totals; print(_load_excel_totals('2026-07-29').empty)"` → 기대: `True` (이 조건에서 `validate_sales`가 `보류` 메시지 반환 + CSV 미생성)
7. [과거월 회귀] `MART_DB/unified_sales_grp_error_list/unified_sales_monthly_2026-01.csv` ~ `2026-06.csv` 값이 수정 전과 동일 → 기대: 변화 없음 (과거월 컷오프 미적용)
8. [기준값 정상 시 회귀] ToOrder 07-29 백필 후 재실행 → 기대: `baseline_lagged=False`, 컷오프 `2026-07-29` 복귀, 축소 로그·※문구 사라짐
9. [기존 테스트] `pytest tests/test_unified_sales_validate_alert.py tests/test_unified_sales_manual_delivery_cleanup.py` → 기대: 전체 PASS

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~9 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 date 컬럼 타입, 문자열 사전순 비교, current_ym 분기, baseline_lagged 전달 경로)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: TC1~9 PASS + Constraints 위반 없음
```

## Constraints
- `_resolve_target_date` / `_resolve_daily_validation_target_date`는 수정 금지 (일별·월별 공유). 컷오프 상한은 `validate_monthly_sales` 내부에서만 계산.
- 과거월(`ym != current_ym`)에는 절대 컷오프 적용 금지 — 회귀 발생.
- `_compute_diff` / `_compute_monthly_diff` / `_alert_summary_rows`의 오차 판정 기준(2% & ±10만원)은 손대지 말 것.
- `KST` 신규 정의 금지, 기존 상단 상수 재사용.
- `_load_parquet_monthly_totals`(unified 쪽)에는 기준값 상한을 따로 넣지 말 것 — `validate_monthly_sales`가 넘기는 `max_date` 하나로 unified/기준값 양쪽이 동시에 깎여야 한다.
- ToOrder 수집 DAG(`dags/sales/DB_Toorder_store_platform_daily_Dags.py`)와 스케줄 상수 `DB_TOORDER_STORE_PLATFORM_TIME`은 이번 범위 밖 — 건드리지 말 것.
- `Strategy_ScheduleGuard_01_Overdue_Dags`(07:10 슬롯 스킵 감지 갭)도 이번 범위 밖 — 별건 태스크.
- 잔존 실오차 10곳(아래)은 데이터 이슈이며 코드로 손대지 말 것. 이번 수정 후에도 정상 잔존해야 한다.

## 잔존 실오차 10곳 (07-29 제외 재계산 결과 — 정상 잔존, 수정 대상 아님)
| 매장 | 차이 | 오차율 | 주 채널 |
|---|---|---|---|
| 광명철산점 | +2,757,800 | 15.05% | 배민 +2.45M (ToOrder 미릴레이, 기존 known) |
| 교대점 | -2,102,000 | 11.68% | 쿠팡 -2.10M |
| 천안성정점 | -1,930,500 | 11.96% | 쿠팡 -1.93M |
| 강원영월점 | +1,330,800 | 17.41% | 기타 +1.33M |
| 부산장림점 | -1,234,699 | 4.65% | 배민 -1.11M |
| 대전관평점 | +923,402 | 3.40% | 쿠팡 +0.75M |
| 서울대입구역점 | -816,702 | 2.98% | 쿠팡 -0.75M |
| 시흥장현점 | -621,490 | 4.26% | 배민 -0.29M / 기타 -0.24M |
| 경북상주점 | -245,519 | 2.70% | 배민 -1.39M ↔ 기타 +1.14M (채널 분류 어긋남 의심) |
| 전주전북대점 | +222,100 | 3.06% | 기타 +0.25M |

## 운영 조치 (코드 수정과 별개, 사람이 실행)
1. `DB_Toorder_store_platform_daily_Dags`를 conf `{"sale_date_from": "2026-07-29", "sale_date_to": "2026-07-29"}`로 트리거 → ToOrder parquet에 07-29 채우기.
2. `DB_UnifiedSales`의 `validate_monthly_sales` 재실행 → 2026-07 알림 재발송, 실오차 10곳만 남는지 확인.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`str | None` 사용)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 헬퍼 배치 위치가 모호하면: 관련 로더 함수 바로 위
