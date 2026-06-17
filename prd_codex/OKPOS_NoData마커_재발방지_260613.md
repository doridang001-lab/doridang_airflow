# OKPOS NO_DATA 마커 오마킹 재발방지

## Task
OKPOS 수집에서 **아직 마감되지 않은 당일(또는 미래) 날짜를 조회**하면 빈 결과가 반환되는데, 시스템이 이를 "휴무/데이터없음"으로 오판하여 `.no_data__*.txt` 마커에 sticky하게 기록한다. 이후 정시 lookback 수집은 `_filter_missing_keys`에서 NO_DATA 날짜를 스킵하므로, 다음날 해당 날짜 매출이 OKPOS에 생겨도 **영구 미수집**된다(2026-06-12 송파삼전점 등 4개 매장 홀매출 0원 사고). 당일/미래 날짜가 NO_DATA로 굳지 않게 막고, 성공 저장 시 stale 마커를 자가 제거하도록 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 기존 파일 스타일 유지: snake_case, 타입힌트 기존과 동일, 주석 최소화(WHY만)

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_OKPOS_Sales.py`
  - `_mark_no_data` (line ~127): 당일/미래 날짜 마킹 차단 가드 추가
  - `save_to_raw` (line ~2262): 성공 저장 직후 `_unmark_no_data` 호출 추가
- **생성/수정 없음**: DAG 파일은 변경 불필요 (collect 함수 직접 import 구조)

## Implementation Steps

### 1) `_mark_no_data`에 당일/미래 마킹 차단 가드 (핵심)
`_mark_no_data(store_short, ym, csv_stem, sale_date, reason)` 함수 **본문 맨 앞**에 가드 추가.
당일/미래 날짜는 아직 마감 전이라 빈 결과가 정상 → NO_DATA로 굳히지 않는다.
```python
def _mark_no_data(store_short: str, ym: str, csv_stem: str, sale_date: str, reason: str) -> None:
    """sale_date를 'NO_DATA'로 마킹하여 이후 재다운로드/누락체크에서 제외되게 한다."""
    # 당일/미래 날짜는 마감 전이라 빈 결과가 정상 → sticky NO_DATA로 굳히면
    # 다음날 데이터가 생겨도 _filter_missing_keys가 영구 스킵하는 사고 발생.
    try:
        if datetime.strptime(sale_date, "%Y-%m-%d").date() >= _kst_now().date():
            logger.info("당일/미래 날짜 → NO_DATA 마킹 스킵 (다음날 재시도): %s | %s", sale_date, reason)
            return
    except Exception:
        pass
    marker_path = _no_data_marker_path(store_short, ym, csv_stem)
    ...  # 기존 로직 그대로 유지
```
- `_kst_now()`(line 39)와 `datetime`은 이미 모듈에 import/정의되어 있음 → 추가 import 불필요.
- 과거 날짜의 진짜 휴무(매출 0)는 그대로 마킹 유지된다(가드에 안 걸림).

### 2) `save_to_raw` 성공 저장 시 stale 마커 자가 제거
`save_to_raw` 내부에서 파티션 CSV(`dest`)에 정상 저장이 끝난 직후(약 line 2380 이후, `onedrive_csv_save` 호출 성공 블록 끝)에 추가.
데이터가 들어오면 과거 오마킹 라인을 제거해 `_filter_missing_keys`가 정상 인식하게 한다.
```python
# 정상 저장됨 → 혹시 과거에 잘못 찍힌 NO_DATA 마커가 있으면 해제(자가치유)
_unmark_no_data(store_short, ym, csv_name, sale_date)
```
- `_unmark_no_data`(line 146)는 이미 정의됨. line 3138에서 reconcile 경로가 동일 패턴으로 호출 중 → 그대로 재사용.
- 변수명 주의: 마커 함수의 csv_stem 인자에는 `save_to_raw`가 쓰는 `csv_name`(예: `okpos_order`, `okpos_order_item`)을 전달. 해당 저장 블록 스코프에 `store_short`, `ym`, `csv_name`, `sale_date`가 모두 존재하는지 확인 후 삽입.

### 3) (선택) `resolve_sale_dates` 당일/미래 제외 — 이번 PR 범위 제외
방어 강화용이나 1)·2)로 충분. 구현하지 않는다(스코프 최소화).

## Reference Code
### modules/transform/pipelines/db/DB_OKPOS_Sales.py (마커 관련)
```python
def _kst_now() -> datetime:                      # line 39
    ...

def _no_data_marker_path(store_short, ym, csv_stem) -> Path:   # line 96
    return (RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}"
            / f"ym={ym}" / f".no_data__{csv_stem}.txt")

def _read_no_data_dates(marker_path) -> set[str]:   # line 111  (포맷: YYYY-MM-DD \t reason \t recorded_at)
    ...

def _mark_no_data(store_short, ym, csv_stem, sale_date, reason) -> None:   # line 127  ← 수정 대상
    marker_path = _no_data_marker_path(store_short, ym, csv_stem)
    existing = _read_no_data_dates(marker_path)
    if sale_date in existing: return
    line = f"{sale_date}\t{reason}\t{datetime.now():%Y-%m-%d %H:%M:%S}\n"
    marker_path.write_text(... + line, encoding="utf-8")

def _unmark_no_data(store_short, ym, csv_stem, sale_date) -> None:   # line 146  ← 조치2에서 호출
    ...  # 마커에서 해당 sale_date 라인 제거

def _filter_missing_keys(sale_dates, csv_name) -> list:   # line 605
    for sale_date in sale_dates:
        for store in STORES:
            ...
            marker = _read_no_data_dates(marker_path)
            if sale_date in marker:      # line 620-621  ← NO_DATA면 스킵 (영구 제외 원인)
                continue
            ...
```
### save_to_raw 저장부 (수정 위치 맥락)
```python
def save_to_raw(**context) -> str:   # line 2262
    ...
    if src_str == "__NO_DATA__":                 # 2296
        _mark_no_data(store_short, ym_guess, csv_name, sale_date, reason=f"{page_type}:download_no_data")
        continue
    ...
    if df.empty and likely_okpos:                # 2315-2320
        _mark_no_data(store_short, ym, csv_name, sale_date, reason=f"{page_type}:empty_after_transform")
        ...
    dest = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / f"{csv_name}.csv"   # 2330
    ...
    result = onedrive_csv_save(df=..., file_path=str(dest), pk_col="_pk", ...)   # 2372 / 2380
    # ← 여기 성공 직후 _unmark_no_data(store_short, ym, csv_name, sale_date) 추가
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import _mark_no_data, _unmark_no_data, save_to_raw; print('ok')"` → 기대: `ok` (ImportError/SyntaxError 없음)
2. [당일 마킹 차단] 아래 실행 → 기대: 마커 파일에 오늘 날짜가 **기록되지 않음**
   ```python
   python -c "
   import datetime as dt
   from modules.transform.pipelines.db import DB_OKPOS_Sales as m
   today = m._kst_now().date().strftime('%Y-%m-%d')
   m._mark_no_data('__TEST_STORE__', today[:7], 'okpos_order', today, 'unit:test')
   p = m._no_data_marker_path('__TEST_STORE__', today[:7], 'okpos_order')
   marked = p.exists() and any(l.startswith(today) for l in p.read_text(encoding='utf-8').splitlines())
   print('today_marked=', marked)   # 기대: today_marked= False
   if p.exists(): p.unlink()
   "
   ```
3. [과거 마킹 정상] 위 스니펫에서 `today` 대신 `'2020-01-01'` 사용 → 기대: `marked= True` (과거 휴무는 정상 마킹) → 테스트 후 마커 파일 정리
4. [회귀] 06-12 강제 재수집(조치1) 후:
   ```python
   python -c "
   from modules.transform.utility.paths import RAW_OKPOS_SALES
   bad=[p for p in RAW_OKPOS_SALES.glob('brand=*/store=*/ym=2026-06/.no_data__*.txt') if any(l.startswith('2026-06-12') for l in p.read_text(encoding='utf-8').splitlines())]
   print('06-12 still marked:', [str(p) for p in bad])   # 기대: []
   "
   ```

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행 (코드 단위 검증)
  2. FAIL 항목 → 원인 분석 → 코드 수정
  3. 전체 재실행
종료 조건: TC1~3 전체 PASS + Constraints 위반 없음
(TC4는 운영 조치1 실행 후 별도 확인 — 코드 PR 범위 아님)
```

## 운영 조치 (코드 PR 후 사용자/운영자가 WSL에서 실행 — Codex 구현 범위 아님)
```bash
# 1) 06-12 강제 재수집 (force가 NO_DATA 마커 무시)
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_OKPOS_Sales_Dags \
  -c '{"sale_date_from":"2026-06-12","sale_date_to":"2026-06-12","force_redownload":true,"force_redownload_today":true,"force_redownload_receipt":true}'
# 2) unified 06-12 재빌드
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_UnifiedSales -c '{"sale_date":"2026-06-12"}'
```

## Constraints
- `_mark_no_data`의 기존 시그니처/반환(None) 유지 — 호출부 다수(line 2299, 2320, 2653 등) 변경 금지.
- 과거 날짜의 진짜 휴무 마킹 동작은 보존(가드는 당일/미래에만 적용).
- `datetime`, `_kst_now`, `_unmark_no_data`는 기존 정의 재사용 — 신규 import/헬퍼 추가 금지.
- 날짜 파싱 실패 시 예외로 파이프라인 중단 금지 → `try/except`로 감싸 기존 동작 유지.
- print 금지, logger 사용.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 해당 함수만 수정(전체 덮어쓰기 금지)
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트/주석 스타일: 기존 파일과 동일, 주석은 WHY만 최소화
- 변수명: snake_case, 기존과 동일
