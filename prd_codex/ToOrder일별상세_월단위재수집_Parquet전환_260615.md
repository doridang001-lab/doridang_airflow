# ToOrder 일별상세 매출 수집 → 월단위 전체 재수집 + Parquet 전환

## Task
토더(ceo.toorder.co.kr) 채널별 일매출 수집을 **하루 단위 스냅샷(1개 매장)** 에서
**월 단위 전체 재수집(전 매장·전 채널)** 으로 바꾼다. 토더가 사후 정정한 값이 반영 안 되는 스냅샷 문제를
해결하기 위해, 매 실행마다 `2025-07-01 ~ 어제` 구간을 **월 단위로 통째로 다시 다운로드**해 덮어쓴다(자가치유).
새 보고서 URL `/sales-report/datedetail`은 1개 파일에 날짜별 시트(+`종합` 요약 시트)를 담고 전 매장×전 채널을
포함한다. 출력은 **CSV → Parquet**로 전환하고, 이를 읽는 다운스트림(`DB_UnifiedSales_toorder`)도 함께 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR` (하드코딩 금지)
- parquet 엔진: `engine="pyarrow"`, `index=False`

## Files to Create / Modify
- **수정** `modules/extract/crawling_toorder_sales_report.py` — datedetail 월단위 다운로드 함수 추가
- **수정** `modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py` — 신규 멀티시트 파서 + 월 분할 + parquet upsert
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py` — 입력 CSV → Parquet
- **수정** `dags/sales/DB_Toorder_store_platform_daily_Dags.py` — 기본 범위 2025-07-01~어제, parquet 경로, 불필요 태스크 제거

## 새 엑셀 구조 (E:\d_down\종합보고서_일별상세_매출보고서.xlsx — 실측 완료)
- 시트: `['종합', '2026-06-01', '2026-06-02', …]` → **첫 시트 `종합` 제외**, 나머지 **시트명 = 날짜(YYYY-MM-DD)**
- 일별 시트 = **매장(열블록) × 채널(행)** 행렬, `max_col=269` (헤더 매장 66개)
  - `header=None`, 0-based 인덱스 기준:
  - **row idx 1**: 날짜 문자열(시트명과 동일)
  - **row idx 2**: 매장 헤더 → col 1=`합계`(제외), 매장명 = **col 5, 9, 13, … (stride 4)**
  - **row idx 5**: 블록 서브헤더 `[총 매출, 매출액, 배달료, 영수 건수]` (블록당 4열)
  - **row idx 6 ~ (합계 직전)**: col 0 = 채널명. 채널 18종:
    `홀, 홀 포장, 홀 배달, 배달의민족, 배민 포장, 배민1, 쿠팡이츠, 쿠팡 포장, 요기요, 요기요 포장, 요기배달, 땡겨요, 땡겨요 포장, 네이버, 배달특급, 먹깨비, 먹깨비 포장, 배달e음`
  - **col 0 == `합계` 행에서 중단**(row idx 24) — 하드코딩 금지, `합계` 만나면 break
  - 매장 i 블록 = cols `[5+i*4 .. 5+i*4+3]` → **price = 총매출(offset 0)**, **receipts_num = 영수건수(offset 3)**

### 실측 검증 결과 (프로토타입 파서 전수 실행 — 이미 통과)
- 16시트 → `종합` 제외 후 **15개 일별 시트 전부 정상**, 시트명 = row1 날짜 100% 일치
- 전 시트 동일 구조(ncol=269, 매장 66, 채널 18, `합계`=row24), **이슈 NONE / 에러 없음 / 음수매출 없음**
- 총 **3,176행** = 15일 × **실매장 65개** × 채널. 66개 중 **`테스트매장`(전 채널 0원) 명시 제외**
- `해운대중동점` 정상 추출 → 다운스트림 brand=`도리당` 자동 부여 확인
- `백석점` 06-01 전 채널 0원 = 실제 휴무(타 날짜 정상), 정렬버그 아님

## Implementation Steps

### 1. 크롤러 — `modules/extract/crawling_toorder_sales_report.py`
1-1. 상수 추가:
```python
SALES_REPORT_DATEDETAIL_URL = "https://ceo.toorder.co.kr/dashboard/sales-report/datedetail"
ORIG_DATEDETAIL_FILENAME = "종합보고서_일별상세_매출보고서.xlsx"
```
1-2. `_set_date_range(driver, account_id, start_date, end_date=None)` 로 일반화 — `end_date` None이면 start와 동일(기존 단일날짜 호출 호환). 시작일=start, 종료일=end 각각 입력.
1-3. 신규 공개 함수 `run_crawling_datedetail_month(toorder_id, toorder_pw, month_start, month_end, download_dir) -> dict`:
   - `_launch_report_browser` → `_do_login` → `driver.get(SALES_REPORT_DATEDETAIL_URL)` (URL에 `datedetail` 포함 확인)
   - `_set_date_range(driver, id, month_start, month_end)` → `보고서 생성` XPath 클릭(기존 동일)
   - **다운로드 감지**: 클릭 직전 `existing = set(download_dir.glob("*"))` → 완료 대기(최대 30s, `.crdownload` 제외) → `new = current - existing`로 새 파일 포착(크롬 `… (1).xlsx`도 포착)
   - 새 파일 → `종합보고서_일별상세_매출보고서_{YYYY-MM}.xlsx` rename. 동일 타깃 존재 시 `_bak_HHMMSS` 백업. 다운로드 직전 base-name 잔여 파일 정리.
   - 반환: `{"success","file","month","error"}`
   - 기존 `run_crawling_single_date` / `run_crawling_date_range`(orderkinds)는 그대로 둔다.

### 2. 파서 + 파이프라인 — `modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py`
2-1. 상수:
```python
_PARQUET_NAME = "toorder_store_platform_daily.parquet"
_DATEDETAIL_PREFIX = "종합보고서_일별상세_매출보고서"
_TEST_STORE = "테스트매장"
_STORE_COL_START = 5
_STORE_COL_STRIDE = 4
_DATA_ROW_START = 6
```
2-2. 신규 파서 `_parse_datedetail_xlsx(xlsx_path) -> pd.DataFrame`:
```python
def _parse_datedetail_xlsx(xlsx_path) -> pd.DataFrame:
    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, engine="openpyxl")
    rows = []
    for sheet_name, df in sheets.items():
        if str(sheet_name).strip() == "종합":
            continue
        date_str = str(sheet_name).strip()           # 시트명 = 날짜
        ncol = df.shape[1]
        r2 = df.iloc[2]                                # 매장 헤더
        stores = []                                   # (col, store)
        for c in range(_STORE_COL_START, ncol, _STORE_COL_STRIDE):
            name = r2.iloc[c]
            if pd.isna(name) or str(name).strip() in ("", _TEST_STORE):
                continue
            stores.append((c, str(name).strip()))
        for ri in range(_DATA_ROW_START, df.shape[0]):
            ch = df.iloc[ri, 0]
            if pd.isna(ch) or str(ch).strip() == "":
                continue
            if str(ch).strip() == "합계":             # 채널 합계 행 → 중단
                break
            platform = str(ch).strip()
            for c, store in stores:
                price = _coerce_int(df.iloc[ri, c])           # 총매출
                receipts = _coerce_int(df.iloc[ri, c + 3])    # 영수건수
                if price == 0 and receipts == 0:
                    continue
                rows.append({"date": date_str, "store": store, "platform": platform,
                             "price": price, "receipts_num": receipts})
    return pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)
```
2-3. `_iter_month_spans(date_from, date_to) -> list[tuple[str,str]]`: 월 1일~말일 구간 리스트, 마지막 월 end는 `date_to`로 클램프.
2-4. `run_toorder_store_platform_daily(*, date_from=None, date_to=None, ...)` 재작성:
   - 기본값: `date_from="2025-07-01"`, `date_to=어제(KST)`
   - 각 월 span: (a) `DOWN_DIR`에 수동 배치된 `{_DATEDETAIL_PREFIX}_*.xlsx` 우선 ingest(있으면 파싱 후 `_rename_to_raw`), (b) 없으면 `run_crawling_datedetail_month`로 다운로드 후 `_parse_datedetail_xlsx`
   - 전체 매장·전체 채널 수집(`테스트매장` 제외). `_filter_store_rows` 미사용.
   - 모든 월 결과 concat → `_upsert_parquet`
2-5. `_upsert_parquet(new_df, parquet_path)` 신설 (`_upsert_csv` 패턴 복제):
   - key=`["date","store","platform"]`, 기존 parquet 있으면 `pd.read_parquet` 후 동일 키 행 제거 → concat → `drop_duplicates(key, keep="last")`
   - dtype: date/store/platform → str, price/receipts_num → int
   - `merged.to_parquet(parquet_path, index=False, engine="pyarrow")`
   - 구 `_parse_manual_xlsx`/`_collect_for_dates`(orderkinds)는 미호출(보존 가능).

### 3. 다운스트림 reader — `modules/transform/pipelines/db/DB_UnifiedSales_toorder.py`
3-1. 경로 상수 변경:
```python
TOORDER_PARQUET = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
```
3-2. `_load_csv()` → `_load_parquet()`:
```python
def _load_parquet() -> pd.DataFrame:
    if not TOORDER_PARQUET.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(TOORDER_PARQUET).fillna("")
    except Exception as exc:
        logger.warning("toorder parquet load failed: %s", exc)
        return pd.DataFrame()
    if not {"date", "store", "platform", "price"}.issubset(df.columns):
        logger.warning("toorder parquet columns invalid")
        return pd.DataFrame()
    return df
```
3-3. 호출부 3곳(`run_toorder`, `run_lookback_toorder`, `backfill_toorder`)에서 `_load_csv()` → `_load_parquet()`. 에러 메시지의 경로도 `TOORDER_PARQUET`로. `_transform_df` 등 변환 로직은 그대로(입력만 parquet).

### 4. DAG — `dags/sales/DB_Toorder_store_platform_daily_Dags.py`
4-1. `from ...paths import ANALYTICS_DB`; `PARQUET_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"`
4-2. `resolve_dates`: conf 우선순위 →
   - `{"month": "2025-07"}` → 해당 월(1일~말일)만
   - `{"sale_date_from","sale_date_to"}` → 임의 구간
   - conf 없음 → `date_from="2025-07-01"`, `date_to=어제(KST)` 전체
   - XCom으로 `date_from`,`date_to` push (월 분할은 파이프라인이 수행)
4-3. `collect_and_save` / `collect_and_save_guard`: `run_toorder_store_platform_daily(date_from=..., date_to=..., log_prefix=...)` 2-pass(누락 보정).
4-4. `normalize_csv_date_column` 태스크 **제거**(신규 소스는 날짜·플랫폼 정상, `해운대중동점`은 store_normalize가 자동 처리).
4-5. `STORE_NAME`/`STORE_SAVE_NAME` 상수 제거. task 체인: `resolve_dates >> collect_and_save >> collect_and_save_guard`.

## Reference Code

### modules/transform/pipelines/sales/DB_Toorder_store_platform_daily.py (기존 핵심)
```python
import logging, os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pendulum
from modules.extract.crawling_toorder_sales_report_daily_date import run_crawling_single_date
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR

_DEFAULT_DEST = ANALYTICS_DB / "toorder_daily_store_platform"
_OUTPUT_COLUMNS = ["date", "store", "platform", "price", "receipts_num"]

def _coerce_int(value) -> int:
    if pd.isna(value): return 0
    try: return int(float(value))
    except (TypeError, ValueError): return 0

def _rename_to_raw(xlsx_path: Path) -> None:        # 처리완료 파일 아카이브 (재사용)
    raw_path = xlsx_path.with_name(f"{xlsx_path.stem}_raw{xlsx_path.suffix}")
    if raw_path.exists():
        raw_path.rename(raw_path.with_name(f"{raw_path.stem}_bak_{pendulum.now('Asia/Seoul').format('HHmmss')}{raw_path.suffix}"))
    xlsx_path.rename(raw_path)

def _upsert_csv(new_df, csv_path):                  # ↓ 이 패턴을 _upsert_parquet로 복제
    key_cols = ["date", "store", "platform"]; numeric_cols = ["price", "receipts_num"]
    # ... 기존 데이터 read → 동일 키 제거 → concat → drop_duplicates(key, keep="last") → 저장
```

### modules/extract/crawling_toorder_sales_report.py (재사용 함수 시그니처)
```python
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
def _launch_report_browser(account_id, download_dir): ...   # uc.Chrome (headless=AIRFLOW_HOME 존재시)
def _do_login(driver, account_id, password) -> bool: ...    # React input, 기업회원 체크박스, submit
def _set_date_range(driver, account_id, target_date) -> bool:   # ← end_date=None 인자 추가해 일반화
    # MUI input 2개(start,end)에 'yy-mm-dd' 입력. 현재는 둘 다 동일값.
def _navigate_to_sales_report(driver, account_id) -> bool: ...  # orderkinds 전용 (그대로 둠)
# 보고서 생성 버튼: WebDriverWait(driver,15).until(EC.element_to_be_clickable((By.XPATH,"//button[contains(text(),'보고서 생성')]")))
# 다운로드 감지: existing=set(glob) → new=current-existing, .crdownload 제외, 완료 후 rename
```

### modules/transform/pipelines/db/DB_UnifiedSales_toorder.py (변환 로직 — 입력만 교체)
```python
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.store_normalize import normalize as _normalize_store_names, strip_brand as _strip_brand
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, _load_store_map, _lookup_store_meta, _make_unified_pk, _save_unified_daily, _to_int_series)
_PLATFORM_MAP = {"배민1":"배달의민족","배달의민족":"배달의민족","쿠팡이츠":"쿠팡이츠","요기요":"요기요","땡겨요":"땡겨요","배민":"배달의민족"}
_BRAND_PREFIXES = {"도리당","나홀로"}
# _transform_df: price>0 필터, store_normalize→brand추출→strip_brand, platform map, groupby(brand,store,platform) sum,
#   store_meta lookup(last token), unified_sales 컬럼 채움. (그대로 유지)
# run_toorder / run_lookback_toorder / backfill_toorder: _load_csv() → _load_parquet() 로만 교체
```

### store_normalize (자동 brand 부여 동작)
```python
STORE_NAME_MAP = { "해운대중동점": "도리당 해운대중동점", ... }   # normalize()가 자동 치환
# 결과: "해운대중동점" → "도리당 해운대중동점" → brand="도리당", store="해운대중동점"
# 주의: 매핑 없는 64개 매장(백석점 등)은 brand="" 로 적재됨 (사용자 승인된 동작)
```

## Test Cases
1. [파서 단위] `python -c "import sys;sys.path.insert(0,r'C:\airflow');from modules.transform.pipelines.sales.DB_Toorder_store_platform_daily import _parse_datedetail_xlsx;df=_parse_datedetail_xlsx(r'E:\d_down\종합보고서_일별상세_매출보고서.xlsx');print(len(df), df['date'].nunique(), df['store'].nunique(), df['platform'].nunique())"` → 기대: `3176 15 65 18`
2. [테스트매장 제외] 위 df에서 `('테스트매장' in df['store'].values)` → 기대: `False`
3. [해운대 정상] `df[(df.date=='2026-06-01')&(df.store=='해운대중동점')]` 행 존재(배민1=47300 등) → 기대: 비어있지 않음
4. [parquet upsert] 임시경로에 `_upsert_parquet(df, tmp)` 후 `pd.read_parquet(tmp)` 재로드 → 행수 동일, key (date,store,platform) 유일
5. [파이프라인 import] `python -c "from modules.transform.pipelines.sales.DB_Toorder_store_platform_daily import run_toorder_store_platform_daily, _parse_datedetail_xlsx, _upsert_parquet"` → ImportError 없음
6. [reader import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import run_toorder, run_lookback_toorder, backfill_toorder, _load_parquet"` → ImportError 없음
7. [크롤러 import] `python -c "from modules.extract.crawling_toorder_sales_report import run_crawling_datedetail_month, SALES_REPORT_DATEDETAIL_URL"` → ImportError 없음
8. [DAG import] `python -c "from dags.sales.DB_Toorder_store_platform_daily_Dags import dag"` → ImportError 없음, 태스크 = `resolve_dates, collect_and_save, collect_and_save_guard`
9. [다운스트림 통합 — parquet 존재 시] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_toorder import run_toorder;print(run_toorder('2026-06-01', overwrite=True))"` → 기대: `toorder 2026-06-01: N rows` (N>0), 에러 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~9 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 출력 컬럼 고정: `["date","store","platform","price","receipts_num"]`
- `종합` 시트, `합계` 행(col0), `합계` 열(col1), `테스트매장`, `price==0 & receipts==0` 행 모두 제외
- 날짜는 **시트명 그대로** 사용(YYYY-MM-DD). 임의 포맷 변환 금지
- `합계` 채널 행은 하드코딩 인덱스 아닌 **col0=='합계' break**로 탐지
- parquet 저장은 `engine="pyarrow", index=False`
- 매 실행 2025-07-01~어제 전체 월단위 재수집 → upsert(부분 실패 시 기존 데이터 보존)
- `max_active_runs=1`, `catchup=False`, `dag_id=Path(__file__).stem` 유지
- 크롤링 페이지 셀렉터/다운로드 동작은 datedetail에서 1개월 실제 다운로드로 검증(orderkinds와 동일 컴포넌트 추정이나 불일치 시 셀렉터 조정)
- print 금지, logging만. 하드코딩 경로 금지(paths.py 상수)

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
