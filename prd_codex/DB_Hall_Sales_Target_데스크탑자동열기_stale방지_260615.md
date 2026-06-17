# DB_Hall_Sales_Target — 데스크탑 자동 열기 + 실매출 stale 방지

## Task
`DB_Hall_Sales_Target_Dags` 주간보고 Excel을 (1) 완료 후 사용자 **데스크탑에 자동으로 띄우고**,
(2) hall DAG가 `unified_sales_grp` parquet 갱신(DB_UnifiedSales) **전에** 실행되어 미완성 매출이
날짜파일로 고정되는 **stale 스냅샷을 방지**한다. Airflow는 Docker(WSL) 컨테이너라 데스크탑에 직접
GUI를 못 띄우므로, 호스트(Windows)측 watcher가 DAG 산출물을 감지해 열고, 컨테이너측은 신선도
가드로 잘못된 숫자를 애초에 쓰지 않게 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 단발성 스크립트: `scripts/`, `.venv`(Windows) 또는 `.venv_wsl`(WSL)로 실행

## Files to Create / Modify
**수정:**
- `modules/transform/pipelines/db/DB_Hall_Sales_Target.py` — 신선도 가드 추가
- `dags/db/DB_Hall_Sales_Target_Dags.py` — retries/retry_delay 강화 + docstring 정정
- `modules/transform/utility/schedule.py` — `DB_HALL_SALES_TARGET_TIME` 시각 이동 + 주석 정정

**생성:**
- `scripts/watch_hall_report.py` — Windows 감시 스크립트 (mtime 감지 → 데스크탑 복사 + Excel 열기)
- `scripts/watch_hall_report.bat` — `.venv` 런처 (로그인 시 자동 시작용)

## Implementation Steps

### 1. 신선도 가드 — `DB_Hall_Sales_Target.py`
`build_hall_sales_target()` 안에서 `df = _load_hall_df()` 직후, `if df.empty:` 분기 **다음에** 추가:
```python
from datetime import date, timedelta
from airflow.exceptions import AirflowException

required_through = pd.Timestamp(date.today() - timedelta(days=1))  # 어제
max_date = df["sale_date"].max()
if pd.isna(max_date) or max_date < required_through:
    logger.error(
        "grp 신선도 미달: max=%s < 필요=%s — DB_UnifiedSales 완료 전 실행 추정, 재시도 대기",
        max_date, required_through,
    )
    raise AirflowException(
        f"unified_sales_grp 신선도 미달 (max={max_date}, 필요>={required_through.date()})"
    )
```
- raise이므로 **skip 아님 → 재시도**. 부분 스냅샷을 절대 쓰지 않는다.
- t_csv(`build_hall_sales_target`) 한 곳만 가드하면 충분(하위 t_excel·t_daily_excel은 all_success라 차단됨).

### 2. DAG 재시도 강화 + docstring 정정 — `DB_Hall_Sales_Target_Dags.py`
`default_args`에서:
```python
"retries": 3,
"retry_delay": timedelta(minutes=15),
```
- 파일 상단 docstring의 "10:30 완료 후 10:55 실행" 문구를 실제 스케줄과 일치하도록 정정
  (실제 cron은 `schedule.py`의 `DB_HALL_SALES_TARGET_TIME` 참조 — 본 작업에서 11:00으로 이동).

### 3. 스케줄 이동 + 주석 정정 — `schedule.py`
```python
DB_HALL_SALES_TARGET_TIME = "0 11 * * 1"  # 매주 월요일 11:00 (DB_UnifiedSales grp 갱신 완료 후)
```
- 기존 `"15 10 * * 1"` + 잘못된 주석("08:05") 교체.

### 4. Windows 감시 스크립트 — `scripts/watch_hall_report.py` (신규)
`scripts/watch_dag_import_errors.py`의 루프/state 패턴 + `run_hall_weekly_report_local.py`의
`os.startfile` 패턴 차용. 동작:
1. 감시 대상: `MART_DB / "hall_sales_target" / "hall_weekly_report.xlsx"` (고정 파일, 매 생성마다
   최신 데이터로 덮임 → mtime 변화가 신뢰 신호).
2. 폴링(기본 60초)으로 mtime 비교, 증가 감지 시:
   - `shutil.copy2`로 데스크탑(`Path.home()/"Desktop"`)에 사본 생성 (online-only placeholder
     materialize + "데스크탑에 옴" 충족). 사본명: `홀_주간보고_{YYMMDD}.xlsx`.
   - `os.startfile(desktop_copy)` → 데스크탑에서 Excel 자동 실행.
   - 처리한 mtime을 state 파일(`logs/hall_report_watch_state.json` 등)에 저장 → 재시작 중복 열기 방지.
3. 경로는 `from modules.transform.utility.paths import MART_DB` 사용(Windows OneDrive 자동 감지).
4. 모듈 import 위해 스크립트 시작에서 프로젝트 루트를 `sys.path`에 추가(기존 `scripts/_base.py`
   방식 참고). `os.startfile`은 Windows 전용이므로 `sys.platform.startswith("win")` 가드.

### 5. 런처 — `scripts/watch_hall_report.bat` (신규)
`run_hall_weekly_report_local.bat` 형식:
```bat
@echo off
cd /d "%~dp0\.."
".venv\Scripts\python.exe" "scripts\watch_hall_report.py"
```
- 로그인 시 자동 시작은 **작업 스케줄러 "사용자가 로그온한 경우에만 실행"** 또는 시작프로그램 바로가기로
  등록(인터랙티브 세션이어야 Excel이 데스크탑에 보임). 등록 절차는 README/주석으로 안내.

## Reference Code

### scripts/run_hall_weekly_report_local.py (자동 열기 패턴)
```python
import os
import sys
from datetime import date
from _base import logger, run_script
from modules.transform.utility.paths import MART_DB

XLSX_DIR = MART_DB / "hall_sales_target"

def _open_in_excel(path) -> None:
    """생성된 파일을 OS 기본 앱(Excel)으로 자동 열기 (Windows 전용)."""
    if not path.exists():
        logger.warning("열 파일이 없습니다: %s", path)
        return
    if sys.platform.startswith("win"):
        os.startfile(str(path))  # type: ignore[attr-defined]
        logger.info("Excel 자동 실행: %s", path)
    else:
        logger.info("자동 열기 생략 (non-Windows): %s", path)
```

### scripts/watch_dag_import_errors.py (폴링 루프 + state 패턴)
```python
import logging, os, time
from pathlib import Path

LOGGER = logging.getLogger(__name__)

def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key, value = key.strip(), value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value

POLL_INTERVAL = int(os.getenv("IMPORT_ERROR_POLL_INTERVAL", "60"))

def main() -> None:
    while True:
        try:
            ...  # mtime 비교 → 변화 시 복사 + os.startfile
        except Exception:
            LOGGER.exception("watch loop error")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
```

### modules/transform/pipelines/db/DB_Hall_Sales_Target.py (가드 부착 지점)
```python
STORE_NAME   = "송파삼전점"
PLATFORM     = "홀"
UNIFIED_ROOT = MART_DB / "unified_sales_grp"

def _load_hall_df() -> pd.DataFrame:
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        raise FileNotFoundError(f"unified_sales parquet 없음: {UNIFIED_ROOT}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    ...
    mask = (df["store"] == STORE_NAME) & (df["platform"] == PLATFORM)
    df = df[mask].copy()
    if df.empty:
        logger.warning("홀 데이터 없음 — store='%s', platform='%s'", STORE_NAME, PLATFORM)
        return df
    df["기준월"] = df["sale_date"].dt.strftime("%Y-%m")
    return classify_hall_time_slots(df)

def build_hall_sales_target(monthly_targets: dict) -> str:
    df = _load_hall_df()
    if df.empty:
        return "홀 데이터 없음, CSV 미생성"
    # ← 여기에 신선도 가드 추가
    weekly = _weekly_agg(df)
    ...
```

## Test Cases
1. [가드 정상 통과] WSL `.venv_wsl`에서:
   `python -c "from modules.transform.pipelines.db.DB_Hall_Sales_Target import build_hall_sales_target; from modules.transform.pipelines.db.DB_Hall_Sales_Target_config import build_targets; mt,_,_=build_targets(); print(build_hall_sales_target(mt))"`
   → 기대: 예외 없이 `완료 N행` 출력 (현재 grp는 6/14까지 포함).
2. [가드 발동] grp max_date가 어제 미만이 되도록 모의(임시로 최신 parquet 제외) → 기대:
   `AirflowException` raise, CSV 미생성.
3. [DAG import] `python -c "from dags.db.DB_Hall_Sales_Target_Dags import dag; print(dag.dag_id)"`
   → 기대: ImportError 없음, `DB_Hall_Sales_Target_Dags` 출력.
4. [스케줄 상수] `python -c "from modules.transform.utility.schedule import DB_HALL_SALES_TARGET_TIME as t; print(t)"`
   → 기대: `0 11 * * 1`.
5. [watcher 구문] `python -c "import ast; ast.parse(open('scripts/watch_hall_report.py',encoding='utf-8').read())"`
   → 기대: 출력 없음(구문 오류 없음).
6. [watcher E2E, Windows] `scripts\watch_hall_report.bat` 실행 후 `hall_weekly_report.xlsx`
   mtime 변경 → 기대: 데스크탑에 `홀_주간보고_YYMMDD.xlsx` 생성 + Excel 자동 실행.
7. [데이터 정합] 생성된 파일 이번주 매출 == 7,381,960 / 201건 (현재 OKPOS 원천)과 일치.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행 (6,7은 Windows GUI 수동 확인)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: Test Cases 1~5 전체 PASS + Constraints 위반 없음
```

## Constraints
- 신선도 가드는 raise(재시도)로 구현 — `AirflowSkipException`(skip) 사용 금지(그 주는 산출물이 안 나옴).
- 가드는 t_csv 한 곳만(상위 차단으로 충분), t_excel/t_daily에 중복 추가 금지.
- watcher는 **Windows 인터랙티브 세션** 전용(session 0 작업 스케줄러로 돌리면 Excel이 데스크탑에 안 보임).
- watcher 감시 대상은 날짜파일이 아니라 **고정 파일** `hall_weekly_report.xlsx`(항상 최신 데이터).
- 경로 하드코딩 금지 — `MART_DB` 사용. `print` 금지 — `logger`.
- 기존 stale `hall_weekly_report_260615.xlsx`는 손대지 않음(최신 고정파일이 이미 정확).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 폴링 간격/state 파일 경로: Reference 패턴 기준 합리적 기본값 직접 결정
