# DB_ToOrder_Daily_Store_Dags 로그인 실패 수정

## Task

`DB_ToOrder_Daily_Store_Dags` 의 `collect_toorder_daily_store` 태스크가 토더 로그인 실패로 죽는다. `sales_employee.csv` 에 `비고` 컬럼이 없어 `account.py` 의 자동화 계정 필터가 platform-only 로 조용히 열화되고, 그 결과 `get_default_account("toorder")` 가 기업계정 `doridang15`(pw 9자) 대신 **매장계정 `doridang100001`(pw 8자)** 을 반환한다. 매장계정에 기업회원(isCompany)을 체크해 submit하면 토더가 `"아이디 또는 비밀번호가 잘못 입력 되었습니다. 계정유형 또는 ..."` 오류를 낸다. `account.py` 한 곳을 고쳐 이 결함을 공유하는 toorder DAG 5개를 동시에 복구하고, 크롤러에 isCompany 교대 재시도를 백포트해 재발을 막는다.

### 실패 로그 (근거)

```
{account.py:115} WARNING - 비고 컬럼 없음 - platform/store 필터 fallback 적용
{account.py:140} INFO - 자동화 연결 계정 로드: platform=토더 total=67 stores=['도리당 백석점', ...]
{crawling_toorder_sales_report.py:555} [doridang100001] 입력 검증 OK: id='doridang100001', pw_len=8
{crawling_toorder_sales_report.py:600} [doridang100001] submit 직전: id='doridang100001', pw_len=8, isCompany=True
{crawling_toorder_sales_report.py:653} ERROR - login failed: account_id=doridang100001,
  error_texts=['아이디 또는 비밀번호가 잘못 입력 되었습니다. 계정유형 또는 아이디와 비밀번호를 정확히 입력해 주세요.']
RuntimeError: toorder 일별매출보고서 다운로드 실패(2026-07-29): 로그인 실패
```

`pw_len=8` = 매장 pw `abcd9142`. 기업 pw `ehfl1819!` 는 9자.

### 이 결함을 공유하는 소비자 5곳 (수정 대상 아님 — account.py 수정으로 자동 복구)

- `modules/transform/pipelines/sales/DB_ai_daily_collection_01_collect.py:25-27` ← 실패 경로
- `dags/db/DB_ToOrderMenu_Dags.py:37`
- `dags/strategy/Strategy_ToOrderVoc_01_Crawl_Dags.py:36`
- `dags/sales/Sales_ToOrderSalesReport_Crawl_Dags.py:42`
- `dags/sales/DB_Sales_Alert_01_Score_AI_Daily_Collection_Dags.py:47`

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 폴더 구조·파일명 변경 금지

## Files to Create / Modify

**Modify**

- `C:\airflow\modules\transform\utility\account.py` — `load_automation_account_df` 에 `require_note` 추가, `get_default_account` 재작성
- `C:\airflow\modules\extract\crawling_toorder_sales_report.py` — `_do_login` → `_do_login_once` 분리 + isCompany 교대 재시도 wrapper
- `C:\airflow\tests\test_account.py` — 테스트 5건 추가

**Create**

- `C:\airflow\tests\test_toorder_login_company_toggle.py`

**절대 수정 금지**

- `C:\Local_DB\영업관리부_DB\sales_employee.csv` 및 OneDrive 사본 — 영업관리부 소유 + `collected_at` 자동 갱신 파일. 코드가 `비고` 컬럼 부재를 감지해 방어한다.

## Implementation Steps

### 1. `account.py` — `load_automation_account_df` 에 `require_note` 추가 (L99-121)

시그니처에 `require_note: bool = False` 를 추가하고, `비고` 컬럼 누락 분기에서 `require_note=True` 일 때만 platform-only 열화를 **하지 않고** 빈 프레임을 반환한다.

```python
def load_automation_account_df(
    *,
    platform: str,
    target_stores: list[str] | None = None,
    exact: bool = True,
    csv_path: Path | None = None,
    require_note: bool = False,
) -> pd.DataFrame:
    """Return rows explicitly marked as automation-linked in sales_employee.csv."""
    df = _read_sales_employee_csv(csv_path)
    required = {"플랫폼", "계정ID", "계정PW", AUTOMATION_NOTE_COLUMN}
    missing = sorted(required - set(df.columns))
    if missing:
        logger.warning("자동화 계정 필터 컬럼 누락: %s columns=%s", missing, list(df.columns))
        if AUTOMATION_NOTE_COLUMN in missing:
            if require_note:
                logger.warning(
                    "비고 컬럼 없음 - require_note=True 이므로 platform-only fallback 미적용: columns=%s",
                    list(df.columns),
                )
                return pd.DataFrame(columns=list(required) + ["매장명"])
            required_without_note = {"플랫폼", "계정ID", "계정PW"}
            if required_without_note.issubset(df.columns):
                logger.warning("비고 컬럼 없음 - platform/store 필터 fallback 적용")
            else:
                return pd.DataFrame(columns=list(required) + ["매장명"])
        else:
            return pd.DataFrame(columns=list(required) + ["매장명"])
    # ... 이하 기존 로직 그대로
```

기본값이 `False` 이므로 기존 호출자 동작은 **바이트 단위로 동일**해야 한다. platform-only fallback(전 매장 행 열거)에 의존하는 소비자를 반드시 보존할 것:

- `modules/transform/pipelines/db/DB_Beamin_collect.py:17`
- `modules/transform/pipelines/db/DB_Coupang_combined.py:55, 306`
- `modules/transform/pipelines/strategy/SMP_ddangyo_policy_collect.py:173`

`get_pw` 는 **절대** `require_note=True` 로 바꾸지 말 것. 바꾸면 `get_pw("baemin", <매장ID>)` 가 전부 빈 문자열이 되어 배민/쿠팡/땡겨요 수집이 전멸한다.

### 2. `account.py` — `get_default_account` 재작성 (L149-166)

```python
def _legacy_account_row(channel: str) -> pd.Series | None:
    legacy_df = account_df[account_df["channel"] == str(channel).strip()]
    return None if legacy_df.empty else legacy_df.iloc[0]


def get_default_account(channel: str) -> tuple[str, str]:
    """채널 대표(기업) 계정을 반환한다.

    우선순위
      1) sales_employee.csv 에서 비고='자동화 연결' 필터가 '실제로' 적용된 행.
         비고 컬럼이 없으면 platform-only 로 열화하지 않는다(매장 계정 오선택 방지).
         자동화 행이 여러 개면 내장 대표 계정ID와 일치하는 행을 우선한다.
      2) 내장 account_df 대표 계정. pw 는 get_pw 로 재조회해 CSV 비밀번호 회전을 반영.
      3) ("", "")
    """
    channel_key = str(channel).strip()
    platform = CHANNEL_TO_PLATFORM.get(channel_key)
    if not platform:
        return "", ""

    legacy_row = _legacy_account_row(channel_key)
    legacy_id = str(legacy_row.get("id", "")).strip() if legacy_row is not None else ""

    df = load_automation_account_df(platform=platform, require_note=True)
    if not df.empty:
        matched = df
        if legacy_id and "계정ID" in df.columns:
            preferred = df[df["계정ID"].astype(str).str.strip() == legacy_id]
            if not preferred.empty:
                matched = preferred
            else:
                logger.warning(
                    "자동화 연결 행에 대표 계정ID 없음 - 첫 행 사용: channel=%s expected=%s",
                    channel_key, legacy_id,
                )
        row = matched.iloc[0]
        return str(row.get("계정ID", "")).strip(), str(row.get("계정PW", "")).strip()

    if legacy_row is None:
        logger.warning("기본 계정 없음: channel=%s platform=%s", channel_key, platform)
        return "", ""

    password = get_pw(channel_key, legacy_id) or str(legacy_row.get("pw", "")).strip()
    logger.warning(
        "자동화 연결(비고) 기본 계정 없음 - 내장 대표 계정 사용: channel=%s account_id=%s",
        channel_key, legacy_id,
    )
    return legacy_id, password
```

`get_pw` 로 pw를 재조회하므로 나중에 CSV에 `비고` 컬럼 + `doridang15` 행이 추가되면 비밀번호 회전이 자동 반영된다.

**의도된 동작 변화**: `baemin`/`coupangeats`/`ddangyo` 의 `get_default_account` 는 "임의 매장 행" → `("", "")` + 경고로 바뀐다. 현재 호출자 0건이므로 무해하며, 원래도 임의 매장 1행 반환은 버그였다. 향후 이 채널의 대표 계정이 필요하면 `account_df` 에 행을 추가하는 것이 정답 경로임을 주석으로 남길 것.

### 3. `crawling_toorder_sales_report.py` — isCompany 교대 재시도 백포트

현재 `_do_login` (L503-654) 은 isCompany 체크박스를 **무조건 체크**하고(L563-571) 반대 상태 재시도가 없다. 이 문제는 `SMP_delivery_account_alert_01.py:366-395` 에서 이미 해결됐는데 이 크롤러에는 백포트되지 않았다.

호출부 4곳(L1034, L1190, L1274, L1402)이 모두 `if not _do_login(...)` 형태이므로 **이름·시그니처·반환형(bool)을 유지한 wrapper** 로 감싼다.

**3-1.** 현재 `_do_login` 본문을 `_do_login_once(driver, account_id, password, *, is_company: bool) -> bool` 로 이름만 변경.

**3-2.** L563-571의 무조건 체크를 desired-state 토글 헬퍼로 교체하고 `_do_login_once` 에서 `_set_is_company_checkbox(driver, account_id, is_company)` 로 호출:

```python
def _set_is_company_checkbox(driver, account_id: str, is_company: bool) -> None:
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        try:
            checked = bool(driver.execute_script("return !!arguments[0].checked;", checkbox))
        except Exception:
            checked = checkbox.is_selected()
        if checked != is_company:
            driver.execute_script("arguments[0].click();", checkbox)
        logger.info("[%s] 기업회원 %s", account_id, "체크" if is_company else "해제")
    except Exception:
        logger.warning("[%s] 기업회원 체크박스 제어 실패(무시)", account_id)
```

**3-3.** `_do_login_once` 안의 `_save_login_debug` 태그를 `f"login_fail_company{int(is_company)}"` 로 바꿔 아티팩트 덮어쓰기를 막는다 (L621).

**3-4.** wrapper 추가 — **계정유형/자격증명 오류로 보일 때만** 2회차를 시도한다:

```python
LOGIN_IS_COMPANY_ATTEMPTS = (True, False)
LOGIN_ACCOUNT_TYPE_HINTS = ("계정유형", "아이디 또는 비밀번호", "비밀번호가 잘못")


def _looks_like_account_type_failure(error: str) -> bool:
    return any(hint in (error or "") for hint in LOGIN_ACCOUNT_TYPE_HINTS)


def _do_login(driver, account_id: str, password: str, *,
              is_company_attempts: tuple[bool, ...] = LOGIN_IS_COMPANY_ATTEMPTS) -> bool:
    """계정유형(기업회원) 토글 때문에 동일한 '아이디/비밀번호 오류'가 나는 케이스가 있어
    1회차 기업회원 체크, 2회차 해제로 자동 전환한다."""
    last_error = ""
    total = len(is_company_attempts)
    for index, is_company in enumerate(is_company_attempts, start=1):
        if _do_login_once(driver, account_id, password, is_company=is_company):
            return True
        last_error = getattr(driver, "_toorder_login_error", "") or ""
        if index >= total:
            break
        if not _looks_like_account_type_failure(last_error):
            logger.warning("[%s] 계정유형 오류로 보이지 않아 토글 재시도 생략: %s", account_id, last_error)
            break
        logger.warning("[%s] 로그인 실패(isCompany=%s) → 계정유형 토글 재시도 %d/%d",
                       account_id, is_company, index + 1, total)
        try:
            driver.delete_all_cookies()
        except Exception:
            pass
        time.sleep(1.0)
    try:
        setattr(driver, "_toorder_login_error",
                f"login failed (isCompany attempts={list(is_company_attempts)}): {last_error}")
    except Exception:
        pass
    return False
```

**중요**: `_do_login_once` 의 L525-527 은 retriable 드라이버 오류를 재raise한다. wrapper를 try/except로 감싸면 상위 브라우저 재기동 로직이 죽으므로 **예외를 잡지 말 것**.

### 4. 테스트 추가 — `tests/test_account.py`

CSV는 `monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)` 로 tmp 파일을 주입한다. CSV 헤더는 실제 파일과 동일하게: `오픈순서,호점,매장명,사업자번호,점주명,담당자,실오픈일,상세주소,광역,시군구,읍면동,email,플랫폼,계정ID,계정PW,collected_at`

1. `test_get_default_account_ignores_store_rows_when_note_column_missing` — 비고 없는 CSV(토더 매장행 3개) → `("doridang15", "ehfl1819!")`. **버그 재현 테스트: 수정 전 반드시 실패해야 한다.**
2. `test_load_automation_account_df_still_returns_store_rows_without_note_column` — 같은 CSV, `platform="배달의 민족"` → 매장행 전부 반환. `DB_Beamin_collect`/`DB_Coupang_combined`/`SMP_ddangyo_policy_collect` 회귀 가드.
3. `test_get_default_account_uses_note_rows_when_column_present` — `비고` 컬럼 + `계정ID=doridang15, 비고=자동화 연결` 행 + 매장행 → CSV의 pw 반환(회전 반영 확인).
4. `test_get_default_account_prefers_legacy_id_among_note_rows` — 자동화 행이 여러 개일 때 `doridang15` 행 우선.
5. `test_get_pw_prefers_csv_row_for_store_account` — `get_pw("toorder","doridang100001")` → CSV pw. `get_pw` 무변경 가드.

### 5. 테스트 생성 — `tests/test_toorder_login_company_toggle.py`

`_do_login_once` 를 몽키패치해 **wrapper 루프만** 단위 테스트한다 (DOM/WebDriverWait 페이크는 비용 대비 이득 없음). driver는 속성 대입이 가능한 더미 객체를 쓴다.

- 1회차 실패(`_toorder_login_error` 에 `계정유형` 포함) + 2회차 성공 → `True`, 기록된 `is_company` 순서 == `[True, False]`.
- 두 번 다 실패 → `False`, `_do_login_once` 2회 호출, 최종 `_toorder_login_error` 에 두 시도 정보 포함.
- 1회차 오류가 타임아웃 문구(`계정유형` 힌트 없음) → 2회차 생략(`_do_login_once` 1회만 호출).
- `_do_login_once` 가 retriable 예외 raise → wrapper 밖으로 전파 (`pytest.raises`).

## Reference Code

### modules/transform/utility/account.py (수정 대상 — 현재 상태)

```python
import logging
import re
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB

logger = logging.getLogger(__name__)

AUTOMATION_NOTE_COLUMN = "비고"
AUTOMATION_NOTE_VALUE = "자동화 연결"
SALES_EMPLOYEE_CSV = ONEDRIVE_DB / "sales_employee.csv"
LOCAL_SALES_EMPLOYEE_CSV = LOCAL_DB / "영업관리부_DB" / "sales_employee.csv"

CHANNEL_TO_PLATFORM = {
       "baemin": "배달의 민족",
       "coupangeats": "쿠팡이츠",
       "ddangyo": "땡겨요",
       "toorder": "토더",
}

account_df = pd.DataFrame({
       "platform": ["아임웹", "카페24", "어도비_구글", "toorder", "포스피드"],
       "channel": ["aimeweb", "cafe24", "adobe_google", "toorder", "posspeed"],
       "id": [..., "doridang15", ...],
       "pw": [..., "ehfl1819!", ...],
})


def get_pw(channel: str, account_id: str | None = None) -> str: ...       # L43 — 변경 금지
def _get_legacy_pw(channel: str, account_id: str | None = None) -> str: ...  # L61
def _default_sales_employee_csv_path() -> Path: ...                       # L68 — 테스트 주입 지점
def _read_sales_employee_csv(path: Path | None = None) -> pd.DataFrame: ...  # L74
def load_automation_account_df(*, platform, target_stores=None,
                               exact=True, csv_path=None) -> pd.DataFrame: ...  # L99 — 수정
def get_default_account(channel: str) -> tuple[str, str]: ...             # L149 — 수정
```

주의: `_read_sales_employee_csv` 는 `modules/transform/pipelines/db/DB_Beamin_retry.py:473` 이 직접 import해 사용한다.

### modules/extract/crawling_toorder_sales_report.py (수정 대상 — 현재 상태)

```python
import logging, os, random, re, shutil, subprocess, time, zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
LOGIN_FAIL_URL_PATTERNS = ["/login", "/auth"]
LOGIN_SUCCESS_URL_PATTERNS = ["/dashboard"]

def _fill_react_input(driver, element, value: str, account_id: str, field_name: str) -> bool: ...  # L120
def _save_login_debug(driver, account_id: str, tag: str) -> str | None: ...  # L162
def _is_retriable_driver_error(message: str) -> bool: ...                   # L181
def _wait_for_react_load(driver, timeout: int = 10) -> bool: ...            # L485
def _do_login(driver, account_id: str, password: str) -> bool: ...          # L503 — 분리 대상
# 호출부: L1034, L1190, L1274, L1402  모두 `if not _do_login(driver, toorder_id, toorder_pw):`
```

### modules/transform/pipelines/strategy/SMP_delivery_account_alert_01.py (복사할 정답 패턴)

```python
def _do_login(driver, account_id: str, password: str) -> None:
    """ToOrder 로그인. 재시도 후에도 실패 시 RuntimeError"""
    logger.info("로그인 시도: %s", account_id)

    last_error = ""
    for attempt in range(1, _LOGIN_MAX_ATTEMPTS + 1):
        logger.info("로그인 시도 %d/%d", attempt, _LOGIN_MAX_ATTEMPTS)
        try:
            # 계정유형(기업회원) 토글로 인해 동일한 "아이디/비밀번호 오류"가 뜨는 케이스가 있어
            # 1회차: 기업회원 체크, 2회차: 기업회원 해제 로 자동 전환한다.
            is_company = (attempt % 2 == 1)
            _do_login_once(driver, account_id, password, is_company=is_company)
            logger.info("로그인 성공 (attempt=%d, URL=%s)", attempt, driver.current_url)
            return
        except Exception as exc:
            last_error = str(exc)
            if _is_webdriver_connection_refused(exc):
                logger.warning("WebDriver 연결이 끊어져 로그인 재시도를 중단합니다: %s", last_error)
                raise
            logger.warning("로그인 실패 (attempt=%d): %s", attempt, last_error)
            _save_login_debug_artifacts(driver, attempt)
            if attempt < _LOGIN_MAX_ATTEMPTS:
                try:
                    driver.delete_all_cookies()
                except Exception:
                    pass
                time.sleep(1.0)
    raise RuntimeError(f"로그인 실패 (재시도 {_LOGIN_MAX_ATTEMPTS}회): {last_error}")

# L430-441: desired-state 토글 (무조건 체크가 아님)
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        try:
            checked = bool(driver.execute_script("return !!arguments[0].checked;", checkbox))
        except Exception:
            checked = checkbox.is_selected()
        if checked != is_company:
            driver.execute_script("arguments[0].click();", checkbox)
        logger.info("기업회원 %s", "체크" if is_company else "해제")
    except Exception:
        pass
```

### tests/test_account.py (기존 계약 — 반드시 통과 유지)

```python
import pandas as pd

from modules.transform.utility import account


def test_get_pw_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_pw("toorder", "doridang15") == "ehfl1819!"


def test_get_default_account_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_default_account("toorder") == ("doridang15", "ehfl1819!")
```

몽키패치가 `lambda **_kwargs` 이므로 `require_note=True` 호출도 같은 람다가 받아 빈 프레임을 반환 → legacy 분기 → 기존 단정이 그대로 통과한다.

## Test Cases

1. [기업계정 해석] `C:\airflow\.venv\Scripts\python.exe -c "from modules.transform.utility.account import get_default_account; i,p=get_default_account('toorder'); print(i, len(p))"` → 기대: `doridang15 9` (수정 전에는 `doridang100001 8`)
2. [매장행 열거 회귀 없음] `C:\airflow\.venv\Scripts\python.exe -c "from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts; print(len(load_accounts([])))"` → 기대: 수정 전/후 동일한 `67`
3. [account 테스트] `C:\airflow\.venv\Scripts\python.exe -m pytest tests/test_account.py -q` → 기대: 기존 2건 + 신규 5건 전부 PASS
4. [로그인 토글 테스트] `C:\airflow\.venv\Scripts\python.exe -m pytest tests/test_toorder_login_company_toggle.py -q` → 기대: 4건 PASS
5. [크롤러 import] `C:\airflow\.venv\Scripts\python.exe -c "from modules.extract.crawling_toorder_sales_report import _do_login, _do_login_once, _set_is_company_checkbox; print('ok')"` → 기대: `ok`, ImportError 없음
6. [파이프라인 import] `C:\airflow\.venv\Scripts\python.exe -c "from modules.transform.pipelines.sales.DB_ai_daily_collection_01_collect import run_ai_daily_collection_to_daily_parquet_dir; print('ok')"` → 기대: `ok`
7. [주변 테스트 회귀] `C:\airflow\.venv\Scripts\python.exe -m pytest tests/test_toorder_store_platform_daily.py tests/test_dag_hygiene.py tests/test_dag_schedule_guard.py -q` → 기대: 전부 PASS
8. [실 DAG] `DB_ToOrder_Daily_Store_Dags` 를 conf `{"sale_date": "2026-07-29"}` 로 트리거 → 기대 로그: `[doridang15] submit 직전: id='doridang15', pw_len=9, isCompany=True` + `[doridang15] 로그인 성공: .../dashboard`, 산출물 `toorder_daily_store/toorder_daily_store_20260729.parquet` 생성

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행 (8번은 코드 검증 후 마지막에 1회)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~7 전체 PASS + Constraints 위반 없음
```

Test Case 1이 `doridang100001 8` 을 계속 뱉으면 `require_note=True` 가 `get_default_account` 에서 실제로 전달되는지, `비고` 컬럼 누락 분기가 빈 프레임을 반환하는지 순서대로 확인한다.

Test Case 2가 67이 아니면 `require_note` 기본값이 `False` 로 남아있는지 확인한다 — 이 값을 `True` 로 바꾸면 배민/쿠팡/땡겨요 수집이 전멸한다.

## Constraints

- `sales_employee.csv`(로컬 + OneDrive 사본) **수정 금지** — 영업관리부 소유, `collected_at` 자동 갱신
- `get_pw` 의 동작·시그니처 **변경 금지**. `require_note=True` 로 바꾸지 말 것
- `load_automation_account_df` 의 `require_note` 기본값은 반드시 `False` — platform-only fallback 소비자 3파일 보존
- `_do_login` 의 이름·반환형(bool) 유지 — 호출부 4곳(L1034, L1190, L1274, L1402)을 수정하지 않기 위함
- `_do_login` wrapper를 try/except로 감싸지 말 것 — `_do_login_once` L525-527의 retriable 예외가 상위 브라우저 재기동 로직에 도달해야 한다
- 2회차 재시도는 `_looks_like_account_type_failure` 게이트를 통과할 때만 — 타임아웃·필드 미발견에서 시도를 늘리면 벤더측 계정 잠금 위험
- resolver 3중복 통합(`_resolve_toorder_id` × 3 파일), CSV mtime 캐시, DAG parse-time 상수 lazy 전환은 **이번 스코프 밖** — 손대지 말 것
- 로그·커밋 메시지·`update_log.md` 에 계정 비밀번호 기록 금지. `pw_len` 같은 간접 표현만 사용
- 작업 후 `C:\airflow\prd_codex\update_log.md` 에 날짜·대상·변경요약·검증결과·잔여위험 3~6줄 추가

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
