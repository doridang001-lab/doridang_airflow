"""
OKPOS 매출 원본파일 자동 다운로드 파이프라인

처리 흐름:
1. 실행 날짜 범위 결정 (conf 또는 yesterday)
2. today 페이지 매장별 엑셀 다운로드
3. receipt_details 페이지 매장별 엑셀 다운로드
4. 파일 이동 및 파일명 표준화 저장 (RAW_OKPOS_SALES)
5. log.parquet 실행 이력 기록
"""

import hashlib
import logging
import os
import re
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo
from zipfile import BadZipFile

import pandas as pd

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException

from modules.transform.utility.paths import ANALYTICS_DB, ONEDRIVE_DB, RAW_OKPOS_SALES, TEMP_DIR
from modules.transform.utility.selenium_uc import configure_uc_data_path

logger = logging.getLogger(__name__)


def _kst_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)

# ============================================================
# 상수
# ============================================================
OKPOS_LOGIN_URL = "https://my.okpos.co.kr/asp/login"
OKPOS_ID        = "ue60"
OKPOS_PW        = "93832"

STORES = [
    {"name": "도리당 동두천지행점", "shopCd": "RL2725"},
    {"name": "도리당 삼송점",       "shopCd": "UE6850"},
    {"name": "도리당 평택비전점",    "shopCd": "UW4935"},
    {"name": "도리당 송파삼전점",    "shopCd": "LQ9726"},
]

PAGE_TYPES = {
    "today": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/today",
        "date_input_id": "datepicker-input",
        "excel_js": "exportDetailSheet()",
    },
    "receipt": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/receipt/details",
        "date_input_id": "saleDate",
        "excel_js": "exportReceiptDetailExcel()",
    },
    # 일자별 종합매출 (실매출 기준의 공식 집계 리포트)
    "daily": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/daily",
        # daily는 기간 조회: startDate/endDate 두 입력을 각각 세팅
        "start_date_input_id": "startDate",
        "end_date_input_id": "endDate",
        # 매장 펼침(상세 펼침) 체크박스
        "shop_open_checkbox_id": "shopOpen",
        "excel_js": "exportSheet()",
    },
}

DOWNLOAD_TIMEOUT = 120
WAIT_TIMEOUT     = 30
HEADLESS_MODE    = os.getenv("AIRFLOW_HOME") is not None

# 어제/그제는 OKPOS 마감 지연으로 거짓 NO_DATA가 남기 쉬워 재확인한다.
_NO_DATA_RETRY_RECENT_DAYS = 3


# ============================================================
# 내부 유틸
# ============================================================

def _to_int_series(series: pd.Series) -> pd.Series:
    """문자열 금액 컬럼을 int 시리즈로 변환 (빈값/NaN/콤마 등 방어)."""
    try:
        s = series.fillna("").astype(str).str.replace(",", "", regex=False).str.strip()
    except Exception:
        s = series
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

def _no_data_marker_path(store_short: str, ym: str, csv_stem: str) -> Path:
    """'해당 날짜는 매출 데이터 없음'을 기록하는 마커 파일 경로.

    - csv_stem 예: okpos_order, okpos_order_item
    - 저장 위치: RAW_OKPOS_SALES/brand=도리당/store=.../ym=YYYY-MM/.no_data__{csv_stem}.txt
    """
    return (
        RAW_OKPOS_SALES
        / "brand=도리당"
        / f"store={store_short}"
        / f"ym={ym}"
        / f".no_data__{csv_stem}.txt"
    )


def _okpos_download_dir(context: dict, page_type: str) -> Path:
    """Airflow run/page별 다운로드 격리 디렉터리."""
    dag_run = context.get("dag_run") if context else None
    run_id = getattr(dag_run, "run_id", "") or str(context.get("run_id", "manual"))
    run_token = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_")[-80:] or "manual"
    return TEMP_DIR / f"okpos_download_{page_type}_{run_token}"


def _read_no_data_dates(marker_path: Path) -> set[str]:
    if not marker_path.exists() or marker_path.stat().st_size == 0:
        return set()
    out: set[str] = set()
    try:
        for line in marker_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = (line or "").strip()
            if not line:
                continue
            # 포맷: YYYY-MM-DD \t reason \t recorded_at
            out.add(line.split("\t", 1)[0].strip())
    except Exception:
        return set()
    return out


def _mark_no_data(store_short: str, ym: str, csv_stem: str, sale_date: str, reason: str) -> None:
    """sale_date를 'NO_DATA'로 마킹하여 이후 재다운로드/누락체크에서 제외되게 한다."""
    try:
        if datetime.strptime(sale_date, "%Y-%m-%d").date() >= _kst_now().date():
            logger.info("당일/미래 데이터 NO_DATA 마킹 스킵: %s | %s", sale_date, reason)
            return
    except Exception:
        pass
    marker_path = _no_data_marker_path(store_short, ym, csv_stem)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_no_data_dates(marker_path)
    if sale_date in existing:
        return
    now_ts = _kst_now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{sale_date}\t{reason}\t{now_ts}\n"
    try:
        marker_path.write_text(
            (marker_path.read_text(encoding="utf-8", errors="ignore") if marker_path.exists() else "") + line,
            encoding="utf-8",
        )
    except Exception:
        # 마커는 보조 기능이므로 실패해도 파이프라인은 진행
        logger.warning(f"NO_DATA 마커 기록 실패(무시): {marker_path}")


def _unmark_no_data(store_short: str, ym: str, csv_stem: str, sale_date: str) -> None:
    """NO_DATA 마커에서 특정 sale_date를 제거(재수집/재검증 시 사용)."""
    marker_path = _no_data_marker_path(store_short, ym, csv_stem)
    if not marker_path.exists() or marker_path.stat().st_size == 0:
        return
    try:
        lines = marker_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        kept: list[str] = []
        removed = 0
        for line in lines:
            raw = (line or "").strip()
            if not raw:
                continue
            d = raw.split("\t", 1)[0].strip()
            if d == sale_date:
                removed += 1
                continue
            kept.append(line)
        if removed:
            marker_path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
            logger.info("NO_DATA 마커 제거: %s | %s (%d줄)", marker_path, sale_date, removed)
    except Exception as e:
        logger.warning("NO_DATA 마커 제거 실패(무시): %s | %s", marker_path, e)


def _is_likely_okpos_report(df: pd.DataFrame) -> bool:
    """OKPOS 리포트로 보이는지(헤더 추정이 맞는지) 대략 판단.

    헤더가 엉뚱하면 NO/영수증번호/실매출액 같은 키 컬럼이 거의 없다.
    """
    try:
        cols = {str(c).strip() for c in df.columns}
    except Exception:
        return False
    key_cols = {
        "NO",
        "No",
        "결제시각",
        "결제시간",
        "영수증번호",
        "상품명",
        "수량",
        "총매출액",
        "실매출액",
        "가액",
        "부가세",
    }
    return len(cols & key_cols) >= 2


_STORE_OPEN_DATE_CACHE: dict[str, date] | None = None
_STORE_OPEN_DATE_MISS_LOGGED: set[str] = set()


def _load_store_open_dates() -> dict[str, date]:
    """sales_employee.csv(OneDrive Repository) 기준 매장 실오픈일 매핑을 로드한다.

    - 파일 위치: ONEDRIVE_DB / sales_employee.csv
    - 같은 매장이 플랫폼별로 여러 줄이면, 가장 빠른(최소) 실오픈일을 사용
    """
    global _STORE_OPEN_DATE_CACHE
    if _STORE_OPEN_DATE_CACHE is not None:
        return _STORE_OPEN_DATE_CACHE

    path = ONEDRIVE_DB / "sales_employee.csv"
    if not path.exists() or path.stat().st_size == 0:
        _STORE_OPEN_DATE_CACHE = {}
        logger.warning(f"sales_employee.csv 없음 → 오픈일 필터 비활성: {path}")
        return _STORE_OPEN_DATE_CACHE

    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    except Exception:
        df = pd.read_csv(path, encoding="cp949", dtype=str)

    if "매장명" not in df.columns or "실오픈일" not in df.columns:
        _STORE_OPEN_DATE_CACHE = {}
        logger.warning(f"sales_employee.csv 컬럼 부족 → 오픈일 필터 비활성: columns={list(df.columns)}")
        return _STORE_OPEN_DATE_CACHE

    df["매장명"] = df["매장명"].astype(str).str.strip()
    open_dt = pd.to_datetime(df["실오픈일"], errors="coerce").dt.date
    df = df.assign(_open=open_dt)
    df = df.dropna(subset=["_open"])

    mapping: dict[str, date] = {}
    for store_name, g in df.groupby("매장명", dropna=True):
        d = g["_open"].min()
        if pd.isna(d):
            continue
        mapping[str(store_name).strip()] = d

    _STORE_OPEN_DATE_CACHE = mapping
    logger.info(f"매장 실오픈일 로드: {len(mapping)}개 (source={path})")
    return mapping


def _store_open_date(store_name: str) -> date | None:
    store_name = (store_name or "").strip()
    if not store_name:
        return None
    mapping = _load_store_open_dates()

    # 1) exact match
    d = mapping.get(store_name)
    if d is not None:
        return d

    # 2) 브랜드 접두어 제거(예: '도리당 송파삼전점' vs '송파삼전점')
    for prefix in ("도리당", "도리당 "):
        if store_name.startswith(prefix):
            stripped = store_name[len(prefix):].strip()
            if stripped:
                d2 = mapping.get(stripped)
                if d2 is not None:
                    return d2

    # 띄어쓰기/특수문자 차이 정도만 완화
    compact = store_name.replace(" ", "")
    for k, v in mapping.items():
        kk = str(k).replace(" ", "")
        if kk == compact:
            return v
        # prefix 제거 후 비교도 허용
        for prefix in ("도리당",):
            c2 = compact
            if c2.startswith(prefix):
                c2 = c2[len(prefix):].strip()
            if kk == c2:
                return v

    if store_name not in _STORE_OPEN_DATE_MISS_LOGGED:
        _STORE_OPEN_DATE_MISS_LOGGED.add(store_name)
        logger.warning(f"실오픈일 매칭 실패(필터 미적용): {store_name}")
    return None


def _should_collect(store_name: str, sale_date: str) -> bool:
    """sale_date(YYYY-MM-DD)가 해당 매장 실오픈일보다 이전이면 수집 제외."""
    try:
        d = datetime.strptime(sale_date, "%Y-%m-%d").date()
    except Exception:
        return True
    od = _store_open_date(store_name)
    if od is None:
        return True
    return d >= od


def prune_preopen_data(**context) -> str:
    """매장 실오픈일 이전에 잘못 저장된 데이터를 정리한다.

    - 목적: 이미 적재된 pre-open 날짜가 '존재'로 인식되어 스킵되거나, 분석에 섞이는 문제 방지
    - 동작: RAW_OKPOS_SALES 하위의 okpos_{daily,order,order_item}.csv에서 sale_date < open_date 행 제거
    - 기준 날짜 범위: resolve_dates XCom sale_dates가 포함하는 ym 파티션만 스캔
    """
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    yms = sorted({sd[:7] for sd in sale_dates if isinstance(sd, str) and len(sd) >= 7})
    if not yms:
        return "preopen 정리: 대상 ym 없음 (스킵)"

    csv_stems = ["okpos_daily", "okpos_order", "okpos_order_item"]
    cleaned_files = 0
    removed_rows = 0

    for store in STORES:
        store_name = store["name"]
        od = _store_open_date(store_name)
        if od is None:
            continue
        store_short = store_name.replace("도리당 ", "", 1)
        for ym in yms:
            base = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}"
            for stem in csv_stems:
                p = base / f"{stem}.csv"
                if not p.exists() or p.stat().st_size == 0:
                    continue
                try:
                    df = pd.read_csv(p, dtype=str, encoding="utf-8-sig")
                except Exception:
                    df = pd.read_csv(p, dtype=str)
                if df.empty or "sale_date" not in df.columns:
                    continue
                s = pd.to_datetime(df["sale_date"].astype(str).str.strip(), errors="coerce").dt.date
                keep = (s.isna()) | (s >= od)
                before = len(df)
                df2 = df[keep].copy()
                dropped = before - len(df2)
                if dropped <= 0:
                    continue
                if df2.empty:
                    # 파일 자체가 pre-open만 있었다면 삭제
                    try:
                        p.unlink(missing_ok=True)
                    except Exception:
                        # 삭제 실패 시 빈 파일로 대체
                        df2.to_csv(p, index=False, encoding="utf-8-sig")
                else:
                    df2.to_csv(p, index=False, encoding="utf-8-sig")
                cleaned_files += 1
                removed_rows += dropped
    logger.warning(f"preopen 정리 완료: files={cleaned_files}, removed_rows={removed_rows}")
    return f"preopen 정리 완료: files={cleaned_files}, removed_rows={removed_rows}"


def purge_okpos_daily(**context) -> str:
    """`okpos_daily.csv`를 전부 삭제(또는 기간/ym 범위만)해서 재적재가 가능하게 만든다.

    안전장치:
    - 기본 동작은 스킵
    - dag_run.conf.purge_okpos_daily=true 또는 env OKPOS_PURGE_OKPOS_DAILY=1 일 때만 수행

    범위:
    - 기본: brand=도리당 하위 모든 store/ym의 okpos_daily.csv 삭제
    - 옵션: dag_run.conf.purge_scope='resolved_ym'이면 resolve_dates의 ym만 삭제
    """
    conf = context.get("dag_run").conf or {}
    enabled = bool(conf.get("purge_okpos_daily") or os.getenv("OKPOS_PURGE_OKPOS_DAILY", ""))
    if not enabled:
        return "purge okpos_daily: disabled (skip)"

    scope = str(conf.get("purge_scope") or os.getenv("OKPOS_PURGE_SCOPE", "") or "all").strip().lower()
    yms: set[str] = set()
    if scope == "resolved_ym":
        sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
        yms = {sd[:7] for sd in sale_dates if isinstance(sd, str) and len(sd) >= 7}

    deleted = 0
    base = RAW_OKPOS_SALES / "brand=도리당"
    if not base.exists():
        return f"purge okpos_daily: base not found ({base})"

    # store=*/ym=*/okpos_daily.csv 스캔
    for store_dir in base.glob("store=*"):
        if not store_dir.is_dir():
            continue
        for ym_dir in store_dir.glob("ym=*"):
            if not ym_dir.is_dir():
                continue
            ym = ym_dir.name.replace("ym=", "", 1)
            if yms and ym not in yms:
                continue
            p = ym_dir / "okpos_daily.csv"
            if p.exists():
                try:
                    p.unlink()
                    deleted += 1
                except Exception:
                    logger.warning(f"okpos_daily 삭제 실패(무시): {p}")

    logger.warning(f"purge okpos_daily 완료: deleted_files={deleted} scope={scope}")
    return f"purge okpos_daily 완료: deleted_files={deleted} scope={scope}"


def _get_chrome_version() -> int | None:
    import platform
    import subprocess

    # Linux/WSL
    candidates = [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["chromium-browser", "--version"],
        ["chromium", "--version"],
    ]
    for cmd in candidates:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            match = re.search(r"(\d+)\.", (result.stdout or "").strip())
            if match:
                return int(match.group(1))
        except Exception:
            continue

    # Windows: Chrome 실행/출력이 막혀도 파일 버전은 읽을 수 있음
    if platform.system() == "Windows":
        exe_candidates: list[str] = []
        env_bin = os.getenv("CHROME_BIN")
        if env_bin:
            exe_candidates.append(env_bin)
        exe_candidates += [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            str(Path.home() / r"AppData\Local\Google\Chrome\Application\chrome.exe"),
        ]

        for exe in exe_candidates:
            try:
                p = Path(exe)
                if not p.exists():
                    continue
                escaped = str(p).replace("'", "''")
                ps = f"(Get-Item '{escaped}').VersionInfo.ProductVersion"
                result = subprocess.run(
                    ["powershell", "-NoProfile", "-Command", ps],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                match = re.search(r"(\d+)\.", (result.stdout or "").strip())
                if match:
                    return int(match.group(1))
            except Exception:
                continue

    return None


def _launch_browser(download_dir: Path) -> uc.Chrome:
    """다운로드 경로가 설정된 Chrome 브라우저 실행"""
    download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"브라우저 실행 (headless={HEADLESS_MODE}, download_dir={download_dir})")
    configure_uc_data_path()

    def _make_options() -> uc.ChromeOptions:
        import platform
        options = uc.ChromeOptions()
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        if platform.system() == "Windows":
            win_default = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
            if chrome_bin == "/usr/bin/google-chrome" and Path(win_default).exists():
                chrome_bin = win_default
        if chrome_bin and Path(chrome_bin).exists():
            options.binary_location = chrome_bin
        if HEADLESS_MODE:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--renderer-process-limit=1")
        options.add_argument("--disable-features=site-per-process")
        options.add_argument("--disk-cache-size=1")
        options.add_argument("--media-cache-size=1")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-ipc-flooding-protection")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--window-size=1920,1080")
        if platform.system() == "Windows":
            options.add_argument("--disable-crash-reporter")
            options.add_argument("--disable-features=Crashpad")
        return options

    # 동시 실행 시 chromedriver 패치 충돌 방지: 인스턴스별 고유 복사본 사용
    import tempfile
    sys_driver = os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")
    unique_driver_path = None
    if Path(sys_driver).exists():
        tmp_fd, unique_driver_path = tempfile.mkstemp(prefix="uc_okpos_")
        os.close(tmp_fd)
        shutil.copy2(sys_driver, unique_driver_path)
        os.chmod(unique_driver_path, 0o755)
        logger.info(f"chromedriver 복사본 생성: {unique_driver_path}")

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        try:
            import platform

            if platform.system() == "Windows":
                kwargs["use_subprocess"] = True
        except Exception:
            pass
        if chrome_version:
            kwargs["version_main"] = chrome_version
        if unique_driver_path:
            kwargs["driver_executable_path"] = unique_driver_path
        driver = uc.Chrome(**kwargs)
        driver.set_window_size(1920, 1080)
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        match = re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning(f"버전 불일치 → {detected} 으로 재시도")
            kwargs2 = {"options": _make_options(), "version_main": detected}
            try:
                import platform

                if platform.system() == "Windows":
                    kwargs2["use_subprocess"] = True
            except Exception:
                pass
            if unique_driver_path:
                kwargs2["driver_executable_path"] = unique_driver_path
            driver = uc.Chrome(**kwargs2)
            driver.set_window_size(1920, 1080)
            return driver
        raise


def _setup_download_dir(driver: uc.Chrome, download_dir: Path) -> None:
    """CDP로 다운로드 경로 설정 (첫 내비게이션 후 호출)"""
    if HEADLESS_MODE:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(download_dir),
            "eventsEnabled": True,
        })


_BROWSER_RETRY_MAX = 5
_BROWSER_RETRY_BASE_WAIT = 5  # seconds; actual waits: 5, 15, 45, ... (×3 per attempt)


def _is_transient_connection_error(exc: Exception) -> bool:
    """WebDriver 연결 끊김 계열 일시적 오류 여부 판정"""
    from http.client import RemoteDisconnected
    try:
        from urllib3.exceptions import ProtocolError, MaxRetryError, NewConnectionError
        if isinstance(exc, (ProtocolError, MaxRetryError, NewConnectionError)):
            return True
    except ImportError:
        pass
    if isinstance(exc, RemoteDisconnected):
        return True
    if isinstance(exc, WebDriverException):
        msg = str(exc).lower()
        return any(
            k in msg
            for k in (
                "browser session died",
                "browser session is not alive",
                "chrome not reachable",
                "target window",
                "no such window",
                "connection aborted",
                "remotedisconnected",
                "connection refused",
                "failed to establish",
                "max retries exceeded",
                "invalid session id",
            )
        )
    # urllib3 ProtocolError이 WebDriverException 안에 체인된 경우
    cause = getattr(exc, "__cause__", None) or getattr(exc, "__context__", None)
    if cause is not None and _is_transient_connection_error(cause):
        return True
    return False


def _is_driver_alive(driver: uc.Chrome) -> bool:
    """WebDriver 세션 유효성 확인"""
    try:
        driver.execute_script("return 1")
        return True
    except Exception:
        return False


def _filter_missing_keys(sale_dates: list, csv_name: str) -> list:
    """sale_dates × STORES 중 csv_name.csv에 해당 날짜 데이터가 없는 (date, store) 조합만 반환."""
    missing = []
    marker_cache: dict[Path, set[str]] = {}
    for sale_date in sale_dates:
        ym = sale_date[:7]
        for store in STORES:
            if not _should_collect(store["name"], sale_date):
                continue
            store_short = store["name"].replace("도리당 ", "", 1)
            csv_path = (RAW_OKPOS_SALES / "brand=도리당"
                        / f"store={store_short}" / f"ym={ym}" / f"{csv_name}.csv")
            marker_path = _no_data_marker_path(store_short, ym, csv_name)
            if marker_path not in marker_cache:
                marker_cache[marker_path] = _read_no_data_dates(marker_path)
            if sale_date in marker_cache[marker_path]:
                try:
                    sale_day = datetime.strptime(sale_date, "%Y-%m-%d").date()
                    is_recent = (_kst_now().date() - sale_day).days <= _NO_DATA_RETRY_RECENT_DAYS
                except Exception:
                    is_recent = False
                if not is_recent:
                    continue
            if not csv_path.exists():
                missing.append((sale_date, store))
                continue
            try:
                df = pd.read_csv(csv_path, dtype=str, usecols=["sale_date"])
                if df[df["sale_date"].str.strip() == sale_date].empty:
                    missing.append((sale_date, store))
            except Exception:
                missing.append((sale_date, store))
    return missing


def _download_with_retry(
    page_type: str,
    sale_dates: list,
    download_dir: Path,
    session_fn: Callable,
    current_key: list | None = None,
) -> dict:
    """
    브라우저 세션 기반 다운로드를 최대 max_attempts 회 재시도.

    - 연결 끊김(RemoteDisconnected / ProtocolError) 계열만 재시도
    - 성공한 key는 results에 기록 → 재시도 시 스킵
    - per-key 크래시 횟수 추적 → MAX_CRASH_PER_KEY 초과 시 NO_DATA 처리
    - max_attempts = max(_BROWSER_RETRY_MAX, pending수 + buffer) → 대용량 백필 대응
    - session_fn(driver, wait, pending_keys, results) → None (results를 in-place 갱신)
    """
    # sale_dates가 이미 (date, store) 튜플 목록이면 그대로 사용 (pre-filtered)
    if sale_dates and isinstance(sale_dates[0], tuple):
        all_keys = sale_dates
    else:
        all_keys = [
            (sale_date, store)
            for sale_date in sale_dates
            for store in STORES
        ]
    results: dict = {}
    crash_counts: dict[str, int] = {}
    MAX_CRASH_PER_KEY = int(os.getenv("OKPOS_MAX_CRASHES_PER_KEY", "3"))
    max_attempts = max(_BROWSER_RETRY_MAX, len(all_keys) + _BROWSER_RETRY_MAX)

    for attempt in range(1, max_attempts + 1):
        # 크래시 한도 초과 key → 이번 세션에서 스킵 (results에 넣지 않음 → 미수집으로 남아 RuntimeError → Airflow retry)
        crash_skip = {key for key, cnt in crash_counts.items() if cnt >= MAX_CRASH_PER_KEY}
        if crash_skip:
            logger.warning(
                f"[{page_type}] Chrome 크래시 {MAX_CRASH_PER_KEY}회 초과 key {len(crash_skip)}건 → "
                f"이번 세션 스킵 (Airflow retry 대상): {', '.join(sorted(crash_skip))}"
            )

        pending = [(sd, st) for sd, st in all_keys
                   if f"{sd}__{st['name']}" not in results
                   and f"{sd}__{st['name']}" not in crash_skip]
        if not pending:
            break

        driver = _launch_browser(download_dir)
        time.sleep(3)  # Chrome 초기화 안정화 대기

        # 브라우저가 즉시 죽은 경우 재시도
        if not _is_driver_alive(driver):
            logger.warning(
                f"[{page_type}] 브라우저 실행 직후 세션 죽음 "
                f"(attempt {attempt}/{max_attempts}) → 재기동"
            )
            try:
                driver.quit()
            except Exception:
                pass
            if attempt < max_attempts:
                time.sleep(5)
                continue
            raise RuntimeError(
                f"[{page_type}] 브라우저가 계속 즉시 종료됩니다. "
                f"최대 재시도({max_attempts}) 초과."
            )

        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        try:
            session_fn(driver, wait, pending, results)
        except Exception as exc:
            if _is_transient_connection_error(exc):
                # 현재 처리 중이던 key의 크래시 카운트 증가
                if current_key is not None and current_key[0]:
                    k = current_key[0]
                    crash_counts[k] = crash_counts.get(k, 0) + 1
                    logger.warning(
                        f"[{page_type}] {k} 크래시 누적 "
                        f"{crash_counts[k]}/{MAX_CRASH_PER_KEY}"
                    )
                    current_key[0] = None

                if attempt < max_attempts:
                    wait_sec = min(_BROWSER_RETRY_BASE_WAIT * (3 ** (min(attempt, 4) - 1)), 45)
                    logger.warning(
                        f"[{page_type}] WebDriver 연결 끊김 "
                        f"(attempt {attempt}/{max_attempts}) → {wait_sec}초 후 재시도: {exc}"
                    )
                    time.sleep(wait_sec)
                else:
                    logger.error(
                        f"[{page_type}] 최대 재시도 횟수({max_attempts}) 초과. 나머지는 미수집 처리."
                    )
                    break
            else:
                raise
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    # 끝까지 다운로드 못 한 key는 빈 문자열로 채움
    for sd, st in all_keys:
        results.setdefault(f"{sd}__{st['name']}", "")

    return results


def _login(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """OKPOS 로그인"""
    driver.get(OKPOS_LOGIN_URL)
    time.sleep(3)

    # ID 입력 필드 (여러 셀렉터 시도)
    _ID_SELECTORS = [
        (By.ID, "userId"),
        (By.NAME, "userId"),
        (By.CSS_SELECTOR, "input[type='text']"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'아이디')]"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'ID')]"),
    ]
    id_input = None
    for sel in _ID_SELECTORS:
        try:
            id_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            logger.info(f"ID 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if id_input is None:
        inputs = [(el.get_attribute("id"), el.get_attribute("name"), el.get_attribute("type"))
                  for el in driver.find_elements(By.TAG_NAME, "input")]
        logger.error(f"ID 입력 필드를 찾을 수 없음 | 현재 URL: {driver.current_url} | inputs: {inputs}")
        raise TimeoutException("OKPOS 로그인 ID 입력 필드를 찾을 수 없습니다.")

    id_input.clear()
    id_input.send_keys(OKPOS_ID)

    # PW 입력 필드 (여러 셀렉터 시도, wait 사용)
    _PW_SELECTORS = [
        (By.ID, "userPw"),
        (By.NAME, "userPw"),
        (By.ID, "password"),
        (By.NAME, "password"),
        (By.CSS_SELECTOR, "input[type='password']"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'비밀번호')]"),
        (By.XPATH, "//input[@placeholder and contains(@placeholder,'PW')]"),
    ]
    pw_input = None
    for sel in _PW_SELECTORS:
        try:
            pw_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            logger.info(f"PW 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if pw_input is None:
        inputs = [(el.get_attribute("id"), el.get_attribute("name"), el.get_attribute("type"))
                  for el in driver.find_elements(By.TAG_NAME, "input")]
        logger.error(f"PW 입력 필드를 찾을 수 없음 | inputs: {inputs}")
        raise TimeoutException("OKPOS 로그인 PW 입력 필드를 찾을 수 없습니다.")

    pw_input.clear()
    pw_input.send_keys(OKPOS_PW)
    pw_input.send_keys(Keys.RETURN)

    wait.until(lambda d: "/login" not in d.current_url)
    time.sleep(2)
    logger.info(f"OKPOS 로그인 완료 | URL: {driver.current_url}")


def _date_to_kst_ms(sale_date: str) -> int:
    """YYYY-MM-DD → KST 00:00:00 기준 Unix timestamp (ms)"""
    dt = datetime.strptime(sale_date, "%Y-%m-%d")
    kst_offset = 9 * 3600
    epoch = datetime(1970, 1, 1)
    ts_utc = (dt - epoch).total_seconds() - kst_offset
    return int(ts_utc * 1000)


def _select_date_tui(driver: uc.Chrome, wait: WebDriverWait, sale_date: str, date_input_id: str) -> None:
    """OKPOS 날짜 입력 필드에 날짜 설정 (다양한 방식 순서대로 시도)"""
    # 날짜 형식 준비
    year, month, day = sale_date.split("-")
    date_slash = f"{year}/{month}/{day}"    # YYYY/MM/DD
    date_dot   = f"{year}.{month}.{day}"    # YYYY.MM.DD
    date_short = f"{year}-{month}-{day}"    # YYYY-MM-DD

    # 입력 필드 찾기 (ID 또는 NAME)
    input_el = None
    for sel in [
        (By.ID, date_input_id),
        (By.NAME, date_input_id),
        (By.CSS_SELECTOR, f"[id='{date_input_id}']"),
    ]:
        try:
            input_el = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if input_el is None:
        raise TimeoutException(f"날짜 입력 필드를 찾을 수 없습니다: id={date_input_id}")

    # 현재 값 및 타입 로깅
    el_type  = input_el.get_attribute("type") or ""
    el_value = input_el.get_attribute("value") or ""
    logger.info(f"날짜 입력 필드 발견: id={date_input_id}, type={el_type}, current_value={el_value}")

    # ── 전략 1: JS로 직접 value 설정 + change/input 이벤트 발생 ──────────
    for fmt in [date_slash, date_dot, date_short]:
        try:
            driver.execute_script(
                "arguments[0].value = arguments[1];"
                "arguments[0].dispatchEvent(new Event('input',  {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                input_el, fmt
            )
            time.sleep(0.3)
            new_val = input_el.get_attribute("value") or ""
            if new_val and new_val != el_value:
                logger.info(f"날짜 설정 완료 (JS setValue): {fmt} → value={new_val}")
                return
        except Exception:
            continue

    # ── 전략 2: 클릭 → 키보드 입력 ──────────────────────────────────────
    try:
        driver.execute_script("arguments[0].click();", input_el)
        time.sleep(0.5)
        _debug_screenshot(driver, f"after_click_{date_input_id}")

        # 달력 팝업 여부 확인
        calendar_visible = False
        for cal_sel in [
            ".tui-datepicker",
            ".datepicker",
            "[class*='calendar']",
            "[class*='datepicker']",
        ]:
            els = driver.find_elements(By.CSS_SELECTOR, cal_sel)
            if any(e.is_displayed() for e in els):
                calendar_visible = True
                logger.info(f"달력 팝업 감지: {cal_sel}")
                break

        if calendar_visible:
            # data-timestamp 방식 (KST 기준, UTC 기준 두 가지 모두 시도)
            for ts_ms in [_date_to_kst_ms(sale_date), int(datetime.strptime(sale_date, "%Y-%m-%d").timestamp() * 1000)]:
                cells = driver.find_elements(By.XPATH, f"//td[@data-timestamp='{ts_ms}']")
                visible = [c for c in cells if c.is_displayed()]
                if visible:
                    driver.execute_script("arguments[0].click();", visible[0])
                    time.sleep(0.5)
                    logger.info(f"날짜 선택 완료 (data-timestamp={ts_ms}): {sale_date}")
                    return

            # 텍스트 기반 일(day) 셀 클릭
            day_str = str(int(day))
            for td_sel in ["td.tui-calendar-day", "td.day", "td[class*='day']", "table td"]:
                tds = driver.find_elements(By.CSS_SELECTOR, td_sel)
                for td in tds:
                    if td.text.strip() == day_str and td.is_displayed():
                        driver.execute_script("arguments[0].click();", td)
                        time.sleep(0.5)
                        logger.info(f"날짜 선택 완료 (텍스트 셀): {sale_date}")
                        return

            # 달력에서 찾는 셀 전체 로깅
            all_tds = [(td.text.strip(), td.get_attribute("data-timestamp"), td.is_displayed())
                       for td in driver.find_elements(By.CSS_SELECTOR, "td") if td.text.strip().isdigit()]
            logger.warning(f"달력 td 목록: {all_tds[:30]}")

        # 달력 없거나 실패 → 직접 키보드 입력
        input_el.click()
        input_el.send_keys(Keys.CONTROL + "a")
        input_el.send_keys(Keys.DELETE)
        for fmt in [date_slash, date_short]:
            input_el.clear()
            input_el.send_keys(fmt)
            input_el.send_keys(Keys.TAB)
            time.sleep(0.3)
            new_val = input_el.get_attribute("value") or ""
            if new_val and new_val != el_value:
                logger.info(f"날짜 설정 완료 (keyboard): {fmt} → value={new_val}")
                return
    except Exception as e:
        logger.warning(f"날짜 선택 전략 2 실패: {e}")

    raise TimeoutException(f"날짜 셀을 찾을 수 없습니다: {sale_date}")


def _dismiss_alert(driver: uc.Chrome) -> str | None:
    """열려있는 alert를 로깅 후 dismiss. 없으면 None 반환."""
    from selenium.common.exceptions import NoAlertPresentException, UnexpectedAlertPresentException
    try:
        alert = driver.switch_to.alert
        text = alert.text
        logger.warning(f"Alert 감지 → dismiss: {text!r}")
        alert.accept()
        time.sleep(0.3)
        return text
    except NoAlertPresentException:
        return None
    except UnexpectedAlertPresentException:
        try:
            alert = driver.switch_to.alert
            text = alert.text
            logger.warning(f"UnexpectedAlert → dismiss: {text!r}")
            alert.accept()
            time.sleep(0.3)
            return text
        except Exception:
            return None
    except WebDriverException as e:
        # 드라이버 세션 종료/연결 오류 등은 "alert 없음"으로 취급 (재시도 로직이 처리)
        if _is_transient_connection_error(e):
            logger.warning(f"Alert dismiss 중 WebDriver 연결 오류(무시): {e}")
            return None
        logger.warning(f"Alert dismiss 중 WebDriver 예외(무시): {e}")
        return None
    except Exception as e:
        logger.warning(f"Alert dismiss 중 알 수 없는 예외(무시): {e}")
        return None


def _select_store(driver: uc.Chrome, wait: WebDriverWait, shopCd: str) -> bool:
    """IBSheet 팝업에서 매장 선택. 성공 시 True."""
    from selenium.webdriver.common.action_chains import ActionChains
    _dismiss_alert(driver)

    store_name = shopCd
    for st in STORES:
        if st.get("shopCd") == shopCd:
            store_name = st.get("name", store_name).replace("도리당 ", "", 1)
            break

    def _force_set_store_fields() -> None:
        # IBSheet 팝업이 깨지거나 row 탐색 실패했을 때 강제 주입 fallback.
        setters = [
            ("shopCd", shopCd),
            ("source_SHOP_CD", shopCd),
            ("source_Hd_SHOP_CD", shopCd),
            ("source_HD_SHOP_CD", shopCd),
            ("shopNms", store_name),
            ("source_SHOP_NM", store_name),
            ("source_Hd_SHOP_NM", store_name),
            ("source_HD_SHOP_NM", store_name),
            ("source_SHOP_NAME", store_name),
            ("shopNm", store_name),
        ]
        script = """
            const el = arguments[0];
            const val = arguments[1];
            if (!el) return;
            el.value = val;
            el.dispatchEvent(new Event('change', { bubbles: true }));
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('blur', { bubbles: true }));
        """
        for _id, value in setters:
            for sel_name in ("ID", "NAME"):
                if sel_name == "ID":
                    els = driver.find_elements(By.ID, _id)
                else:
                    els = driver.find_elements(By.NAME, _id)
                for e in els:
                    try:
                        driver.execute_script(script, e, value)
                        logger.info(f"매장 강제 세팅: {_id}={value}")
                    except Exception:
                        continue
        # 매장 검색 버튼 입력 상태 강제 반영.
        driver.execute_script("if(document.activeElement && document.activeElement.blur){document.activeElement.blur();} return true;")

    # ── 팝업 열기 ─────────────────────────────────────────────────────────
    shop_btn = None
    for sel in [
        (By.ID, "shopNms"),
        (By.NAME, "shopNms"),
        (By.CSS_SELECTOR, "input[id*='shop']"),
        (By.CSS_SELECTOR, "button[id*='shop']"),
        (By.CSS_SELECTOR, "a[id*='shop']"),
        # receipt/details 페이지: 별도 "매장선택" 버튼
        (By.XPATH, "//button[normalize-space(text())='매장선택']"),
        (By.CSS_SELECTOR, "button[onclick*='Popup']"),
    ]:
        try:
            shop_btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(sel))
            logger.info(f"매장 입력 필드 발견: {sel}")
            break
        except TimeoutException:
            continue
    if shop_btn is None:
        logger.error(f"매장 선택 버튼을 찾을 수 없음: shopCd={shopCd}")
        return False

    ActionChains(driver).move_to_element(shop_btn).click().perform()
    time.sleep(2)
    _dismiss_alert(driver)
    _force_set_store_fields()

    # ── 팝업 내 행 찾기 ───────────────────────────────────────────────────
    # 우선순위: shopCd를 포함한 TD 자체 → 그 TD가 속한 TR의 마지막 TD(매장명)
    target_el = None
    target_desc = ""

    # 1순위: shopCd TD 안에 <a> 링크가 있으면 그것 클릭
    for xpath in [
        f"//td[normalize-space(text())='{shopCd}']/a",
        f"//td[normalize-space(text())='{shopCd}']",
        f"//td[contains(text(),'{shopCd}')]/a",
        f"//td[contains(text(),'{shopCd}')]",
        # TR의 마지막 TD (매장명 컬럼) 클릭
        f"//tr[td[normalize-space(text())='{shopCd}']]/td[last()]",
        f"//tr[td[contains(text(),'{shopCd}')]]/td[last()]",
    ]:
        try:
            el = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, xpath)))
            if el.is_displayed():
                target_el = el
                target_desc = xpath
                logger.info(f"클릭 대상 발견: {xpath}")
                break
        except TimeoutException:
            continue

    if target_el is None:
        all_tds = [td.text.strip() for td in driver.find_elements(By.CSS_SELECTOR, "td") if td.text.strip()]
        logger.error(f"shopCd={shopCd} 셀 없음 | td 목록: {all_tds[:30]}")
        _force_set_store_fields()
        return True

    # ── 팝업 닫힘 감지용: "매장 검색" 텍스트를 포함한 컨테이너 ─────────────
    popup_text_el = None
    for xpath in [
        "//*[normalize-space(text())='매장 검색']",
        "//*[contains(text(),'매장 검색')]",
        "//*[contains(text(),'매장검색')]",
    ]:
        found = driver.find_elements(By.XPATH, xpath)
        visible = [e for e in found if e.is_displayed()]
        if visible:
            popup_text_el = visible[0]
            break

    # ── 클릭 전략: ActionChains → 더블클릭 → JS 순으로 시도 ─────────────
    def _try_click(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.3)
        try:
            ActionChains(driver).move_to_element(el).click().perform()
            return "ActionChains.click"
        except Exception:
            pass
        try:
            ActionChains(driver).move_to_element(el).double_click().perform()
            return "ActionChains.double_click"
        except Exception:
            pass
        driver.execute_script("arguments[0].click();", el)
        return "JS.click"

    method = _try_click(target_el)
    logger.info(f"클릭 실행: {method} | 대상={target_desc}")
    time.sleep(1)
    _dismiss_alert(driver)

    # ── 팝업 닫힘 확인 ───────────────────────────────────────────────────
    closed = False
    if popup_text_el is not None:
        try:
            WebDriverWait(driver, 6).until(EC.staleness_of(popup_text_el))
            closed = True
            logger.info(f"팝업 닫힘 확인 (staleness): shopCd={shopCd}")
        except TimeoutException:
            pass

    if not closed:
        # 매장선택 input에 값이 채워졌는지 확인
        try:
            for sel in [(By.ID, "shopNms"), (By.NAME, "shopNms")]:
                els = driver.find_elements(*sel)
                for e in els:
                    val = e.get_attribute("value") or ""
                    if val.strip():
                        closed = True
                        logger.info(f"매장 선택 input 확인: value={val!r}")
                        break
        except Exception:
            pass

    if not closed:
        logger.warning(f"팝업 닫힘 미확인 — 현재 URL: {driver.current_url}")

    logger.info(f"매장 선택: shopCd={shopCd} | closed={closed} | method={method}")
    return True  # closed 여부와 무관하게 True (조회 후 alert로 확인)


def _wait_for_download(directory: Path, existing_files: set, timeout: int = DOWNLOAD_TIMEOUT) -> Path | None:
    """새로 다운로드된 파일 대기 (기존 파일 제외)"""
    end_time = time.time() + timeout

    def _is_partial(p: Path) -> bool:
        return p.suffix.lower() in {".crdownload", ".tmp", ".part"}

    # 다운로드 완료(파일 생성) 후에도 짧은 시간 동안 size가 변하는 경우가 있어 안정화까지 기다린다.
    stable_window_sec = float(os.getenv("OKPOS_DOWNLOAD_STABLE_WINDOW_SEC", "1.5"))
    stable_poll_sec = 0.5
    last_sizes: dict[Path, tuple[int, float]] = {}

    # 파일/partial이 no_activity_sec 내 하나도 나타나지 않으면 조기 종료.
    # 데이터가 없는 날짜/매장은 JS 클릭 후 아무것도 다운로드되지 않으므로
    # 전체 timeout을 기다리지 않고 빠르게 "데이터 없음"을 감지한다.
    no_activity_sec = float(os.getenv("OKPOS_DOWNLOAD_NO_ACTIVITY_SEC", "30"))
    no_activity_deadline = time.time() + no_activity_sec
    first_activity_seen = False

    while time.time() < end_time:
        try:
            current = {p for p in directory.iterdir() if p.is_file() and not _is_partial(p)}
        except FileNotFoundError:
            current = set()

        new_files = current - existing_files
        partials = list(directory.glob("*.crdownload")) + list(directory.glob("*.part")) + list(directory.glob("*.tmp"))

        if not first_activity_seen:
            if new_files or partials:
                first_activity_seen = True
            elif time.time() > no_activity_deadline:
                return None

        if new_files:
            latest = sorted(new_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
            try:
                size = latest.stat().st_size
            except FileNotFoundError:
                time.sleep(stable_poll_sec)
                continue

            prev = last_sizes.get(latest)
            now = time.time()
            if prev is not None and prev[0] == size and (now - prev[1]) >= stable_window_sec and not partials:
                return latest
            # size가 변했거나(혹은 최초 관측) 아직 partial 파일이 남아있으면 계속 대기
            last_sizes[latest] = (size, prev[1] if (prev and prev[0] == size) else now)

        time.sleep(stable_poll_sec)
    return None


def _wait_for_download(
    directory: Path,
    existing_files: set,
    timeout: int = DOWNLOAD_TIMEOUT,
    expected_suffixes: set[str] | None = None,
    filename_predicate: Callable[[Path], bool] | None = None,
) -> Path | None:
    """Filtered download waiter used by sales/product flows."""
    end_time = time.time() + timeout
    expected_suffixes_normalized = {s.lower() for s in expected_suffixes} if expected_suffixes else None
    blocked_suffixes = {".html", ".htm", ".png", ".jpg", ".jpeg", ".svg", ".webp"}
    blocked_names = {"downloads.html", "download.html"}

    def _is_partial(p: Path) -> bool:
        return p.suffix.lower() in {".crdownload", ".tmp", ".part"}

    def _is_candidate(p: Path) -> bool:
        suffix = p.suffix.lower()
        if _is_partial(p):
            return False
        if p.name.lower() in blocked_names or suffix in blocked_suffixes:
            return False
        if p.name.startswith("debug__"):
            return False
        if expected_suffixes_normalized and suffix not in expected_suffixes_normalized:
            return False
        if filename_predicate and not filename_predicate(p):
            return False
        return True

    existing_meta: dict[Path, tuple[float, int]] = {}
    for p in existing_files:
        try:
            if p.is_file() and _is_candidate(p):
                st = p.stat()
                existing_meta[p] = (st.st_mtime, st.st_size)
        except FileNotFoundError:
            continue

    stable_window_sec = float(os.getenv("OKPOS_DOWNLOAD_STABLE_WINDOW_SEC", "1.5"))
    stable_poll_sec = 0.5
    last_sizes: dict[Path, tuple[int, float]] = {}
    no_activity_sec = float(os.getenv("OKPOS_DOWNLOAD_NO_ACTIVITY_SEC", "30"))
    no_activity_deadline = time.time() + no_activity_sec
    first_activity_seen = False

    while time.time() < end_time:
        try:
            current_all = {p for p in directory.iterdir() if p.is_file()}
        except FileNotFoundError:
            current_all = set()

        current = {p for p in current_all if _is_candidate(p)}
        new_any_files = current_all - existing_files
        new_files = current - existing_files
        changed_files = set()
        for p in current & existing_files:
            baseline = existing_meta.get(p)
            if baseline is None:
                continue
            try:
                st = p.stat()
            except FileNotFoundError:
                continue
            if (st.st_mtime, st.st_size) != baseline:
                changed_files.add(p)
        candidate_files = new_files | changed_files
        partials = list(directory.glob("*.crdownload")) + list(directory.glob("*.part")) + list(directory.glob("*.tmp"))

        if not first_activity_seen:
            if new_any_files or changed_files or partials:
                first_activity_seen = True
            elif time.time() > no_activity_deadline:
                return None

        if candidate_files:
            latest = sorted(candidate_files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
            try:
                size = latest.stat().st_size
            except FileNotFoundError:
                time.sleep(stable_poll_sec)
                continue

            prev = last_sizes.get(latest)
            now = time.time()
            if prev is not None and prev[0] == size and (now - prev[1]) >= stable_window_sec and not partials:
                return latest
            last_sizes[latest] = (size, prev[1] if (prev and prev[0] == size) else now)

        time.sleep(stable_poll_sec)
    return None


def _click_search_btn(driver: uc.Chrome, wait: WebDriverWait) -> None:
    """조회 버튼 클릭.

    IBSheet 기반 OKPOS 페이지에서 XPATH text()/contains(@onclick,...) 스캔은
    전체 DOM 순회를 유발해 Chrome OOM을 일으킨다.
    → CSS/ID 셀렉터(빠름) 먼저, 실패 시 JS querySelectorAll(안전) 로 클릭.
    → 세션 연결 오류는 즉시 re-raise (브라우저 재생성 로직에 위임).
    """
    # 1단계: CSS/ID 셀렉터 (DOM 전체 스캔 없음, 빠름)
    _FAST_SELECTORS = [
        (By.ID, "searchBtn"),
        (By.CSS_SELECTOR, "button.btn-search"),
        (By.CSS_SELECTOR, "a.btn-search"),
        (By.CSS_SELECTOR, "input[type='button'][value='조회'], input[type='button'][value='검색']"),
    ]
    for sel in _FAST_SELECTORS:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(sel))
            logger.info(f"조회 버튼 발견: {sel}")
            driver.execute_script("arguments[0].click();", btn)
            return
        except Exception as _e:
            if _is_transient_connection_error(_e):
                raise
            continue

    # 2단계: JS querySelectorAll (브라우저 내부 실행 → Chrome 안전, 빠름)
    try:
        clicked = driver.execute_script("""
            var els = Array.from(
                document.querySelectorAll('button, a, input[type=button], input[type=submit]')
            );
            var btn = els.find(function(b) {
                var t = (b.textContent || b.value || '').trim();
                return t === '조회' || t === '검색' || t.indexOf('조회') === 0;
            });
            if (btn) { btn.click(); return true; }
            return false;
        """)
        if clicked:
            logger.info("조회 버튼 클릭 성공 (JS)")
            return
        logger.error("조회 버튼을 찾을 수 없음 (JS에서도 미발견)")
    except Exception as _js_err:
        if _is_transient_connection_error(_js_err):
            raise
        logger.error(f"조회 버튼 JS 탐색 실패: {_js_err}")

    raise TimeoutException("조회 버튼을 찾을 수 없습니다.")


def _download_excel_for_store(
    driver: uc.Chrome,
    wait: WebDriverWait,
    page_type_key: str,
    page_cfg: dict,
    sale_date: str,
    store: dict,
    download_dir: Path,
) -> Path | None:
    """단일 매장 엑셀 다운로드 시퀀스 (매장마다 페이지 재로드)"""
    store_name = store["name"]
    shop_cd    = store["shopCd"]
    store_slug = store_name.replace(" ", "_")
    tag        = f"[{page_type_key}|{store_name}|{sale_date}]"

    # store-level retry: (날짜, 매장) 단위로 다운로드가 누락되는 경우를 대비
    store_retry_max = int(os.getenv("OKPOS_STORE_RETRY_MAX", "2"))
    store_retry_wait = float(os.getenv("OKPOS_STORE_RETRY_WAIT", "2.5"))

    for store_attempt in range(1, store_retry_max + 1):
        attempt_tag = f"{tag}[try {store_attempt}/{store_retry_max}]"
        try:
            if not _is_driver_alive(driver):
                raise WebDriverException("browser session is not alive before store retry")

            # ── 0. 매장마다 페이지 재로드 (IBSheet 중복 방지) ──────────────────
            logger.info(f"{attempt_tag} 페이지 재로드: {page_cfg['url']}")
            driver.get(page_cfg["url"])
            time.sleep(2)
            _dismiss_alert(driver)

            # ── 1. 날짜 선택 ─────────────────────────────────────────────────
            logger.info(f"{attempt_tag} 날짜 선택 시작")
            # daily는 기간 조회(start/end)로 동작
            if "start_date_input_id" in page_cfg and "end_date_input_id" in page_cfg:
                _select_date_tui(driver, wait, sale_date, page_cfg["start_date_input_id"])
                _dismiss_alert(driver)
                _select_date_tui(driver, wait, sale_date, page_cfg["end_date_input_id"])
            else:
                _select_date_tui(driver, wait, sale_date, page_cfg["date_input_id"])
            _dismiss_alert(driver)
            logger.info(f"{attempt_tag} 날짜 선택 완료")

            # ── 2. 매장 선택 ─────────────────────────────────────────────────
            logger.info(f"{attempt_tag} 매장 선택 시작: shopCd={shop_cd}")
            ok = _select_store(driver, wait, shop_cd)
            if not ok:
                logger.error(f"{attempt_tag} 매장 선택 실패 → skip")
                return None
            logger.info(f"{attempt_tag} 매장 선택 완료")

            # ── 2.5 매장 펼침(옵션) ──────────────────────────────────────────
            if "shop_open_checkbox_id" in page_cfg:
                try:
                    cb = driver.find_element(By.ID, page_cfg["shop_open_checkbox_id"])
                    checked = driver.execute_script("return arguments[0].checked === true;", cb)
                    if checked:
                        driver.execute_script("arguments[0].click();", cb)
                        time.sleep(0.2)
                except Exception:
                    # 없어도 진행
                    pass

            # ── 3. 조회 버튼 클릭 ────────────────────────────────────────────
            logger.info(f"{attempt_tag} 조회 버튼 클릭")
            _dismiss_alert(driver)
            _click_search_btn(driver, wait)
            time.sleep(2)
            alert = _dismiss_alert(driver)
            if alert:
                logger.error(f"{attempt_tag} 조회 후 alert 발생: {alert!r}")
                # "매장을 선택" alert면 팝업이 열려있으므로 닫기 시도
                if "매장" in alert:
                    logger.info(f"{attempt_tag} 팝업 닫기 후 재시도")
                    for close_xpath in [
                        "//button[@class='modal-close' or contains(@class,'close')]",
                        "//*[contains(@class,'modal')]//button[contains(@class,'close')]",
                        "//div[contains(text(),'매장 검색')]/following-sibling::button",
                        "//*[@aria-label='close' or @title='close' or @title='닫기']",
                    ]:
                        try:
                            btn = driver.find_element(By.XPATH, close_xpath)
                            if btn.is_displayed():
                                driver.execute_script("arguments[0].click();", btn)
                                time.sleep(0.5)
                                break
                        except Exception:
                            continue
                    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                    time.sleep(0.5)
                raise TimeoutException(f"조회 alert: {alert}")
            logger.info(f"{attempt_tag} 조회 완료")

            # ── 4. 엑셀 다운로드 ─────────────────────────────────────────────
            # partial(.crdownload/.tmp/.part) 제외한 모든 파일을 existing에 포함.
            # 특정 확장자만 나열하면 .png 등 예상 외 포맷이 누락되어 이전 매장의
            # 파일을 다음 매장 다운로드 결과로 오인하는 버그가 발생한다.
            existing = {
                p for p in download_dir.iterdir()
                if p.is_file() and p.suffix.lower() not in {".crdownload", ".tmp", ".part"}
            }
            logger.info(f"{attempt_tag} 엑셀 다운로드 JS 실행: {page_cfg['excel_js']}")
            _dismiss_alert(driver)
            driver.execute_script(page_cfg["excel_js"])
            time.sleep(1)
            _dismiss_alert(driver)

            # ── 5. 다운로드 완료 대기 ─────────────────────────────────────────
            downloaded = _wait_for_download(download_dir, existing)
            if downloaded is None:
                raise TimeoutException("다운로드 타임아웃")

            # 즉시 rename — 다음 매장 다운로드 시 동일 파일명 충돌 방지
            # (Chrome은 같은 파일명으로 덮어쓰므로 existing_files에서 새 파일로 감지 불가)
            suffix = downloaded.suffix.lower()
            renamed = download_dir / f"{sale_date}__{page_type_key}__{store_slug}{suffix}"
            shutil.move(str(downloaded), str(renamed))
            logger.info(f"{attempt_tag} 다운로드 완료: {renamed.name}")
            return renamed

        except Exception as exc:
            # 드라이버 세션/연결 오류는 브라우저 재생성 로직으로 넘긴다.
            if _is_transient_connection_error(exc):
                logger.warning(f"{attempt_tag} WebDriver 연결 오류 감지 → 세션 재생성으로 위임: {exc}")
                raise

            if store_attempt < store_retry_max:
                if not _is_driver_alive(driver):
                    logger.warning(
                        f"{attempt_tag} 실패 후 WebDriver 세션이 죽음 → 브라우저 재생성 경로로 위임: {exc}"
                    )
                    raise WebDriverException("browser session died after store attempt") from exc
                logger.warning(f"{attempt_tag} 실패 → {store_retry_wait}s 후 재시도: {exc}")
                time.sleep(store_retry_wait)
                continue

            # 디버깅 아티팩트: 최종 실패 시점의 화면/소스/디렉토리 상태 저장 (가능한 경우만)
            try:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_slug = re.sub(r"[^0-9A-Za-z가-힣_\\-]+", "_", store_slug)
                base = download_dir / f"debug__{sale_date}__{page_type_key}__{safe_slug}__{ts}"
                try:
                    driver.save_screenshot(str(base.with_suffix(".png")))
                except Exception:
                    pass
                try:
                    base.with_suffix(".html").write_text(driver.page_source or "", encoding="utf-8", errors="replace")
                except Exception:
                    pass
                try:
                    files = [p.name for p in sorted(download_dir.iterdir()) if p.is_file()]
                    base.with_suffix(".dir.txt").write_text("\n".join(files), encoding="utf-8", errors="replace")
                except Exception:
                    pass
                logger.warning(f"{attempt_tag} 디버그 아티팩트 저장: {base.name}.*")
            except Exception:
                pass

            logger.exception(f"{attempt_tag} 다운로드 최종 실패 (상세 traceback)")
            _dismiss_alert(driver)
            # TimeoutException은 "데이터 없음"이 아니라 다운로드 실패로 본다.
            # 여기서 __NO_DATA__로 바꾸면 Airflow task가 success 처리되고,
            # .no_data__*.txt 마커가 생겨 이후 재수집까지 차단된다.
            # 실제 NO_DATA는 OKPOS 화면/엑셀 파싱 단계에서 명확히 확인된 경우에만 마킹한다.
            if isinstance(exc, TimeoutException):
                logger.warning(f"{attempt_tag} 타임아웃 최종 실패({exc}) → 다운로드 실패로 처리")
                return None
            return None


# ============================================================
# Task 함수
# ============================================================

def resolve_sale_dates(manual_date_range=None, lookback_days=None, **context) -> str:
    """실행 날짜 범위 결정 (MANUAL_DATE_RANGE > dag_run.conf > yesterday 순)

    Args:
        manual_date_range: DAG 파일 상단 MANUAL_DATE_RANGE 변수.
                           None이면 어제, ("2026-03-01", "2026-03-31")이면 해당 기간 1일씩 수집.
    """
    # 1순위: DAG 파일 상단 MANUAL_DATE_RANGE
    if manual_date_range is not None:
        if not (isinstance(manual_date_range, (tuple, list)) and len(manual_date_range) == 2):
            raise ValueError(
                f"MANUAL_DATE_RANGE 형식 오류: (시작날짜, 종료날짜) 튜플이어야 합니다. 받은 값: {manual_date_range!r}"
            )
        date_from, date_to = str(manual_date_range[0]), str(manual_date_range[1])
        source = "MANUAL_DATE_RANGE"
    else:
        # 2순위: dag_run.conf
        conf = context.get("dag_run").conf or {}
        date_from = conf.get("sale_date_from")
        date_to   = conf.get("sale_date_to")
        source = "dag_run.conf"

    if date_from or date_to:
        if not date_from or not date_to:
            raise ValueError("시작일과 종료일은 함께 지정해야 합니다.")
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            dt_to   = datetime.strptime(date_to,   "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"날짜 형식 오류 (YYYY-MM-DD 필요): {e}")
        if dt_from > dt_to:
            raise ValueError(f"시작일({date_from}) > 종료일({date_to})")
        sale_dates = []
        current = dt_from
        while current <= dt_to:
            sale_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif lookback_days:
        # 각 download task가 이미 수집된 (date, store)는 skip한다.
        today = _kst_now()
        sale_dates = [
            (today - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(lookback_days, 0, -1)
        ]
        source = f"lookback {lookback_days}일"
    else:
        # 3순위: 어제 (기본값)
        yesterday = (_kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")
        sale_dates = [yesterday]
        source = "yesterday(기본)"

    context["ti"].xcom_push(key="sale_dates", value=sale_dates)
    logger.info(f"실행 날짜 범위 [{source}]: {sale_dates}")
    return f"날짜 범위 결정 완료 [{source}]: {sale_dates[0]} ~ {sale_dates[-1]} ({len(sale_dates)}일)"


def download_today_stores(**context) -> str:
    """today 페이지 매장별 엑셀 다운로드"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    conf = context.get("dag_run").conf or {}
    force = bool(conf.get("force_redownload") or conf.get("force_redownload_today") or os.getenv("OKPOS_FORCE_REDOWNLOAD_TODAY", ""))

    download_dir = _okpos_download_dir(context, "today")
    download_dir.mkdir(parents=True, exist_ok=True)
    page_cfg = PAGE_TYPES["today"]

    _current_key: list[str | None] = [None]

    def _session(driver, wait, pending, results):
        _login(driver, wait)
        _setup_download_dir(driver, download_dir)
        driver.get(page_cfg["url"])
        time.sleep(2)
        for sale_date, store in pending:
            key = f"{sale_date}__{store['name']}"
            _current_key[0] = key
            downloaded = _download_excel_for_store(
                driver, wait, "today", page_cfg, sale_date, store, download_dir
            )
            _current_key[0] = None
            if downloaded == "__NO_DATA__":
                results[key] = "__NO_DATA__"
            elif downloaded:
                results[key] = str(downloaded)

    # 이미 수집된 (날짜, 매장) 조합 제외 → 누락분만 다운로드
    if force:
        pending_keys = [(sd, st) for sd in sale_dates for st in STORES if _should_collect(st["name"], sd)]
        logger.warning(f"today 강제 재다운로드 모드: {len(pending_keys)}건")
    else:
        pending_keys = _filter_missing_keys(sale_dates, "okpos_order")
    if not pending_keys:
        logger.info("today: 모든 날짜/매장 데이터 이미 수집됨, 스킵")
        context["ti"].xcom_push(key="today_files", value={})
        return "today 다운로드: 전체 이미 수집됨 (스킵)"
    logger.info(f"today 누락 {len(pending_keys)}건 다운로드 시작")

    results = _download_with_retry("today", pending_keys, download_dir, _session, current_key=_current_key)

    success = sum(1 for v in results.values() if v and v != "__NO_DATA__")
    no_data = sum(1 for v in results.values() if v == "__NO_DATA__")
    context["ti"].xcom_push(key="today_files", value=results)
    logger.info(f"today 다운로드 완료: {success}/{len(results)}건 (데이터없음: {no_data}건)")
    missing = [k for k, v in results.items() if not v]
    if missing:
        preview = ", ".join(missing[:10])
        raise RuntimeError(
            f"today 다운로드 미수집 {len(missing)}/{len(results)}건 (예: {preview}). "
            "Airflow task retry로 재시도합니다. "
            "필요 시 OKPOS_STORE_RETRY_MAX/OKPOS_STORE_RETRY_WAIT 환경변수로 store-level 재시도 횟수를 조정하세요."
        )
    return f"today 다운로드: {success}/{len(results)}건 성공"


def download_receipt_stores(**context) -> str:
    """receipt/details 페이지 매장별 엑셀 다운로드 (영수증별매출상세현황)"""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    conf = context.get("dag_run").conf or {}
    force = bool(conf.get("force_redownload") or conf.get("force_redownload_receipt") or os.getenv("OKPOS_FORCE_REDOWNLOAD_RECEIPT", ""))

    download_dir = _okpos_download_dir(context, "receipt")
    download_dir.mkdir(parents=True, exist_ok=True)
    page_cfg = PAGE_TYPES["receipt"]

    _current_key: list[str | None] = [None]

    def _session(driver, wait, pending, results):
        _login(driver, wait)
        _setup_download_dir(driver, download_dir)
        driver.get(page_cfg["url"])
        time.sleep(2)
        for sale_date, store in pending:
            key = f"{sale_date}__{store['name']}"
            _current_key[0] = key
            downloaded = _download_excel_for_store(
                driver, wait, "receipt", page_cfg, sale_date, store, download_dir
            )
            _current_key[0] = None
            if downloaded == "__NO_DATA__":
                results[key] = "__NO_DATA__"
            elif downloaded:
                results[key] = str(downloaded)

    # 이미 수집된 (날짜, 매장) 조합 제외 → 누락분만 다운로드
    if force:
        pending_keys = [(sd, st) for sd in sale_dates for st in STORES if _should_collect(st["name"], sd)]
        logger.warning(f"receipt 강제 재다운로드 모드: {len(pending_keys)}건")
    else:
        pending_keys = _filter_missing_keys(sale_dates, "okpos_order_item")
    if not pending_keys:
        logger.info("receipt: 모든 날짜/매장 데이터 이미 수집됨, 스킵")
        context["ti"].xcom_push(key="receipt_files", value={})
        return "receipt 다운로드: 전체 이미 수집됨 (스킵)"
    logger.info(f"receipt 누락 {len(pending_keys)}건 다운로드 시작")

    results = _download_with_retry("receipt", pending_keys, download_dir, _session, current_key=_current_key)

    success = sum(1 for v in results.values() if v and v != "__NO_DATA__")
    no_data = sum(1 for v in results.values() if v == "__NO_DATA__")
    context["ti"].xcom_push(key="receipt_files", value=results)
    logger.info(f"receipt 다운로드 완료: {success}/{len(results)}건 (데이터없음: {no_data}건)")
    missing = [k for k, v in results.items() if not v]
    if missing:
        preview = ", ".join(missing[:10])
        raise RuntimeError(
            f"receipt 다운로드 미수집 {len(missing)}/{len(results)}건 (예: {preview}). "
            "Airflow task retry로 재시도합니다. "
            "필요 시 OKPOS_STORE_RETRY_MAX/OKPOS_STORE_RETRY_WAIT 환경변수로 store-level 재시도 횟수를 조정하세요."
        )
    return f"receipt 다운로드: {success}/{len(results)}건 성공"


def download_daily_stores(**context) -> str:
    """daily(일자별 종합매출) 페이지 매장별 엑셀 다운로드.

    - startDate/endDate를 동일 sale_date로 설정해 '1일치'로 다운로드한다.
    - 이미 수집된 날짜/매장 조합은 스킵한다(누락분만 다운로드).
    """
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    conf = context.get("dag_run").conf or {}
    force = bool(conf.get("force_redownload") or conf.get("force_redownload_daily") or os.getenv("OKPOS_FORCE_REDOWNLOAD_DAILY", ""))

    download_dir = _okpos_download_dir(context, "daily")
    download_dir.mkdir(parents=True, exist_ok=True)
    page_cfg = PAGE_TYPES["daily"]

    _current_key: list[str | None] = [None]

    def _session(driver, wait, pending, results):
        _login(driver, wait)
        _setup_download_dir(driver, download_dir)
        driver.get(page_cfg["url"])
        time.sleep(2)
        for sale_date, store in pending:
            key = f"{sale_date}__{store['name']}"
            _current_key[0] = key
            downloaded = _download_excel_for_store(
                driver, wait, "daily", page_cfg, sale_date, store, download_dir
            )
            _current_key[0] = None
            if downloaded == "__NO_DATA__":
                results[key] = "__NO_DATA__"
            elif downloaded:
                results[key] = str(downloaded)

    if force:
        pending_keys = [(sd, st) for sd in sale_dates for st in STORES if _should_collect(st["name"], sd)]
        logger.warning(f"daily 강제 재다운로드 모드: {len(pending_keys)}건")
    else:
        pending_keys = _filter_missing_keys(sale_dates, "okpos_daily")
    if not pending_keys:
        logger.info("daily: 모든 날짜/매장 데이터 이미 수집됨, 스킵")
        context["ti"].xcom_push(key="daily_files", value={})
        return "daily 다운로드: 전체 이미 수집됨 (스킵)"

    logger.info(f"daily 누락 {len(pending_keys)}건 다운로드 시작")
    results = _download_with_retry("daily", pending_keys, download_dir, _session, current_key=_current_key)

    success = sum(1 for v in results.values() if v and v != "__NO_DATA__")
    no_data = sum(1 for v in results.values() if v == "__NO_DATA__")
    context["ti"].xcom_push(key="daily_files", value=results)
    logger.info(f"daily 다운로드 완료: {success}/{len(results)}건 (데이터없음: {no_data}건)")
    missing = [k for k, v in results.items() if not v]
    if missing:
        preview = ", ".join(missing[:10])
        raise RuntimeError(
            f"daily 다운로드 미수집 {len(missing)}/{len(results)}건 (예: {preview}). Airflow task retry로 재시도합니다."
        )
    return f"daily 다운로드: {success}/{len(results)}건 성공"


def report_missing_daily(**context) -> str:
    """기간 내 okpos_daily.csv 누락 (date, store) 조합을 점검용으로 로그로 출력한다."""
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates")
    if not sale_dates:
        raise ValueError("sale_dates XCom 값이 없습니다.")

    missing = _filter_missing_keys(sale_dates, "okpos_daily")
    missing_pairs = [(sd, st["name"]) for sd, st in missing]
    context["ti"].xcom_push(key="missing_daily", value=missing_pairs)

    if not missing_pairs:
        logger.info(f"daily 누락 없음: {sale_dates[0]} ~ {sale_dates[-1]}")
        return f"daily 누락 없음 ({sale_dates[0]} ~ {sale_dates[-1]})"

    preview_n = int(os.getenv("OKPOS_MISSING_REPORT_PREVIEW", "50"))
    preview = ", ".join([f"{sd}:{name}" for sd, name in missing_pairs[:preview_n]])
    logger.warning(f"daily 누락 {len(missing_pairs)}건 ({sale_dates[0]} ~ {sale_dates[-1]}). 예: {preview}")
    return f"daily 누락 {len(missing_pairs)}건 ({sale_dates[0]} ~ {sale_dates[-1]})"


def _read_okpos_excel(path: str) -> pd.DataFrame:
    """OKPOS 파일 읽기: xlsx 또는 txt(탭 구분) 자동 감지.

    - xlsx: 상단 메타 행을 건너뛰고 실제 컬럼 헤더 행 자동 감지
    - txt: OKPOS가 탭 구분 텍스트로 내려보내는 경우 처리
    - #NAME? 수식 오류 셀 → 빈 문자열로 대체
    """
    p = Path(path)
    suffix = p.suffix.lower()

    def _read_tsv_file(file_path: str) -> pd.DataFrame:
        try:
            parsed_df = pd.read_csv(file_path, sep="\t", dtype=str, encoding="utf-8-sig")
        except UnicodeDecodeError:
            parsed_df = pd.read_csv(file_path, sep="\t", dtype=str, encoding="cp949")
        parsed_df.columns = [str(c).strip() for c in parsed_df.columns]
        parsed_df = parsed_df.replace("#NAME?", "", regex=False)
        parsed_df = parsed_df.replace(r"^\s*$", pd.NA, regex=True)
        logger.info(f"txt 읽기(TSV): {file_path} | shape={parsed_df.shape} | columns={list(parsed_df.columns[:6])}")
        return parsed_df

    def _read_html_file(file_path: str) -> pd.DataFrame:
        try:
            tables = pd.read_html(file_path)
        except UnicodeDecodeError:
            tables = pd.read_html(file_path, encoding="cp949")
        except ImportError as e:
            raise ImportError(
                "HTML receipt 파싱을 위해 pandas read_html 의존성(lxml/bs4 등)이 필요합니다."
            ) from e
        except ValueError:
            raise ValueError(f"HTML에 테이블이 없습니다 (다운로드 오류/로그인 페이지 가능): {file_path}")

        header_candidates = {
            "NO", "포스번호", "영수증번호", "구분", "테이블명", "최초주문", "결제시간", "결제시각",
            "상품코드", "바코드", "상품명", "수량", "총매출액", "할인액", "할인구분",
            "실매출액", "매출액", "가액", "부가세",
        }

        best_df: pd.DataFrame | None = None
        best_score = -1
        for table in tables:
            if table is None or table.empty:
                continue
            df_try = table.copy()
            if isinstance(df_try.columns, pd.MultiIndex):
                df_try.columns = [
                    " ".join([str(x) for x in col if pd.notna(x)]).strip()
                    for col in df_try.columns
                ]
            df_try.columns = [str(c).strip() for c in df_try.columns]
            score = sum(1 for c in df_try.columns if c in header_candidates)
            if score > best_score:
                best_score = score
                best_df = df_try

        if best_df is None or best_df.empty or best_score < 2:
            raise ValueError(
                f"HTML 테이블을 찾았지만 OKPOS 매출 컬럼을 감지하지 못했습니다 "
                f"(다운로드 오류 가능): {file_path}"
            )

        best_df = best_df.astype(str)
        best_df.columns = [str(c).strip() for c in best_df.columns]
        best_df = best_df.replace("#NAME?", "", regex=False)
        best_df = best_df.replace(r"^\s*$", pd.NA, regex=True)
        logger.info(f"html 읽기: {file_path} | shape={best_df.shape} | columns={list(best_df.columns[:6])}")
        return best_df

    def _sniff_text_prefix(raw_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "cp949", "utf-8"):
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode("latin1", errors="ignore")

    # ── txt/tsv 파일: OKPOS receipt가 txt로 다운되는 경우 ──────────────────
    if suffix in {".txt", ".tsv", ".csv"}:
        return _read_tsv_file(path)

    # ── html 파일: OKPOS receipt/details가 HTML 테이블로 내려오는 경우 ──────
    if suffix in {".html", ".htm"}:
        return _read_html_file(path)

    # ── xlsx/xls: 헤더 행 자동 감지 ─────────────────────────────────────────
    # 확장자가 xlsx여도 실제 다운로드는 HTML/TSV인 경우가 있어 바이트 시그니처로 판별한다.
    with open(path, "rb") as _f:
        sniff_bytes = _f.read(4096)

    _IMAGE_MAGIC = (b'\x89PNG', b'\xff\xd8\xff', b'GIF8')
    if any(sniff_bytes.startswith(sig) for sig in _IMAGE_MAGIC):
        raise ValueError(f"이미지 파일입니다 - 데이터 아님 (다운로드 오류): {path}")

    stripped = sniff_bytes.lstrip()
    sniff_text = _sniff_text_prefix(stripped[:1024]).strip().lower()
    if stripped.startswith((b"<", b"<!DO", b"<htm", b"<HTM")) or "<html" in sniff_text or "<!doctype" in sniff_text:
        logger.warning(f"xlsx 확장자지만 HTML 내용 감지 → HTML 파서로 폴백: {path}")
        return _read_html_file(path)

    is_zip_container = sniff_bytes.startswith(b"PK\x03\x04") or sniff_bytes.startswith(b"PK\x05\x06") or sniff_bytes.startswith(b"PK\x07\x08")
    if not is_zip_container:
        if "\t" in sniff_text or any(token in sniff_text for token in ("no\t", "상품명", "총매출액", "영수증번호", "포스번호")):
            logger.warning(f"xlsx 확장자지만 TSV 내용 감지 → TSV 파서로 폴백: {path}")
            return _read_tsv_file(path)
        raise ValueError(f"xlsx 파일이 zip 포맷이 아닙니다 (다운로드 오류): {path}")

    try:
        preview = pd.read_excel(path, header=None, nrows=12, dtype=str, engine="openpyxl")
    except BadZipFile as e:
        raise ValueError(f"xlsx 파일이 zip 포맷이 아닙니다 (다운로드 오류): {path}") from e

    header_candidates = {
        "NO", "포스번호", "영수증번호", "구분", "테이블명", "최초주문", "결제시간",
        "상품코드", "바코드", "상품명", "수량", "총매출액", "할인액", "할인구분",
        "실매출액", "가액", "부가세",
        # daily(일자별 종합매출) 전용 컬럼명
        "일자", "영업매장", "합매출", "총매출", "실매출", "영수건수",
    }

    header_row = 0
    best_score = -1
    for i in range(len(preview)):
        row_vals = [str(v).strip() for v in preview.iloc[i].tolist() if pd.notna(v)]
        if not row_vals:
            continue
        score = sum(1 for v in row_vals if v in header_candidates)
        if score > best_score:
            best_score = score
            header_row = i

    # 점수가 너무 낮으면 기존 규칙 fallback
    if best_score < 2:
        for i in range(min(4, len(preview))):
            val = str(preview.iloc[i, 0]).strip().upper()
            if val == "NO":
                header_row = i

    try:
        df = pd.read_excel(path, header=header_row, dtype=str, engine="openpyxl")
    except BadZipFile as e:
        raise ValueError(f"xlsx 파일이 zip 포맷이 아닙니다 (다운로드 오류): {path}") from e
    df.columns = [str(c).strip() for c in df.columns]
    df = df.replace("#NAME?", "", regex=False)
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    logger.info(f"xlsx 읽기: {path} | header_row={header_row} | shape={df.shape} | columns={list(df.columns[:6])}")
    return df


def _transform_okpos_df(df: pd.DataFrame, store_name: str, sale_date: str):
    """xlsx DataFrame 정제 + 파생 컬럼 추가. (cleaned_df, ym) 반환."""
    def _normalize_time_str(raw) -> str:
        s = str(raw).strip()
        if not s or s == "nan":
            return ""
        tail = s[-8:]
        if len(tail) == 8 and tail[2] == ":" and tail[5] == ":":
            return tail
        return s

    # 엑셀 메타 행/빈 열로 생긴 Unnamed 컬럼 제거
    df = df[[c for c in df.columns if not str(c).startswith("Unnamed")]].copy()

    # 합계 행 제거: 첫 번째 컬럼(NO)이 숫자가 아닌 행 제거
    no_col = df.columns[0]
    df = df[df[no_col].astype(str).str.strip().ne(str(no_col).strip())].copy()
    df = df[pd.to_numeric(df[no_col], errors="coerce").notna()].copy()

    # 시각 컬럼 정규화 (엑셀 서식 문자열이 붙는 케이스 방지: 예 ':mm:ss17:13:37' → '17:13:37')
    for col in ("결제시각", "결제시간", "최초주문", "최초주문시각"):
        if col in df.columns:
            df[col] = df[col].map(_normalize_time_str)

    # 반품 포함 영수증 전체 제거: 영수증번호에 반품이 하나라도 있으면 해당 영수증 전체 제외
    if "구분" in df.columns and "영수증번호" in df.columns:
        cancelled = df[df["구분"].astype(str).str.strip() == "반품"]["영수증번호"].unique()
        if len(cancelled):
            before = len(df)
            df = df[~df["영수증번호"].isin(cancelled)].copy()
            logger.info(f"반품 포함 영수증 제거: {before - len(df)}행 ({len(cancelled)}건 영수증)")

    # 포스번호·영수증번호 선행 0 정규화: OKPOS가 다운로드마다 '1' vs '01' vs '0001'로 달리
    # 반환해 _pk가 달라지는 것을 방지한다.
    for _norm_col in ("포스번호", "영수증번호"):
        if _norm_col in df.columns:
            _normed = df[_norm_col].astype(str).str.lstrip("0")
            df[_norm_col] = _normed.where(_normed != "", "0")

    # 안정적 정렬: 다운로드마다 행 순서가 달라도 row_idx가 동일하게 부여되도록 한다.
    # (영수증번호, 상품코드, 총매출액) 기준 정렬 → 같은 영수증 내 동일 상품이 여러 개여도
    # 정렬 순서가 결정론적이므로 _pk 충돌 없이 tie-breaker 역할 유지.
    _sort_cols = [c for c in ("영수증번호", "상품코드", "총매출액") if c in df.columns]
    if _sort_cols:
        df = df.sort_values(_sort_cols, kind="stable").reset_index(drop=True)
    else:
        df.reset_index(drop=True, inplace=True)

    ym = sale_date[:7]  # YYYY-MM
    n_orig = len(df.columns)

    df["매장명"]       = store_name
    df["sale_date"]    = sale_date
    df["ym"]           = ym
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # PK: 원본 컬럼 값 결정론적 hash (파생 컬럼 제외) — hashlib.md5 사용
    # Python 내장 hash()는 PYTHONHASHSEED 랜덤화로 프로세스마다 값이 달라져 dedup 불가
    #
    # IMPORTANT:
    # 영수증별매출(okpos_order_item) 리포트는 "동일 상품/수량/금액" 행이 여러 번 존재할 수 있다.
    # 원본 컬럼만으로 PK를 만들면(전 컬럼 동일) 정상 행이 충돌해 누락될 수 있으므로,
    # 정제 후 행 위치(row index)를 tie-breaker로 포함한다.
    # 단, 위에서 선행 0 정규화 + 안정 정렬을 수행했으므로 row_idx가 다운로드 간 일관됨.
    row_idx = df.index.astype(str)
    df["_pk"] = df.iloc[:, :n_orig].apply(
        lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
        axis=1,
    )
    df["_pk"] = df["_pk"].astype(str) + "|" + row_idx
    df["_pk"] = df["_pk"].map(lambda s: hashlib.md5(s.encode()).hexdigest())
    return df, ym


def _transform_okpos_daily_df(df: pd.DataFrame, store_name: str, sale_date: str) -> tuple[pd.DataFrame, str]:
    """일자별 종합매출(daily) 엑셀 → (sale_date 단일 행) DataFrame 변환.

    출력 컬럼(최소):
    - sale_date, ym, 매장명, 총매출액, 총할인액, 실매출액, 영수건수, collected_at, _pk
    """
    def _clean_col(value) -> str:
        return re.sub(r"\s+", "", str(value or "").strip())

    def _normalize_store(value) -> str:
        return re.sub(r"\s+", "", str(value or "").strip())

    def _normalize_daily_columns(frame: pd.DataFrame) -> pd.DataFrame:
        aliases = {
            "일자": {"일자", "날짜", "영업일자"},
            "영업매장": {"영업매장", "매장명", "매장", "점포명"},
            "총매출액": {"총매출액", "총매출", "합매출", "매출액"},
            "총할인액": {"총할인액", "총할인", "할인액", "할인금액"},
            "실매출액": {"실매출액", "실매출", "순매출", "순매출액"},
            "영수건수": {"영수건수", "영수증건수", "객수(수)", "객수", "건수"},
        }
        rename_map: dict[str, str] = {}
        used: set[str] = set()
        for col in frame.columns:
            compact = _clean_col(col)
            for canonical, names in aliases.items():
                if canonical in used:
                    continue
                if compact in {_clean_col(name) for name in names}:
                    rename_map[col] = canonical
                    used.add(canonical)
                    break
        return frame.rename(columns=rename_map)

    def _to_amount(value) -> int:
        text = str(value if value is not None else "").strip()
        if not text or text.lower() in {"nan", "none", "<na>"}:
            return 0
        text = text.replace(",", "")
        if text.startswith("(") and text.endswith(")"):
            text = "-" + text[1:-1]
        text = re.sub(r"[^\d.-]", "", text)
        result = pd.to_numeric(text, errors="coerce")
        return int(result) if pd.notna(result) else 0

    def _amount_from_row(row: pd.Series, *cols: str) -> int:
        for col in cols:
            if col in row.index:
                return _to_amount(row.get(col))
        return 0

    raw = df.copy()
    raw = raw.replace(r"^\s*$", pd.NA, regex=True)

    # 1) 정상 케이스: 컬럼이 이미 존재
    raw = _normalize_daily_columns(raw)
    _DAILY_AMT_COLS = {"실매출액", "총매출액", "총할인액"}
    if "일자" in raw.columns and bool(_DAILY_AMT_COLS & set(raw.columns)):
        data = raw.copy()
    else:
        # 2) 비정상/구버전 케이스: 헤더 행을 다시 찾는다.
        header_row = None
        for i in range(min(8, len(raw))):
            row = raw.iloc[i].astype(str).map(lambda s: (s or "").strip()).tolist()
            normalized_row = [_clean_col(v) for v in row]
            row_set = set(normalized_row)
            if "일자" in row_set and bool(row_set & {_clean_col(c) for c in _DAILY_AMT_COLS | {"실매출", "합매출", "총매출"}}):
                header_row = i
                break
        if header_row is None:
            header_row = 0

        cols = raw.iloc[header_row].astype(str).map(lambda s: (s or "").strip()).tolist()
        data = raw.iloc[header_row + 1 :].copy()
        data.columns = cols
        data = _normalize_daily_columns(data)

        for c in list(data.columns):
            if str(c).startswith("Unnamed"):
                data = data.drop(columns=[c])

    if "일자" not in data.columns:
        raise ValueError("daily 엑셀 파싱 실패: '일자' 컬럼 없음")
    if not bool(_DAILY_AMT_COLS & set(data.columns)):
        raise ValueError(f"daily 엑셀 파싱 실패: 금액 컬럼 없음 columns={list(data.columns)}")

    data["일자"] = data["일자"].astype(str).str.strip()

    # 케이스 A) 현재 포맷: 날짜 x 영업매장 행에 매장별 금액이 있다.
    row = pd.DataFrame()
    if "영업매장" in data.columns:
        dt = pd.to_datetime(data["일자"].astype(str).str.strip(), errors="coerce")
        rows = data[dt.notna()].copy()
        if not rows.empty:
            rows["_sale_date"] = dt[dt.notna()].dt.strftime("%Y-%m-%d")
            rows["_store_key"] = rows["영업매장"].map(_normalize_store)
            wanted_store = _normalize_store(store_name)
            row = rows[(rows["_sale_date"] == sale_date) & (rows["_store_key"] == wanted_store)].copy()
            if row.empty:
                store_short = store_name.replace("도리당 ", "", 1)
                row = rows[
                    (rows["_sale_date"] == sale_date)
                    & rows["_store_key"].str.contains(re.escape(_normalize_store(store_short)), na=False)
                ].copy()
            if not row.empty:
                row = row.iloc[:1].copy()

    # 케이스 B) (구 포맷) 기간 리포트: '소계:' 행에 날짜별 합계가 있다.
    if row.empty:
        sub = data[data["일자"].str.startswith("소계:")].copy()
    else:
        sub = pd.DataFrame()
    if row.empty and not sub.empty:
        sub["sale_date"] = sub["일자"].str.extract(r"(\d{4}-\d{2}-\d{2})")[0]
        row = sub[sub["sale_date"] == sale_date].copy()
        if row.empty:
            raise ValueError(f"daily 엑셀에 해당 날짜 소계 없음: {sale_date}")
        row = row.iloc[:1].copy()

    # 케이스 C) 단일 일자 리포트: '영업일수합:' 행 1개만 존재한다.
    if row.empty:
        row = data[data["일자"].str.startswith("영업일수합:")].copy()
        if row.empty and ("영업매장" in data.columns):
            # 일부 포맷에서 '영업일수합:' 문자열이 누락될 수 있어, '합계' 행으로도 시도한다.
            store_col = data["영업매장"].astype(str).str.strip()
            row = data[store_col.isin(["합계", "총계", "전체"])].copy()
        if row.empty and len(data) == 1:
            row = data.copy()
        if row.empty:
            raise ValueError("daily 엑셀 파싱 실패: '소계:'/'영업일수합:'/합계 행 모두 없음")
        row = row.iloc[:1].copy()

    row_series = row.iloc[0]

    out = pd.DataFrame([{
        "sale_date": sale_date,
        "ym": sale_date[:7],
        "매장명": store_name,
        "총매출액": _amount_from_row(row_series, "총매출액"),
        "총할인액": _amount_from_row(row_series, "총할인액"),
        "실매출액": _amount_from_row(row_series, "실매출액"),
        "영수건수": _amount_from_row(row_series, "영수건수"),
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])

    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        logger.warning(
            f"daily 0원 감지 | {sale_date} | {store_name} | "
            f"columns={list(data.columns)} | "
            f"selected_row={row_series.to_dict()} | "
            f"data_head=\n{data.head(10).to_string()}"
        )
        amount_cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in data.columns]
        nonzero_source_found = False
        if amount_cols:
            dt = pd.to_datetime(data["일자"].astype(str).str.strip(), errors="coerce")
            same_date_rows = data[dt.dt.strftime("%Y-%m-%d") == sale_date].copy()
            scan_rows = same_date_rows if not same_date_rows.empty else data
            for _, src_row in scan_rows.iterrows():
                if any(_to_amount(src_row.get(c)) != 0 for c in amount_cols):
                    nonzero_source_found = True
                    break
        if nonzero_source_found:
            logger.warning(
                f"daily 0원 의심 | {sale_date} | {store_name} | "
                f"columns={list(data.columns)} | "
                f"selected_row={row_series.to_dict()} | "
                f"data_head=\\n{data.head(10).to_string()}"
            )
            raise ValueError(
                "daily 엑셀 파싱 실패: 선택된 행은 0원이지만 같은 날짜 원본에 0이 아닌 금액이 있습니다 "
                f"(sale_date={sale_date}, store={store_name}, selected={row_series.to_dict()})"
            )

    out["_pk"] = out.apply(
        lambda r: hashlib.md5(f"{r['sale_date']}|{r['매장명']}|daily".encode()).hexdigest(),
        axis=1,
    )
    return out, sale_date[:7]


def _read_okpos_csv(csv_path: Path) -> pd.DataFrame:
    """OKPOS raw CSV를 UTF-8 BOM 유무와 무관하게 읽는다."""
    try:
        return pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(csv_path, dtype=str)


def _daily_amount_sum(df_daily: pd.DataFrame) -> int:
    cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in df_daily.columns]
    if not cols or df_daily.empty:
        return 0
    return int(df_daily[cols].apply(_to_int_series).sum(axis=1).iloc[0])


def _fallback_daily_from_order_csv(df_daily: pd.DataFrame, store_short: str, sale_date: str, ym: str) -> pd.DataFrame:
    """daily 0원 의심값을 같은 날짜 today(order) 합계로 보정한다.

    OKPOS daily 페이지가 특정 날짜를 조회하지 못하면 정상 엑셀처럼 보이지만
    '영업일수합: 0 일' 행만 내려오는 경우가 있다. 같은 날짜 order가 비0원이면
    daily 0원은 수집 오류로 보고 order 합계로 교체한다.
    """
    if df_daily.empty or _daily_amount_sum(df_daily) != 0:
        return df_daily

    order_csv = (
        RAW_OKPOS_SALES
        / "brand=도리당"
        / f"store={store_short}"
        / f"ym={ym}"
        / "okpos_order.csv"
    )
    if not order_csv.exists() or order_csv.stat().st_size == 0:
        return df_daily

    order_df = _read_okpos_csv(order_csv)
    if order_df.empty or "sale_date" not in order_df.columns:
        return df_daily

    order_df = order_df[order_df["sale_date"].astype(str).str.strip() == sale_date].copy()
    if order_df.empty:
        return df_daily

    total = int((_to_int_series(order_df["총매출액"]) if "총매출액" in order_df.columns else pd.Series(0, index=order_df.index)).sum())
    discount = int((_to_int_series(order_df["총할인액"]) if "총할인액" in order_df.columns else pd.Series(0, index=order_df.index)).sum())
    net = int((_to_int_series(order_df["실매출액"]) if "실매출액" in order_df.columns else pd.Series(0, index=order_df.index)).sum())
    receipts = int(len(order_df))
    if total == 0 and discount == 0 and net == 0 and receipts == 0:
        return df_daily

    out = pd.DataFrame([{
        "sale_date": sale_date,
        "ym": ym,
        "매장명": df_daily["매장명"].iloc[0],
        "총매출액": total,
        "총할인액": discount,
        "실매출액": net,
        "영수건수": receipts,
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])
    out["_pk"] = out.apply(
        lambda r: hashlib.md5(f"{r['sale_date']}|{r['매장명']}|daily".encode()).hexdigest(),
        axis=1,
    )
    logger.warning(
        "daily 0원 수집 오류 의심 → order fallback 보정: %s | %s | total=%s discount=%s net=%s receipts=%s",
        sale_date,
        df_daily["매장명"].iloc[0],
        total,
        discount,
        net,
        receipts,
    )
    return out


def ingest_manual_daily_xlsx(
    manual_xlsx_path: str | None = None,
    store_name: str = "",
    **context,
) -> str:
    """수동 다운로드된 '일자별 종합매출' 엑셀을 okpos_daily.csv로 적재한다.

    목적:
    - 자동 수집(daily 페이지)이 누락/오작동한 날짜를 수동 엑셀로 보완
    - 이후 reconcile_against_daily_summary에서 이 값을 기준으로 today/receipt 재수집 수행

    사용:
    - DAG에서 op_kwargs로 manual_xlsx_path 전달 또는 dag_run.conf로 manual_xlsx_path 전달
    - Windows 경로를 그대로 넘길 경우 컨테이너에서 접근 불가할 수 있으니,
      가능하면 `/opt/airflow/...` 경로로 마운트된 위치에 파일을 두는 것을 권장
    """
    from modules.load.load_onedrive import onedrive_csv_save

    conf = (context.get("dag_run").conf or {}) if context else {}

    # 우선순위:
    # 1) op_kwargs manual_xlsx_path
    # 2) dag_run.conf manual_xlsx_path
    # 3) env OKPOS_DAILY_MANUAL_XLSX
    # 4) (자동탐지) watch dir에서 '일자별 종합매출*.xlsx' 최신 파일
    path = manual_xlsx_path or conf.get("manual_xlsx_path") or os.getenv("OKPOS_DAILY_MANUAL_XLSX", "")
    path = str(path or "").strip()

    # store_name도 dag_run.conf/env로 덮어쓸 수 있게 한다(수동다운은 파일명에 매장명이 없을 수 있음).
    store_name = str(conf.get("store_name") or os.getenv("OKPOS_DAILY_MANUAL_STORE", "") or store_name).strip()

    def _auto_pick_from_watch_dir() -> str:
        watch_dir = str(conf.get("manual_watch_dir") or os.getenv("OKPOS_DAILY_MANUAL_WATCH_DIR", "")).strip()
        if not watch_dir:
            # 기본값: 컨테이너 내에서 접근 가능한 위치(호스트에서는 C:\airflow\download 권장)
            watch_dir = "/opt/airflow/download"
        d = Path(watch_dir)
        if not d.exists() or not d.is_dir():
            return ""
        # Windows에서 수동 다운로드가 '일자별 종합매출.xlsx'처럼 고정 파일명으로 저장되는 케이스 대응
        # ~$ 접두사 파일은 Excel 임시 잠금 파일이므로 제외
        candidates = [
            p for p in (list(d.glob("일자별 종합매출*.xlsx")) + list(d.glob("*일자별 종합매출*.xlsx")))
            if not p.name.startswith("~$")
        ]
        if not candidates:
            return ""
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return str(candidates[0])

    if not path:
        path = _auto_pick_from_watch_dir()
        path = str(path or "").strip()
    if not path:
        return "manual daily xlsx 없음(지정/자동탐지 모두 실패), 스킵"

    p = Path(path)
    if not p.exists():
        return f"manual daily xlsx 경로 없음, 스킵: {path}"

    raw = _read_okpos_excel(str(p))

    # 통합 일자별 종합매출 포맷 처리:
    # _read_okpos_excel이 서브헤더 행(합매출, 할인액, 실매출...)을 header로 감지해
    # '일자', '영업매장'이 Unnamed:0, Unnamed:2로 반환되는 경우 올바른 이름으로 복원한다.
    if "합매출" in raw.columns and "일자" not in raw.columns:
        unnamed_cols = [c for c in raw.columns if str(c).startswith("Unnamed:")]
        rename_map: dict[str, str] = {}
        if len(unnamed_cols) >= 1:
            rename_map[unnamed_cols[0]] = "일자"
        if len(unnamed_cols) >= 2:
            rename_map[unnamed_cols[1]] = "영업일"
        if len(unnamed_cols) >= 3:
            rename_map[unnamed_cols[2]] = "영업매장"
        if rename_map:
            raw = raw.rename(columns=rename_map)
            logger.info("통합 daily xlsx 포맷 감지: 컬럼 리네임 %s", list(rename_map.values()))

    def _infer_sale_date_from_xlsx(xlsx_path: str) -> str | None:
        """단일일자 daily 리포트에서 조회일자를 추론한다.

        - 상단에 '조회일자 : 2026/05/03' 같은 문자열이 들어오는 케이스 지원
        - 파일명이 항상 날짜를 포함하지 않는(예: '일자별 종합매출.xlsx') Windows 다운로드 케이스 대응
        """
        try:
            sniff = pd.read_excel(xlsx_path, header=None)
        except Exception:
            return None
        for v in sniff.iloc[:10, :5].astype(str).values.ravel().tolist():
            s = (v or "").strip()
            if not s or s.lower() == "nan":
                continue
            m = re.search(r"(\d{4})[./-](\d{2})[./-](\d{2})", s)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return None

    # 수동 엑셀은 (1) 기간 리포트(소계:) 또는 (2) 단일일자 합계(영업일수합:),
    # 또는 (3) 날짜×매장별 행(스크린샷 형태: '일자','영업매장','총매출','총할인','실매출' 등)을 포함할 수 있다.
    # 목적은 okpos_daily.csv(일자×매장 1행) 재적재이므로 최소 컬럼만 사용한다.

    # 헤더 파싱
    # _read_okpos_excel()이 이미 header_row를 추정해 컬럼을 세팅해준다.
    # 따라서 컬럼에 '일자'/'실매출'이 이미 있으면 재헤더링하지 않는다.
    tmp = raw.copy()
    tmp = tmp.replace(r"^\s*$", pd.NA, regex=True)
    if "일자" in tmp.columns and "실매출" in tmp.columns:
        data = tmp.copy()
    else:
        header_row = None
        for i in range(min(8, len(tmp))):
            row = tmp.iloc[i].astype(str).map(lambda s: (s or "").strip()).tolist()
            if ("일자" in row) and ("실매출" in row):
                header_row = i
                break
        if header_row is None:
            header_row = 0

        cols = tmp.iloc[header_row].astype(str).map(lambda s: (s or "").strip()).tolist()
        data = tmp.iloc[header_row + 1 :].copy()
        data.columns = cols
        for c in list(data.columns):
            if str(c).startswith("Unnamed"):
                data = data.drop(columns=[c])

    if "일자" not in data.columns:
        raise ValueError("manual daily xlsx 파싱 실패: '일자' 컬럼 없음")

    data["일자"] = data["일자"].astype(str).str.strip()

    # (3) 날짜×매장별 행 포맷: '일자'가 YYYY-MM-DD 형태로 들어있고, '영업매장'에 매장명이 들어있다.
    if "영업매장" in data.columns:
        dt = pd.to_datetime(data["일자"].astype(str).str.strip(), errors="coerce")
        rows = data[dt.notna()].copy()
        if not rows.empty:
            rows["sale_date"] = dt.dt.strftime("%Y-%m-%d")
            rows["영업매장"] = rows["영업매장"].astype(str).str.strip()

            # 소계 행 제거: 영업매장이 'N 개' 패턴인 행 (예: '3 개', '5 개 ')
            mask_subtotal = rows["영업매장"].str.match(r"^\d+\s*개\s*$", na=False)
            if mask_subtotal.any():
                logger.info("소계 행 제거: %d건 ('N 개' 패턴)", mask_subtotal.sum())
                rows = rows[~mask_subtotal].copy()

            # 미등록 매장 제거: STORES에 없는 매장명 (테스트 매장 등)
            known_store_names = {s["name"] for s in STORES}
            mask_unknown = ~rows["영업매장"].isin(known_store_names)
            if mask_unknown.any():
                unknown = rows.loc[mask_unknown, "영업매장"].unique().tolist()
                logger.warning("미등록 매장 제거: %s", unknown)
                rows = rows[~mask_unknown].copy()

            # 수동파일이 여러 매장을 담을 수 있으므로 store_name이 지정되지 않았으면 전부 적재한다.
            # 지정된 경우에는 해당 매장만 필터.
            if store_name:
                rows = rows[rows["영업매장"].str.replace(" ", "", regex=False) == store_name.replace(" ", "")].copy() if store_name else rows
            if not rows.empty:
                sub = rows.copy()
            else:
                sub = pd.DataFrame()
        else:
            sub = pd.DataFrame()
    else:
        sub = pd.DataFrame()

    # (1) 기간 리포트: '소계:' 행 추출
    if sub.empty:
        sub = data[data["일자"].str.startswith("소계:")].copy()
        sub["sale_date"] = sub["일자"].str.extract(r"(\d{4}-\d{2}-\d{2})")[0]
        sub = sub[sub["sale_date"].notna()].copy()

    # (2) 단일일자 합계 리포트: '영업일수합:' 또는 합계행 1개
    if sub.empty:
        # 신 포맷(단일 일자 리포트)은 '소계:'가 없고 '영업일수합:'(또는 합계 행) 1줄만 존재할 수 있다.
        one = data[data["일자"].str.startswith("영업일수합:")].copy()
        if one.empty and ("영업매장" in data.columns):
            store_col = data["영업매장"].astype(str).str.strip()
            one = data[store_col.isin(["합계", "총계", "전체"])].copy()
        if one.empty and len(data) == 1:
            one = data.copy()
        if one.empty:
            raise ValueError("manual daily xlsx 파싱 실패: 소계/영업일수합/합계 행 없음")

        # 수동 적재는 날짜 범위가 아닐 수 있으므로, 환경변수/인자 기반으로 날짜를 받아 처리한다.
        # 우선: dag_run.conf/manual_xlsx_path로 호출되는 케이스에서 파일명이 YYYY-MM-DD로 시작하는 경우가 많다.
        inferred = None
        try:
            inferred = re.search(r"(\d{4}-\d{2}-\d{2})", str(path)).group(1)
        except Exception:
            inferred = None
        if not inferred:
            inferred = _infer_sale_date_from_xlsx(str(p))
        if not inferred:
            raise ValueError("manual daily xlsx 파싱 실패: 단일일자 포맷인데 날짜를 파일명/엑셀내용에서 추론 실패")
        one = one.iloc[:1].copy()
        one["sale_date"] = inferred
        sub = one[["sale_date"] + [c for c in one.columns if c != "sale_date"]].copy()

    def _num(v) -> int: 
        return int(pd.to_numeric(str(v).replace(",", "").strip(), errors="coerce") or 0) 

    inserted = 0
    for _, r in sub.iterrows():
        sale_date = str(r["sale_date"]).strip()
        ym = sale_date[:7]
        # 날짜×매장별 포맷이면 해당 행의 매장명을 우선 사용
        row_store = str(r.get("영업매장") or r.get("매장명") or store_name).strip()
        store_short = row_store.replace("도리당 ", "", 1)
        out = pd.DataFrame([{
            "sale_date": sale_date,
            "ym": ym,
            "매장명": row_store,
            # 통합 포맷 컬럼명 fallback: 총매출→합매출, 총할인→할인액, 실매출→실매출(동일), 영수건수→객수(수)
            "총매출액": _num(r.get("총매출", r.get("합매출", 0))),
            "총할인액": _num(r.get("총할인", r.get("할인액", 0))),
            "실매출액": _num(r.get("실매출", r.get("실매출액", 0))),
            "영수건수": _num(r.get("영수건수", r.get("객수(수)", 0))),
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }])
        out["_pk"] = out.apply(
            lambda rr: hashlib.md5(f"{rr['sale_date']}|{rr['매장명']}|daily".encode()).hexdigest(),
            axis=1,
        )

        dest = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_daily.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)

        # 해당 날짜는 replace-by-date로 교체
        if dest.exists() and dest.stat().st_size > 0:
            try:
                existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
            except Exception:
                existing = pd.read_csv(dest, dtype=str)
            existing = existing[existing["sale_date"].astype(str).str.strip() != sale_date].copy() if "sale_date" in existing.columns else existing
            merged = pd.concat([existing, out], ignore_index=True)
        else:
            merged = out

        onedrive_csv_save(df=merged, file_path=str(dest), pk_col="_pk", timestamp_col="collected_at", if_exists="replace") 
        inserted += 1 

    # watch dir에서 자동탐지한 파일은 재처리를 방지하기 위해 processed/로 이동
    try:
        watch_dir = str(conf.get("manual_watch_dir") or os.getenv("OKPOS_DAILY_MANUAL_WATCH_DIR", "")).strip() or "/opt/airflow/download"
        watch_dir = str(Path(watch_dir))
        if str(p).startswith(watch_dir) and p.exists():
            processed = Path(watch_dir) / "processed"
            processed.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{p.stem}__ingested__{ts}{p.suffix}"
            p.rename(processed / new_name)
    except Exception:
        # 이동 실패는 적재 성공을 막지 않는다.
        pass

    return f"manual daily xlsx 적재 완료: {inserted}일 | path={path} | store={store_name}"


def save_to_raw(**context) -> str:
    """xlsx 다운로드 파일 → 합계 제거 + 매장명 추가 → 파티션 CSV 저장"""
    from modules.load.load_onedrive import onedrive_csv_save

    today_files   = context["ti"].xcom_pull(task_ids="download_today",   key="today_files")   or {}
    receipt_files = context["ti"].xcom_pull(task_ids="download_receipt", key="receipt_files") or {}
    daily_files   = context["ti"].xcom_pull(task_ids="download_daily",   key="daily_files")   or {}

    saved: list = []
    save_errors: list[str] = []
    # page_type → (xcom_files, csv_filename)
    page_map = {
        "today":   (today_files,   "okpos_order"),        # 당일매출 → okpos_order.csv
        "receipt": (receipt_files, "okpos_order_item"),   # 영수증별매출 → okpos_order_item.csv
        "daily":   (daily_files,   "okpos_daily"),        # 일자별 종합매출 → okpos_daily.csv
    }

    conf = context.get("dag_run").conf or {}
    replace_by_date = bool(conf.get("replace_by_date") or conf.get("force_redownload") or os.getenv("OKPOS_REPLACE_BY_DATE", ""))

    for page_type, (files, csv_name) in page_map.items():
        for key, src_str in files.items():
            # key format: {sale_date}__{store_name}
            try:
                sale_date, store_name = key.split("__", 1)
            except Exception:
                sale_date, store_name = "unknown", key
            store_short = store_name.replace("도리당 ", "", 1)
            ym_guess = sale_date[:7] if sale_date and sale_date != "unknown" else None

            if not src_str:
                logger.warning(f"파일 없음 (다운로드 실패): {page_type} | {key}")
                save_errors.append(f"{page_type}:{key}:download_missing_path")
                continue
            if src_str == "__NO_DATA__":
                logger.info(f"데이터 없음(휴무/미영업) → skip: {page_type} | {key}")
                if ym_guess:
                    _mark_no_data(store_short, ym_guess, csv_name, sale_date, reason=f"{page_type}:download_no_data")
                continue
            src = Path(src_str)
            if not src.exists():
                logger.warning(f"파일 경로 없음: {src}")
                save_errors.append(f"{page_type}:{key}:path_not_exists")
                continue

            try:
                raw_df = _read_okpos_excel(str(src))
                likely_okpos = _is_likely_okpos_report(raw_df)

                if page_type == "daily":
                    df, ym = _transform_okpos_daily_df(raw_df, store_name, sale_date)
                    df = _fallback_daily_from_order_csv(df, store_short, sale_date, ym)
                else:
                    df, ym = _transform_okpos_df(raw_df, store_name, sale_date)
                if df.empty:
                    # 매출 없음(휴무/미영업)이어도 엑셀은 내려올 수 있다.
                    # 헤더가 정상으로 보이면 NO_DATA로 마킹하여 이후 누락 체크/재시도에서 제외한다.
                    if likely_okpos:
                        logger.info(f"매출 없음 → NO_DATA 마킹: {page_type} | {key}")
                        _mark_no_data(store_short, ym, csv_name, sale_date, reason=f"{page_type}:empty_after_transform")
                        src.unlink(missing_ok=True)
                        continue

                    logger.warning(f"변환 후 빈 DataFrame(헤더 의심) → 스킵(누락으로 남김): {page_type} | {key}")
                    save_errors.append(f"{page_type}:{key}:empty_df_suspect_header")
                    # 디버깅 위해 원본 파일은 남긴다.
                    continue

                # 파티션 경로: brand=도리당/store=동두천지행점/ym=2026-04/{okpos_order|okpos_order_item}.csv
                dest = (
                    RAW_OKPOS_SALES
                    / "brand=도리당"
                    / f"store={store_short}"
                    / f"ym={ym}"
                    / f"{csv_name}.csv"
                )
                dest.parent.mkdir(parents=True, exist_ok=True)

                # 기존 CSV의 _pk가 hash()로 생성된 경우 → MD5로 재계산 후 중복 제거
                # (hash()는 PYTHONHASHSEED 랜덤화로 프로세스마다 값이 달라 dedup 무력화)
                if dest.exists():
                    try:
                        existing_df = pd.read_csv(dest)
                        if "_pk" in existing_df.columns:
                            n_orig_ex = len(existing_df.columns) - 5  # 파생 컬럼 5개 제외
                            existing_df["_pk"] = existing_df.iloc[:, :n_orig_ex].apply(
                                lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
                                axis=1,
                            )
                            before = len(existing_df)
                            existing_df.drop_duplicates(subset=["_pk"], keep="last", inplace=True)
                            existing_df.to_csv(dest, index=False, encoding="utf-8-sig")
                            logger.info(
                                f"기존 CSV _pk 재계산: {dest.name} | "
                                f"중복제거={before - len(existing_df)}건"
                            )
                    except Exception:
                        logger.warning(f"기존 CSV _pk 재계산 실패 (무시): {dest}")

                if replace_by_date and ("sale_date" in df.columns):
                    # 강제 재다운로드/보정 목적: 같은 날짜 데이터는 교체(덮어쓰기)한다.
                    if dest.exists() and dest.stat().st_size > 0:
                        try:
                            existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
                        except Exception:
                            existing = pd.read_csv(dest, dtype=str)
                        if "sale_date" in existing.columns:
                            existing = existing[existing["sale_date"].astype(str).str.strip() != str(sale_date).strip()].copy()
                        merged = pd.concat([existing, df], ignore_index=True)
                    else:
                        merged = df
                    result = onedrive_csv_save(
                        df=merged,
                        file_path=str(dest),
                        pk_col="_pk",
                        timestamp_col="collected_at",
                        if_exists="replace",
                    )
                else:
                    result = onedrive_csv_save(
                        df=df,
                        file_path=str(dest),
                        pk_col="_pk",
                        timestamp_col="collected_at",
                        if_exists="append",
                    )
                src.unlink(missing_ok=True)
                saved.append(str(dest))
                _unmark_no_data(store_short, ym, csv_name, sale_date)
                logger.info(
                    f"저장 완료: {dest.relative_to(RAW_OKPOS_SALES)} | "
                    f"신규={result.get('inserted',0)} 중복={result.get('duplicated',0)}"
                )
            except ValueError as e:
                msg = str(e)
                # 다운로드 실패/로그인 페이지/빈 HTML 등은 운영상 흔한 케이스라 경고 후 스킵
                if (
                    "xlsx 파일이 HTML입니다" in msg
                    or "xlsx 파일이 zip 포맷이 아닙니다" in msg
                    or "HTML에 테이블이 없습니다" in msg
                    or "OKPOS 매출 컬럼을 감지하지 못했습니다" in msg
                    or "이미지 파일입니다" in msg
                ):
                    logger.warning(f"변환 스킵(다운로드 오류 추정): {page_type} | {key} | {msg}")
                    src.unlink(missing_ok=True)
                    save_errors.append(f"{page_type}:{key}:download_parse_error")
                    continue
                logger.exception(f"변환/저장 실패: {page_type} | {key}")
                save_errors.append(f"{page_type}:{key}:{type(e).__name__}")
            except ImportError as e:
                logger.warning(f"변환 스킵(HTML 파서 의존성 없음): {page_type} | {key} | {e}")
                src.unlink(missing_ok=True)
                save_errors.append(f"{page_type}:{key}:missing_html_deps")
                continue
            except Exception:
                logger.exception(f"변환/저장 실패: {page_type} | {key}")
                save_errors.append(f"{page_type}:{key}:unknown_error")

    context["ti"].xcom_push(key="saved_files", value=saved)
    logger.info(f"save_to_raw 완료: {len(saved)}개 저장")
    if save_errors:
        preview = ", ".join(save_errors[:10])
        critical_errors = [err for err in save_errors if err.startswith("daily:")]
        if critical_errors:
            critical_preview = ", ".join(critical_errors[:10])
            raise RuntimeError(f"save_to_raw 실패 {len(critical_errors)}건 (예: {critical_preview})")
        logger.warning(f"save_to_raw 일부 스킵 {len(save_errors)}건 (후속 누락 보정 대상, 예: {preview})")
    return f"변환 저장 완료: {len(saved)}개"


def check_and_fill_missing_today(**context) -> str:
    """today/receipt 저장 결과를 대조해서 누락분만 재다운로드해 채운다.

    save_to_raw 이후 실행:
    - today는 있는데 receipt가 없는 (또는 반대) 조합을 찾아서, 없는 쪽만 재다운로드/저장
    - 둘 다 없으면 수동 확인 대상으로만 남김 (자동 다운로드하지 않음)
    """
    from modules.load.load_onedrive import onedrive_csv_save

    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    if not sale_dates:
        return "sale_dates 없음, 스킵"

    # NOTE: 기존 구현은 (sale_date, store) 조합마다 CSV를 매번 읽어서,
    # 날짜 범위가 길면 I/O 과다로 느려지거나 시간초과가 날 수 있다.
    # CSV별로 한 번만 읽어서 sale_date set을 캐시한다.
    sale_date_cache: dict[Path, tuple[set[str] | None, bool]] = {}
    no_data_cache: dict[Path, set[str]] = {}

    def _is_marked_no_data(csv_path: Path, sale_date: str) -> bool:
        marker_path = csv_path.parent / f".no_data__{csv_path.stem}.txt"
        cached = no_data_cache.get(marker_path)
        if cached is None:
            cached = _read_no_data_dates(marker_path)
            no_data_cache[marker_path] = cached
        return sale_date in cached

    def _load_sale_date_set(csv_path: Path) -> tuple[set[str] | None, bool]:
        """(sale_date_set | None, exists) 반환.

        - 정상: sale_date_set={...}, exists=True
        - 파일 없음: sale_date_set=set(), exists=False
        - 컬럼/포맷 문제 등으로 정확한 추출 불가: sale_date_set=None, exists=True
          (이 경우, 해당 파일이 '어떤 날짜든' 데이터가 있다고 보고 보수적으로 True 처리)
        """
        cached = sale_date_cache.get(csv_path)
        if cached is not None:
            return cached

        if not csv_path.exists():
            sale_date_cache[csv_path] = (set(), False)
            return sale_date_cache[csv_path]

        # 1) 표준: sale_date 컬럼을 chunk로 읽어 set 구성
        try:
            import re as _re

            pat = _re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
            dates: set[str] = set()
            for chunk in pd.read_csv(
                csv_path,
                dtype=str,
                usecols=["sale_date"],
                encoding="utf-8-sig",
                chunksize=200_000,
            ):
                ser = chunk["sale_date"].astype(str).str.strip()
                # "nan"/빈 값 제거
                for d in ser.unique().tolist():
                    if not d or d == "nan":
                        continue
                    m = pat.search(d)
                    dates.add(m.group(0) if m else d)
            sale_date_cache[csv_path] = (dates, True)
            return sale_date_cache[csv_path]
        except Exception as e:
            logger.warning(f"sale_date 컬럼 읽기 실패 (fallback 시도): {csv_path} | {e}")

        # 2) Fallback: 파일 내에서 YYYY-MM-DD 패턴을 제한적으로 스캔
        try:
            import re as _re

            pat = _re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
            found: set[str] = set()
            read_chars = 0
            max_chars = 2_000_000  # 너무 큰 파일 전체 스캔 방지 (대략 2MB)

            with csv_path.open("r", encoding="utf-8-sig", errors="ignore") as f:
                for line in f:
                    read_chars += len(line)
                    for m in pat.findall(line):
                        found.add(m)
                    if read_chars >= max_chars:
                        break

            if found:
                sale_date_cache[csv_path] = (found, True)
            else:
                # 날짜를 못 찾았더라도 파일이 존재/비어있지 않으면 "있다고 가정"할 수밖에 없음
                # (구버전 CSV 등으로 sale_date 컬럼이 없을 때, 누락 채우기를 막지 않기 위함)
                if csv_path.stat().st_size > 0:
                    sale_date_cache[csv_path] = (None, True)
                else:
                    sale_date_cache[csv_path] = (set(), False)
            return sale_date_cache[csv_path]
        except Exception as e:
            logger.warning(f"fallback 스캔 실패 (있다고 가정): {csv_path} | {e}")
            sale_date_cache[csv_path] = (None, True)
            return sale_date_cache[csv_path]

    def _has_sale_date(csv_path: Path, sale_date: str) -> bool:
        # NOTE:
        # `.no_data__*.txt`는 "해당 날짜는 데이터가 없음"을 의미한다.
        # 대조/누락 채우기 단계에서는 이를 '존재'로 취급하면
        # (오늘(today)은 있는데 receipt만 NO_DATA로 잘못 마킹된 케이스 등)
        # 누락 채우기가 영원히 막히므로, '없음'으로 취급한다.
        if _is_marked_no_data(csv_path, sale_date):
            return False
        dates, exists = _load_sale_date_set(csv_path)
        if not exists:
            return False
        if dates is None:
            return True
        return sale_date in dates

    missing_today_keys: list[tuple[str, dict]] = []
    missing_receipt_keys: list[tuple[str, dict]] = []
    for sale_date in sale_dates:
        ym = sale_date[:7]
        for store in STORES:
            if not _should_collect(store["name"], sale_date):
                continue
            store_short = store["name"].replace("도리당 ", "", 1)

            today_csv = (
                RAW_OKPOS_SALES
                / "brand=도리당"
                / f"store={store_short}"
                / f"ym={ym}"
                / "okpos_order.csv"
            )
            receipt_csv = (
                RAW_OKPOS_SALES
                / "brand=도리당"
                / f"store={store_short}"
                / f"ym={ym}"
                / "okpos_order_item.csv"
            )

            has_today = _has_sale_date(today_csv, sale_date)
            has_receipt = _has_sale_date(receipt_csv, sale_date)

            # today는 있는데 receipt가 없는 경우 → receipt만 재다운로드
            if has_today and not has_receipt:
                missing_receipt_keys.append((sale_date, store))
            # receipt는 있는데 today가 없는 경우 → today만 재다운로드
            elif has_receipt and not has_today:
                missing_today_keys.append((sale_date, store))
            # 둘 다 없으면 today+receipt 모두 재다운로드
            elif (not has_today) and (not has_receipt):
                missing_today_keys.append((sale_date, store))
                missing_receipt_keys.append((sale_date, store))

    logger.info(
        "today/receipt 대조 완료: sale_dates=%d, stores=%d, csv_cache=%d",
        len(sale_dates),
        len(STORES),
        len(sale_date_cache),
    )

    if not missing_today_keys and not missing_receipt_keys:
        logger.info("today/receipt 누락 항목 없음")
        return "누락 항목 없음"

    if missing_today_keys:
        logger.warning(
            "today 누락 %d건 재다운로드 시작: %s",
            len(missing_today_keys),
            [(d, s["name"]) for d, s in missing_today_keys],
        )
    if missing_receipt_keys:
        logger.warning(
            "receipt 누락 %d건 재다운로드 시작: %s",
            len(missing_receipt_keys),
            [(d, s["name"]) for d, s in missing_receipt_keys],
        )
    def _fill_page(page_type: str, pending_keys: list[tuple[str, dict]]) -> tuple[int, list[str]]:
        if not pending_keys:
            return 0, []

        download_dir = TEMP_DIR / f"okpos_download_fill_{page_type}"
        download_dir.mkdir(parents=True, exist_ok=True)
        page_cfg = PAGE_TYPES[page_type]
        csv_name = "okpos_order" if page_type == "today" else "okpos_order_item"

        def _session(driver, wait, pending, results):
            _login(driver, wait)
            _setup_download_dir(driver, download_dir)
            driver.get(page_cfg["url"])
            import time as _time
            _time.sleep(2)
            for sale_date, store in pending:
                key = f"{sale_date}__{store['name']}"
                downloaded = _download_excel_for_store(
                    driver, wait, page_type, page_cfg, sale_date, store, download_dir
                )
                if downloaded == "__NO_DATA__":
                    results[key] = "__NO_DATA__"
                elif downloaded:
                    results[key] = str(downloaded)

        results = _download_with_retry(page_type, pending_keys, download_dir, _session)

        filled = 0
        failed: list[str] = []
        for key, src_str in results.items():
            if not src_str:
                failed.append(key)
                continue
            if src_str == "__NO_DATA__":
                sale_date, store_name = key.split("__", 1)
                store_short = store_name.replace("도리당 ", "", 1)
                ym = sale_date[:7]
                _mark_no_data(store_short, ym, csv_name, sale_date, reason=f"{page_type}:download_no_data_fill")
                continue
            src = Path(src_str)
            if not src.exists():
                failed.append(key)
                continue
            sale_date, store_name = key.split("__", 1)
            store_short = store_name.replace("도리당 ", "", 1)
            try:
                raw_df = _read_okpos_excel(str(src))
                likely_okpos = _is_likely_okpos_report(raw_df)
                df, ym = _transform_okpos_df(raw_df, store_name, sale_date)
                if df.empty:
                    if likely_okpos:
                        logger.info(f"재다운로드 결과 매출 없음 → NO_DATA 마킹: {page_type} | {key}")
                        _mark_no_data(store_short, ym, csv_name, sale_date, reason=f"{page_type}:empty_after_transform_fill")
                        src.unlink(missing_ok=True)
                        continue
                    logger.warning(f"재다운로드 후 빈 데이터(헤더 의심) → 실패로 남김: {page_type} | {key}")
                    failed.append(key)
                    continue
                dest = (
                    RAW_OKPOS_SALES
                    / "brand=도리당"
                    / f"store={store_short}"
                    / f"ym={ym}"
                    / f"{csv_name}.csv"
                )
                dest.parent.mkdir(parents=True, exist_ok=True)
                result = onedrive_csv_save(
                    df=df,
                    file_path=str(dest),
                    pk_col="_pk",
                    timestamp_col="collected_at",
                    if_exists="append",
                )
                src.unlink(missing_ok=True)
                filled += 1
                logger.info(f"재저장 완료: {page_type} | {key} | 신규={result.get('inserted', 0)}")
            except Exception:
                logger.exception(f"재저장 실패: {page_type} | {key}")
                failed.append(key)

        return filled, failed

    filled_today, failed_today = _fill_page("today", missing_today_keys)
    filled_receipt, failed_receipt = _fill_page("receipt", missing_receipt_keys)

    msg = (
        "today/receipt 누락 채우기 완료"
        f" | today 성공={filled_today}, 실패={len(failed_today)}"
        f" | receipt 성공={filled_receipt}, 실패={len(failed_receipt)}"
    )
    if failed_today:
        msg += f" | today 실패 목록: {failed_today}"
    if failed_receipt:
        msg += f" | receipt 실패 목록: {failed_receipt}"
    logger.info(msg)
    return msg


def reconcile_against_daily_summary(**context) -> str:
    """daily(일자별 종합매출)와 today(order) 정합성을 맞춘다.

    - daily가 0원인데 order가 비0원이면 daily 수집 오류로 보고 daily를 먼저 재수집한다.
    - daily 재수집도 0원이면 order 합계 fallback으로 daily를 보정한다.
    - 그 외 mismatch는 기존처럼 today를 재다운로드한다.
    """
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    if not sale_dates:
        return "sale_dates 없음, 스킵"

    tol = int(os.getenv("OKPOS_DAILY_VALIDATE_TOLERANCE", "0"))
    max_attempts = int(os.getenv("OKPOS_DAILY_RECONCILE_MAX_ATTEMPTS", "2"))

    def _signed_amount(df: pd.DataFrame, amount: pd.Series) -> pd.Series:
        if "구분" not in df.columns:
            return amount
        gbn = df["구분"].fillna("").astype(str).str.strip()
        sign = gbn.map(lambda v: -1 if v == "반품" else 1).fillna(1).astype(int)
        return amount * sign

    def _load_daily_expected(sale_date: str, store: dict) -> tuple[int | None, str | None]:
        ym = sale_date[:7]
        store_short = store["name"].replace("도리당 ", "", 1)
        daily_csv = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_daily.csv"
        if not daily_csv.exists():
            return None, "daily_missing"
        if sale_date in _read_no_data_dates(daily_csv.parent / ".no_data__okpos_daily.txt"):
            return None, "daily_no_data"
        try:
            ddf = pd.read_csv(daily_csv, dtype=str)
        except Exception as e:
            return None, f"daily_read_error({type(e).__name__})"
        ddf = ddf[ddf["sale_date"].astype(str).str.strip() == sale_date].copy()
        if ddf.empty:
            return None, "daily_empty_date"
        if "실매출액" not in ddf.columns:
            return None, "daily_no_net_col"
        expected = int(pd.to_numeric(ddf["실매출액"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int).sum())
        return expected, None

    def _sum_csv(csv_path: Path, sale_date: str, col: str) -> tuple[int | None, str | None]:
        if not csv_path.exists():
            return None, "missing"
        if sale_date in _read_no_data_dates(csv_path.parent / f".no_data__{csv_path.stem}.txt"):
            return None, "no_data"
        try:
            df = pd.read_csv(csv_path, dtype=str)
        except Exception as e:
            return None, f"read_error({type(e).__name__})"
        if "sale_date" not in df.columns:
            return None, "no_sale_date"
        df = df[df["sale_date"].astype(str).str.strip() == sale_date].copy()
        if df.empty:
            return None, "empty_date"
        # CSV에 저장된 실매출액은 반품 행이 이미 음수로 저장되어 있으므로 직접 합산
        amt = _to_int_series(df[col]) if col in df.columns else pd.Series(0, index=df.index)
        return int(amt.sum()), None

    def _upsert_by_date(dest: Path, df_new: pd.DataFrame, sale_date: str) -> None:
        from modules.load.load_onedrive import onedrive_csv_save

        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.stat().st_size > 0:
            try:
                existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
            except Exception:
                existing = pd.read_csv(dest, dtype=str)
            if "sale_date" in existing.columns:
                existing = existing[existing["sale_date"].astype(str).str.strip() != sale_date].copy()
            merged = pd.concat([existing, df_new], ignore_index=True)
        else:
            merged = df_new.copy()
        onedrive_csv_save(df=merged, file_path=str(dest), pk_col="_pk", timestamp_col="collected_at", if_exists="replace")

    def _redownload_page(page_type: str, pending_keys: list[tuple[str, dict]], attempt_tag: str) -> int:
        if not pending_keys:
            return 0
        download_dir = TEMP_DIR / f"okpos_download_daily_reconcile_{page_type}_{attempt_tag}"
        download_dir.mkdir(parents=True, exist_ok=True)
        page_cfg = PAGE_TYPES[page_type]
        csv_stem = "okpos_order" if page_type == "today" else "okpos_order_item" if page_type == "receipt" else "okpos_daily"

        _current_key: list[str | None] = [None]

        def _session(driver, wait, pending, results):
            _login(driver, wait)
            _setup_download_dir(driver, download_dir)
            driver.get(page_cfg["url"])
            time.sleep(2)
            for sale_date, store in pending:
                key = f"{sale_date}__{store['name']}"
                _current_key[0] = key
                downloaded = _download_excel_for_store(driver, wait, page_type, page_cfg, sale_date, store, download_dir)
                _current_key[0] = None
                if downloaded == "__NO_DATA__":
                    results[key] = "__NO_DATA__"
                elif downloaded:
                    results[key] = str(downloaded)

        results = _download_with_retry(page_type, pending_keys, download_dir, _session, current_key=_current_key)
        saved = 0
        for key, src_str in results.items():
            sale_date, store_name = key.split("__", 1)
            store_short = store_name.replace("도리당 ", "", 1)
            ym = sale_date[:7]
            dest = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / f"{csv_stem}.csv"

            if src_str == "__NO_DATA__":
                _mark_no_data(store_short, ym, csv_stem, sale_date, reason=f"daily_reconcile:{page_type}:no_data({attempt_tag})")
                continue
            if not src_str:
                continue
            src = Path(src_str)
            if not src.exists():
                continue

            try:
                raw_df = _read_okpos_excel(str(src))
                likely_okpos = _is_likely_okpos_report(raw_df)
                if page_type == "daily":
                    df_new, ym_new = _transform_okpos_daily_df(raw_df, store_name, sale_date)
                    df_new = _fallback_daily_from_order_csv(df_new, store_short, sale_date, ym_new)
                else:
                    df_new, ym_new = _transform_okpos_df(raw_df, store_name, sale_date)
                if df_new.empty:
                    if likely_okpos:
                        _mark_no_data(store_short, ym, csv_stem, sale_date, reason=f"daily_reconcile:{page_type}:empty({attempt_tag})")
                    continue
                dest = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym_new}" / f"{csv_stem}.csv"
                _upsert_by_date(dest, df_new, sale_date)
                saved += 1
            except Exception:
                logger.exception("daily reconcile 저장 실패: %s | %s", attempt_tag, key)
            finally:
                src.unlink(missing_ok=True)
        return saved

    remaining: list[tuple[str, dict]] = [(d, s) for d in sale_dates for s in STORES if _should_collect(s["name"], d)]
    for attempt in range(1, max_attempts + 1):
        mism_daily: list[tuple[str, dict]] = []
        mism_today: list[tuple[str, dict]] = []
        mism_receipt: list[tuple[str, dict]] = []
        details: list[str] = []

        for sale_date, store in remaining:
            expected, reason = _load_daily_expected(sale_date, store)
            if reason or expected is None:
                continue
            ym = sale_date[:7]
            store_short = store["name"].replace("도리당 ", "", 1)
            order_csv = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_order.csv"
            item_csv = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_order_item.csv"

            order_sum, oreason = _sum_csv(order_csv, sale_date, "실매출액")

            # order vs daily 비교 (order CSV는 반품 행이 음수로 저장되어 직접 합산 = 순매출)
            if order_sum is not None and abs(expected - order_sum) > tol:
                if expected == 0 and order_sum != 0:
                    mism_daily.append((sale_date, store))
                    details.append(f"{sale_date}__{store['name']}:daily_zero_order_nonzero daily={expected}, order={order_sum}, diff={expected-order_sum}")
                else:
                    mism_today.append((sale_date, store))
                    details.append(f"{sale_date}__{store['name']}:daily={expected}, order={order_sum}, diff={expected-order_sum}")
            # NOTE: order_item(receipt) vs daily 비교는 제외 — receipt 페이지가 반품 영수증을
            # 미포함하므로 반품 있는 날은 구조적으로 daily와 불일치하며, 재다운로드로 해결 불가

        if not mism_daily and not mism_today and not mism_receipt:
            msg = f"daily 기준 reconcile 완료: mismatch 없음 (tol={tol}) | attempts={attempt}"
            logger.info(msg)
            return msg

        logger.warning(
            "daily/order mismatch 발견(attempt=%d): daily=%d, today=%d, receipt=%d | 예: %s",
            attempt,
            len(mism_daily),
            len(mism_today),
            len(mism_receipt),
            details[:10],
        )

        saved_d = _redownload_page("daily", mism_daily, attempt_tag=str(attempt))
        saved_t = _redownload_page("today", mism_today, attempt_tag=str(attempt))
        saved_r = _redownload_page("receipt", mism_receipt, attempt_tag=str(attempt))
        logger.info(
            "daily/order reconcile attempt %d: daily 재저장=%d, today 재저장=%d, receipt 재저장=%d",
            attempt,
            saved_d,
            saved_t,
            saved_r,
        )

    # 최대 재시도 후에도 mismatch 남으면 로그/XCom로 남김(실패시키지 않음)
    try:
        context["ti"].xcom_push(key="okpos_daily_reconcile_details", value=details)
    except Exception:
        pass
    return f"daily 기준 reconcile 종료: mismatch 남음 (tol={tol}) | 예: {details[:20]}"


def validate_today_vs_receipt(**context) -> str:
    """today(okpos_order) vs receipt(okpos_order_item) 합계 정합성 검증.

    - (sale_date, store) 기준으로 okpos_order 합계와 okpos_order_item 합계가 일치하는지 확인
    - 불일치 시 수집 누락/중복/PK 충돌 가능성이 높으므로 실패 처리(알림/수동 조치 유도)

    비교 기준:
    - okpos_order: '실매출액' 합(할인 반영 후 금액) + 반품은 음수 차감
    - okpos_order_item: '실매출액' 합 + 반품은 음수 차감

    허용 오차:
    - OKPOS_VALIDATE_TOLERANCE 환경변수(기본 0원)

    NOTE:
    - `.no_data__*.txt`로 마킹된 날짜는 검증에서 제외한다.
    """
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    if not sale_dates:
        return "sale_dates 없음, 스킵"

    tol = int(os.getenv("OKPOS_VALIDATE_TOLERANCE", "0"))
    failures: list[str] = []
    checked = 0

    def _signed_amount(df: pd.DataFrame, amount: pd.Series) -> pd.Series:
        if "구분" not in df.columns:
            return amount
        gbn = df["구분"].fillna("").astype(str).str.strip()
        sign = gbn.map(lambda v: -1 if v == "반품" else 1).fillna(1).astype(int)
        return amount * sign

    for sale_date in sale_dates:
        ym = sale_date[:7]
        for store in STORES:
            if not _should_collect(store["name"], sale_date):
                continue
            store_short = store["name"].replace("도리당 ", "", 1)

            order_csv = (
                RAW_OKPOS_SALES
                / "brand=도리당"
                / f"store={store_short}"
                / f"ym={ym}"
                / "okpos_order.csv"
            )
            item_csv = (
                RAW_OKPOS_SALES
                / "brand=도리당"
                / f"store={store_short}"
                / f"ym={ym}"
                / "okpos_order_item.csv"
            )

            # NO_DATA 마커가 있으면 검증 제외
            if sale_date in _read_no_data_dates(order_csv.parent / ".no_data__okpos_order.txt"):
                continue
            if sale_date in _read_no_data_dates(item_csv.parent / ".no_data__okpos_order_item.txt"):
                continue

            if not order_csv.exists() or not item_csv.exists():
                failures.append(
                    f"{sale_date}__{store['name']}:missing_csv(order={order_csv.exists()}, item={item_csv.exists()})"
                )
                continue

            try:
                order_df = pd.read_csv(order_csv, dtype=str)
                item_df = pd.read_csv(item_csv, dtype=str)
            except Exception as e:
                failures.append(f"{sale_date}__{store['name']}:read_error({type(e).__name__})")
                continue

            order_df = order_df[order_df["sale_date"].astype(str).str.strip() == sale_date].copy()
            item_df = item_df[item_df["sale_date"].astype(str).str.strip() == sale_date].copy()

            if order_df.empty or item_df.empty:
                failures.append(
                    f"{sale_date}__{store['name']}:empty_date_rows(order={len(order_df)}, item={len(item_df)})"
                )
                continue

            # order: 매출 행만 합산 (반품 행은 receipt 페이지에 미포함이므로 매출분만 비교)
            if "구분" in order_df.columns:
                order_sales_df = order_df[order_df["구분"].fillna("").astype(str).str.strip() != "반품"]
            else:
                order_sales_df = order_df
            order_sum = int(
                (_to_int_series(order_sales_df["실매출액"]) if "실매출액" in order_sales_df.columns else pd.Series(0, index=order_sales_df.index)).sum()
            )

            # item sum: receipt 페이지는 매출 행만 존재 → 직접 합산
            item_sum = int(
                (_to_int_series(item_df["실매출액"]) if "실매출액" in item_df.columns else pd.Series(0, index=item_df.index)).sum()
            )

            checked += 1
            diff = abs(order_sum - item_sum)
            if diff > tol:
                failures.append(
                    f"{sale_date}__{store['name']}:mismatch(order={order_sum}, item={item_sum}, diff={diff}, tol={tol})"
                )

    msg = f"today vs receipt 검증 완료: checked={checked}, failures={len(failures)}, tol={tol}"
    if failures:
        preview = ", ".join(failures[:50])
        logger.error("%s | 예: %s", msg, preview)
        try:
            context["ti"].xcom_push(key="okpos_validate_failures", value=failures)
        except Exception:
            pass
        # 운영 안정성: mismatch가 있어도 task는 실패시키지 않는다.
        return f"{msg} | WARN 예: {preview}"
    logger.info(msg)
    return msg


def reconcile_today_vs_receipt(**context) -> str:
    """okpos_order 합계 기준으로 today/receipt를 재수집/재저장하여 정합성을 최대한 맞춘다.

    - mismatch 탐지 → receipt 재수집/덮어쓰기
    - 그래도 mismatch 남으면 today+receipt 모두 재수집/덮어쓰기

    IMPORTANT:
    - OKPOS 리포트 정의 차이로 원천 자체가 불일치하는 케이스가 존재할 수 있어,
      이 task는 기본적으로 실패시키지 않고 mismatch 상세를 XCom/로그로 남긴다.
    """
    from modules.load.load_onedrive import onedrive_csv_save

    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    if not sale_dates:
        return "sale_dates 없음, 스킵"

    tol = int(os.getenv("OKPOS_VALIDATE_TOLERANCE", "0"))
    max_attempts = int(os.getenv("OKPOS_RECONCILE_MAX_ATTEMPTS", "2"))

    def _signed_amount(df: pd.DataFrame, amount: pd.Series) -> pd.Series:
        if "구분" not in df.columns:
            return amount
        gbn = df["구분"].fillna("").astype(str).str.strip()
        sign = gbn.map(lambda v: -1 if v == "반품" else 1).fillna(1).astype(int)
        return amount * sign

    def _sums_for_one(sale_date: str, store: dict) -> tuple[int | None, int | None, int | None, str | None]:
        """(order_sum, item_sum_actual, item_sum_total, reason_if_skipped)"""
        ym = sale_date[:7]
        store_short = store["name"].replace("도리당 ", "", 1)
        order_csv = (
            RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_order.csv"
        )
        item_csv = (
            RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_order_item.csv"
        )

        # NO_DATA는 비교 제외(휴무 등)
        if sale_date in _read_no_data_dates(order_csv.parent / ".no_data__okpos_order.txt"):
            return None, None, None, "order_no_data"
        if sale_date in _read_no_data_dates(item_csv.parent / ".no_data__okpos_order_item.txt"):
            return None, None, None, "item_no_data"

        if not order_csv.exists() or not item_csv.exists():
            return None, None, None, f"missing_csv(order={order_csv.exists()}, item={item_csv.exists()})"

        try:
            order_df = pd.read_csv(order_csv, dtype=str)
            item_df = pd.read_csv(item_csv, dtype=str)
        except Exception as e:
            return None, None, None, f"read_error({type(e).__name__})"

        order_df = order_df[order_df["sale_date"].astype(str).str.strip() == sale_date].copy()
        item_df = item_df[item_df["sale_date"].astype(str).str.strip() == sale_date].copy()
        if order_df.empty or item_df.empty:
            return None, None, None, f"empty_date_rows(order={len(order_df)}, item={len(item_df)})"

        # CSV 저장 시 반품 행이 이미 음수로 저장 → 직접 합산 (sign flip 금지)
        # order: 매출 행만 합산 (반품 행은 receipt 페이지에 미포함이므로 매출분만 비교)
        if "구분" in order_df.columns:
            order_sales_df = order_df[order_df["구분"].fillna("").astype(str).str.strip() != "반품"]
        else:
            order_sales_df = order_df
        order_sum = int(
            (_to_int_series(order_sales_df["실매출액"]) if "실매출액" in order_sales_df.columns else pd.Series(0, index=order_sales_df.index)).sum()
        )
        item_sum_actual = int(
            (_to_int_series(item_df["실매출액"]) if "실매출액" in item_df.columns else pd.Series(0, index=item_df.index)).sum()
        )
        item_sum_total = int(
            (_to_int_series(item_df["총매출액"]) if "총매출액" in item_df.columns else pd.Series(0, index=item_df.index)).sum()
        )
        return order_sum, item_sum_actual, item_sum_total, None

    def _find_mismatches() -> tuple[list[tuple[str, dict]], list[str], list[str]]:
        mismatches: list[tuple[str, dict]] = []
        skipped: list[str] = []
        details: list[str] = []
        for sale_date in sale_dates:
            for store in STORES:
                if not _should_collect(store["name"], sale_date):
                    continue
                o_sum, i_sum_a, i_sum_t, reason = _sums_for_one(sale_date, store)
                if reason:
                    skipped.append(f"{sale_date}__{store['name']}:{reason}")
                    continue
                if o_sum is None or i_sum_a is None or i_sum_t is None:
                    continue
                if abs(o_sum - i_sum_a) > tol:
                    mismatches.append((sale_date, store))
                    details.append(
                        f"{sale_date}__{store['name']}:order={o_sum}, item_actual={i_sum_a}, item_total={i_sum_t}, diff_actual={o_sum-i_sum_a}, diff_total={o_sum-i_sum_t}"
                    )
        return mismatches, skipped, details

    def _upsert_csv_by_date(dest: Path, df_new: pd.DataFrame, sale_date: str) -> None:
        """dest CSV에서 sale_date 행을 교체하고 저장 (월 파일 단위)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.stat().st_size > 0:
            try:
                existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
            except Exception:
                existing = pd.read_csv(dest, dtype=str)
            if "sale_date" in existing.columns:
                existing = existing[existing["sale_date"].astype(str).str.strip() != sale_date].copy()
            merged = pd.concat([existing, df_new], ignore_index=True)
        else:
            merged = df_new.copy()

        # onedrive_csv_save는 전체 replace만 지원하므로 여기서는 직접 replace 저장
        onedrive_csv_save(df=merged, file_path=str(dest), pk_col="_pk", timestamp_col="collected_at", if_exists="replace")

    def _redownload_and_upsert(page_type: str, pending_keys: list[tuple[str, dict]], attempt_tag: str) -> int:
        if not pending_keys:
            return 0

        download_dir = TEMP_DIR / f"okpos_download_reconcile_{page_type}_{attempt_tag}"
        download_dir.mkdir(parents=True, exist_ok=True)
        page_cfg = PAGE_TYPES[page_type]
        csv_stem = "okpos_order" if page_type == "today" else "okpos_order_item"

        _current_key: list[str | None] = [None]

        def _session(driver, wait, pending, results):
            _login(driver, wait)
            _setup_download_dir(driver, download_dir)
            driver.get(page_cfg["url"])
            time.sleep(2)
            for sale_date, store in pending:
                key = f"{sale_date}__{store['name']}"
                _current_key[0] = key
                downloaded = _download_excel_for_store(driver, wait, page_type, page_cfg, sale_date, store, download_dir)
                _current_key[0] = None
                if downloaded == "__NO_DATA__":
                    results[key] = "__NO_DATA__"
                elif downloaded:
                    results[key] = str(downloaded)

        # 마커 제거(이전 오판/실패로 인해 영구 차단된 케이스 해제)
        for sale_date, store in pending_keys:
            ym = sale_date[:7]
            store_short = store["name"].replace("도리당 ", "", 1)
            _unmark_no_data(store_short, ym, csv_stem, sale_date)

        results = _download_with_retry(page_type, pending_keys, download_dir, _session, current_key=_current_key)

        saved = 0
        for key, src_str in results.items():
            sale_date, store_name = key.split("__", 1)
            store_short = store_name.replace("도리당 ", "", 1)
            ym = sale_date[:7]
            dest = (
                RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / f"{csv_stem}.csv"
            )

            if src_str == "__NO_DATA__":
                _mark_no_data(store_short, ym, csv_stem, sale_date, reason=f"reconcile:{page_type}:download_no_data({attempt_tag})")
                continue
            if not src_str:
                continue
            src = Path(src_str)
            if not src.exists():
                continue

            try:
                raw_df = _read_okpos_excel(str(src))
                likely_okpos = _is_likely_okpos_report(raw_df)
                df_new, ym_new = _transform_okpos_df(raw_df, store_name, sale_date)
                if df_new.empty:
                    if likely_okpos:
                        _mark_no_data(store_short, ym, csv_stem, sale_date, reason=f"reconcile:{page_type}:empty_after_transform({attempt_tag})")
                    continue
                dest = (
                    RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym_new}" / f"{csv_stem}.csv"
                )
                _upsert_csv_by_date(dest, df_new, sale_date)
                saved += 1
            except Exception:
                logger.exception("reconcile 저장 실패: %s | %s", attempt_tag, key)
            finally:
                src.unlink(missing_ok=True)
        return saved

    # mismatch 탐지 및 재수집 loop
    def _run_reconcile() -> str:
        mismatches, skipped, details = _find_mismatches()
        if not mismatches:
            msg = f"reconcile 스킵: mismatch 없음 | skipped={len(skipped)}"
            logger.info(msg)
            return msg

        logger.warning("reconcile 시작: mismatch %d건 | 예: %s", len(mismatches), details[:10])

        remaining = mismatches
        for attempt in range(1, max_attempts + 1):
            # 1차: receipt만 재수집/덮어쓰기
            saved_receipt = _redownload_and_upsert("receipt", remaining, attempt_tag=f"{attempt}")
            logger.info("reconcile attempt %d/%d: receipt 재저장 %d건", attempt, max_attempts, saved_receipt)

            next_remaining: list[tuple[str, dict]] = []
            next_details: list[str] = []
            for sale_date, store in remaining:
                o_sum, i_sum_a, i_sum_t, reason = _sums_for_one(sale_date, store)
                if reason:
                    next_remaining.append((sale_date, store))
                    continue
                if o_sum is None or i_sum_a is None or i_sum_t is None:
                    continue
                if abs(o_sum - i_sum_a) > tol:
                    next_remaining.append((sale_date, store))
                    next_details.append(
                        f"{sale_date}__{store['name']}:order={o_sum}, item_actual={i_sum_a}, item_total={i_sum_t}, diff_actual={o_sum-i_sum_a}, diff_total={o_sum-i_sum_t}"
                    )

            remaining = next_remaining
            if not remaining:
                msg = f"reconcile 완료: mismatch 0건 (tol={tol}) | attempts={attempt}"
                logger.info(msg)
                return msg

            # 2차: receipt만으로 해결 안 되면 today+receipt 모두 재수집/덮어쓰기
            logger.warning("receipt 재수집 후 mismatch %d건 남음 → today+receipt 동시 재수집 시도 | 예: %s", len(remaining), next_details[:10])
            saved_today = _redownload_and_upsert("today", remaining, attempt_tag=f"{attempt}_full")
            saved_receipt2 = _redownload_and_upsert("receipt", remaining, attempt_tag=f"{attempt}_full")
            logger.info("reconcile attempt %d/%d(full): today %d건, receipt %d건 재저장", attempt, max_attempts, saved_today, saved_receipt2)
            logger.info("reconcile attempt %d/%d(full) 완료 후 재검사 진행", attempt, max_attempts)

        # 재다운로드로도 해결되지 않는 케이스는 상세를 남기고 성공 처리
        _, _, details2 = _find_mismatches()
        try:
            context["ti"].xcom_push(key="okpos_reconcile_remaining", value=[(d, s["name"]) for d, s in remaining])
            context["ti"].xcom_push(key="okpos_reconcile_details", value=details2)
        except Exception:
            pass
        detail_preview = " | ".join(details2[:50])
        msg = f"reconcile 완료(부분): mismatch {len(remaining)}건 남음 (tol={tol}) | 예: {detail_preview}"
        logger.error(msg)
        return msg

    try:
        return _run_reconcile()
    except Exception as e:
        # 어떤 예외가 떠도 DAG를 실패시키지 않음(운영 안정성)
        logger.exception("reconcile 예외(무시하고 진행): %s", e)
        try:
            context["ti"].xcom_push(key="okpos_reconcile_exception", value=str(e))
        except Exception:
            pass
        return f"reconcile 예외(무시): {type(e).__name__}: {e}"


def write_log(**context) -> str:
    """log.parquet 실행 이력 기록"""
    saved_files = context["ti"].xcom_pull(task_ids="save_to_raw", key="saved_files") or []
    sale_dates = context["ti"].xcom_pull(task_ids="resolve_dates", key="sale_dates") or []
    log_path = RAW_OKPOS_SALES / "log.parquet"

    try:
        RAW_OKPOS_SALES.mkdir(parents=True, exist_ok=True)

        now_ts = pd.Timestamp.now(tz="Asia/Seoul")
        rows = []
        for file_str in saved_files:
            try:
                p = Path(file_str)
                parts = p.parts
                # 경로: brand=도리당 / store=동두천지행점 / ym=2026-04 / {order|order_item}.csv
                store_short = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("store=")), "unknown")
                ym = next((pt.split("=", 1)[1] for pt in parts if pt.startswith("ym=")), "unknown")
                csv_name = p.stem  # okpos_order or okpos_order_item
                page_type = "today" if csv_name in ("order", "okpos_order") else "receipt" if csv_name in ("order_item", "okpos_order_item") else csv_name
                sale_date = sale_dates[0] if sale_dates else "unknown"

                rows.append({
                    "run_at": now_ts,
                    "sale_date": sale_date,
                    "page_type": page_type,
                    "store": store_short,
                    "ym": ym,
                    "result": "success",
                    "file_path": str(p),
                })
            except Exception as e:
                logger.warning(f"로그 행 파싱 실패: {file_str} | {e}")

        new_df = pd.DataFrame(
            rows,
            columns=["run_at", "sale_date", "page_type", "store", "ym", "result", "file_path"],
        )

        if log_path.exists() and log_path.stat().st_size > 0:
            prev_df = pd.read_parquet(log_path)
            out_df = pd.concat([prev_df, new_df], ignore_index=True)
        else:
            out_df = new_df

        if not out_df.empty and "run_at" in out_df.columns:
            out_df["run_at"] = pd.to_datetime(out_df["run_at"], errors="coerce")
            out_df = out_df.sort_values("run_at", ascending=False).reset_index(drop=True)

        out_df.to_parquet(log_path, index=False)

        logger.info(f"log.parquet 기록 완료: {log_path} | {len(new_df)}건")
        return f"log.parquet 기록 완료: {len(new_df)}건"

    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
