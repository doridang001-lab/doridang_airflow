"""
육계 시세 수집 파이프라인

수집 출처:
- poultry.or.kr (대한양계협회)
- chicken.or.kr (한국육계협회)

처리 흐름:
1. 각 사이트에서 육계 시세 (대) 금일/전일/전월/전년 수집
2. 이상치 검증 (전일 대비 20% 이상 변동 시 플래그)
3. 결과 병합 → CSV 누적 저장
4. 이동평균(20/60/120일) 포함 이메일 알림 발송
"""

import logging
import os
import re
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from modules.transform.utility.mailer import send_email
from modules.transform.utility.paths import CHICKEN_PRICE_CSV_PATH

logger = logging.getLogger(__name__)


# ============================================================
# 내부 유틸
# ============================================================

def _build_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """retry 설정이 적용된 requests.Session 반환"""
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _parse_int(val: str) -> int:
    """숫자 외 문자를 제거하고 int 변환"""
    cleaned = re.sub(r"[^\d]", "", val)
    return int(cleaned) if cleaned else 0


def _launch_headless_browser():
    """헤드리스 Chrome 브라우저 실행 (undetected_chromedriver)

    AIRFLOW_HOME 환경변수가 있으면 headless, 없으면 표시 모드 (로컬 디버깅용)
    """
    import undetected_chromedriver as uc
    from modules.transform.utility.selenium_uc import configure_uc_data_path

    headless = os.getenv("AIRFLOW_HOME") is not None

    def _make_options(version_main: int | None = None):
        """매 시도마다 새 ChromeOptions 생성 (재사용 불가 제약 회피)"""
        opts = uc.ChromeOptions()
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        if Path(chrome_bin).exists():
            opts.binary_location = chrome_bin
        if headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--ignore-certificate-errors")
        return opts

    try:
        configure_uc_data_path()
        driver = uc.Chrome(options=_make_options())
    except Exception as e:
        # Chrome 버전 불일치 시 새 옵션으로 버전 자동 감지 재시도
        version_match = re.search(r"Current browser version is (\d+)", str(e))
        if version_match:
            ver = int(version_match.group(1))
            configure_uc_data_path()
            driver = uc.Chrome(options=_make_options(), version_main=ver)
        else:
            raise
    return driver


def _fetch_chicken_page_source_via_http(url: str) -> tuple[str | None, str | None]:
    """chicken.or.kr 페이지를 HTTP로 먼저 시도해 HTML을 가져온다.

    Selenium이 죽거나(로컬 포트 connection refused) 차단되는 케이스가 있어
    가능한 경우 HTTP 파싱을 우선 사용한다.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    session = _build_session()

    # 컨테이너/런타임에 따라 시스템 CA 번들이 없어서 SSL 검증이 실패하는 케이스가 있음.
    # 1) certifi CA 번들을 우선 사용
    # 2) 그래도 실패하면(수집 목적 한정) SSL 검증을 끄고 재시도
    verify_opt: str | bool = True
    try:
        import certifi  # type: ignore

        verify_opt = certifi.where()
    except Exception:
        verify_opt = True

    try:
        response = session.get(url, headers=headers, timeout=15, verify=verify_opt)
    except requests.exceptions.SSLError as e:
        logger.warning(f"[chicken.or.kr] SSL 검증 실패 → verify=False 재시도: {e}")
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        response = session.get(url, headers=headers, timeout=15, verify=False)
    if not response.ok:
        return None, None
    response.encoding = response.apparent_encoding

    html = response.text
    if "sub_priceTable" not in html:
        return None, None

    return html, url


_CHICKEN_PRICE_DATE_RE = re.compile(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})")


def _normalize_ymd(raw: str) -> str | None:
    """HTML 텍스트에서 YYYY-MM-DD를 뽑아 정규화. 실패 시 None."""
    m = _CHICKEN_PRICE_DATE_RE.search(raw)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _parse_chicken_price_from_html(page_source: str, used_url: str) -> dict:
    """chicken.or.kr HTML에서 table.sub_priceTable을 파싱해 결과 dict를 만든다."""
    source_site = "chicken.or.kr"
    soup = BeautifulSoup(page_source, "html.parser")
    table = soup.select_one("div.sub_priceTableWrap table.sub_priceTable") or soup.select_one("table.sub_priceTable")
    if table is None:
        raise ValueError("table.sub_priceTable 를 찾을 수 없습니다.")

    # thead 에 colspan/rowspan 이 섞여 있어 `thead th` 텍스트 순서로 인덱스를 잡으면
    # tbody 컬럼 인덱스와 어긋날 수 있음.
    # tbody 기준 고정 컬럼 인덱스를 사용한다:
    # 0=일자, 1=요일, 2=대, 3=중, 4=소, 5=병아리, 6=종계노계
    item_size = "대"
    price_col_idx = 2

    candidate_rows = table.select("tbody tr")
    if not candidate_rows:
        # 일부 페이지는 tbody 없이 tr이 바로 내려올 수 있음
        candidate_rows = [tr for tr in table.select("tr") if tr.find_parent("thead") is None]

    rows: list[tuple[str, int, str]] = []
    for tr in candidate_rows:
        cells = tr.find_all(["th", "td"], recursive=False)
        if not cells:
            continue

        base_date = _normalize_ymd(cells[0].get_text(strip=True))
        if not base_date:
            continue

        cell_texts = [c.get_text(strip=True) for c in cells]
        if price_col_idx >= len(cell_texts):
            continue

        raw_value = cell_texts[price_col_idx]
        price_today = _parse_int(raw_value)
        if price_today <= 0:
            continue

        rows.append((base_date, price_today, raw_value))

    if not rows:
        raise ValueError("가격 데이터 행이 없습니다.")

    rows.sort(key=lambda x: x[0], reverse=True)
    base_date, price_today, raw_value = rows[0]
    price_prev_day = rows[1][1] if len(rows) >= 2 else None

    result = {
        "success": True,
        "source_site": source_site,
        "source_url": used_url,
        "base_date": base_date,
        "item_size": item_size,
        "price_today": price_today,
        "price_prev_day": price_prev_day,
        "price_prev_month": None,
        "price_prev_year": None,
        "raw_value": raw_value,
        "error_message": None,
    }
    logger.info(f"[{source_site}] 수집 완료: 기준일={base_date}, 금일={price_today:,}원")
    return result


def _context_year(**context) -> int:
    """Airflow context 에서 논리 날짜의 year 를 우선 사용 (백필/과거 실행 지원)."""
    for key in ("logical_date", "data_interval_end", "execution_date"):
        dt = context.get(key)
        if dt is None:
            continue
        try:
            return dt.in_timezone("Asia/Seoul").year  # pendulum.DateTime
        except Exception:
            try:
                return dt.year
            except Exception:
                pass
    return datetime.now().year


def _normalize_date(raw: str) -> str:
    """MM/DD 형식을 당해연도 YYYY-MM-DD로 변환. 실패 시 오늘 날짜 반환."""
    try:
        match = re.search(r"(\d{1,2})[/\-.](\d{1,2})", raw)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = datetime.now().year
            return f"{year:04d}-{month:02d}-{day:02d}"
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d")


# ============================================================
# Task 1-A: poultry.or.kr 수집
# ============================================================

def extract_poultry_price(**context) -> dict:
    """대한양계협회(poultry.or.kr)에서 육계 '대' 시세 수집"""
    source_site = "poultry.or.kr"
    url = "https://www.poultry.or.kr/"

    try:
        session = _build_session()
        response = session.get(url, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding

        soup = BeautifulSoup(response.text, "html.parser")

        # 기준일자: table.t_price > thead tr th:first-child
        table = soup.select_one("table.t_price")
        if table is None:
            raise ValueError("table.t_price 를 찾을 수 없습니다.")

        base_date_raw = ""
        thead_th = table.select("thead tr th")
        if thead_th:
            base_date_raw = thead_th[0].get_text(strip=True)
        base_date = _normalize_date(base_date_raw)

        # tbody 중 scope="row" 텍스트 == "대" 행 탐색
        target_row = None
        for tr in table.select("tbody tr"):
            th = tr.find("th", attrs={"scope": "row"})
            if th and th.get_text(strip=True) == "대":
                target_row = tr
                break

        if target_row is None:
            raise ValueError("'대' 행을 찾을 수 없습니다.")

        tds = target_row.find_all("td")
        if len(tds) < 4:
            raise ValueError(f"td 수 부족: {len(tds)}")

        raw_value = tds[0].get_text(strip=True)
        price_today = _parse_int(raw_value)
        price_prev_day = _parse_int(tds[1].get_text(strip=True))
        price_prev_month = _parse_int(tds[2].get_text(strip=True))
        price_prev_year = _parse_int(tds[3].get_text(strip=True))

        result = {
            "success": True,
            "source_site": source_site,
            "base_date": base_date,
            "price_today": price_today,
            "price_prev_day": price_prev_day,
            "price_prev_month": price_prev_month,
            "price_prev_year": price_prev_year,
            "raw_value": raw_value,
            "error_message": None,
        }
        logger.info(f"[{source_site}] 수집 완료: 기준일={base_date}, 금일={price_today:,}원")

    except Exception as e:
        logger.error(f"[{source_site}] 수집 실패: {e}")
        result = {
            "success": False,
            "source_site": source_site,
            "base_date": None,
            "price_today": None,
            "price_prev_day": None,
            "price_prev_month": None,
            "price_prev_year": None,
            "raw_value": None,
            "error_message": str(e),
        }

    context["ti"].xcom_push(key="poultry_price_result", value=result)
    return result


# ============================================================
# Task 1-B: chicken.or.kr 수집
# ============================================================

def extract_chicken_price(**context) -> dict:
    """한국육계협회(chicken.or.kr)에서 육계 '대' 시세 수집 — Selenium 방식

    테이블 구조 (table.sub_priceTable):
    - thead: 일자 | 요일 | 대 | 중 | 소 | 병아리 | 종계노계
    - tbody: 날짜별 행 (최근 약 6일, 첫 행이 최신)
      td[0]=일자(YYYY-MM-DD), td[1]=요일, td[2]=대, ...
    - 전일가: tbody 두 번째 행 td[2]
    - 전월/전년: 사이트 미제공 → None

    URL 정책: 현재 연도 → 이전 연도 순으로 시도 (404 자동 폴백)
    """
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    source_site = "chicken.or.kr"
    current_year = _context_year(**context)
    candidate_urls = [
        f"https://chicken.or.kr/ch_price/price_{current_year}.php",
        f"https://chicken.or.kr/ch_price/price_{current_year - 1}.php",
    ]

    driver = None
    try:
        last_error: Exception | None = None

        # 1) HTTP 우선 시도
        # NOTE: 일부 페이지는 JS로 tbody를 채워서 HTTP 응답이 스켈레톤일 수 있음 (파싱 실패 시 Selenium 폴백)
        for url in candidate_urls:
            try:
                http_html, http_used_url = _fetch_chicken_page_source_via_http(url)
                if not http_html:
                    continue

                try:
                    result = _parse_chicken_price_from_html(http_html, http_used_url or url)
                    logger.info(f"[{source_site}] HTTP 수집 성공: {url}")
                    context["ti"].xcom_push(key="chicken_price_result", value=result)
                    return result
                except Exception as e:
                    # 페이지가 JS로 데이터를 채우는 구조일 수 있어 Selenium으로 폴백
                    last_error = e
                    logger.warning(f"[{source_site}] HTTP 파싱 실패 → Selenium 폴백 ({url}): {e}")
                    continue
            except Exception as e:
                last_error = e
                logger.warning(f"[{source_site}] HTTP 접근 실패 ({url}): {e}")
                continue

        # 2) Selenium 폴백
        driver = _launch_headless_browser()
        for url in candidate_urls:
            try:
                driver.get(url)
                page_source = None
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.sub_priceTable"))
                    )
                    # 데이터가 JS로 채워지는 경우를 위해 날짜 패턴이 나타날 때까지 조금 더 대기
                    WebDriverWait(driver, 15).until(
                        lambda d: _CHICKEN_PRICE_DATE_RE.search(d.page_source or "") is not None
                    )
                    page_source = driver.page_source
                except TimeoutException:
                    # 일부 케이스에서 table 이 iframe 안에 있을 수 있어 프레임을 순회하며 탐색
                    for iframe in driver.find_elements(By.CSS_SELECTOR, "iframe"):
                        driver.switch_to.frame(iframe)
                        try:
                            WebDriverWait(driver, 8).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sub_priceTable"))
                            )
                            WebDriverWait(driver, 8).until(
                                lambda d: _CHICKEN_PRICE_DATE_RE.search(d.page_source or "") is not None
                            )
                            page_source = driver.page_source
                            break
                        finally:
                            driver.switch_to.default_content()
                    if page_source is None:
                        raise

                result = _parse_chicken_price_from_html(page_source, url)
                logger.info(f"[{source_site}] Selenium 수집 성공: {url}")
                context["ti"].xcom_push(key="chicken_price_result", value=result)
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"[{source_site}] Selenium 접근/파싱 실패 ({url}): {e}")

                # 드라이버가 죽으면(localhost 포트 connection refused 등) 다음 시도에서 재생성
                err = str(e)
                if "Connection refused" in err or "Failed to establish a new connection" in err or "Max retries exceeded" in err:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = _launch_headless_browser()
                continue

        if last_error is not None:
            raise last_error
        raise ValueError(f"유효한 URL 없음: {candidate_urls}")

    except Exception as e:
        logger.error(f"[{source_site}] 수집 실패: {e}")
        result = {
            "success": False,
            "source_site": source_site,
            "source_url": None,
            "base_date": None,
            "item_size": "대",
            "price_today": None,
            "price_prev_day": None,
            "price_prev_month": None,
            "price_prev_year": None,
            "raw_value": None,
            "error_message": str(e),
        }
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    context["ti"].xcom_push(key="chicken_price_result", value=result)
    return result


# ============================================================
# Task 2: 이상치 검증
# ============================================================

def validate_price(task_id: str, **context) -> dict:
    """수집 결과 검증 및 이상치 플래그 추가

    Args:
        task_id: XCom을 가져올 upstream task_id
    """
    ti = context["ti"]
    result = ti.xcom_pull(task_ids=task_id)

    if result is None:
        logger.warning(f"[validate_price] task_id={task_id} XCom 결과 없음")
        return {"success": False, "error_message": "XCom 결과 없음", "anomaly": False, "anomaly_reason": ""}

    # 수집 자체가 실패한 경우 이상치 판정 없이 반환
    if not result.get("success", False):
        result.update({"anomaly": False, "anomaly_reason": ""})
        logger.info(f"[validate_price] {result.get('source_site')} 수집 실패 — 이상치 검사 생략")
        return result

    price_today = result.get("price_today")
    price_prev_day = result.get("price_prev_day")

    # 금일 가격 유효성 체크
    if price_today is None or price_today <= 0:
        result.update({"success": False, "error_message": "금일 가격 유효하지 않음", "anomaly": False, "anomaly_reason": ""})
        logger.warning(f"[validate_price] {result.get('source_site')} 금일 가격 비정상: {price_today}")
        return result

    # 전일 대비 이상치 체크
    anomaly = False
    anomaly_reason = ""
    if price_prev_day is not None and price_prev_day != 0:
        rate = abs(price_today - price_prev_day) / price_prev_day
        if rate >= 0.2:
            anomaly = True
            anomaly_reason = f"전일 대비 {rate * 100:.1f}% 변동"
            logger.warning(f"[validate_price] {result.get('source_site')} 이상치 감지: {anomaly_reason}")

    result.update({"anomaly": anomaly, "anomaly_reason": anomaly_reason})
    logger.info(f"[validate_price] {result.get('source_site')} 검증 완료 (이상치={anomaly})")
    return result


# ============================================================
# Task 3: 결과 병합
# ============================================================

def merge_results(**context) -> str:
    """검증된 두 사이트 결과를 DataFrame으로 병합 후 JSON 반환"""
    ti = context["ti"]
    poultry_result = ti.xcom_pull(task_ids="validate_poultry_price")
    chicken_result = ti.xcom_pull(task_ids="validate_chicken_price")

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if poultry_result:
        poultry_result["collected_at"] = collected_at
    if chicken_result:
        chicken_result["collected_at"] = collected_at

    records = [r for r in [poultry_result, chicken_result] if r is not None]
    df = pd.DataFrame(records)
    json_str = df.to_json(orient="records", force_ascii=False)

    logger.info(f"[merge_results] 병합 완료: {len(records)}건")
    return json_str


# ============================================================
# Task 4: CSV 저장
# ============================================================

def save_results(**context) -> None:
    """병합 결과를 CHICKEN_PRICE_CSV_PATH에 누적 저장"""
    ti = context["ti"]
    json_str = ti.xcom_pull(task_ids="merge_results")
    if not json_str:
        logger.warning("[save_results] merge_results 결과가 없습니다. (저장 스킵)")
        return

    df_new = pd.read_json(StringIO(json_str), orient="records")
    if df_new.empty:
        logger.warning("[save_results] 저장할 데이터가 없습니다 (빈 결과).")
        return

    required_cols = {"source_site", "base_date", "collected_at"}
    missing = required_cols - set(df_new.columns)
    if missing:
        logger.warning(f"[save_results] 저장 스킵: 필수 컬럼 누락 {sorted(missing)}")
        return

    CHICKEN_PRICE_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    if CHICKEN_PRICE_CSV_PATH.exists():
        df_existing = pd.read_csv(CHICKEN_PRICE_CSV_PATH, encoding="utf-8-sig")
        df_all = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    # 같은 사이트/기준일은 재실행 시 마지막 수집값만 유지한다.
    df_all = df_all.drop_duplicates(subset=["source_site", "base_date"], keep="last")

    df_all.to_csv(CHICKEN_PRICE_CSV_PATH, index=False, encoding="utf-8-sig")
    logger.info(f"저장 완료: {len(df_new)}행 추가, 총 {len(df_all)}행 → {CHICKEN_PRICE_CSV_PATH}")


# ============================================================
# 내부: HTML 이메일 빌더
# ============================================================

def _build_email_html(results: list, ma_info: dict, base_date: str, any_failure: bool) -> str:
    """육계 시세 일일 알림 HTML 이메일 생성"""
    import math

    status_color = "#e74c3c" if any_failure else "#27ae60"
    status_text = "일부 실패" if any_failure else "정상"

    def _valid_price(v) -> int | None:
        """None / NaN / 0 이하 → None, 그 외 → int 변환"""
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f) or f <= 0:
                return None
            return int(f)
        except (TypeError, ValueError):
            return None

    def delta_html(delta: int | None) -> str:
        if delta is None:
            return '<span style="color:#95a5a6">-</span>'
        if delta > 0:
            return f'<span style="color:#e74c3c;font-weight:bold">▲ {delta:+,}원</span>'
        if delta < 0:
            return f'<span style="color:#3498db;font-weight:bold">▼ {delta:+,}원</span>'
        return f'<span style="color:#7f8c8d">0원</span>'

    # 사이트 URL 매핑 (source_url 없을 때 기본값)
    _default_urls = {
        "poultry.or.kr": "https://www.poultry.or.kr/",
        "chicken.or.kr": f"https://chicken.or.kr/ch_price/price_{datetime.now().year}.php",
    }

    # 사이트 카드 생성
    site_cards = ""
    for idx, r in enumerate(results, start=1):
        site = r.get("source_site", "")
        site_url = r.get("source_url") or _default_urls.get(site, "#")
        site_link = f'<a href="{site_url}" style="color:inherit;text-decoration:none;" target="_blank">{idx}. {site} ↗</a>'

        if not r.get("success", False):
            site_cards += f"""
            <div style="background:#fdf2f2;border-left:4px solid #e74c3c;padding:14px 18px;margin-bottom:12px;border-radius:4px;">
              <div style="font-weight:bold;color:#c0392b;margin-bottom:6px;">{site_link}</div>
              <div style="color:#c0392b;font-size:14px;">❌ 수집 실패: {r.get('error_message','알 수 없는 오류')}</div>
            </div>"""
        else:
            price_today    = _valid_price(r.get("price_today")) or 0
            price_prev_day   = _valid_price(r.get("price_prev_day"))
            price_prev_month = _valid_price(r.get("price_prev_month"))
            price_prev_year  = _valid_price(r.get("price_prev_year"))
            delta = (price_today - price_prev_day) if price_prev_day is not None else None

            anomaly_block = ""
            if r.get("anomaly"):
                anomaly_block = f"""
              <div style="background:#fff3cd;border-left:3px solid #f39c12;padding:8px 12px;margin-top:10px;font-size:13px;color:#856404;border-radius:3px;">
                ⚠️ 주의: 전일 대비 {r.get('anomaly_reason','')}
              </div>"""

            prev_rows = ""
            if price_prev_day is not None:
                prev_rows += f'<tr><td style="color:#777;padding:3px 0;font-size:13px;">전일</td><td style="text-align:right;padding:3px 0;font-size:13px;">{price_prev_day:,}원</td></tr>'
            if price_prev_month is not None:
                prev_rows += f'<tr><td style="color:#777;padding:3px 0;font-size:13px;">전월</td><td style="text-align:right;padding:3px 0;font-size:13px;">{price_prev_month:,}원</td></tr>'
            if price_prev_year is not None:
                prev_rows += f'<tr><td style="color:#777;padding:3px 0;font-size:13px;">전년</td><td style="text-align:right;padding:3px 0;font-size:13px;">{price_prev_year:,}원</td></tr>'

            site_cards += f"""
            <div style="background:#fff;border:1px solid #e8e8e8;padding:16px 18px;margin-bottom:12px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
              <div style="font-weight:bold;color:#2c3e50;margin-bottom:12px;font-size:15px;">{site_link}</div>
              <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:12px;">
                <span style="font-size:28px;font-weight:bold;color:#2c3e50;">{price_today:,}원</span>
                <span style="font-size:16px;">{delta_html(delta)}</span>
              </div>
              <table style="width:100%;border-collapse:collapse;">
                {prev_rows}
              </table>
              {anomaly_block}
            </div>"""

    # 이동평균 섹션
    ma_section = ""
    ma_rows = ""
    for site in ("poultry.or.kr", "chicken.or.kr"):
        if site not in ma_info:
            continue
        ma = ma_info[site]
        if ma.get("ma20") is not None:
            ma_rows += f'<tr><td style="color:#555;padding:4px 0;font-size:13px;">{site} 20일 평균</td><td style="text-align:right;font-size:13px;color:#2c3e50;">{ma["ma20"]:,.0f}원</td></tr>'
        if ma.get("ma60") is not None:
            ma_rows += f'<tr><td style="color:#555;padding:4px 0;font-size:13px;">{site} 60일 평균</td><td style="text-align:right;font-size:13px;color:#2c3e50;">{ma["ma60"]:,.0f}원</td></tr>'
        if ma.get("ma120") is not None:
            ma_rows += f'<tr><td style="color:#555;padding:4px 0;font-size:13px;">{site} 120일 평균</td><td style="text-align:right;font-size:13px;color:#2c3e50;">{ma["ma120"]:,.0f}원</td></tr>'
    if ma_rows:
        ma_section = f"""
            <div style="background:#f8f9fa;border-radius:6px;padding:14px 18px;margin-bottom:16px;">
              <div style="font-weight:bold;color:#555;margin-bottom:10px;font-size:14px;">📊 이동평균</div>
              <table style="width:100%;border-collapse:collapse;">{ma_rows}</table>
            </div>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;">
  <div style="max-width:560px;margin:24px auto;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 4px 16px rgba(0,0,0,0.10);">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1a252f 0%,#2c3e50 100%);padding:24px 20px;text-align:center;">
      <div style="color:#f39c12;font-size:13px;letter-spacing:2px;margin-bottom:6px;">DAILY MARKET PRICE</div>
      <div style="color:#fff;font-size:22px;font-weight:bold;">🐔 大 계육시세</div>
      <div style="color:rgba(255,255,255,0.7);font-size:13px;margin-top:6px;">{base_date}</div>
    </div>

    <!-- Body -->
    <div style="padding:20px 18px;">
      {site_cards}
      {ma_section}
    </div>

    <!-- Status Footer -->
    <div style="border-top:1px solid #eee;padding:12px 18px;display:flex;justify-content:space-between;align-items:center;background:#fafafa;">
      <span style="font-size:12px;color:#aaa;">문제 시 조민준 PM에게 문의(a17019@doridang.com)</span>
      <span style="font-size:13px;font-weight:bold;color:{status_color};">{'✅' if not any_failure else '⚠️'} 수집상태: {status_text}</span>
    </div>

  </div>
</body>
</html>"""


# ============================================================
# Task 5: 이메일 알림
# ============================================================

def send_notification(**context) -> None:
    """육계 시세 일일 알림 이메일 발송 (이동평균 포함)"""
    ti = context["ti"]
    json_str = ti.xcom_pull(task_ids="merge_results")
    if json_str:
        results = pd.read_json(StringIO(json_str), orient="records").to_dict(orient="records")
    else:
        results = []

    dag = context.get("dag")
    email_on_failure = False
    try:
        email_on_failure = bool(getattr(dag, "default_args", {}).get("email_on_failure", False))
    except Exception:
        email_on_failure = False

    # 이동평균 계산
    ma_info: dict = {}
    if CHICKEN_PRICE_CSV_PATH.exists():
        df_csv = pd.read_csv(CHICKEN_PRICE_CSV_PATH, encoding="utf-8-sig")
        df_csv["price_today"] = pd.to_numeric(df_csv["price_today"], errors="coerce")
        df_csv["base_date"] = pd.to_datetime(df_csv["base_date"], errors="coerce")

        for site in df_csv["source_site"].unique():
            site_df = (
                df_csv[df_csv["source_site"] == site]
                .dropna(subset=["price_today"])
                .drop_duplicates(subset=["base_date"], keep="last")
                .sort_values("base_date")
            )
            ma20 = site_df["price_today"].rolling(20).mean().iloc[-1] if len(site_df) >= 20 else None
            ma60 = site_df["price_today"].rolling(60).mean().iloc[-1] if len(site_df) >= 60 else None
            ma120 = site_df["price_today"].rolling(120).mean().iloc[-1] if len(site_df) >= 120 else None
            ma_info[site] = {"ma20": ma20, "ma60": ma60, "ma120": ma120}

        # chicken.or.kr는 사이트가 비교값을 제공하지 않아 누적 CSV에서 보정한다.
        ch_csv = (
            df_csv[df_csv["source_site"] == "chicken.or.kr"]
            .dropna(subset=["price_today"])
            .drop_duplicates(subset=["base_date"], keep="last")
            .copy()
        )
        ch_csv["base_date"] = pd.to_datetime(ch_csv["base_date"], errors="coerce")
        ch_csv = ch_csv.dropna(subset=["base_date"])

        def _nearest_price(target_date, days_tol):
            if ch_csv.empty:
                return None
            diff = (ch_csv["base_date"] - target_date).abs()
            mask = diff <= pd.Timedelta(days=days_tol)
            if not mask.any():
                return None
            idx = diff[mask].idxmin()
            return int(ch_csv.loc[idx, "price_today"])

        for r in results:
            if r.get("source_site") == "chicken.or.kr" and r.get("success"):
                base = pd.to_datetime(r.get("base_date"), errors="coerce")
                if pd.isna(base):
                    continue
                r["price_prev_month"] = _nearest_price(base - pd.DateOffset(months=1), 7)
                r["price_prev_year"] = _nearest_price(base - pd.DateOffset(years=1), 14)

    # 기준일자 (첫 번째 성공 결과 기준)
    base_date = next(
        (r.get("base_date") for r in results if r.get("success")),
        datetime.now().strftime("%Y-%m-%d"),
    )
    any_failure = (not results) or any(not r.get("success", False) for r in results)

    html_content = _build_email_html(results, ma_info, base_date, any_failure)
    subject_prefix = "⚠️" if any_failure else "🐔"
    subject = f"{subject_prefix} [大 계육시세] {base_date}"

    pm_emails = ["a17019@kakao.com"]
    all_emails = ["a17019@kakao.com", "siw22222@kakao.com", "simjeong01@kakao.com", "simjeong00@kakao.com"]
    to_emails = all_emails

    try:
        send_email(
            subject=subject,
            html_content=html_content,
            to_emails=to_emails,
            **context,
        )
        logger.info(f"[send_notification] 알림 발송 완료: {subject}")
    except Exception as exc:
        logger.exception("[send_notification] 알림 발송 실패(수집 결과는 유지): %s", exc)
