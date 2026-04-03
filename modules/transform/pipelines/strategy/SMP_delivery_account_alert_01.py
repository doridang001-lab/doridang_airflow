"""
ToOrder 배달 계정 관리 현황 수집·분석·알림 파이프라인

Tasks:
    crawl_delivery_account_excel  → XCom: excel_path
    load_and_filter_alerts        → XCom: alert_parquet_path
    save_alerts_to_csv            → None
    send_delivery_alert_emails    → None
"""

import datetime as dt
import logging
import os
import random
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.utility.mailer import send_email, text_to_html
from modules.transform.utility.paths import DOWN_DIR, LOCAL_DB, MART_DB, ONEDRIVE_DB, TEMP_DIR

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
TOORDER_ID = "doridang1"
TOORDER_PW = "ehfl0109!!"
LOGIN_URL = "https://ceo.toorder.co.kr/auth/login?returnTo=%2Fdashboard"
DELIVERY_URL = (
    "https://ceo.toorder.co.kr/dashboard/stores-manage/"
    "delivery-manage-company-hide-password"
)
HEADLESS_MODE = os.getenv("AIRFLOW_HOME") is not None

ALERT_CON = ["등록 실패", "연결 오류", "가게없음", "매장연결 필요"]
ADMIN_EMAIL = ["a17019@kakao.com", "sanbogaja81@kakao.com","bulu1017@kakao.com", "siw22222@kakao.com"]
# bulu1017@kakao.com 오나영 차장
# siw22222@kakao.com 대표님
CC_EMAILS = ADMIN_EMAIL  # list[str]

CSV_PATH = MART_DB / "torder_delivery_account_monitoring" / "torder_delivery_account_alt.csv"

STATUS_COLS = [
    "상태_배달의민족",
    "상태_쿠팡이츠",
    "상태_요기요",
    "상태_땡겨요",
    "상태_먹깨비",
    "상태_대구로",
    "상태_배달특급",
    "상태_네이버 지도",
    "상태_구글 지도",
    "상태_카카오맵",
]

# ⚠️ 실제 Excel 헤더 구조 확인 후 조정 필요
# pd.read_excel(file, header=[0,1]).columns.tolist() 결과에 맞게 수정
CHANNEL_RENAME = {
    "배달의민족_배달 플랫폼": "상태_배달의민족",
    "쿠팡이츠_배달 플랫폼": "상태_쿠팡이츠",
    "요기요_배달 플랫폼": "상태_요기요",
    "땡겨요_배달 플랫폼": "상태_땡겨요",
    "먹깨비_배달 플랫폼": "상태_먹깨비",
    "대구로_배달 플랫폼": "상태_대구로",
    "배달특급_배달 플랫폼": "상태_배달특급",
    "네이버 지도_배달 플랫폼": "상태_네이버 지도",
    "구글 지도_배달 플랫폼": "상태_구글 지도",
    "카카오맵_배달 플랫폼": "상태_카카오맵",
}

_DOWNLOAD_TIMEOUT = 30  # 초


# ============================================================
# Public Task 함수
# ============================================================

def crawl_delivery_account_excel(**context) -> str:
    """
    ToOrder 배달 계정 관리 페이지에서 Excel 다운로드.
    XCom key: "excel_path"
    """
    ti = context["task_instance"]
    driver = None
    try:
        driver = _launch_browser()
        _do_login(driver, TOORDER_ID, TOORDER_PW)
        excel_path = _download_delivery_excel(driver)
        ti.xcom_push(key="excel_path", value=str(excel_path))
        logger.info("크롤링 완료: %s", excel_path)
        return str(excel_path)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def load_and_filter_alerts(**context) -> str:
    """
    Excel 읽기 → 멀티레벨 헤더 평탄화 → 필터링 → 담당자 JOIN → parquet 저장.
    XCom key: "alert_parquet_path"
    """
    ti = context["task_instance"]
    excel_path = ti.xcom_pull(task_ids="crawl_delivery_account", key="excel_path")
    if not excel_path or not Path(excel_path).exists():
        raise FileNotFoundError(f"Excel 파일 없음: {excel_path}")

    df = _read_and_flatten_excel(excel_path)
    df = _remove_test_stores(df)
    df = _filter_alert_rows(df)
    df = _join_sales_employee(df)
    df["updated_at"] = dt.date.today().isoformat()

    ds_nodash = context.get("ds_nodash", dt.date.today().strftime("%Y%m%d"))
    parquet_path = _save_to_parquet(df, ds_nodash)
    ti.xcom_push(key="alert_parquet_path", value=str(parquet_path))
    logger.info("필터링 완료: %d행 → %s", len(df), parquet_path)
    return str(parquet_path)


def save_alerts_to_csv(**context) -> None:
    """
    parquet → OneDrive CSV 저장 + 날짜 검증.
    실패 시 관리자 이메일 발송.
    """
    ti = context["task_instance"]
    parquet_path = ti.xcom_pull(task_ids="filter_alert_targets", key="alert_parquet_path")
    if not parquet_path or not Path(parquet_path).exists():
        raise FileNotFoundError(f"parquet 파일 없음: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    logger.info("CSV 저장 완료: %s (%d행)", CSV_PATH, len(df))

    today = dt.date.today().isoformat()
    try:
        df_check = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
        if "updated_at" not in df_check.columns or today not in df_check["updated_at"].astype(str).values:
            _send_failure_email(f"CSV 저장 검증 실패: updated_at에 {today} 없음", context)
            raise ValueError("CSV 저장 검증 실패")
        logger.info("CSV 저장 검증 통과 (updated_at=%s)", today)
    except FileNotFoundError:
        _send_failure_email("CSV 파일 재읽기 실패 (파일 없음)", context)
        raise


def send_delivery_alert_emails(test_mode: bool = True, **context) -> None:
    """
    parquet → 담당자별 그룹화 → HTML 이메일 발송.

    Parameters:
        test_mode: True이면 a17019@kakao.com 단독 발송 (CC 없음)
                   False이면 담당자 이메일 + CC_EMAILS
    """
    ti = context["task_instance"]
    parquet_path = ti.xcom_pull(task_ids="filter_alert_targets", key="alert_parquet_path")
    if not parquet_path or not Path(parquet_path).exists():
        logger.info("alert_parquet_path 없음 → 이메일 발송 스킵")
        return

    df = pd.read_parquet(parquet_path)
    if df.empty:
        logger.info("알림 대상 0건 → 발송 스킵")
        return

    if "email" not in df.columns:
        logger.warning("email 컬럼 없음 → 발송 스킵")
        return

    df["email"] = df["email"].fillna(ADMIN_EMAIL[0]).astype(str).str.strip()
    df.loc[df["email"] == "", "email"] = ADMIN_EMAIL[0]

    for recipient_email, group_df in df.groupby("email"):
        try:
            _send_single_alert(
                recipient_email=str(recipient_email),
                group_df=group_df,
                test_mode=test_mode,
                context=context,
            )
        except Exception as e:
            logger.error("이메일 발송 실패 [%s]: %s", recipient_email, e)


def cleanup_downloaded_excel(**context) -> None:
    """
    크롤링으로 다운로드된 Excel 파일 삭제.
    XCom 'excel_path'를 참조하며, DOWN_DIR 내 '배달 계정 관리*.xlsx' 잔여 파일도 정리.
    """
    ti = context["task_instance"]
    excel_path = ti.xcom_pull(task_ids="crawl_delivery_account", key="excel_path")

    removed = 0
    # 1) XCom에 저장된 파일 삭제
    if excel_path:
        p = Path(excel_path)
        if p.exists():
            p.unlink()
            logger.info("삭제 완료: %s", p)
            removed += 1

    # 2) DOWN_DIR 내 잔여 파일 정리
    for f in DOWN_DIR.glob("배달 계정 관리*.xlsx"):
        try:
            f.unlink()
            logger.info("잔여 파일 삭제: %s", f)
            removed += 1
        except Exception as e:
            logger.warning("파일 삭제 실패: %s (%s)", f, e)

    logger.info("다운로드 파일 정리 완료: %d개 삭제", removed)


# ============================================================
# Private: 크롤링
# ============================================================

def _get_chrome_version():
    """설치된 Chrome 메이저 버전 반환"""
    import subprocess, re as _re
    for cmd in [
        ["google-chrome", "--version"],
        ["google-chrome-stable", "--version"],
        ["chromium-browser", "--version"],
        ["chromium", "--version"],
    ]:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            match = _re.search(r"(\d+)\.", result.stdout.strip())
            if match:
                return int(match.group(1))
        except Exception:
            continue
    return None


def _launch_browser():
    """undetected_chromedriver 브라우저 실행"""
    import re as _re

    def _make_options():
        opts = uc.ChromeOptions()
        chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
        if Path(chrome_bin).exists():
            try:
                opts.binary_location = str(chrome_bin)
            except Exception:
                pass
        if HEADLESS_MODE:
            opts.add_argument("--headless=new")
        for arg in ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--disable-web-resources"]:
            opts.add_argument(arg)
        prefs = {
            "download.default_directory": str(DOWN_DIR.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        opts.add_experimental_option("prefs", prefs)
        return opts

    chrome_version = _get_chrome_version()
    try:
        kwargs = {"options": _make_options()}
        if chrome_version:
            kwargs["version_main"] = chrome_version
            logger.info("ChromeDriver 버전: %s", chrome_version)
        driver = uc.Chrome(**kwargs)
        logger.info("브라우저 실행 성공 (headless=%s)", HEADLESS_MODE)
        return driver
    except Exception as e:
        match = _re.search(r"Current browser version is (\d+)", str(e))
        if match:
            detected = int(match.group(1))
            logger.warning("버전 불일치 → %s 으로 재시도", detected)
            driver = uc.Chrome(options=_make_options(), version_main=detected)
            logger.info("브라우저 실행 성공 (재시도)")
            return driver
        raise


def _do_login(driver, account_id: str, password: str) -> None:
    """ToOrder 로그인. 실패 시 RuntimeError"""
    logger.info("로그인 시도: %s", account_id)
    driver.get(LOGIN_URL)

    # React 앱 로드 대기
    end_time = time.time() + 15
    while time.time() < end_time:
        ready = driver.execute_script(
            "return document.querySelector('input[name=\"id\"]') !== null;"
        )
        if ready:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("React 앱 로드 타임아웃 (15초)")

    time.sleep(1.0)

    wait = WebDriverWait(driver, 10)
    id_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='id']")))
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='id']")))
    _human_type(id_input, account_id)
    logger.info("ID 입력 완료")

    try:
        pw_input = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        _human_type(pw_input, password)
        logger.info("비밀번호 입력 완료")
    except NoSuchElementException:
        raise RuntimeError("비밀번호 필드 없음")

    time.sleep(0.3)
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, "input[name='isCompany']")
        driver.execute_script("arguments[0].click();", checkbox)
        logger.info("기업회원 체크")
    except Exception:
        pass

    time.sleep(0.5)

    submit_btn = None
    try:
        submit_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
    except NoSuchElementException:
        pass
    if not submit_btn:
        try:
            submit_btn = driver.find_element(By.XPATH, "//button[contains(text(), '로그인')]")
        except NoSuchElementException:
            pass

    if submit_btn:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit_btn)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", submit_btn)
        logger.info("로그인 버튼 클릭")
    else:
        pw_input.send_keys(Keys.RETURN)
        logger.info("Enter 제출")

    time.sleep(3.0)

    current_url = driver.current_url
    if "/auth" in current_url and "/dashboard" not in current_url:
        raise RuntimeError(f"로그인 실패 (URL: {current_url})")
    logger.info("로그인 성공 (URL: %s)", current_url)


def _human_type(element, text: str) -> None:
    """사람처럼 타이핑"""
    element.clear()
    time.sleep(0.2)
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.03, 0.08))
    time.sleep(0.3)


def _download_delivery_excel(driver) -> Path:
    """
    배달 계정 관리 페이지 이동 → 내보내기 → Excel 다운로드 대기.
    반환: 다운로드된 파일 Path
    """
    logger.info("배달 계정 관리 페이지 이동")
    driver.get(DELIVERY_URL)
    time.sleep(random.uniform(2.0, 3.0))

    if "delivery-manage-company" not in driver.current_url:
        raise RuntimeError(f"페이지 이동 실패: {driver.current_url}")

    # 다운로드 전 파일 스냅샷
    existing_files = set(DOWN_DIR.glob("배달 계정 관리*.xlsx"))

    # '내보내기' 버튼 클릭
    wait = WebDriverWait(driver, 20)
    export_btn = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="내보내기"]'))
    )
    driver.execute_script("arguments[0].click();", export_btn)
    logger.info("내보내기 버튼 클릭")
    time.sleep(random.uniform(1.5, 2.5))

    # 드롭다운 메뉴에서 "Excel로 내보내기" 클릭
    menu_items = wait.until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[role='menuitem']"))
    )
    excel_item = None
    for item in menu_items:
        if "Excel로 내보내기" in item.text:
            excel_item = item
            break
    if excel_item is None:
        raise RuntimeError("'Excel로 내보내기' 메뉴 항목을 찾을 수 없음")

    driver.execute_script("arguments[0].click();", excel_item)
    logger.info("'Excel로 내보내기' 클릭")

    # 다운로드 완료 대기 (최대 30초)
    deadline = time.time() + _DOWNLOAD_TIMEOUT
    downloaded = None
    while time.time() < deadline:
        time.sleep(1.0)
        current_files = set(DOWN_DIR.glob("배달 계정 관리*.xlsx"))
        new_files = current_files - existing_files
        completed = [f for f in new_files if not f.name.endswith(".crdownload")]
        if completed:
            downloaded = max(completed, key=lambda p: p.stat().st_mtime)
            break

    if downloaded is None:
        raise RuntimeError(f"Excel 다운로드 실패 ({_DOWNLOAD_TIMEOUT}초 초과)")

    logger.info("다운로드 완료: %s", downloaded.name)
    return downloaded


# ============================================================
# Private: 데이터 처리
# ============================================================

def _read_and_flatten_excel(excel_path: str) -> pd.DataFrame:
    """
    멀티레벨 헤더 Excel 읽기 및 평탄화.

    규칙:
    - level_0이 "Unnamed"면 → level_1만 사용
    - level_1이 "Unnamed"면 → level_0만 사용
    - 둘 다 유효 → "{level_1}_{level_0}" 형식으로 결합 (채널 상태 컬럼)
    """
    df = pd.read_excel(excel_path, header=[0, 1])
    logger.info("Excel 원본 컬럼: %s", df.columns.tolist())

    new_columns = []
    for col0, col1 in df.columns:
        col0_str = str(col0).strip()
        col1_str = str(col1).strip()
        is_col0_unnamed = col0_str.startswith("Unnamed")
        is_col1_unnamed = col1_str.startswith("Unnamed")

        if is_col0_unnamed and is_col1_unnamed:
            new_columns.append(f"col_{len(new_columns)}")
        elif is_col0_unnamed:
            new_columns.append(col1_str)
        elif is_col1_unnamed:
            new_columns.append(col0_str)
        else:
            new_columns.append(f"{col1_str}_{col0_str}")

    df.columns = new_columns
    logger.info("평탄화 후 컬럼: %s", new_columns)

    df = df.rename(columns=CHANNEL_RENAME)
    logger.info("rename 후 컬럼: %s", df.columns.tolist())
    return df


def _remove_test_stores(df: pd.DataFrame) -> pd.DataFrame:
    """매장 태그에 '테스트' 포함 매장 제거"""
    tag_col = "매장 태그"
    if tag_col not in df.columns:
        logger.warning("'매장 태그' 컬럼 없음 → 테스트 매장 제거 생략")
        return df
    mask = df[tag_col].fillna("").astype(str).str.contains("테스트", na=False)
    removed = mask.sum()
    if removed:
        logger.info("테스트 매장 제거: %d개", removed)
    return df[~mask].copy()


def _filter_alert_rows(df: pd.DataFrame) -> pd.DataFrame:
    """ALERT_CON 조건에 해당하는 행 필터링"""
    existing_status_cols = [c for c in STATUS_COLS if c in df.columns]
    if not existing_status_cols:
        logger.warning("상태 컬럼 없음 → 전체 반환 (헤더 구조 확인 필요)")
        return df

    alert_mask = (
        df[existing_status_cols]
        .apply(lambda col: col.fillna("").astype(str).isin(ALERT_CON))
        .any(axis=1)
    )
    result = df[alert_mask].copy()
    logger.info("알림 대상: %d / %d 행", len(result), len(df))
    return result


def _normalize_store_name(name: str) -> str:
    """'도리당 청라점' / '청라점' 모두 → '청라점'으로 통일"""
    import re
    s = str(name).strip()
    s = re.sub(r'^도리당\s*', '', s)
    return s


def _join_sales_employee(df: pd.DataFrame) -> pd.DataFrame:
    """sales_employee.csv에서 담당자, email 컬럼 LEFT JOIN"""
    emp_path = LOCAL_DB / "영업관리부_DB" / "sales_employee.csv"
    if not emp_path.exists():
        logger.warning("sales_employee.csv 없음: %s → 담당자 매핑 생략", emp_path)
        df["담당자"] = ""
        df["email"] = ""
        return df

    emp = None
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            emp = pd.read_csv(emp_path, encoding=enc)
            break
        except Exception:
            continue

    if emp is None:
        logger.error("sales_employee.csv 인코딩 실패")
        df["담당자"] = ""
        df["email"] = ""
        return df

    keep = [c for c in ["매장명", "담당자", "email"] if c in emp.columns]
    emp = emp[keep].drop_duplicates(subset=["매장명"], keep="first")

    if "매장명" in df.columns and "매장명" in emp.columns:
        # 매장명 정규화 키로 JOIN ("도리당 청라점" ↔ "청라점" 매칭)
        df["_join_key"] = df["매장명"].map(_normalize_store_name)
        emp["_join_key"] = emp["매장명"].map(_normalize_store_name)
        emp_dedup = emp[["_join_key", "담당자", "email"]].drop_duplicates(subset=["_join_key"], keep="first")

        df = df.merge(emp_dedup, on="_join_key", how="left", suffixes=("", "_emp"))
        for col in ["담당자", "email"]:
            emp_col = f"{col}_emp"
            if emp_col in df.columns:
                df[col] = df[col].fillna(df[emp_col])
                df.drop(columns=[emp_col], inplace=True)
        df.drop(columns=["_join_key"], inplace=True)

        null_count = df["담당자"].isna().sum()
        if null_count:
            failed_stores = df.loc[df["담당자"].isna(), "매장명"].tolist()
            logger.warning("담당자 매핑 실패 매장: %d개 %s", null_count, failed_stores)
    else:
        df["담당자"] = ""
        df["email"] = ""

    return df


def _save_to_parquet(df: pd.DataFrame, ds_nodash: str) -> Path:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = TEMP_DIR / f"delivery_alert_{ds_nodash}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info("parquet 저장: %s (%d행)", parquet_path, len(df))
    return parquet_path


# ============================================================
# Private: 이메일
# ============================================================

def _send_single_alert(
    recipient_email: str,
    group_df: pd.DataFrame,
    test_mode: bool,
    context: dict,
) -> None:
    """단일 담당자에게 이메일 발송"""
    manager_name = ""
    if "담당자" in group_df.columns:
        names = group_df["담당자"].dropna().unique()
        if len(names) > 0:
            manager_name = str(names[0])

    n_stores = len(group_df)

    if test_mode:
        to_emails = ADMIN_EMAIL
        cc_list = []
    else:
        to_emails = [recipient_email]
        cc_list = CC_EMAILS

    subject = f"[토더 배달계정 미연결] {manager_name} 담당 {n_stores}개 매장 연결 오류"
    html_content = _build_alert_html(manager_name, group_df)

    _send_with_cc(
        subject=subject,
        html_content=html_content,
        to_emails=to_emails,
        cc_emails=cc_list,
    )
    logger.info("이메일 발송: to=%s, cc=%s, 매장 %d개", to_emails, cc_list, n_stores)


def _build_alert_html(manager_name: str, df: pd.DataFrame) -> str:
    """담당자별 알림 이메일 HTML 생성"""
    rows = []
    for _, row in df.iterrows():
        store_name = row.get("매장명", "")
        error_channels = []
        error_statuses = []
        for col in STATUS_COLS:
            if col in row.index:
                val = str(row[col]).strip()
                if val in ALERT_CON:
                    error_channels.append(col.replace("상태_", ""))
                    error_statuses.append(val)
        rows.append({
            "매장명": store_name,
            "오류 채널": ", ".join(error_channels),
            "상태": ", ".join(error_statuses),
        })

    table_df = pd.DataFrame(rows)

    # 상태별 색상 매핑
    def _status_badge(status_str: str) -> str:
        badges = []
        for s in status_str.split(", "):
            s = s.strip()
            if s in ("등록 실패", "연결 오류"):
                badges.append(f'<span style="background:#ff5252;color:#fff;padding:2px 8px;border-radius:3px;font-size:12px;">{s}</span>')
            elif s in ("가게없음", "매장연결 필요"):
                badges.append(f'<span style="background:#ff9800;color:#fff;padding:2px 8px;border-radius:3px;font-size:12px;">{s}</span>')
            elif s:
                badges.append(f'<span style="background:#757575;color:#fff;padding:2px 8px;border-radius:3px;font-size:12px;">{s}</span>')
        return " ".join(badges)

    today_str = dt.date.today().strftime("%Y-%m-%d")
    n_stores = len(table_df)
    greeting = f"{manager_name}님" if manager_name else "담당자님"

    # 테이블 행 HTML
    table_rows = ""
    for i, r in table_df.iterrows():
        bg = "#fff" if i % 2 == 0 else "#fafafa"
        table_rows += f"""<tr style="background:{bg};">
  <td style="padding:10px 14px;border-bottom:1px solid #eee;">{r['매장명']}</td>
  <td style="padding:10px 14px;border-bottom:1px solid #eee;">{r['오류 채널']}</td>
  <td style="padding:10px 14px;border-bottom:1px solid #eee;">{_status_badge(r['상태'])}</td>
</tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f4f4f7;font-family:'Malgun Gothic','Apple SD Gothic Neo',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;padding:30px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#d32f2f,#f44336);padding:28px 32px;">
      <h1 style="margin:0;color:#fff;font-size:20px;">&#9888;&#65039; 토더 배달계정 연결오류 알림</h1>
      <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">{today_str} 기준 · {n_stores}개 매장</p>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:28px 32px;">
      <p style="margin:0 0 18px;font-size:15px;color:#333;line-height:1.7;">
        <strong>{greeting}</strong>,<br>
        아래 매장의 토더 배달계정 연결에 오류가 발생하였습니다.<br>
        확인 부탁드립니다.(미해결 시 매일 알림이 발송됩니다.)<br>
        "네이버 지도" <strong>가게없음</strong> 알람 시 조민준 PM에게 연락 바랍니다.
      </p>

      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden;">
        <thead>
          <tr style="background:#f5f5f5;">
            <th style="padding:10px 14px;text-align:left;font-size:13px;color:#555;border-bottom:2px solid #e0e0e0;">매장명</th>
            <th style="padding:10px 14px;text-align:left;font-size:13px;color:#555;border-bottom:2px solid #e0e0e0;">오류 채널</th>
            <th style="padding:10px 14px;text-align:left;font-size:13px;color:#555;border-bottom:2px solid #e0e0e0;">상태</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#fafafa;padding:18px 32px;border-top:1px solid #eee;">
      <p style="margin:0;color:#999;font-size:11px;">본 메일은 <strong>도리당 본사 시스템</strong>에서 자동 발송되었습니다.<br>
      문의사항은 조민준 PM에게 연락 바랍니다.</p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def _send_with_cc(
    subject: str,
    html_content: str,
    to_emails: list,
    cc_emails: list,
    conn_id: str = "doridang_conn_smtp_gmail",
) -> None:
    """CC 지원 이메일 발송 (Airflow SMTP Connection 사용)"""
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    from_email = conn.extra_dejson.get("from_email") or conn.login

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    all_recipients = to_emails + cc_emails

    with smtplib.SMTP(conn.host, conn.port) as server:
        server.starttls()
        server.login(conn.login, conn.password)
        server.sendmail(from_email, all_recipients, msg.as_string())

    logger.info("이메일 발송 완료: to=%s, cc=%s", to_emails, cc_emails)


def _send_failure_email(reason: str, context: dict) -> None:
    """관리자에게 CSV 저장 실패 알림 이메일 발송"""
    subject = "[토더 배달계정 미연결] CSV 저장 실패"
    body = text_to_html(
        f"토더 배달계정 미연결 파이프라인 CSV 저장 단계에서 오류가 발생했습니다.\n\n"
        f"사유: {reason}\n"
        f"실행일: {context.get('ds', '')}"
    )
    try:
        send_email(subject=subject, html_content=body, to_emails=ADMIN_EMAIL, **context)
    except Exception as e:
        logger.error("실패 알림 이메일 발송 오류: %s", e)
