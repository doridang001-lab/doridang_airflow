"""배민 우리가게 클릭 현황 수집 파이프라인.

수집 흐름 (계정 단위):
  로그인 → 매장 목록 조회 → 각 매장별 이번달/저번달 우리가게 클릭 데이터 수집 → 로그아웃

수집 URL:
  https://self.baemin.com/shops/{store_id}/stat/marketing/woori-shop-click
      ?initialDateOption=MONTHLY&initialMonth={YYYY-MM}

저장 경로 (덮어쓰기):
  analytics/baemin_macro/metrics_our_store_clicks/
    brand={brand}/store={store}/ym={YYYY-MM}/woori_shop_click.csv

수집 컬럼:
  collected_at, store_id, store_name, 날짜, 광고지출, 노출수, 클릭수, 주문수, 주문금액, 광고효과

참조: doridang_collector_1.3/content/02_baemin.js → _collectMarketingData()
"""

import logging
import random
import re
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    TIMING,
    get_store_options,
    launch_browser,
    login_baemin,
    logout_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_OUR_STORE_CLICKS_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["도리당", "나홀로"]
_ORIGINAL_BAEMIN_OUR_STORE_CLICKS_DB = BAEMIN_OUR_STORE_CLICKS_DB


def _short_error(exc: Exception) -> str:
    text = str(exc).splitlines()[0].strip()
    return text[:240]


_CRASH_KEYWORDS = (
    "Remote end closed",
    "Connection aborted",
    "RemoteDisconnected",
    "Connection refused",
    "Max retries exceeded",
    "NewConnectionError",
    "invalid session id",
    "chrome not reachable",
    "disconnected",
    "tab crashed",
    "session deleted",
    "Timed out receiving message from renderer",
)


def _is_driver_crash_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(keyword.lower() in msg for keyword in _CRASH_KEYWORDS)


def _is_renderer_timeout(exc: Exception) -> bool:
    return "timed out receiving message from renderer" in str(exc).lower()

_TABLE_CSS = "table[data-atelier-component='Table']"
_TABLE_TIMEOUT = 18  # seconds
_FAST_EMPTY_TIMEOUT = 8  # seconds
_PAGE_SETTLE = 4.0   # URL 이동 후 React 렌더링 안정화 대기(초)
_WOORI_EMPTY_EXPECTED_BRANDS = {"나홀로"}


def _woori_empty_is_expected(store_info: dict) -> bool:
    return store_info.get("brand") in _WOORI_EMPTY_EXPECTED_BRANDS


def collect_woori_shop_click(account_list: list[dict]) -> str:
    """각 배민 계정에 대해 우리가게 클릭 현황을 수집한다."""
    success, fail = 0, 0

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        try:
            driver = launch_browser(account_id)

            if not login_baemin(driver, account_id, account["password"]):
                logger.warning("로그인 실패: %s", account_id)
                fail += 1
                continue

            logger.info("로그인 성공: %s", account_id)

            from urllib.parse import urlparse as _urlparse

            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception as e:
                    logger.warning("대시보드 이동 중 타임아웃 (계속 진행): %s", e)
            else:
                time.sleep(2)

            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                logger.warning("메인 대시보드 로드 실패: %s", account_id)
                fail += 1
                continue

            raw_options = get_store_options(driver)
            store_list = [_parse_store_option(o) for o in raw_options]
            store_list = [s for s in store_list if s]

            if not store_list:
                logger.warning(
                    "수집 대상 브랜드 없음: %s (옵션=%s)", account_id, raw_options
                )
                fail += 1
                continue

            logger.info("수집 대상 매장: %s", store_list)

            target_months = _get_target_months()

            for store_info in store_list:
                store_id = store_info["store_id"]

                for ym in target_months:
                    url = (
                        f"https://self.baemin.com/shops/{store_id}"
                        f"/stat/marketing/woori-shop-click"
                        f"?initialDateOption=MONTHLY&initialMonth={ym}"
                    )
                    logger.info("수집 이동: %s %s", store_info["store"], ym)

                    try:
                        driver.set_page_load_timeout(45)
                        driver.get(url)
                    except Exception as e:
                        logger.info(
                            "페이지 로드 지연/F5 재시도 (%s %s): %s", store_id, ym, _short_error(e)
                        )

                    # 배민 SPA: 직접 URL 접근 시 빈 상태 → 새로고침(F5)으로 데이터 로드
                    time.sleep(random.uniform(1.5, 2.5))
                    try:
                        driver.refresh()
                    except Exception:
                        pass

                    rows = _extract_table_rows(driver, store_id, f"{store_info['brand']} {store_info['store']}", ym)

                    if rows:
                        saved = _save_csv(rows, store_info["brand"], store_info["store"], ym)
                        logger.info(
                            "저장 완료: brand=%s store=%s ym=%s -> %s",
                            store_info["brand"],
                            store_info["store"],
                            ym,
                            saved,
                        )
                    else:
                        logger.info(
                            "데이터 없음(정상): brand=%s store=%s ym=%s", store_info["brand"], store_info["store"], ym
                        )

                    time.sleep(random.uniform(1.5, 3.0))

            logout_baemin(driver, account_id)
            logger.info("로그아웃 완료: %s", account_id)

            wait_sec = random.uniform(*TIMING["logout_wait"])
            logger.info("다음 계정까지 %.0f초 대기", wait_sec)
            time.sleep(wait_sec)

            success += 1

        except Exception as e:
            logger.error("처리 실패 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    summary = f"성공 {success}/{success + fail} 계정"
    logger.info(summary)
    return summary


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------

def _get_target_months() -> list[str]:
    """이번달 + 저번달 YYYY-MM 목록 반환."""
    now = pendulum.now(KST)
    return [now.format("YYYY-MM"), now.subtract(months=1).format("YYYY-MM")]


def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None."""
    text = opt.get("text", "")
    brand = next((b for b in KNOWN_BRANDS if b in text), None)
    if not brand:
        return None

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _extract_table_rows(driver, store_id: str, store_name: str, ym: str) -> list[dict]:
    """우리가게 클릭 현황 테이블에서 행 데이터 추출.

    - 테이블 로드 대기
    - '전체보기' 버튼 클릭 (있으면)
    - tbody 행 파싱
    """
    def _has_rows(d) -> bool:
        # 첫 행 날짜 셀에 M.DD 패턴이 있을 때만 True
        # 배민 테이블: CSS 구조상 Selenium .text는 빈값 → innerText로만 읽을 수 있음
        # M.DD 패턴 체크로 스켈레톤 DOM 오판 방지
        try:
            rows = d.find_elements(By.CSS_SELECTOR, f"{_TABLE_CSS} tbody tr")
            if not rows:
                return False
            cells = rows[0].find_elements(By.TAG_NAME, "td")
            if len(cells) < 7:
                return False
            try:
                val = (cells[0].find_element(
                    By.CSS_SELECTOR, ".styles-module__x4Tk"
                ).get_attribute("innerText") or "").strip()
            except Exception:
                val = (cells[0].get_attribute("innerText") or cells[0].text or "").strip()
            return bool(re.search(r"\d+\.\d+", val))
        except Exception as e:
            # Chrome 세션 종료 신호 → WebDriverWait를 즉시 중단시키기 위해 재발생
            if _is_driver_crash_error(e):
                raise
            return False

    def _ready_empty(d) -> bool:
        try:
            state = d.execute_script("""
                const trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr").length;
                const busy = document.querySelectorAll('[aria-busy="true"], [class*="Loading"], [class*="Spinner"]').length;
                return {trs, busy, ready: document.readyState};
            """) or {}
            return (
                int(state.get("trs") or 0) == 0
                and int(state.get("busy") or 0) == 0
                and state.get("ready") == "complete"
            )
        except Exception as e:
            if _is_driver_crash_error(e):
                raise
            return False

    try:
        fast_state = WebDriverWait(driver, _FAST_EMPTY_TIMEOUT).until(
            lambda d: "rows" if _has_rows(d) else ("empty" if _ready_empty(d) else False)
        )
        if fast_state == "empty":
            try:
                cur_url = driver.current_url
                debug_info = driver.execute_script("""
                    var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
                    return JSON.stringify({tr: trs.length, fast_empty: true});
                """)
                logger.info("타임아웃 디버그 URL=%s DOM=%s", cur_url, debug_info)
            except Exception as _de:
                logger.info("타임아웃 디버그 실패: %s", _de)
            logger.info("테이블 빠른 빈값 종료 (store=%s ym=%s): 데이터 없음", store_name, ym)
            return []
    except TimeoutException:
        try:
            WebDriverWait(driver, _TABLE_TIMEOUT).until(_has_rows)
        except TimeoutException:
            try:
                cur_url = driver.current_url
                debug_info = driver.execute_script("""
                    var trs = document.querySelectorAll("table[data-atelier-component='Table'] tbody tr");
                    if (!trs.length) return JSON.stringify({tr:0});
                    var cells = trs[0].querySelectorAll('td');
                    var val = cells.length > 0 ? cells[0].innerText.slice(0, 30) : '';
                    var hasClass = !!(cells.length > 0 && cells[0].querySelector('.styles-module__x4Tk'));
                    return JSON.stringify({tr: trs.length, cells: cells.length, val: val, hasClass: hasClass});
                """)
                logger.info("타임아웃 디버그 URL=%s DOM=%s", cur_url, debug_info)
            except Exception as _de:
                logger.info("타임아웃 디버그 실패: %s", _de)
            # 빈 테이블은 과거월·광고 미집행 시 정상. WARNING 판단은 호출부(데이터 없음)에 위임.
            logger.info("테이블 로드 타임아웃 (store=%s ym=%s): 데이터 없음", store_name, ym)
            return []
    except Exception as e:
        logger.warning("테이블 대기 중 Chrome 오류 (store=%s ym=%s): %s", store_name, ym, e)
        raise

    try:
        # 전체보기 버튼 클릭
        try:
            btns = driver.find_elements(
                By.CSS_SELECTOR, "button[data-atelier-component='Button']"
            )
            expand_btn = next(
                (b for b in btns if "전체보기" in (b.get_attribute("innerText") or "")),
                None,
            )
            if expand_btn:
                driver.execute_script("arguments[0].scrollIntoView(true);", expand_btn)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", expand_btn)
                try:
                    WebDriverWait(driver, 10).until(
                        lambda d: len(d.find_elements(
                            By.CSS_SELECTOR, f"{_TABLE_CSS} tbody tr"
                        )) > 10
                    )
                except TimeoutException:
                    time.sleep(1.0)
        except Exception:
            pass  # 전체보기 없으면 그냥 진행

        table = driver.find_element(By.CSS_SELECTOR, _TABLE_CSS)
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

        year, month = ym.split("-")
        iso = pendulum.now(KST).isoformat()
        result = []

        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 7:
                continue

            def get_val(cell) -> str:
                # 배민 테이블: CSS 구조상 .text가 빈값 → innerText로 읽어야 함
                try:
                    return (cell.find_element(
                        By.CSS_SELECTOR, ".styles-module__x4Tk"
                    ).get_attribute("innerText") or "").strip()
                except Exception:
                    return (cell.get_attribute("innerText") or cell.text or "").strip()

            raw_date = get_val(cells[0])
            day_match = re.search(r"(\d+)\.(\d+)", raw_date)
            formatted_date = ""
            if day_match:
                page_month = day_match.group(1).zfill(2)
                day = day_match.group(2).zfill(2)
                formatted_date = f"{year}-{page_month}-{day}"

            def clean_num(text: str) -> str:
                return re.sub(r"[,원회건배]", "", text).strip()

            result.append(
                {
                    "collected_at": iso,
                    "store_id": store_id,
                    "store_name": store_name,
                    "날짜": formatted_date,
                    "광고지출": clean_num(get_val(cells[1])),
                    "노출수": clean_num(get_val(cells[2])),
                    "클릭수": clean_num(get_val(cells[3])),
                    "주문수": clean_num(get_val(cells[4])),
                    "주문금액": clean_num(get_val(cells[5])),
                    "광고효과": clean_num(get_val(cells[6])),
                }
            )

        logger.info(
            "테이블 추출 완료: store=%s ym=%s → %d행", store_name, ym, len(result)
        )
        return result

    except Exception as e:
        logger.warning("테이블 파싱 실패 (store=%s ym=%s): %s", store_name, ym, e)
        return []


def _save_csv(rows: list[dict], brand: str, store: str, ym: str) -> Path:
    """덮어쓰기 방식으로 CSV 저장."""
    out_dir = (
        BAEMIN_OUR_STORE_CLICKS_DB
        / f"brand={brand}"
        / f"store={store}"
        / f"ym={ym}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "woori_shop_click.csv"

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("저장 완료: %s (%d행)", out_path, len(df))
    return out_path


def _navigate_to_woori_url(driver, store_id: str, ym: str, url: str) -> None:
    """우가클 URL로 이동. 세션 문제는 상위 복구 흐름으로 전파한다."""
    # 이전 페이지 DOM 해제 → Chrome 메모리 압박 감소
    try:
        driver.set_page_load_timeout(5)
        driver.get("about:blank")
    except Exception:
        pass
    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
    except Exception as e:
        if _is_driver_crash_error(e):
            raise
        logger.info("페이지 이동 지연/F5 재시도 (%s %s): %s", store_id, ym, _short_error(e))


def collect_woori_for_driver(driver, store_list: list[dict]) -> None:
    """이미 로그인된 driver로 우리가게 클릭 현황을 수집한다 (login/logout 없음).

    combined 파이프라인에서 now 수집과 같은 브라우저 세션을 공유할 때 사용.
    실패 시: 1차=F5 새로고침 후 재시도. 그래도 없으면 데이터 없음으로 처리.
    """
    target_months = _get_target_months()

    for store_info in store_list:
        if store_info.get("brand") not in KNOWN_BRANDS:
            logger.info(
                "우가클 광고 없는 브랜드 스킵: %s (%s)",
                store_info["store"], store_info.get("brand"),
            )
            continue

        store_id = store_info["store_id"]

        for ym in target_months:
            url = (
                f"https://self.baemin.com/shops/{store_id}"
                f"/stat/marketing/woori-shop-click"
                f"?initialDateOption=MONTHLY&initialMonth={ym}"
            )
            logger.info("수집 이동: %s %s", store_info["store"], ym)

            _navigate_to_woori_url(driver, store_id, ym, url)
            time.sleep(random.uniform(1.5, 2.5))
            rows = _extract_table_rows(driver, store_id, f"{store_info['brand']} {store_info['store']}", ym)

            if not rows:
                # 1차 재시도: F5 새로고침 후 대기
                logger.info("재시도1 (F5): %s %s", store_info["store"], ym)
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(5.0)
                rows = _extract_table_rows(driver, store_id, f"{store_info['brand']} {store_info['store']}", ym)

            if rows:
                saved = _save_csv(rows, store_info["brand"], store_info["store"], ym)
                logger.info(
                    "저장 완료: brand=%s store=%s ym=%s -> %s",
                    store_info["brand"], store_info["store"], ym, saved,
                )
            else:
                logger.info(
                    "데이터 없음(정상): brand=%s store=%s ym=%s",
                    store_info["brand"], store_info["store"], ym,
                )

            time.sleep(random.uniform(1.5, 3.0))


def _woori_csv_path(brand: str, store: str, ym: str) -> Path:
    return (
        BAEMIN_OUR_STORE_CLICKS_DB
        / f"brand={brand}"
        / f"store={store}"
        / f"ym={ym}"
        / "woori_shop_click.csv"
    )


def _existing_woori_row_count(brand: str, store: str, ym: str) -> int:
    candidates = [_woori_csv_path(brand, store, ym)]
    original_path = (
        _ORIGINAL_BAEMIN_OUR_STORE_CLICKS_DB
        / f"brand={brand}"
        / f"store={store}"
        / f"ym={ym}"
        / "woori_shop_click.csv"
    )
    if original_path != candidates[0]:
        candidates.append(original_path)

    for out_path in candidates:
        if not out_path.exists():
            continue
        try:
            return len(pd.read_csv(out_path, dtype=str))
        except Exception as e:
            logger.warning("기존 우리가게 클릭 CSV 읽기 실패 (%s): %s", out_path, e)
    return 0


def _retry_woori_after_empty(driver, store_info: dict, ym: str, url: str) -> list[dict]:
    store_id = store_info["store_id"]
    store_name = f"{store_info['brand']} {store_info['store']}"

    logger.info("재시도2 (revisit): %s %s", store_name, ym)
    try:
        driver.set_page_load_timeout(20)
        driver.get("https://self.baemin.com/")
        time.sleep(2.0)
    except Exception as e:
        if _is_driver_crash_error(e):
            raise
        logger.warning("홈 복귀 실패 (%s %s): %s", store_name, ym, e)

    revisit_url = f"{url}&_ts={int(time.time() * 1000)}"
    _navigate_to_woori_url(driver, store_id, ym, revisit_url)
    time.sleep(5.0)
    return _extract_table_rows(driver, store_id, store_name, ym)


def collect_woori_for_driver(driver, store_list: list[dict]) -> None:
    """Override earlier implementation with stronger empty-page recovery."""
    target_months = _get_target_months()
    previous_month = pendulum.now(KST).subtract(months=1).format("YYYY-MM")

    for store_info in store_list:
        if store_info.get("brand") not in KNOWN_BRANDS:
            logger.info(
                "우가클 광고 없는 브랜드 스킵: %s (%s)",
                store_info["store"], store_info.get("brand"),
            )
            continue

        store_id = store_info["store_id"]

        for ym in target_months:
            url = (
                f"https://self.baemin.com/shops/{store_id}"
                f"/stat/marketing/woori-shop-click"
                f"?initialDateOption=MONTHLY&initialMonth={ym}"
            )
            logger.info("수집 이동: %s %s", store_info["store"], ym)

            _navigate_to_woori_url(driver, store_id, ym, url)
            time.sleep(_PAGE_SETTLE)
            rows = _extract_table_rows(driver, store_id, f"{store_info['brand']} {store_info['store']}", ym)
            existing_count = _existing_woori_row_count(
                store_info["brand"], store_info["store"], ym
            )

            if not rows and existing_count > 0:
                logger.info("재시도1 (F5): %s %s", store_info["store"], ym)
                try:
                    driver.set_page_load_timeout(20)
                    driver.refresh()
                except Exception as e:
                    if _is_driver_crash_error(e) and not _is_renderer_timeout(e):
                        raise
                    logger.info("우가클 F5 지연(빈값 처리 계속) (%s %s): %s", store_info["store"], ym, _short_error(e))
                time.sleep(8.0)
                rows = _extract_table_rows(driver, store_id, f"{store_info['brand']} {store_info['store']}", ym)

            if not rows and existing_count > 0:
                rows = _retry_woori_after_empty(driver, store_info, ym, url)

            if rows:
                saved = _save_csv(rows, store_info["brand"], store_info["store"], ym)
                logger.info(
                    "저장 완료: brand=%s store=%s ym=%s -> %s",
                    store_info["brand"], store_info["store"], ym, saved,
                )
            else:
                if _woori_empty_is_expected(store_info):
                    logger.info("우가클 데이터없음(정상): brand=%s store=%s ym=%s", store_info["brand"], store_info["store"], ym)
                else:
                    logger.info("데이터 없음(정상): brand=%s store=%s ym=%s", store_info["brand"], store_info["store"], ym)
                if ym == previous_month and existing_count > 0 and not _woori_empty_is_expected(store_info):
                    raise RuntimeError(
                        f"woori previous month unexpectedly empty: "
                        f"{store_info['store']} {ym} existing_rows={existing_count}"
                    )

            time.sleep(random.uniform(1.5, 3.0))
