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
KNOWN_BRANDS = ["나홀로", "도리당"]

_TABLE_CSS = "table[data-atelier-component='Table']"
_TABLE_TIMEOUT = 20  # seconds (데이터 있는 경우 실측 8초면 로드됨)
_PAGE_SETTLE = 4.0   # URL 이동 후 React 렌더링 안정화 대기(초)


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
                        logger.warning(
                            "페이지 로드 타임아웃 (%s %s): %s", store_id, ym, e
                        )

                    rows = _extract_table_rows(driver, store_id, store_info["store"], ym)

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
                        logger.warning(
                            "데이터 없음: store=%s ym=%s", store_info["store"], ym
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
        # WebDriverException 등 Chrome 내부 오류가 lambda 밖으로
        # propagate되지 않도록 모든 예외를 잡아 False 반환
        try:
            return len(d.find_elements(
                By.CSS_SELECTOR, f"{_TABLE_CSS} tbody tr"
            )) > 0
        except Exception:
            return False

    try:
        # tbody tr 데이터 행이 실제로 채워질 때까지 대기
        WebDriverWait(driver, _TABLE_TIMEOUT).until(_has_rows)

        # 전체보기 버튼 클릭
        try:
            btns = driver.find_elements(
                By.CSS_SELECTOR, "button[data-atelier-component='Button']"
            )
            expand_btn = next((b for b in btns if "전체보기" in b.text), None)
            if expand_btn:
                expand_btn.click()
                time.sleep(random.uniform(0.5, 1.0))
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
                # 확인된 클래스명으로 먼저 시도, 없으면 cell 전체 텍스트
                try:
                    return cell.find_element(
                        By.CSS_SELECTOR, ".styles-module__x4Tk"
                    ).text.strip()
                except Exception:
                    return cell.text.strip()

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
        logger.warning("테이블 추출 실패 (store=%s ym=%s): %s", store_name, ym, e)
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


def collect_woori_for_driver(driver, store_list: list[dict]) -> None:
    """이미 로그인된 driver로 우리가게 클릭 현황을 수집한다 (login/logout 없음).

    combined 파이프라인에서 now 수집과 같은 브라우저 세션을 공유할 때 사용.
    """
    target_months = _get_target_months()

    for store_info in store_list:
        store_id = store_info["store_id"]

        for ym in target_months:
            url = (
                f"https://self.baemin.com/shops/{store_id}"
                f"/stat/marketing/woori-shop-click"
                f"?initialDateOption=MONTHLY&initialMonth={ym}"
            )
            logger.info("\uC218\uC9D1 \uC774\uB3D9: %s %s", store_info["store"], ym)

            try:
                driver.set_page_load_timeout(45)
                driver.get(url)
            except Exception as e:
                logger.warning(
                    "\uD398\uC774\uC9C0 \uB85C\uB4DC \uD0C0\uC784\uC544\uC6C3 (%s %s): %s",
                    store_id, ym, e,
                )

            time.sleep(_PAGE_SETTLE)  # React 초기 렌더링 대기

            rows = _extract_table_rows(driver, store_id, store_info["store"], ym)

            if rows:
                saved = _save_csv(rows, store_info["brand"], store_info["store"], ym)
                logger.info(
                    "\uC800\uC7A5 \uC644\uB8CC: brand=%s store=%s ym=%s -> %s",
                    store_info["brand"], store_info["store"], ym, saved,
                )
            else:
                logger.warning(
                    "\uB370\uC774\uD130 \uC5C6\uC74C: store=%s ym=%s",
                    store_info["store"], ym,
                )

            time.sleep(random.uniform(1.5, 3.0))
