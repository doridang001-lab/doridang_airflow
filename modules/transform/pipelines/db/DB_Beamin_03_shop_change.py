"""배민 매장 변경이력 수집 파이프라인.

수집 흐름:
  로그인 → 매장별 변경이력 페이지 조회 → 테이블 추출 → 로그아웃

수집 URL:
  https://self.baemin.com/history/change/shop

저장 경로 (덮어쓰기):
  analytics/baemin_macro/shop_change/
    brand={brand}/store={store}/ym={YYYY-MM}/shop_change.csv
"""

import logging
import random
import re
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    TIMING,
    get_store_options,
    human_click,
    launch_browser,
    login_baemin,
    logout_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_SHOP_CHANGE_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["나홀로", "도리당"]

SHOP_CHANGE_URL = "https://self.baemin.com/history/change/shop"
SELECT_CSS = "select[class*='Select-module']"
QUERY_BTN_CSS = "button.button.medium.primary.self-ds"
ITEM_CSS = "li.ListItem.self-ds"   # 테이블 아닌 리스트 구조
ITEM_TIMEOUT = 20
_PAGE_SETTLE = 4.0
_SCROLL_PX = 400
_SCROLL_WAIT = 0.4
_SCROLL_NO_NEW_MAX = 5
_SCROLL_MAX_ITER = 200


def collect_shop_change(account_list: list[dict]) -> str:
    """각 배민 계정에 대해 매장 변경이력을 수집한다 (단독 실행)."""
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
                    logger.warning("대시보드 이동 타임아웃 (계속 진행): %s", e)
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

            collect_shop_change_for_driver(driver, store_list)

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


def collect_shop_change_for_driver(driver, store_list: list[dict]) -> None:
    """이미 로그인된 driver로 매장 변경이력을 수집한다 (login/logout 없음).

    combined 파이프라인에서 같은 브라우저 세션을 공유할 때 사용.
    매장 루프마다 페이지를 재방문해 StaleElementReferenceException을 방지한다.
    """
    # 변경이력 페이지 최초 진입해 select 옵션 교차 검증
    try:
        driver.set_page_load_timeout(45)
        driver.get(SHOP_CHANGE_URL)
    except Exception as e:
        logger.warning("변경이력 페이지 첫 진입 타임아웃 (계속): %s", e)

    if not wait_for_page(driver, SELECT_CSS, timeout=30):
        logger.warning("변경이력 페이지 select 로드 실패 → 수집 건너뜀")
        return

    page_options = _get_page_options(driver)  # {store_id: text}
    matched_stores = [s for s in store_list if s["store_id"] in page_options]

    if not matched_stores:
        logger.warning(
            "변경이력 페이지에 수집 대상 매장 없음 (page=%s)", list(page_options.keys())
        )
        return

    logger.info("변경이력 수집 대상 매장: %s", [s["store"] for s in matched_stores])

    for store_info in matched_stores:
        store_id = store_info["store_id"]
        logger.info("변경이력 수집 시작: %s (%s)", store_info["store"], store_id)

        # 매장마다 페이지 재방문 → StaleElementException 방지
        try:
            driver.set_page_load_timeout(45)
            driver.get(SHOP_CHANGE_URL)
        except Exception as e:
            logger.warning("페이지 재방문 타임아웃 (%s): %s", store_id, e)

        if not wait_for_page(driver, SELECT_CSS, timeout=30):
            logger.warning("select 로드 실패, 건너뜀: %s", store_info["store"])
            continue

        try:
            sel_elem = driver.find_element(By.CSS_SELECTOR, SELECT_CSS)
            Select(sel_elem).select_by_value(store_id)
            time.sleep(1.0)

            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, QUERY_BTN_CSS))
            )
            human_click(driver, btn)
            logger.info("조회 클릭 완료: %s", store_info["store"])

        except Exception as e:
            logger.warning("매장 선택/조회 실패 (%s): %s", store_info["store"], e)
            continue

        time.sleep(_PAGE_SETTLE)  # React 초기 렌더링 안정화 대기
        rows = _extract_change_rows(driver, store_info)

        if rows:
            saved = _save_csv(rows, store_info["brand"], store_info["store"])
            logger.info(
                "저장 완료: brand=%s store=%s → %s (%d행)",
                store_info["brand"],
                store_info["store"],
                saved,
                len(rows),
            )
        else:
            logger.warning("데이터 없음: %s", store_info["store"])

        time.sleep(random.uniform(1.5, 3.0))


# ---------------------------------------------------------------------------
# 내부 헬퍼
# ---------------------------------------------------------------------------

def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → 매장 메타데이터. 대상 브랜드 아니면 None."""
    text = opt.get("text", "")
    brand = next((b for b in KNOWN_BRANDS if b in text), None)
    if not brand:
        return None

    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _get_page_options(driver) -> dict[str, str]:
    """변경이력 페이지 select에서 {store_id: text} 반환."""
    return driver.execute_script(r"""
        const sel = document.querySelector('select[class*="Select-module"]');
        if (!sel) return {};
        const res = {};
        for (const o of sel.options) res[o.value] = o.textContent.trim();
        return res;
    """) or {}


_EXTRACT_JS = r"""
return (function() {
    const items = document.querySelectorAll('li.ListItem.self-ds');
    const rows = [];
    for (const item of items) {
        const title = item.querySelector('h5.flex-1')?.textContent.trim() || '';
        const date  = item.querySelector('time.ListItem-date')?.getAttribute('date') || '';

        let category = '', worker = '';
        item.querySelectorAll('.HistoryItemContents-module__Zcx3').forEach(row => {
            const label = row.querySelector('.HistoryItemContents-module__sGh2')?.textContent.trim() || '';
            const value = row.querySelector('.HistoryItemContents-module__FXZ7')?.textContent.trim() || '';
            if (label === '분류')   category = value;
            else if (label === '작업자') worker = value;
        });

        let after = '', before = '';
        const zone = item.querySelector('.HistoryItemContents-module__ZwKd');
        if (zone) {
            const divs = zone.querySelectorAll(':scope > div');
            if (divs[0]) after  = divs[0].textContent.replace('[변경 후]', '').trim();
            const beforeEl = zone.querySelector('.HistoryItemContents-module__fKIx');
            if (beforeEl) before = beforeEl.textContent.replace('[변경 전]', '').trim();
        }

        const TARGET = ['영업임시중지', '휴무일', '운영시간'];
        if ((title || date) && TARGET.some(t => title.includes(t)))
            rows.push({ title, date, category, worker, after, before });
    }
    return rows;
})();
"""


def _scroll_to_load_all(driver) -> int:
    """가상 스크롤을 내려 모든 변경이력 항목을 DOM에 로드한다.

    새 항목이 _SCROLL_NO_NEW_MAX 회 연속 없으면 종료.
    반환: 최종 로드된 항목 수.
    """
    prev_count = 0
    no_new = 0
    current = 0

    for _ in range(_SCROLL_MAX_ITER):
        driver.execute_script("window.scrollBy(0, arguments[0]);", _SCROLL_PX)
        time.sleep(_SCROLL_WAIT)

        current = driver.execute_script(
            "return document.querySelectorAll('li.ListItem.self-ds').length;"
        )
        if current == prev_count:
            no_new += 1
            if no_new >= _SCROLL_NO_NEW_MAX:
                break
        else:
            no_new = 0
            prev_count = current

    logger.info("스크롤 완료: %d항목 로드", current)
    return current


def _extract_change_rows(driver, store_info: dict) -> list[dict]:
    """변경이력 리스트 항목 파싱.

    DOM 구조: li.ListItem.self-ds (테이블 없음, 익스텐션 동일 로직)
    조회 클릭 + _PAGE_SETTLE 대기 후 호출된다.
    항목이 없으면 변경이력 없음 (0행), 새로고침 재시도 1회.
    """
    def _has_items(d) -> bool:
        try:
            return len(d.find_elements(By.CSS_SELECTOR, ITEM_CSS)) > 0
        except Exception:
            return False

    loaded = False
    for attempt in range(2):
        try:
            WebDriverWait(driver, ITEM_TIMEOUT).until(_has_items)
            loaded = True
            break
        except TimeoutException:
            if attempt == 0:
                logger.info(
                    "리스트 %d초 초과 → 새로고침 재시도 (%s)",
                    ITEM_TIMEOUT,
                    store_info["store"],
                )
                driver.refresh()
                time.sleep(random.uniform(3.0, 5.0))
        except Exception as e:
            logger.warning("리스트 대기 중 오류 (%s): %s", store_info["store"], e)
            break

    if not loaded:
        logger.info("변경이력 없음 (리스트 미검출): %s", store_info["store"])
        return []

    _scroll_to_load_all(driver)

    try:
        raw = driver.execute_script(_EXTRACT_JS) or []
        iso = pendulum.now(KST).isoformat()
        rows = [
            {
                "title": r.get("title", ""),
                "date": r.get("date", ""),
                "category": r.get("category", ""),
                "worker": r.get("worker", ""),
                "after": r.get("after", ""),
                "before": r.get("before", ""),
                "brand": store_info["brand"],
                "store_id": store_info["store_id"],
                "store_name": store_info["store"],
                "collected_at": iso,
            }
            for r in raw
        ]
        logger.info("변경이력 추출 완료: store=%s → %d행", store_info["store"], len(rows))
        return rows

    except Exception as e:
        logger.warning("변경이력 추출 실패 (%s): %s", store_info["store"], e)
        return []


def _save_csv(rows: list[dict], brand: str, store: str) -> Path:
    """upsert 방식으로 CSV 저장 (ym 파티션).

    같은 date+title 이면 덮어쓰고, 신규 항목은 추가.
    """
    ym = pendulum.now(KST).format("YYYY-MM")
    out_dir = BAEMIN_SHOP_CHANGE_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "shop_change.csv"

    new_df = pd.DataFrame(rows).astype(str)

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # 신규 rows의 (date, title) 키와 겹치는 기존 행 제거
        new_keys = set(zip(new_df.get("date", pd.Series(dtype=str)), new_df.get("title", pd.Series(dtype=str))))
        mask = ~existing.apply(
            lambda r: (r.get("date", ""), r.get("title", "")) in new_keys, axis=1
        )
        combined = pd.concat([existing[mask], new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("저장 완료: %s (%d행)", out_path, len(combined))
    return out_path
