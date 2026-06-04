"""배민 광고 funnel 통계 수집 파이프라인.

수집 흐름:
  account_id/password → 매장별 독립 Chrome 세션
    → stat/advertisement?initialDateOption=MONTH 이동
    → Filter 버튼(최근 4주) 클릭
    → 일별(DAILY) 라디오 선택 → 어제(YESTERDAY) 선택 → 적용
    → 지표 추출 (노출수 / 클릭수 / 주문수 / 주문금액)
    → CSV append 저장 (upsert by target_date)
    → Chrome quit

저장 경로:
  analytics/baemin_macro/ad_funnel/
    brand={brand}/store={store}/ym={YYYY-MM}/baemin_ad_funnel.csv
"""

import logging
import random
import time
from pathlib import Path

import pandas as pd
import pendulum
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from modules.extract.croling_beamin import (
    human_click,
    launch_browser,
    login_baemin,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_AD_FUNNEL_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

_AD_URL_TEMPLATE = (
    "https://self.baemin.com/shops/{store_id}/stat/advertisement?initialDateOption=MONTH"
)
_FILTER_BTN_CSS   = "button.Filter-module__lRdH"
_METRIC_LABELS    = ["노출수", "클릭수", "주문수", "주문금액"]
_COLUMNS          = ["collected_at", "target_date", "store_name"] + _METRIC_LABELS


# ---------------------------------------------------------------------------
# 공개 함수
# ---------------------------------------------------------------------------

def collect_ad_funnel_for_driver(
    driver,
    store_info: dict,
    target_date: str | None = None,
) -> bool:
    """기존 Chrome 세션으로 단일 매장의 광고 funnel 통계를 수집한다.

    combined.py의 per-store 루프에서 로그인 없이 호출.
    광고 없는 매장은 True 반환 (정상).

    Returns:
        True if succeeded (광고 없음 포함), False if failed.
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    store_id = store_info["store_id"]
    brand    = store_info["brand"]
    store    = store_info["store"]

    logger.info("광고 funnel 수집 시작: %s (%s) / %s", store, store_id, target_date)

    try:
        driver.set_page_load_timeout(45)
        driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

        if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
            logger.warning("광고 통계 페이지 로드 실패, 건너뜀: %s", store)
            return False

        if not _set_ad_filter_yesterday(driver, store):
            logger.warning("필터 설정 실패, 건너뜀: %s", store)
            return False

        metrics = _extract_ad_metrics(driver, store)
        if metrics is None:
            logger.info("광고 없음 또는 지표 추출 실패: %s / %s", store, target_date)
            return True

        saved = _save_ad_funnel_csv(metrics, brand, store, target_date)
        logger.info("저장 완료: brand=%s store=%s → %s", brand, store, saved)
        return True

    except Exception as e:
        logger.warning("광고 funnel 수집 실패 (%s): %s", store, e)
        return False


def collect_ad_funnel_for_account(
    account_id: str,
    password: str,
    store_list: list[dict],
    target_date: str | None = None,
) -> list[dict]:
    """매장별 독립 Chrome 세션으로 광고 funnel 통계를 수집한다.

    Returns:
        수집 실패한 store_info 리스트 (빈 리스트 = 전부 성공).
    """
    if target_date is None:
        target_date = pendulum.yesterday(KST).format("YYYY-MM-DD")

    failed_stores: list[dict] = []

    for store_info in store_list:
        store_id = store_info["store_id"]
        brand    = store_info["brand"]
        store    = store_info["store"]

        logger.info("광고 funnel 수집 시작: %s (%s) / %s", store, store_id, target_date)

        succeeded = False
        for attempt in range(2):
            driver = None
            try:
                driver = launch_browser(account_id)

                if not login_baemin(driver, account_id, password):
                    logger.warning("로그인 실패: %s / %s", account_id, store)
                    break

                driver.set_page_load_timeout(45)
                driver.get(_AD_URL_TEMPLATE.format(store_id=store_id))

                if not wait_for_page(driver, _FILTER_BTN_CSS, timeout=30):
                    logger.warning("광고 통계 페이지 로드 실패, 건너뜀: %s", store)
                    if attempt == 0:
                        continue
                    break

                if not _set_ad_filter_yesterday(driver, store):
                    if attempt == 0:
                        logger.warning("필터 설정 실패, Chrome 재시작 후 재시도: %s", store)
                        continue
                    logger.warning("필터 설정 최종 실패, 건너뜀: %s", store)
                    break

                metrics = _extract_ad_metrics(driver, store)
                if metrics is None:
                    logger.info("광고 없음 또는 지표 추출 실패: %s / %s", store, target_date)
                    succeeded = True
                    break

                saved = _save_ad_funnel_csv(metrics, brand, store, target_date)
                logger.info("저장 완료: brand=%s store=%s → %s", brand, store, saved)
                succeeded = True
                break

            except Exception as e:
                logger.warning("매장 수집 실패 (%s) attempt=%d: %s", store, attempt + 1, e)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            if attempt == 0:
                time.sleep(random.uniform(3.0, 5.0))

        if not succeeded:
            failed_stores.append(store_info)

        time.sleep(random.uniform(0.5, 1.5))

    return failed_stores


# ---------------------------------------------------------------------------
# 필터 설정
# ---------------------------------------------------------------------------

def _set_ad_filter_yesterday(driver, store_name: str) -> bool:
    """광고 통계 기간 필터 전체를 '일별 → 어제'로 설정한다.

    페이지에 Filter-module__lRdH 버튼이 2개 존재:
      [0] 노출수·클릭수 담당
      [1] 주문수·주문금액 담당
    둘 다 YESTERDAY로 설정해야 전체 지표가 어제 기준으로 갱신된다.
    """
    try:
        # 전체 Filter 버튼 개수 확인
        btn_count = driver.execute_script(
            "return document.querySelectorAll('button.Filter-module__lRdH').length;"
        )
        if not btn_count:
            logger.warning("Filter 버튼(Filter-module__lRdH) 미발견: %s", store_name)
            return False
        logger.info("Filter 버튼 %d개 발견: %s", btn_count, store_name)

        for btn_idx in range(btn_count):
            if not _apply_yesterday_to_filter(driver, btn_idx, store_name):
                logger.warning("Filter[%d] 적용 실패: %s", btn_idx, store_name)
                return False
            time.sleep(0.3)

        # 최종 검증: 모든 Filter 버튼 텍스트 확인
        filter_texts = driver.execute_script(
            """
            return [...document.querySelectorAll('button.Filter-module__lRdH')]
                .map(b => b.textContent.trim());
            """
        )
        logger.info("날짜 필터 최종 확인: %s / %s", filter_texts, store_name)
        if not all("어제" in t for t in filter_texts):
            logger.warning("일부 Filter 버튼이 '어제'가 아님: %s", filter_texts)
            return False

        return True

    except (TimeoutException, Exception) as e:
        logger.warning("광고 필터 설정 오류 (%s): %s", store_name, e)
        return False


def _apply_yesterday_to_filter(driver, btn_idx: int, store_name: str) -> bool:
    """지정된 인덱스의 Filter-module__lRdH 버튼을 클릭하고 DAILY → YESTERDAY → 적용."""
    # 1. btn_idx 번째 Filter 버튼 클릭
    clicked = driver.execute_script(
        """
        const btns = document.querySelectorAll('button.Filter-module__lRdH');
        const btn = btns[arguments[0]];
        if (!btn) return false;
        btn.click();
        return btn.textContent.trim().slice(0, 30);
        """,
        btn_idx,
    )
    if not clicked:
        logger.warning("Filter[%d] 버튼 없음: %s", btn_idx, store_name)
        return False
    logger.info("Filter[%d] 클릭: %r / %s", btn_idx, clicked, store_name)
    # DAILY 라디오 팝업이 나타날 때까지 대기 (고정 1.0s 제거)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'input[name="ad-stats-period-filter"][value="DAILY"]')
        )
    )

    # 2. DAILY 라디오 → label.click() (readonly=True)
    daily = driver.execute_script(
        """
        const radio = document.querySelector('input[name="ad-stats-period-filter"][value="DAILY"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if daily == "not_found":
        logger.warning("Filter[%d] DAILY 라디오 미발견: %s", btn_idx, store_name)
        return False
    logger.info("Filter[%d] DAILY: %s", btn_idx, daily)
    # DAILY 클릭 후 React 배치 업데이트 완료 대기
    # checked=True 만으로는 부족 — 렌더 사이클 완료까지 sleep 필수
    time.sleep(0.5)

    # 3. YESTERDAY 라디오 — visible한지 확인 후 label.click()
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script(
            "const r = document.querySelector('input[name=\"period\"][value=\"YESTERDAY\"]');"
            "return r && r.offsetParent !== null;"
        )
    )
    yest = driver.execute_script(
        """
        const radio = document.querySelector('input[name="period"][value="YESTERDAY"]');
        if (!radio) return 'not_found';
        const lbl = document.querySelector('label[for="' + radio.id + '"]');
        if (lbl) { lbl.click(); return 'label_clicked:' + lbl.textContent.trim(); }
        radio.click(); return 'radio_fallback';
        """
    )
    if yest == "not_found":
        logger.warning("Filter[%d] YESTERDAY 라디오 미발견: %s", btn_idx, store_name)
        return False
    logger.info("Filter[%d] YESTERDAY: %s", btn_idx, yest)
    time.sleep(0.5)  # React YESTERDAY 선택 반영 (0.3 → 0.5)

    # 4. 적용 버튼 클릭 (JS click — headless 환경에서 ActionChains viewport 이탈 방지)
    applied = driver.execute_script(
        """
        const btn = [...document.querySelectorAll('button')]
            .find(b => b.innerText.trim() === '적용' || b.textContent.trim() === '적용');
        if (!btn) return false;
        btn.click();
        return true;
        """
    )
    if not applied:
        logger.warning("Filter[%d] 적용 버튼 미발견: %s", btn_idx, store_name)
        return False
    # 필터 버튼 텍스트가 "어제"로 바뀔 때까지 대기
    try:
        WebDriverWait(driver, 15).until(
            lambda d: "어제" in (d.execute_script(
                "const btns = document.querySelectorAll('button.Filter-module__lRdH');"
                "return btns[arguments[0]]?.textContent || '';",
                btn_idx,
            ) or "")
        )
    except TimeoutException:
        logger.warning("Filter[%d] 적용 후 버튼 텍스트 미변경 (15s 초과): %s", btn_idx, store_name)
        return False
    return True


# ---------------------------------------------------------------------------
# 지표 추출
# ---------------------------------------------------------------------------

def _extract_ad_metrics(driver, store_name: str) -> dict | None:
    """광고 funnel 지표 카드에서 노출수/클릭수/주문수/주문금액을 추출한다.

    탭 버튼(Tab_b_r4ax)과 값 span(style*="margin") 순서를 index로 매핑한다.
    광고가 없는 매장은 None 반환 (저장 스킵, 정상 처리).
    Filter[1](주문수·주문금액) React 렌더 지연 대비: 4개 값 모두 로드될 때까지 최대 10초 대기.
    """
    try:
        # 4개 지표 값이 모두 DOM에 나타날 때까지 대기
        # (Filter[1] 적용 후 주문수·주문금액 렌더 지연 방지)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: (d.execute_script(
                    "return [...document.querySelectorAll('span[style*=\"margin\"]')]"
                    ".map(s=>s.textContent.trim()).filter(v=>/^[\\d,]+$/.test(v)).length;"
                ) or 0) >= 4
            )
        except TimeoutException:
            logger.warning("지표 4개 로드 10초 초과, 현재 상태로 추출 시도: %s", store_name)

        result = driver.execute_script(
            """
            const LABELS = ['노출수', '클릭수', '주문수', '주문금액'];

            // 광고 없음 체크
            const body = document.body.textContent;
            if (body.includes('등록된 광고가 없') || body.includes('광고를 설정')) {
                return {status: 'no_ads'};
            }

            // 탭 버튼 순서 수집
            const tabs = [...document.querySelectorAll('button[class*="Tab_b_r4ax"]')]
                .map(b => b.textContent.trim())
                .filter(t => LABELS.includes(t));

            // 값 span 순서 수집 (style에 "margin" 포함 + 순수 숫자,콤마 패턴)
            const vals = [...document.querySelectorAll('span[style*="margin"]')]
                .map(s => s.textContent.trim())
                .filter(v => /^[\\d,]+$/.test(v));

            if (tabs.length === 0 || vals.length === 0) {
                return {status: 'not_found', tabs, vals};
            }

            const metrics = {};
            tabs.forEach((t, i) => { if (vals[i] !== undefined) metrics[t] = vals[i]; });
            return {status: 'ok', metrics};
            """
        )

        if result["status"] == "no_ads":
            logger.info("광고 없는 매장: %s", store_name)
            return None

        if result["status"] == "not_found":
            logger.warning(
                "지표 추출 실패 (tabs=%s, vals=%s): %s",
                result.get("tabs"), result.get("vals"), store_name,
            )
            return None

        metrics = result["metrics"]
        logger.info("지표 추출 완료: %s → %s", store_name, metrics)
        return metrics

    except Exception as e:
        logger.warning("지표 추출 오류 (%s): %s", store_name, e)
        return None


# ---------------------------------------------------------------------------
# 빈값 점검 + 재수집
# ---------------------------------------------------------------------------

def _validate_and_retry_ad_funnel(
    store_infos: list[dict],
    target_date: str,
) -> dict:
    """저장된 ad_funnel CSV에서 빈값(주문수·주문금액) 있는 매장 찾아 재수집.

    store_infos 각 항목: {"account_id", "password", "store_id", "brand", "store"}

    Returns:
        {"empty_stores": [...], "retried": [...], "still_empty": [...]}
    """
    ym = target_date[:7]
    empty_stores: list[dict] = []

    for si in store_infos:
        brand, store = si["brand"], si["store"]
        csv_path = (
            BAEMIN_AD_FUNNEL_DB
            / f"brand={brand}"
            / f"store={store}"
            / f"ym={ym}"
            / "baemin_ad_funnel.csv"
        )
        if not csv_path.exists():
            continue
        df = pd.read_csv(csv_path, dtype=str)
        row = df[df["target_date"] == target_date]
        if row.empty:
            continue
        r = row.iloc[0]
        if str(r.get("주문수", "")).strip() == "" or str(r.get("주문금액", "")).strip() == "":
            logger.warning("ad_funnel 빈값 발견: %s / %s", store, target_date)
            empty_stores.append(si)

    if not empty_stores:
        return {"empty_stores": [], "retried": [], "still_empty": []}

    logger.warning(
        "ad_funnel 빈값 재수집 대상: %s", [s["store"] for s in empty_stores]
    )

    # account_id 기준으로 그룹화해 재수집
    by_account: dict[tuple, list] = {}
    for s in empty_stores:
        key = (s["account_id"], s["password"])
        by_account.setdefault(key, []).append(s)

    for (account_id, password), stores in by_account.items():
        collect_ad_funnel_for_account(
            account_id, password, stores, target_date=target_date
        )

    # 재수집 후 재점검
    still_empty: list[dict] = []
    for si in empty_stores:
        brand, store = si["brand"], si["store"]
        csv_path = (
            BAEMIN_AD_FUNNEL_DB
            / f"brand={brand}"
            / f"store={store}"
            / f"ym={ym}"
            / "baemin_ad_funnel.csv"
        )
        if not csv_path.exists():
            still_empty.append(si)
            continue
        df2 = pd.read_csv(csv_path, dtype=str)
        row2 = df2[df2["target_date"] == target_date]
        if row2.empty or str(row2.iloc[0].get("주문수", "")).strip() == "":
            still_empty.append(si)
            logger.warning("재수집 후에도 빈값 잔존: %s / %s", store, target_date)
        else:
            logger.info("재수집 성공: %s / %s", store, target_date)

    return {
        "empty_stores": empty_stores,
        "retried": empty_stores,
        "still_empty": still_empty,
    }


# ---------------------------------------------------------------------------
# CSV 저장
# ---------------------------------------------------------------------------

def _save_ad_funnel_csv(
    metrics: dict,
    brand: str,
    store: str,
    target_date: str,
) -> Path:
    """광고 funnel 지표를 월별 CSV에 upsert 저장한다."""
    ym = target_date[:7]  # "YYYY-MM"
    out_dir = BAEMIN_AD_FUNNEL_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "baemin_ad_funnel.csv"

    row = {
        "collected_at": pendulum.now(KST).isoformat(),
        "target_date":  target_date,
        "store_name":   store,
    }
    for label in _METRIC_LABELS:
        row[label] = metrics.get(label, "")

    new_df = pd.DataFrame([row], columns=_COLUMNS)

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # 같은 날짜 행 제거 후 append (upsert)
        existing = existing[existing["target_date"] != target_date]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path
