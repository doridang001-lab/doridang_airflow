"""배민 우리가게NOW 지표 수집 (Phase 1)

흐름 (계정별 순차):
  login → ShopSelect 로드 → 드롭다운 옵션 파싱 → 브랜드별 매장 선택
  → WooriShopNowCard 로드 → JS 수집 → 파티션 CSV 저장 → logout → sleep
"""

import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from modules.extract.croling_beamin import (
    TIMING,
    collect_single_store_stats,
    get_store_options,
    launch_browser,
    login_baemin,
    logout_baemin,
    select_store_by_id,
    wait_for_metrics_data,
    wait_for_page,
)
from modules.transform.utility.paths import BAEMIN_METRICS_DB

logger = logging.getLogger(__name__)

KNOWN_BRANDS = ["나홀로", "도리당"]


# ── 공개 API ──────────────────────────────────────────────────────────────────

def collect_now_stats(account_list: list[dict]) -> str:
    """우리가게NOW 지표 수집 (계정별 순차, 계정당 다중 브랜드 지원).

    반환: "성공 N/M 계정" 요약 문자열 (XCom 로그용).
    """
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

            # 이미 self.baemin.com HOST에 있으면 재로드 생략 (강제 reload → 메모리 부족 hang 방지)
            # returnUrl 파라미터에 self.baemin.com이 포함된 로그인 페이지와 구분하기 위해 urlparse 사용
            from urllib.parse import urlparse as _urlparse
            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception as e:
                    logger.warning("대시보드 이동 중 타임아웃 (계속 진행): %s", e)
            else:
                time.sleep(2)  # 로그인 리다이렉트 직후 React 초기화 안정화
            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                logger.warning("메인 대시보드 로드 실패: %s", account_id)
                fail += 1
                continue

            # 드롭다운 옵션 파싱 → 브랜드 필터
            raw_options = get_store_options(driver)
            store_list = [_parse_store_option(o) for o in raw_options]
            store_list = [s for s in store_list if s]

            if not store_list:
                logger.warning("수집 대상 브랜드 없음: %s (옵션=%s)", account_id, raw_options)
                fail += 1
                continue

            logger.info("수집 대상 매장: %s", store_list)

            # 매장별 순차 수집 (드롭다운 선택 + 20초 대기)
            for store_info in store_list:
                logger.info("매장 선택 시도: %s", store_info)
                if not select_store_by_id(driver, store_info["store_id"]):
                    logger.warning("매장 선택 실패: %s", store_info)
                    continue

                # React metrics 데이터 로드 대기 (실제 데이터 화면 표시될 때까지 폴링)
                logger.info("매장 데이터 로드 대기: %s", store_info["store"])
                if not wait_for_metrics_data(driver, timeout=45):
                    logger.warning("매쟁 데이터 로드 타임아웃: %s", store_info["store"])
                    continue

                # 진단: 수집 시점의 URL과 WooriShopNowItem 개수 확인
                try:
                    dom_info = driver.execute_script(r"""
                        return {
                            url: location.href,
                            itemCount: document.querySelectorAll(
                                '.WooriShopNowItem-module__TKcC'
                            ).length,
                            cardCount: document.querySelectorAll(
                                '.WooriShopNowCard-module__rcFf'
                            ).length
                        };
                    """)
                    logger.info("수집 시점 DOM: %s", dom_info)
                except Exception:
                    pass

                stats = collect_single_store_stats(
                    driver, store_info["store_id"], account_id
                )
                stats["brand"] = store_info["brand"]
                stats["store"] = store_info["store"]

                saved = _save_metrics_csv(stats, store_info["brand"], store_info["store"])
                logger.info(
                    "수집 완료: brand=%s store=%s → %s",
                    store_info["brand"], store_info["store"], saved,
                )

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


# ── 내부 함수 ─────────────────────────────────────────────────────────────────

def _parse_store_option(opt: dict) -> dict | None:
    """드롭다운 옵션 → {store_id, brand, store}.

    브랜드 미감지(임시가게 등)이면 None 반환.
    opt 예시: {"store_id": "14822058", "text": "[음식배달] 나홀로 1인 곱도리탕 역삼점 / 찜·탕·찌개 14822058"}
    """
    text = opt.get("text", "")
    brand = next((b for b in KNOWN_BRANDS if b in text), None)
    if not brand:
        return None

    # "[음식배달] 나홀로 1인 곱도리탕 역삼점 / 찜·탕·찌개 14822058"
    # → 마지막 '점' 으로 끝나는 한글 단어 추출
    matches = re.findall(r"[가-힣]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _save_metrics_csv(stats: dict, brand: str, store: str) -> Path:
    """파티션 경로에 수집 지표 저장.

    경로: BAEMIN_METRICS_DB/brand={brand}/store={store}/ym={YYYY-MM}/baemin_now.csv
    같은 date(당일) 행이 있으면 최신 데이터로 덮어쓴다.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    ym = today[:7]  # "2026-05"
    out_dir = BAEMIN_METRICS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "baemin_now.csv"

    stats["date"] = today
    new_df = pd.DataFrame([stats])

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # 같은 date 행 제거 후 새 데이터 추가 (당일 재수집 시 최신으로 덮어쓰기)
        existing = existing[existing.get("date", pd.Series(dtype=str)) != today]
        combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    else:
        new_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info("저장 완료: %s", out_path)
    return out_path
