"""Baemin now metrics collection pipeline."""

import logging
import random
import re
import time
from pathlib import Path

import pandas as pd
import pendulum

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

KST = pendulum.timezone("Asia/Seoul")
KNOWN_BRANDS = ["\uB098\uD640\uB85C", "\uB3C4\uB9AC\uB2F9"]


def collect_now_stats(account_list: list[dict]) -> str:
    """Collect now metrics for each Baemin account."""
    success, fail = 0, 0

    for account in account_list:
        account_id = account["account_id"]
        driver = None
        try:
            driver = launch_browser(account_id)

            if not login_baemin(driver, account_id, account["password"]):
                logger.warning("\uB85C\uADF8\uC778 \uC2E4\uD328: %s", account_id)
                fail += 1
                continue

            logger.info("\uB85C\uADF8\uC778 \uC131\uACF5: %s", account_id)

            from urllib.parse import urlparse as _urlparse

            if _urlparse(driver.current_url).hostname != "self.baemin.com":
                try:
                    driver.set_page_load_timeout(45)
                    driver.get("https://self.baemin.com/")
                except Exception as e:
                    logger.warning(
                        "\uB300\uC2DC\uBCF4\uB4DC \uC774\uB3D9 \uC911 \uD0C0\uC784\uC544\uC6C3 (\uACC4\uC18D \uC9C4\uD589): %s",
                        e,
                    )
            else:
                time.sleep(2)

            if not wait_for_page(driver, "select[class*='ShopSelect']", timeout=60):
                logger.warning(
                    "\uBA54\uC778 \uB300\uC2DC\uBCF4\uB4DC \uB85C\uB4DC \uC2E4\uD328: %s",
                    account_id,
                )
                fail += 1
                continue

            raw_options = get_store_options(driver)
            store_list = [_parse_store_option(o) for o in raw_options]
            store_list = [s for s in store_list if s]

            if not store_list:
                logger.warning(
                    "\uC218\uC9D1 \uB300\uC0C1 \uBE0C\uB79C\uB4DC \uC5C6\uC74C: %s (\uC635\uC158=%s)",
                    account_id,
                    raw_options,
                )
                fail += 1
                continue

            logger.info("\uC218\uC9D1 \uB300\uC0C1 \uB9E4\uC7A5: %s", store_list)

            for store_info in store_list:
                logger.info("\uB9E4\uC7A5 \uC120\uD0DD \uC2DC\uB3C4: %s", store_info)
                if not select_store_by_id(driver, store_info["store_id"]):
                    logger.warning("\uB9E4\uC7A5 \uC120\uD0DD \uC2E4\uD328: %s", store_info)
                    continue

                logger.info(
                    "\uB9E4\uC7A5 \uB370\uC774\uD130 \uB85C\uB4DC \uB300\uAE30: %s",
                    store_info["store"],
                )
                if not wait_for_metrics_data(driver, timeout=45):
                    logger.warning(
                        "\uB9E4\uC7A5 \uB370\uC774\uD130 \uB85C\uB4DC \uD0C0\uC784\uC544\uC6C3: %s",
                        store_info["store"],
                    )
                    continue

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
                    logger.info("\uC218\uC9D1 \uC2DC\uC810 DOM: %s", dom_info)
                except Exception:
                    pass

                stats = collect_single_store_stats(
                    driver, store_info["store_id"], account_id
                )
                stats["brand"] = store_info["brand"]
                stats["store"] = store_info["store"]

                saved = _save_metrics_csv(stats, store_info["brand"], store_info["store"])
                logger.info(
                    "\uC218\uC9D1 \uC644\uB8CC: brand=%s store=%s -> %s",
                    store_info["brand"],
                    store_info["store"],
                    saved,
                )

            logout_baemin(driver, account_id)
            logger.info("\uB85C\uADF8\uC544\uC6C3 \uC644\uB8CC: %s", account_id)

            wait_sec = random.uniform(*TIMING["logout_wait"])
            logger.info("\uB2E4\uC74C \uACC4\uC815\uAE4C\uC9C0 %.0f\uCD08 \uB300\uAE30", wait_sec)
            time.sleep(wait_sec)

            success += 1

        except Exception as e:
            logger.error("\uCC98\uB9AC \uC2E4\uD328 [%s]: %s", account_id, e, exc_info=True)
            fail += 1
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    summary = f"\uC131\uACF5 {success}/{success + fail} \uACC4\uC815"
    logger.info(summary)
    return summary


def _parse_store_option(opt: dict) -> dict | None:
    """Convert a dropdown option into store metadata."""
    text = opt.get("text", "")
    brand = next((b for b in KNOWN_BRANDS if b in text), None)
    if not brand:
        return None

    matches = re.findall(r"[\uAC00-\uD7A3]+(?:\uC810|\uC9C0\uC810|\uBD84\uC810|\uC9C1\uC601\uC810)", text)
    store = matches[-1] if matches else text[:20]

    return {"store_id": str(opt["store_id"]), "brand": brand, "store": store}


def _save_metrics_csv(stats: dict, brand: str, store: str) -> Path:
    """Save metrics CSV partitioned by KST date and month."""
    kst_now = pendulum.now(KST)
    today = kst_now.format("YYYY-MM-DD")
    ym = kst_now.format("YYYY-MM")
    out_dir = BAEMIN_METRICS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "baemin_now.csv"

    stats["date"] = today
    new_df = pd.DataFrame([stats])

    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        existing = existing[existing.get("date", pd.Series(dtype=str)) != today]
        combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    else:
        new_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info("\uC800\uC7A5 \uC644\uB8CC: %s", out_path)
    return out_path


def collect_now_for_driver(driver, account_id: str, store_list: list[dict]) -> None:
    """이미 로그인된 driver로 now 지표를 수집한다 (login/logout 없음).

    combined 파이프라인에서 우가클 수집과 같은 브라우저 세션을 공유할 때 사용.
    """
    for store_info in store_list:
        try:
            logger.info("매장 선택 시도: %s", store_info)
            if not select_store_by_id(driver, store_info["store_id"]):
                logger.warning("매장 선택 실패: %s", store_info)
                raise RuntimeError(f"store selection failed: {store_info['store_id']}")

            logger.info("매장 데이터 로드 대기: %s", store_info["store"])
            if not wait_for_metrics_data(driver, timeout=45):
                logger.warning("매장 데이터 로드 타임아웃: %s", store_info["store"])
                raise RuntimeError(f"metrics load timeout: {store_info['store_id']}")

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
                "수집 완료: brand=%s store=%s -> %s",
                store_info["brand"],
                store_info["store"],
                saved,
            )

        except Exception as e:
            # Connection refused / Max retries = Chrome OOM 크래시 → 상위로 전파
            if "Connection refused" in str(e) or "Max retries" in str(e):
                logger.warning("Chrome 세션 종료, now 수집 중단: %s", e)
                raise
            logger.warning("매장 now 수집 실패 (건너뜀): %s / %s", store_info["store"], e)
