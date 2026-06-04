"""orders TotalSummary 검증 로직 단계별 테스트 스크립트.

Phase 1: TotalSummary DOM 선택자 확인
Phase 2: 수집 후 건수/금액 일치 확인
Phase 3: 강제 mismatch → 감지 확인

실행 (WSL):
  docker exec airflow-airflow-worker-1 bash -c \
    "cd /opt/airflow && DISPLAY=:99 python tmp/test_orders_validation.py 2>&1 | tail -80"
"""
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page
from modules.transform.pipelines.db.DB_Beamin_collect import load_accounts
from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    ORDERS_URL, _TABLE_ROW_CSS,
    _select_order_store, _select_status_cancelled, _set_date_yesterday,
    _collect_all_pages,
    _read_total_summary, _validate_collected,
)

STORE_NAME = "도리당 역삼점"
# debug_orders_buttons.py 에서 확인된 역삼점 store_id
STORE_INFO = {"store_id": "14822058", "brand": "도리당", "store": "역삼점"}

# ── 계정 로드 ──────────────────────────────────────────────────
accounts = load_accounts([STORE_NAME], exact=True)
if not accounts:
    logger.error("계정 없음: %s", STORE_NAME)
    sys.exit(1)

account = accounts[0]
account_id = account["account_id"]
logger.info("계정=%s store_info=%s", account_id, STORE_INFO)

# ── 브라우저 실행 ──────────────────────────────────────────────
driver = launch_browser(account_id)
store_info = STORE_INFO
try:
    ok = login_baemin(driver, account_id, account["password"])
    logger.info("로그인: %s", ok)
    if not ok:
        sys.exit(1)

    driver.set_page_load_timeout(45)
    driver.get(ORDERS_URL)
    if not wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
        logger.error("orders 페이지 로드 실패")
        sys.exit(1)

    # ── 가게 + 날짜 필터 적용 ─────────────────────────────────
    _select_order_store(driver, store_info["store_id"], store_info["store"])
    _set_date_yesterday(driver)
    time.sleep(2.0)

    # ════════════════════════════════════════════════════════════
    # Phase 1: TotalSummary DOM 선택자 확인 (배달완료)
    # ════════════════════════════════════════════════════════════
    logger.info("=== Phase 1: TotalSummary 읽기 (배달완료) ===")
    summary_normal = _read_total_summary(driver)
    logger.info("TotalSummary(배달완료): %s", summary_normal)
    if summary_normal is None:
        logger.warning("TotalSummary None → DOM 스냅샷 저장 (확인 필요)")
        html = driver.execute_script("return document.body.innerHTML")
        out = Path("/opt/airflow/tmp/orders_dom_debug.html")
        out.write_text(html, encoding="utf-8")
        logger.info("DOM 저장: %s (%d chars)", out, len(html))
    else:
        logger.info("✅ Phase 1 통과: count=%d amount=%s", summary_normal["count"], summary_normal.get("amount"))

    # ════════════════════════════════════════════════════════════
    # Phase 2: 수집 후 건수/금액 검증 (배달완료)
    # ════════════════════════════════════════════════════════════
    logger.info("=== Phase 2: 전 페이지 수집 + 검증 (배달완료) ===")
    rows_normal = _collect_all_pages(driver, store_info)
    logger.info("수집 행수(raw): %d", len(rows_normal))

    vr_normal = _validate_collected(rows_normal, summary_normal)
    logger.info("검증 결과(배달완료): %s", vr_normal)
    if vr_normal["matched"] is True:
        logger.info("✅ Phase 2 통과 (배달완료): 건수=%d 금액=%s 일치",
                    vr_normal["actual_count"], vr_normal["actual_amount"])
    elif vr_normal["matched"] is False:
        logger.warning("❌ Phase 2 불일치: 수집=%d건/%s원 기대=%s건/%s원",
                       vr_normal["actual_count"], vr_normal["actual_amount"],
                       vr_normal["expected_count"], vr_normal["expected_amount"])
    else:
        logger.info("⚠️  Phase 2: TotalSummary 없어 검증 스킵")

    # ════════════════════════════════════════════════════════════
    # Phase 3: 강제 mismatch 감지 테스트
    # 옵션 분리로 1 주문 = N행 → 주문번호 하나를 통째로 제거해야 count 변함
    # ════════════════════════════════════════════════════════════
    logger.info("=== Phase 3: 강제 mismatch 감지 ===")
    if rows_normal and summary_normal:
        first_order_num = next((r["주문번호"] for r in rows_normal if r.get("주문번호")), None)
        if first_order_num:
            rows_truncated = [r for r in rows_normal if r.get("주문번호") != first_order_num]
            vr_forced = _validate_collected(rows_truncated, summary_normal)
            logger.info("강제 mismatch 결과 (주문번호 %s... 제거): %s", first_order_num[:8], vr_forced)
            if vr_forced["matched"] is False:
                logger.info("✅ Phase 3 통과: mismatch 정상 감지 (count %d→%d)",
                            vr_normal["actual_count"], vr_forced["actual_count"])
            else:
                logger.warning("⚠️  Phase 3: 주문번호 제거했는데 matched=%s", vr_forced["matched"])
        else:
            logger.warning("Phase 3 스킵: 주문번호 없음")
    else:
        logger.info("Phase 3 스킵: 수집 데이터 없거나 TotalSummary None")

    # ════════════════════════════════════════════════════════════
    # Phase 1D: TotalSummary 읽기 (주문취소)
    # ════════════════════════════════════════════════════════════
    logger.info("=== Phase 1D: TotalSummary 읽기 (주문취소) ===")
    driver.get(ORDERS_URL)
    if wait_for_page(driver, _TABLE_ROW_CSS, timeout=30):
        _select_order_store(driver, store_info["store_id"], store_info["store"])
        _select_status_cancelled(driver)
        _set_date_yesterday(driver)
        time.sleep(2.0)

        summary_cancelled = _read_total_summary(driver)
        logger.info("TotalSummary(주문취소): %s", summary_cancelled)

        rows_cancelled = _collect_all_pages(driver, store_info)
        vr_cancelled = _validate_collected(rows_cancelled, summary_cancelled)
        logger.info("검증 결과(주문취소): %s", vr_cancelled)
        if vr_cancelled["matched"] is True:
            logger.info("✅ Phase 1D 통과 (주문취소): 건수=%d 금액=%s 일치",
                        vr_cancelled["actual_count"], vr_cancelled["actual_amount"])
        elif vr_cancelled["matched"] is False:
            logger.warning("❌ Phase 1D 불일치(주문취소): 수집=%d건/%s원 기대=%s건/%s원",
                           vr_cancelled["actual_count"], vr_cancelled["actual_amount"],
                           vr_cancelled["expected_count"], vr_cancelled["expected_amount"])
    else:
        logger.warning("주문취소 페이지 로드 실패, 스킵")

    logger.info("=== 테스트 완료 ===")

finally:
    try:
        driver.quit()
    except Exception:
        pass
