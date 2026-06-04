r"""쿠팡 수집을 호스트의 '진짜 크롬'에 attach 해서 실행 (Docker 봇탐지 회피).

전제:
  1) scripts/coupang_host_chrome.ps1 로 전용 크롬을 디버그포트 9222로 띄우고
     쿠팡 로그인 1회 (세션 유지). 창은 최소화 OK.

실행 (Windows, .venv 사용):
  C:\airflow\.venv\Scripts\python.exe C:\airflow\scripts\run_coupang_host.py
  C:\airflow\.venv\Scripts\python.exe C:\airflow\scripts\run_coupang_host.py 2026-06-01   # 특정일

동작:
  COUPANG_CHROME_DEBUGGER 환경변수를 설정 → croling_coupang.launch_browser 가
  새 크롬을 띄우지 않고 9222의 실제 크롬에 attach. 나머지 수집 로직은 동일.
  저장 경로는 Windows OneDrive(analytics/coupang_macro/orders/...)로 자동 해석됨.
"""

import logging
import os
import sys
from pathlib import Path

# 레포 루트(C:\airflow)를 import 경로에 추가
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# attach 모드 활성화 (호스트의 실제 크롬, localhost)
os.environ.setdefault("COUPANG_CHROME_DEBUGGER", "127.0.0.1:9222")

import pendulum  # noqa: E402

from modules.transform.pipelines.db.DB_Coupang_combined import (  # noqa: E402
    collect_all,
    load_coupang_accounts,
    retry_once_failed,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("run_coupang_host")

KST = pendulum.timezone("Asia/Seoul")

# DAG의 TARGET_STORES와 동일하게 유지
TARGET_STORES = [
    "도리당 송파삼전점",
]


def main() -> int:
    target_date = sys.argv[1] if len(sys.argv) > 1 else pendulum.yesterday(KST).format("YYYY-MM-DD")
    logger.info("attach=%s, target_date=%s", os.environ.get("COUPANG_CHROME_DEBUGGER"), target_date)

    accounts = load_coupang_accounts(target_stores=TARGET_STORES, exact=True)
    if not accounts:
        logger.warning("수집 대상 계정 없음")
        return 1

    try:
        result = collect_all(accounts, target_date=target_date)
    except Exception as exc:
        logger.error(
            "수집 실패 — 전용 크롬이 9222로 떠 있고 쿠팡 로그인 상태인지 확인하세요. (%s)",
            exc,
            exc_info=True,
        )
        return 2

    logger.info(result["summary"])

    failed = result.get("failed", [])
    if failed:
        logger.info(retry_once_failed(failed, target_date=target_date))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
