"""Autoheal supervisor 자체가 실패했을 때 Telegram으로 운영 장애를 알린다."""

from __future__ import annotations

import argparse
import logging

from modules.transform.utility.notifier import send_telegram


logger = logging.getLogger(__name__)


def notify(reason: str) -> bool:
    if "exit_code=0" in reason:
        logger.info("감시기 정상 종료이므로 장애 알림을 생략합니다: %s", reason)
        return True
    message = (
        "[자동복구 감시기 장애]\n"
        "상태: WSL/Windows watcher 시작 실패\n"
        f"원인: {reason[:500]}\n"
        "다음조치: 작업 스케줄러와 autoheal heartbeat 수동 확인"
    )
    sent = bool(send_telegram(message))
    if not sent:
        logger.error("자동복구 감시기 장애 Telegram 발송 실패")
    return sent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    return 0 if notify(args.reason) else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main())
