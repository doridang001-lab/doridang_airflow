"""
WSL에서 상시 실행:
nohup python /mnt/c/airflow/watch_heal_queue.py >> /tmp/heal_watch.log 2>&1 &
"""

import json
import logging
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

QUEUE_PATH = Path(os.getenv("HEAL_QUEUE_PATH", "/mnt/c/airflow/logs/heal_queue.jsonl"))
POLL_INTERVAL = int(os.getenv("HEAL_QUEUE_POLL_INTERVAL", "60"))
CODEX_COMMAND = os.getenv("CODEX_COMMAND", "cc")
CODEX_MODEL = os.getenv("CODEX_MODEL", "codex-spark")

os.environ["HEAL_QUEUE_PATH"] = str(QUEUE_PATH)

from modules.transform.utility.airflow_api import claim_heal_task, get_task_log  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram(text: str) -> None:
    if not _BOT_TOKEN or not _CHAT_ID:
        logger.warning("Telegram 환경변수 없음 - 전송 스킵")
        return
    try:
        payload = urllib.parse.urlencode({"chat_id": _CHAT_ID, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage",
            data=payload,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        logger.warning("Telegram 전송 실패: %s", e)


def _read_first_unclaimed() -> dict | None:
    if not QUEUE_PATH.exists():
        return None
    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("heal_queue JSON 파싱 실패: %s", line[:200])
                continue
            if entry.get("claimed_by") is None:
                return entry
    return None


def _codex_args(prompt: str) -> list[str]:
    return [CODEX_COMMAND, "--model", CODEX_MODEL, prompt]


def _build_prompt(entry: dict, log_text: str) -> str:
    return (
        "아래 Airflow DAG 오류를 분석하고 사용자 승인 없이 수정 후 재실행해줘.\n\n"
        f"dag_id={entry['dag_id']}\n"
        f"task_id={entry['task_id']}\n"
        f"run_id={entry['run_id']}\n"
        f"try_number={entry['try_number']}\n"
        f"error={entry.get('error', '')}\n\n"
        f"--- 로그 ---\n{log_text}\n\n"
        "필수 처리 순서:\n"
        "1. 원인을 분석하고 필요한 파일을 직접 수정한다.\n"
        "2. 수정 후 modules.transform.utility.airflow_api.trigger_dag(dag_id)를 호출해 DAG를 재실행한다.\n"
        "3. modules.transform.utility.notifier.send_telegram()으로 아래 형식의 결과를 보고한다.\n\n"
        "[자동 수정 완료 by Codex]\n"
        "수정 파일: <파일경로>\n"
        "변경: <요약>\n"
        "재실행: <run_id>\n"
        "상태: 성공/실패"
    )


def process_once() -> bool:
    entry = _read_first_unclaimed()
    if not entry:
        return False

    dag_id = entry.get("dag_id")
    run_id = entry.get("run_id")
    task_id = entry.get("task_id")
    try_number = entry.get("try_number")
    if not all([dag_id, run_id, task_id, try_number]):
        logger.warning("heal_queue 필수 필드 누락: %s", entry)
        return False

    if not claim_heal_task(dag_id, run_id, task_id, "codex"):
        return False

    log_text = get_task_log(dag_id, run_id, task_id, try_number)
    prompt = _build_prompt(entry, log_text)
    try:
        result = subprocess.run(_codex_args(prompt), check=False, timeout=None)
        if result.returncode != 0:
            send_telegram(
                "[Auto-Heal] Codex 호출 실패 - 수동 확인 필요\n"
                f"dag_id={dag_id}\n"
                f"task_id={task_id}\n"
                f"run_id={run_id}\n"
                f"returncode={result.returncode}"
            )
    except Exception as e:
        send_telegram(
            "[Auto-Heal] Codex 호출 실패 - 수동 확인 필요\n"
            f"dag_id={dag_id}\n"
            f"task_id={task_id}\n"
            f"run_id={run_id}\n"
            f"error={e}"
        )
    return True


def main() -> None:
    logger.info("heal_queue watcher 시작: %s", QUEUE_PATH)
    while True:
        try:
            process_once()
        except Exception as e:
            logger.exception("heal_queue 처리 실패: %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
