#!/usr/bin/env python3
"""Telegram polling loop: getUpdates → claude -p → sendMessage"""
import subprocess, time, json, urllib.request, urllib.error, logging, os, sys

TOKEN = "8679545327:AAHbAklrw4rL1yjuhm0DBt5sylxLRwYlgzs"
ALLOWED = {"8233236443"}
WORKDIR = "/mnt/c/airflow"
LOG_FILE = "/mnt/c/airflow/scripts/telegram_loop.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def tg(method, **params):
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    data = json.dumps(params).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        resp = urllib.request.urlopen(req, timeout=40)
        return json.loads(resp.read())
    except Exception as e:
        log.error(f"tg API {method}: {e}")
        return None


def send(chat_id, text):
    # Telegram limit: 4096 chars
    for chunk in [text[i : i + 4096] for i in range(0, len(text), 4096)]:
        tg("sendMessage", chat_id=chat_id, text=chunk)
    log.info(f"→ {chat_id}: {text[:80]}")


def ask_claude(text):
    prompt = (
        "아래 요청에 반드시 한국어로만 답하세요.\n"
        "답변은 사용자가 바로 이해할 수 있게 쉽게 정리하세요.\n"
        "불필요한 영어 제목이나 내부 용어는 피하고, 필요한 경우 한글 설명을 붙이세요.\n"
        "가능하면 5줄 이내로 핵심만 먼저 말하세요.\n\n"
        "역할 분리 규칙:\n"
        "- Claude 루프는 질문 답변, 상태 정리, 쉬운 설명을 담당합니다.\n"
        "- Airflow DAG 에러 자동감지와 코드수정은 Codex auto-heal이 담당합니다.\n"
        "- 사용자가 명시적으로 수정/실행을 요청하지 않으면 파일을 고치거나 DAG를 재실행하지 마세요.\n"
        "- DAG 실패 알림을 받으면 원인과 다음 확인 포인트를 한국어로 요약하세요.\n\n"
        f"요청:\n{text}"
    )
    try:
        result = subprocess.run(
            ["claude", "-p", "--dangerously-skip-permissions", prompt],
            capture_output=True,
            text=True,
            cwd=WORKDIR,
            timeout=120,
            env={**os.environ, "TERM": "xterm"},
        )
        return (result.stdout or "").strip() or "응답을 생성하지 못했습니다."
    except subprocess.TimeoutExpired:
        return "응답 시간이 초과되었습니다. (2분)"
    except Exception as e:
        log.error(f"claude error: {e}")
        return f"오류: {e}"


def main():
    log.info("=== Telegram Claude Loop 시작 ===")
    offset = 0

    while True:
        try:
            updates = tg("getUpdates", offset=offset, timeout=30)
            if not updates or not updates.get("ok"):
                time.sleep(5)
                continue

            for upd in updates.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                from_id = str(msg.get("from", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                if not text or from_id not in ALLOWED:
                    continue

                log.info(f"← {from_id}: {text[:80]}")
                reply = ask_claude(text)
                send(chat_id, reply)

        except KeyboardInterrupt:
            log.info("종료")
            break
        except Exception as e:
            log.error(f"loop error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
