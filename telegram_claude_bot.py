#!/usr/bin/env python3
import os, time, logging, requests, subprocess
from collections import defaultdict, deque
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.expanduser('~/telegram_claude_bot.log'))
    ]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
ANTHROPIC_API_KEY = os.environ['ANTHROPIC_API_KEY']
API_BASE = f'https://api.telegram.org/bot{BOT_TOKEN}'
ALLOWED_IDS = {8233236443, 7985739076}
MAX_HISTORY = 40

import anthropic
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

histories = defaultdict(lambda: deque(maxlen=MAX_HISTORY))

SYSTEM = (
    "당신은 도리당 F&B 브랜드 데이터 엔지니어링 팀의 AI 어시스턴트입니다.\n"
    "Apache Airflow DAG 관리, 배민/쿠팡 데이터 파이프라인, Python/Pandas 분석을 전담합니다.\n\n"
    "## 환경 정보\n"
    "- Airflow: Docker 컨테이너 실행 중 (WSL에서 docker 명령 가능)\n"
    "- DAG 파일: /mnt/c/airflow/dags/ (sales/, strategy/, db/, etl/ 하위)\n"
    "- 스케줄러: airflow-airflow-scheduler-1\n"
    "- 워커: airflow-airflow-worker-1\n"
    "- UI: http://localhost:8080\n\n"
    "## DAG 에러 처리 절차 (사용자가 에러 언급 시 자동 수행)\n"
    "1. 최근 실패 런 확인: docker exec airflow-airflow-scheduler-1 airflow dags list-runs -d <dag_id> --state failed -o plain\n"
    "2. 실패 태스크 확인: docker exec airflow-airflow-scheduler-1 airflow tasks states-for-dag-run <dag_id> <run_id> -o plain\n"
    "3. 로그 확인: docker exec airflow-airflow-worker-1 bash -c \"cat '/opt/airflow/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=1.log'\"\n"
    "4. DAG 파일 읽기: read_file 도구로 /mnt/c/airflow/dags/.../파일.py 확인\n"
    "5. 원인 파악 후 수정: write_file로 DAG 파일 직접 수정\n"
    "6. 재트리거: docker exec airflow-airflow-scheduler-1 airflow dags trigger <dag_id>\n\n"
    "## 핵심 원칙\n"
    "- 허락 없이 도구를 적극 사용해 끝까지 처리하라. 중간에 '확인할까요?' 하지 말고 직접 해결하라.\n"
    "- 에러 원인을 찾으면 파일을 직접 고치고 재트리거까지 완료하라.\n"
    "- 완료 후 결과를 간결하게 보고하라.\n"
    "- 응답은 한국어로, 영어 질문엔 영어로.\n"
)

TOOLS = [
    {
        "name": "run_bash",
        "description": (
            "WSL bash 명령어를 실행합니다. "
            "Airflow 로그 확인, docker exec, DAG 트리거, 파일 수정 등에 사용합니다.\n"
            "유용한 명령 예시:\n"
            "- 최근 실패 로그: docker exec airflow-airflow-scheduler-1 airflow tasks logs <dag_id> <task_id> <run_id>\n"
            "- DAG 목록: docker exec airflow-airflow-scheduler-1 airflow dags list\n"
            "- DAG 트리거: docker exec airflow-airflow-scheduler-1 airflow dags trigger <dag_id>\n"
            "- 파일 수정: sed, awk, python3 등"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "실행할 bash 명령어"}
            },
            "required": ["command"]
        }
    },
    {
        "name": "read_file",
        "description": "파일 내용을 읽습니다. DAG 파일, 로그 파일, 설정 파일 등을 확인할 때 사용합니다.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "읽을 파일의 절대 경로"}
            },
            "required": ["path"]
        }
    },
    {
        "name": "write_file",
        "description": "파일 내용을 덮어씁니다. DAG 파일 수정 시 사용합니다. 수정 전 반드시 read_file로 내용 확인 후 사용하세요.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "쓸 파일의 절대 경로"},
                "content": {"type": "string", "description": "파일에 쓸 내용"}
            },
            "required": ["path", "content"]
        }
    }
]


def exec_tool(name: str, inp: dict) -> str:
    try:
        if name == "run_bash":
            result = subprocess.run(
                inp["command"], shell=True, capture_output=True, text=True,
                timeout=60, cwd="/mnt/c/airflow"
            )
            output = (result.stdout + result.stderr).strip()
            logger.info(f"bash: {inp['command'][:80]} -> {output[:80]}")
            return output[:4000] if output else "(출력 없음)"

        elif name == "read_file":
            with open(inp["path"], encoding="utf-8", errors="replace") as f:
                content = f.read()
            logger.info(f"read_file: {inp['path']} ({len(content)} chars)")
            return content[:6000]

        elif name == "write_file":
            with open(inp["path"], "w", encoding="utf-8") as f:
                f.write(inp["content"])
            logger.info(f"write_file: {inp['path']}")
            return f"저장 완료: {inp['path']}"

    except Exception as e:
        return f"도구 오류: {e}"


def call_claude(messages: list, now: str) -> str:
    while True:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            system=SYSTEM + f"\n현재 시각: {now} (KST)",
            tools=TOOLS,
            messages=messages,
        )

        if resp.stop_reason == "end_turn":
            return next((b.text for b in resp.content if hasattr(b, "text")), "")

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = []
            for block in resp.content:
                if block.type == "tool_use":
                    logger.info(f"tool_use: {block.name} {str(block.input)[:100]}")
                    result = exec_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result
                    })
            messages.append({"role": "user", "content": tool_results})
        else:
            return ""


def tg(method, **kwargs):
    try:
        r = requests.post(f'{API_BASE}/{method}', json=kwargs, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"Telegram {method} error: {e}")


def handle(msg):
    uid = msg.get('from', {}).get('id')
    chat_id = msg.get('chat', {}).get('id')
    mid = msg.get('message_id')
    text = msg.get('text', '').strip()

    if not text or not uid:
        return
    if uid not in ALLOWED_IDS:
        logger.warning(f"blocked uid={uid}")
        return

    logger.info(f"[{uid}] -> {text[:100]}")
    tg('sendChatAction', chat_id=chat_id, action='typing')

    hist = histories[uid]
    hist.append({"role": "user", "content": text})

    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    messages = list(hist)

    try:
        reply = call_claude(messages, now)
        if reply:
            hist.append({"role": "assistant", "content": reply})
            logger.info(f"[{uid}] <- {reply[:100]}")
            tg('sendMessage', chat_id=chat_id, text=reply, reply_to_message_id=mid)
        else:
            tg('sendMessage', chat_id=chat_id, text="(응답 없음)", reply_to_message_id=mid)
    except subprocess.TimeoutExpired:
        tg('sendMessage', chat_id=chat_id, text="⏱ 응답 시간 초과", reply_to_message_id=mid)
    except Exception as e:
        logger.error(f"Claude error: {e}")
        tg('sendMessage', chat_id=chat_id, text=f"오류: {e}", reply_to_message_id=mid)


def main():
    logger.info("=== 텔레그램 Claude 봇 시작 (tool use 활성화) ===")
    offset = None
    while True:
        try:
            params = {'timeout': 30, 'allowed_updates': ['message']}
            if offset:
                params['offset'] = offset
            r = requests.get(f'{API_BASE}/getUpdates', params=params, timeout=40)
            data = r.json()
            if not data.get('ok'):
                logger.error(f"getUpdates: {data}")
                time.sleep(5)
                continue
            for upd in data.get('result', []):
                offset = upd['update_id'] + 1
                if 'message' in upd:
                    handle(upd['message'])
        except requests.exceptions.Timeout:
            continue
        except Exception as e:
            logger.error(f"poll error: {e}")
            time.sleep(5)


if __name__ == '__main__':
    main()
