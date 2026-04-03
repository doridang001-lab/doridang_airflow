#!/bin/bash
# Windows 부팅 시 텔레그램 Claude 자동 실행 스크립트
# Task Scheduler: 로그인 시 실행 (WSL 환경 보장됨)

LOG="/tmp/autostart_telegram.log"
exec >> "$LOG" 2>&1
echo "[$(date)] autostart_telegram.sh 시작"

# 환경 변수 설정
export HOME="/home/myuser"
export USER="myuser"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/myuser/.local/bin"

AIRFLOW_DIR="/mnt/c/airflow"
VENV="$AIRFLOW_DIR/.venv_wsl/bin/activate"

# WSL 마운트 대기 (최대 30초)
for i in $(seq 1 15); do
    if [ -f "$VENV" ]; then
        echo "[$(date)] venv 확인됨 (${i}번째 시도)"
        break
    fi
    echo "[$(date)] venv 대기 중... (${i}/15)"
    sleep 2
done

if [ ! -f "$VENV" ]; then
    echo "[$(date)] ERROR: venv 없음, 종료"
    exit 1
fi

# tmux 서버가 실행 중인지 확인
if ! tmux list-sessions 2>/dev/null; then
    echo "[$(date)] tmux 서버 시작"
fi

# claude-channels-session: 텔레그램 채널 연동
if ! tmux has-session -t claude-channels-session 2>/dev/null; then
    echo "[$(date)] claude-channels-session 생성"
    tmux new-session -d -s claude-channels-session -c "$AIRFLOW_DIR" \
        "bash -c 'source $VENV; while true; do claude --channels plugin:telegram@claude-plugins-official --dangerously-skip-permissions; sleep 5; done'"
    echo "[$(date)] claude-channels-session 시작 완료"
else
    echo "[$(date)] claude-channels-session 이미 실행 중"
fi

echo "[$(date)] autostart_telegram.sh 완료"
