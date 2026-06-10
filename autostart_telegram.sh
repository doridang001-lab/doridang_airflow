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

# 네트워크 대기 (최대 2분)
echo "[$(date)] 네트워크 연결 대기 중..."
for i in $(seq 1 24); do
    if ping -c 1 -W 2 8.8.8.8 &>/dev/null; then
        echo "[$(date)] 네트워크 연결됨 (${i}번째 시도)"
        break
    fi
    echo "[$(date)] 네트워크 대기 중... (${i}/24)"
    sleep 5
done

# 가상환경 활성화 및 `claude` 실행 파일 존재 확인
echo "[$(date)] 가상환경 활성화 및 claude 확인"
source "$VENV"
if ! command -v claude >/dev/null 2>&1; then
    echo "[$(date)] ERROR: 'claude' 명령을 찾을 수 없습니다. 자동 재시작을 중지합니다."
    echo "[$(date)] PATH=", "$PATH"
    exit 1
fi

# 중복 Claude 텔레그램 프로세스 제거 (claude-channels-session 외 다른 세션의 claude 종료)
echo "[$(date)] 중복 Claude 프로세스 확인..."
CHANNEL_PANE_PID=$(tmux list-panes -t claude-channels-session -F "#{pane_pid}" 2>/dev/null | head -1)
for PID in $(pgrep -x claude 2>/dev/null); do
    # claude-channels-session 하위 프로세스인지 확인
    SESSION_PANE=$(tmux list-panes -a -F "#{pane_pid}" 2>/dev/null | grep -w "$PID")
    if [ -z "$SESSION_PANE" ] && [ "$PID" != "$CHANNEL_PANE_PID" ]; then
        echo "[$(date)] 중복 Claude 종료: PID=$PID"
        kill "$PID" 2>/dev/null
    fi
done

# tmux 서버가 실행 중인지 확인
if ! tmux list-sessions 2>/dev/null; then
    echo "[$(date)] tmux 서버 시작"
fi

# claude-channels-session: 텔레그램 채널 연동
if ! tmux has-session -t claude-channels-session 2>/dev/null; then
    echo "[$(date)] claude-channels-session 생성"
    tmux new-session -d -s claude-channels-session -c "$AIRFLOW_DIR" \
        "bash -c 'source $VENV; while true; do echo \"[재시작: \$(date)]\"; claude --channels plugin:telegram@claude-plugins-official --dangerously-skip-permissions 2>&1 | tee -a /tmp/claude_channels.log; sleep 5; done'"
    echo "[$(date)] claude-channels-session 시작 완료"
else
    echo "[$(date)] claude-channels-session 이미 실행 중"
    # 세션은 있지만 claude 프로세스가 죽었는지 확인
    if ! tmux list-panes -t claude-channels-session -F "#{pane_current_command}" 2>/dev/null | grep -q "claude\|bash"; then
        echo "[$(date)] 세션 재시작 (프로세스 없음)"
        tmux kill-session -t claude-channels-session
        tmux new-session -d -s claude-channels-session -c "$AIRFLOW_DIR" \
            "bash -c 'source $VENV; while true; do echo \"[재시작: \$(date)]\"; claude --channels plugin:telegram@claude-plugins-official --dangerously-skip-permissions 2>&1 | tee -a /tmp/claude_channels.log; sleep 5; done'"
    fi
fi

echo "[$(date)] autostart_telegram.sh 완료"
