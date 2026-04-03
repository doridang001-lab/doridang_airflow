#!/bin/bash
# WSL 부팅 시 tmux 세션 자동 생성 스크립트
# Windows 시작 → VBScript → wsl.exe → systemd → 이 스크립트 실행

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export HOME="/home/myuser"

sleep 2  # 환경 안정화 대기

AIRFLOW_DIR="/mnt/c/airflow"
VENV="$AIRFLOW_DIR/.venv_wsl/bin/activate"

# main 세션 생성 (없을 때만)
if ! tmux has-session -t main 2>/dev/null; then
    tmux new-session -d -s main -c "$AIRFLOW_DIR"
    tmux send-keys -t main "source $VENV" Enter
fi

# telegram 세션 생성 + claude 실행 (없을 때만)
if ! tmux has-session -t telegram 2>/dev/null; then
    tmux new-session -d -s telegram -c "$AIRFLOW_DIR"
    tmux send-keys -t telegram "source $VENV && claude" Enter
fi

# claude-channels-session: 텔레그램 채널 연동 (없을 때만)
if ! tmux has-session -t claude-channels-session 2>/dev/null; then
    tmux new-session -d -s claude-channels-session -c "$AIRFLOW_DIR"
    tmux send-keys -t claude-channels-session "source $VENV" Enter
    tmux send-keys -t claude-channels-session "while true; do claude --channels plugin:telegram@claude-plugins-official --dangerously-skip-permissions; sleep 5; done" Enter
fi
