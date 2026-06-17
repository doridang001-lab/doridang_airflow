#!/bin/bash
# Telegram Claude loop — tmux 세션이 없을 때만 시작
SESSION="telegram_claude"

tmux has-session -t "$SESSION" 2>/dev/null && exit 0

tmux new-session -d -s "$SESSION" -c "/mnt/c/airflow"
sleep 2
tmux send-keys -t "$SESSION" "claude --dangerously-skip-permissions" Enter
sleep 8
tmux send-keys -t "$SESSION" "/loop 텔레그램 메시지가 오면 답변해줘" Enter
