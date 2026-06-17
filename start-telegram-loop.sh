#!/bin/bash
SESSION="telegram_claude"
if tmux has-session -t $SESSION 2>/dev/null; then
    exit 0
fi
tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION '~/run-telegram-bot.sh' Enter
