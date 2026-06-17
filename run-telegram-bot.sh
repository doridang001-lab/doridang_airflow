#!/bin/bash
set -a
source ~/.telegram_claude.env
set +a
cd /mnt/c/airflow
source .venv_wsl/bin/activate
while true; do
    echo "[시작: $(date)]"
    python ~/telegram_claude_bot.py
    echo "[종료됨, 5초후 재시작]"
    sleep 5
done
