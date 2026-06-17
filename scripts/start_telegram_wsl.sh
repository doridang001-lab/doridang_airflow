#!/bin/bash
# Start Doridang ops windows in one WSL tmux session:
#   1. claude       - Telegram -> Claude loop
#   2. codex-import - Airflow DAG import-error watcher
#   3. codex-heal   - Airflow task failure queue watcher

set -euo pipefail

SESSION="${DORIDANG_OPS_TMUX_SESSION:-doridang_ops}"
WORKDIR="${CODEX_DAG_AUTOHEAL_WORKDIR:-/mnt/c/airflow}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
IMPORT_LOG="${IMPORT_ERROR_WATCHER_LOG:-/tmp/dag_import_error_watcher.log}"
HEAL_LOG="${HEAL_QUEUE_WATCHER_LOG:-/tmp/heal_queue_watcher.log}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 1
    fi
}

window_exists() {
    tmux list-windows -t "$SESSION" -F '#W' 2>/dev/null | grep -Fxq "$1"
}

start_window() {
    local name="$1"
    local command_text="$2"

    if window_exists "$name"; then
        echo "tmux window exists: ${SESSION}:${name}"
        return
    fi

    tmux new-window -t "$SESSION" -n "$name" -c "$WORKDIR" bash -lc "$command_text"
    echo "started window: ${SESSION}:${name}"
}

reset_window() {
    local name="$1"
    local command_text="$2"

    if window_exists "$name"; then
        tmux kill-window -t "${SESSION}:${name}"
        echo "restarted window: ${SESSION}:${name}"
    fi

    tmux new-window -t "$SESSION" -n "$name" -c "$WORKDIR" bash -lc "$command_text"
    echo "started window: ${SESSION}:${name}"
}

require_cmd tmux
require_cmd "$PYTHON_BIN"
require_cmd docker

for legacy_session in telegram_claude codex-import-error codex-heal-queue; do
    if tmux has-session -t "$legacy_session" 2>/dev/null; then
        tmux kill-session -t "$legacy_session"
        echo "stopped legacy tmux session: $legacy_session"
    fi
done

if [ ! -d "$WORKDIR" ]; then
    echo "workdir not found: $WORKDIR" >&2
    exit 1
fi

if [ -f "${WORKDIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${WORKDIR}/.env"
    set +a
fi

if [ -z "${CODEX_COMMAND:-}" ]; then
    if command -v codex >/dev/null 2>&1 && codex exec --help >/dev/null 2>&1; then
        export CODEX_COMMAND="codex"
    elif command -v cmd.exe >/dev/null 2>&1 && codex_cmd_win="$(cmd.exe /c where codex 2>/dev/null | tr -d '\r' | head -n 1)" && [ -n "$codex_cmd_win" ]; then
        codex_cmd_wsl="$(wslpath "$codex_cmd_win")"
        export CODEX_COMMAND="$codex_cmd_wsl"
    else
        echo "codex exec is unavailable in this WSL shell; run Codex login/setup first" >&2
        exit 1
    fi
fi

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux new-session -d -s "$SESSION" -n bootstrap -c "$WORKDIR" \
        bash -lc "sleep 3600"
    echo "started session: ${SESSION}"
fi

reset_window "claude" "cd '$WORKDIR' && '$PYTHON_BIN' '$WORKDIR/scripts/telegram_poll_loop.py'"
reset_window "codex-import" "cd '$WORKDIR' && '$PYTHON_BIN' '$WORKDIR/scripts/watch_dag_import_errors.py' >> '$IMPORT_LOG' 2>&1"
reset_window "codex-heal" "cd '$WORKDIR' && '$PYTHON_BIN' '$WORKDIR/watch_heal_queue.py' >> '$HEAL_LOG' 2>&1"

if window_exists "bootstrap"; then
    tmux kill-window -t "${SESSION}:bootstrap"
fi

tmux list-windows -t "$SESSION"
