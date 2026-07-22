#!/bin/bash
# Start the Codex auto-heal watcher as an extra window in the shared WSL tmux session.

set -euo pipefail

SESSION="${DORIDANG_OPS_TMUX_SESSION:-doridang_ops}"
WINDOW="${CODEX_AUTOHEAL_TMUX_WINDOW:-codex-autoheal}"
WORKDIR="${CODEX_DAG_AUTOHEAL_WORKDIR:-/mnt/c/airflow}"
AUTOHEAL_WORKTREE="${CODEX_AUTOHEAL_WORKTREE:-/mnt/c/tmp/airflow-autoheal}"
export CODEX_WORKDIR="${CODEX_WORKDIR:-$AUTOHEAL_WORKTREE}"
export AIRFLOW_RUNTIME_WORKDIR="${AIRFLOW_RUNTIME_WORKDIR:-$WORKDIR}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
HEAL_LOG="${CODEX_AUTOHEAL_LOG:-/tmp/codex_autoheal_queue.log}"
export AUTOHEAL_HEARTBEAT_PATH="${AUTOHEAL_HEARTBEAT_PATH:-$WORKDIR/logs/autoheal_heartbeat.json}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 1
    fi
}

window_exists() {
    tmux list-windows -t "$SESSION" -F '#W' 2>/dev/null | grep -Fxq "$1"
}

wait_for_heartbeat() {
    local started_at="$1"

    for _ in $(seq 1 12); do
        if [ -f "$AUTOHEAL_HEARTBEAT_PATH" ]; then
            local mtime
            mtime="$(stat -c %Y "$AUTOHEAL_HEARTBEAT_PATH" 2>/dev/null || echo 0)"
            if [ "$mtime" -ge "$started_at" ]; then
                echo "heartbeat ok: $AUTOHEAL_HEARTBEAT_PATH"
                return 0
            fi
        fi
        sleep 1
    done

    echo "heartbeat missing or stale: $AUTOHEAL_HEARTBEAT_PATH" >&2
    tmux capture-pane -t "${SESSION}:${WINDOW}" -p | tail -n 40 >&2 || true
    return 1
}

require_cmd tmux
require_cmd "$PYTHON_BIN"
require_cmd docker

if [ ! -d "$WORKDIR" ]; then
    echo "workdir not found: $WORKDIR" >&2
    exit 1
fi

if [ ! -d "$CODEX_WORKDIR" ]; then
    echo "codex worktree not found: $CODEX_WORKDIR" >&2
    echo "create it with: git -C /mnt/c/airflow worktree add -b autoheal/workspace $CODEX_WORKDIR HEAD" >&2
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

if [ ! -f "${WORKDIR}/watch_heal_queue.py" ]; then
    echo "watcher not found: ${WORKDIR}/watch_heal_queue.py" >&2
    exit 1
fi

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    if tmux new-session -d -s "$SESSION" -n bootstrap -c "$WORKDIR" bash -lc "sleep 3600"; then
        echo "started session: ${SESSION}"
    elif tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "session appeared during startup: ${SESSION}"
    else
        echo "failed to start session: ${SESSION}" >&2
        exit 1
    fi
fi

if [ "$WINDOW" != "codex-heal" ] && window_exists "codex-heal"; then
    tmux kill-window -t "${SESSION}:codex-heal"
    echo "stopped legacy window: ${SESSION}:codex-heal"
fi

if window_exists "$WINDOW"; then
    tmux kill-window -t "${SESSION}:${WINDOW}"
    echo "restarted window: ${SESSION}:${WINDOW}"
fi

heartbeat_started_at="$(date +%s)"
tmux new-window -t "$SESSION" -n "$WINDOW" -c "$WORKDIR" \
    bash -lc "cd '$WORKDIR' && '$PYTHON_BIN' '$WORKDIR/watch_heal_queue.py' >> '$HEAL_LOG' 2>&1"

if window_exists "bootstrap"; then
    tmux kill-window -t "${SESSION}:bootstrap"
fi

echo "started window: ${SESSION}:${WINDOW}"
wait_for_heartbeat "$heartbeat_started_at"
tmux list-windows -t "$SESSION"
