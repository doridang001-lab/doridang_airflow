#!/usr/bin/env bash
# Start Codex queue processor watcher in dedicated tmux session.

set -euo pipefail

SESSION_NAME="${HEAL_QUEUE_WATCHER_SESSION:-codex-heal-queue}"
WORKDIR="${HEAL_QUEUE_WATCHER_WORKDIR:-/mnt/c/airflow}"
LOG_PATH="${HEAL_QUEUE_WATCHER_LOG:-/tmp/heal_queue_watcher.log}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CMD="'${PYTHON_BIN}' ${WORKDIR}/watch_heal_queue.py >> ${LOG_PATH} 2>&1"

if [ -f "${WORKDIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${WORKDIR}/.env"
  set +a
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "missing required command: tmux" >&2
  exit 1
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "missing required command: ${PYTHON_BIN}" >&2
  exit 1
fi

if [ -z "${CODEX_COMMAND:-}" ]; then
  if command -v codex >/dev/null 2>&1 && codex exec --help >/dev/null 2>&1; then
    export CODEX_COMMAND="codex"
  elif [ -x "/mnt/c/Program Files/nodejs/node.exe" ] && [ -f "/mnt/c/Users/민준/AppData/Roaming/npm/node_modules/@openai/codex/bin/codex.js" ]; then
    export CODEX_COMMAND='"/mnt/c/Program Files/nodejs/node.exe" "/mnt/c/Users/민준/AppData/Roaming/npm/node_modules/@openai/codex/bin/codex.js"'
  else
    echo "codex exec is unavailable in this WSL shell; run Codex login/setup first" >&2
    exit 1
  fi
fi

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
  echo "tmux session exists: ${SESSION_NAME}"
  exit 0
fi

tmux new-session -d -s "${SESSION_NAME}" -c "${WORKDIR}" bash -lc "${CMD}"
echo "started: ${SESSION_NAME}"
