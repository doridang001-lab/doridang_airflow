#!/usr/bin/env bash
# Start DAG import-error detection and Codex queue processing in tmux.

set -euo pipefail

WORKDIR="${CODEX_DAG_AUTOHEAL_WORKDIR:-/mnt/c/airflow}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
IMPORT_SESSION="${IMPORT_ERROR_WATCHER_SESSION:-codex-import-error}"
HEAL_SESSION="${HEAL_QUEUE_WATCHER_SESSION:-codex-heal-queue}"
IMPORT_LOG="${IMPORT_ERROR_WATCHER_LOG:-/tmp/dag_import_error_watcher.log}"
HEAL_LOG="${HEAL_QUEUE_WATCHER_LOG:-/tmp/heal_queue_watcher.log}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

start_session() {
  local session_name="$1"
  local command_text="$2"

  if tmux has-session -t "${session_name}" 2>/dev/null; then
    echo "tmux session exists: ${session_name}"
    return
  fi

  tmux new-session -d -s "${session_name}" -c "${WORKDIR}" bash -lc "${command_text}"
  echo "started: ${session_name}"
}

require_cmd tmux
require_cmd "${PYTHON_BIN}"
require_cmd docker

if [ ! -d "${WORKDIR}" ]; then
  echo "workdir not found: ${WORKDIR}" >&2
  exit 1
fi

if [ -f "${WORKDIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${WORKDIR}/.env"
  set +a
fi

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
  echo "warning: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set; Telegram reports may be skipped" >&2
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

start_session "${IMPORT_SESSION}" "cd '${WORKDIR}' && '${PYTHON_BIN}' '${WORKDIR}/scripts/watch_dag_import_errors.py' >> '${IMPORT_LOG}' 2>&1"
start_session "${HEAL_SESSION}" "cd '${WORKDIR}' && '${PYTHON_BIN}' '${WORKDIR}/watch_heal_queue.py' >> '${HEAL_LOG}' 2>&1"

echo "logs:"
echo "  import watcher: ${IMPORT_LOG}"
echo "  heal watcher:   ${HEAL_LOG}"
