#!/usr/bin/env bash
# Start DAG import error watcher in dedicated tmux session.

set -euo pipefail

SESSION_NAME="${IMPORT_ERROR_WATCHER_SESSION:-codex-import-error}"
WORKDIR="${IMPORT_ERROR_WATCHER_WORKDIR:-/mnt/c/airflow}"
LOG_PATH="${IMPORT_ERROR_WATCHER_LOG:-/tmp/dag_import_error_watcher.log}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CMD="'${PYTHON_BIN}' ${WORKDIR}/scripts/watch_dag_import_errors.py >> ${LOG_PATH} 2>&1"

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

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
  echo "tmux session exists: ${SESSION_NAME}"
  exit 0
fi

tmux new-session -d -s "${SESSION_NAME}" -c "${WORKDIR}" bash -lc "${CMD}"
echo "started: ${SESSION_NAME}"
