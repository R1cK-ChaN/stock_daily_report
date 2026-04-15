#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"
ENV_FILE="${REPO_ROOT}/.env"

timestamp() {
  TZ=Asia/Shanghai date +"%Y-%m-%dT%H:%M:%S%z"
}

log_info() {
  printf '[%s] INFO %s\n' "$(timestamp)" "$*"
}

log_error() {
  printf '[%s] ERROR %s\n' "$(timestamp)" "$*" >&2
}

usage() {
  cat <<'EOF'
Usage:
  scripts/run_full_pipeline.sh
EOF
}

if [[ $# -ne 0 ]]; then
  usage >&2
  exit 64
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
  log_error "Missing virtualenv interpreter: ${VENV_PYTHON}"
  log_error "Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  log_error "Missing environment file: ${ENV_FILE}"
  log_error "Run: cp .env.example .env"
  exit 1
fi

set +u
set -a
# shellcheck disable=SC1090
. "${ENV_FILE}"
set +a
set -u

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  log_error "OPENROUTER_API_KEY is empty in ${ENV_FILE}"
  exit 1
fi

export TZ="Asia/Shanghai"
export PYTHONUNBUFFERED="1"

cd "${REPO_ROOT}"

log_info "Starting full pipeline from ${REPO_ROOT}"
exec "${VENV_PYTHON}" src/main.py
