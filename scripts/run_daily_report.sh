#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"
ENV_FILE="${REPO_ROOT}/.env"
LOG_DIR="${REPO_ROOT}/output/scheduler_logs"
HOSTNAME="$(scutil --get LocalHostName 2>/dev/null || hostname)"

mkdir -p "${LOG_DIR}"

timestamp() {
  TZ=Asia/Shanghai date +"%Y-%m-%dT%H:%M:%S%z"
}

log_info() {
  printf '[%s] INFO %s\n' "$(timestamp)" "$*"
}

log_error() {
  printf '[%s] ERROR %s\n' "$(timestamp)" "$*" >&2
}

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

send_feishu_alert() {
  local message="$1"

  if [[ -z "${FEISHU_WEBHOOK_URL:-}" ]]; then
    return 0
  fi

  /usr/bin/env python3 - "${message}" "${FEISHU_WEBHOOK_URL}" "${FEISHU_SECRET:-}" <<'PY'
import base64
import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.request

message, webhook_url, secret = sys.argv[1], sys.argv[2], sys.argv[3]

payload = {
    "msg_type": "text",
    "content": {
        "text": message,
    },
}

if secret:
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    payload["timestamp"] = timestamp
    payload["sign"] = base64.b64encode(digest).decode("utf-8")

request = urllib.request.Request(
    webhook_url,
    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    headers={"Content-Type": "application/json"},
)

try:
    with urllib.request.urlopen(request, timeout=10) as response:
        print(response.read().decode("utf-8", errors="ignore"))
except (urllib.error.URLError, TimeoutError, OSError) as exc:
    print(f"preflight Feishu alert failed: {exc}", file=sys.stderr)
    sys.exit(1)
PY
}

preflight() {
  local -a errors=()
  local source_status=0

  if [[ ! -x "${VENV_PYTHON}" ]]; then
    errors+=("Missing virtualenv interpreter: ${VENV_PYTHON}")
  fi

  if [[ ! -f "${ENV_FILE}" ]]; then
    errors+=("Missing environment file: ${ENV_FILE}")
  fi

  if [[ ${#errors[@]} -eq 0 ]]; then
    set +u
    set -a
    # shellcheck disable=SC1090
    . "${ENV_FILE}"
    source_status=$?
    set +a
    set -u

    if [[ ${source_status} -ne 0 ]]; then
      errors+=("Failed to load environment file: ${ENV_FILE}")
    fi
  fi

  if [[ ${#errors[@]} -eq 0 ]]; then
    if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
      errors+=("OPENROUTER_API_KEY is empty in ${ENV_FILE}")
    fi
    if ! truthy "${FEISHU_ENABLED:-}"; then
      errors+=("FEISHU_ENABLED must be true for scheduled delivery")
    fi
    if [[ -z "${FEISHU_WEBHOOK_URL:-}" ]]; then
      errors+=("FEISHU_WEBHOOK_URL is empty in ${ENV_FILE}")
    fi
  fi

  if [[ ${#errors[@]} -eq 0 ]]; then
    return 0
  fi

  local joined_errors
  joined_errors="$(printf ' - %s\n' "${errors[@]}")"
  log_error "Preflight failed:"
  printf '%s' "${joined_errors}" >&2

  local alert_message
  alert_message=$(
    cat <<EOF
[Alert] Scheduled run preflight failed
Time: $(timestamp)
Host: ${HOSTNAME}
Repo: ${REPO_ROOT}
Errors:
${joined_errors}
EOF
  )

  if ! send_feishu_alert "${alert_message}"; then
    log_error "Failed to send Feishu preflight alert"
  fi

  return 1
}

usage() {
  cat <<'EOF'
Usage:
  scripts/run_daily_report.sh [--preflight-only]
EOF
}

PRECHECK_ONLY=false

if [[ $# -gt 1 ]]; then
  usage >&2
  exit 64
fi

if [[ $# -eq 1 ]]; then
  if [[ "$1" == "--preflight-only" ]]; then
    PRECHECK_ONLY=true
  else
    usage >&2
    exit 64
  fi
fi

export TZ="Asia/Shanghai"
export PYTHONUNBUFFERED="1"

log_info "Starting scheduled wrapper from ${REPO_ROOT}"

if ! preflight; then
  exit 1
fi

log_info "Preflight passed"

if [[ "${PRECHECK_ONLY}" == "true" ]]; then
  log_info "Preflight-only mode requested; exiting without running the pipeline"
  exit 0
fi

cd "${REPO_ROOT}" || {
  log_error "Failed to enter repo root: ${REPO_ROOT}"
  exit 1
}

log_info "Running pipeline with ${VENV_PYTHON}"
"${VENV_PYTHON}" src/main.py
status=$?

if [[ ${status} -eq 0 ]]; then
  log_info "Pipeline finished successfully"
else
  log_error "Pipeline exited with status ${status}"
fi

exit ${status}
