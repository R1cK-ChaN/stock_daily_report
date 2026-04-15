#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE_PATH="${REPO_ROOT}/launchd/com.kingjason.stock-daily-report.plist.template"
TARGET_DIR="${HOME}/Library/LaunchAgents"
TARGET_PATH="${TARGET_DIR}/com.kingjason.stock-daily-report.plist"
RUN_SCRIPT="${REPO_ROOT}/scripts/run_daily_report.sh"
LOG_DIR="${REPO_ROOT}/output/scheduler_logs"
STDOUT_LOG="${LOG_DIR}/launchd.stdout.log"
STDERR_LOG="${LOG_DIR}/launchd.stderr.log"
LABEL="com.kingjason.stock-daily-report"
GUI_DOMAIN="gui/$(id -u)"

mkdir -p "${TARGET_DIR}" "${LOG_DIR}"
touch "${STDOUT_LOG}" "${STDERR_LOG}"

sed \
  -e "s|__REPO_ROOT__|${REPO_ROOT}|g" \
  -e "s|__RUN_SCRIPT__|${RUN_SCRIPT}|g" \
  -e "s|__STDOUT_LOG__|${STDOUT_LOG}|g" \
  -e "s|__STDERR_LOG__|${STDERR_LOG}|g" \
  "${TEMPLATE_PATH}" > "${TARGET_PATH}"

chmod 644 "${TARGET_PATH}"
plutil -lint "${TARGET_PATH}" >/dev/null

launchctl bootout "${GUI_DOMAIN}/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "${GUI_DOMAIN}" "${TARGET_PATH}"
launchctl enable "${GUI_DOMAIN}/${LABEL}"

printf 'Installed LaunchAgent: %s\n' "${TARGET_PATH}"
printf 'Wrapper script: %s\n' "${RUN_SCRIPT}"
printf 'Stdout log: %s\n' "${STDOUT_LOG}"
printf 'Stderr log: %s\n' "${STDERR_LOG}"
