#!/usr/bin/env bash
# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

set -euo pipefail

SONAR_HOST_URL="${SONAR_HOST_URL:-http://sonarqube:9000}"
SONAR_ADMIN_LOGIN="${SONAR_ADMIN_LOGIN:-admin}"
SONAR_DEFAULT_ADMIN_PASSWORD="${SONAR_DEFAULT_ADMIN_PASSWORD:-admin}"
SONAR_ADMIN_PASSWORD="${SONAR_ADMIN_PASSWORD:-MddsLocalSonarAdmin2026_A9xQ7mZ2}"
SONAR_TOKEN_FILE="${SONAR_TOKEN_FILE:-.sonar_token}"
SONAR_TOKEN_NAME="${SONAR_TOKEN_NAME:-mdds-local-sonar-token}"

log_info() {
  echo "[INFO] $*"
}

log_done() {
  echo "[INFO] ✅ $*"
}

log_error() {
  echo "[ERROR] ❌ $*" >&2
}

validate_credentials() {
  local login="$1"
  local password="$2"

  curl -fsS \
    -u "${login}:${password}" \
    "${SONAR_HOST_URL}/api/authentication/validate" |
    jq -e '.valid == true' >/dev/null
}

validate_token() {
  local token="$1"

  curl -fsS \
    -u "${token}:" \
    "${SONAR_HOST_URL}/api/authentication/validate" |
    jq -e '.valid == true' >/dev/null
}

wait_for_sonarqube() {
  log_info "Waiting for SonarQube at ${SONAR_HOST_URL}..."

  for attempt in $(seq 1 60); do
    status="$(
      curl -s "${SONAR_HOST_URL}/api/system/status" |
        jq -r '.status // empty' 2>/dev/null || true
    )"

    if [ "${status}" = "UP" ] || [ "${status}" = "DEGRADED" ]; then
      log_done "SonarQube is ready: ${status}"
      return 0
    fi

    log_info "Attempt ${attempt}/60: SonarQube status is '${status}'. Retrying..."
    sleep 5
  done

  log_error "SonarQube did not become ready."
  exit 1
}

change_admin_password() {
  local response_file
  local http_code

  response_file="$(mktemp)"

  http_code="$(
    curl -sS \
      -u "${SONAR_ADMIN_LOGIN}:${SONAR_DEFAULT_ADMIN_PASSWORD}" \
      -X POST \
      "${SONAR_HOST_URL}/api/users/change_password" \
      --data-urlencode "login=${SONAR_ADMIN_LOGIN}" \
      --data-urlencode "previousPassword=${SONAR_DEFAULT_ADMIN_PASSWORD}" \
      --data-urlencode "password=${SONAR_ADMIN_PASSWORD}" \
      -o "${response_file}" \
      -w '%{http_code}'
  )"

  if [ "${http_code}" != "204" ]; then
    log_error "Could not change SonarQube admin password. HTTP status: ${http_code}"
    log_error "SonarQube response:"
    cat "${response_file}" >&2
    rm -f "${response_file}"
    exit 1
  fi

  rm -f "${response_file}"
}

ensure_admin_password() {
  if validate_credentials "${SONAR_ADMIN_LOGIN}" "${SONAR_ADMIN_PASSWORD}"; then
    log_done "SonarQube admin password is already configured."
    return 0
  fi

  log_info "Configured admin password did not work. Trying default SonarQube credentials..."

  if ! validate_credentials "${SONAR_ADMIN_LOGIN}" "${SONAR_DEFAULT_ADMIN_PASSWORD}"; then
    log_error "Could not authenticate with configured or default SonarQube admin credentials."
    log_error "SONAR_ADMIN_LOGIN=${SONAR_ADMIN_LOGIN}"
    log_error "SONAR_HOST_URL=${SONAR_HOST_URL}"
    log_error "If this is an existing SonarQube volume, set SONAR_ADMIN_PASSWORD to the actual admin password."
    exit 1
  fi

  log_info "Default SonarQube credentials are valid. Changing admin password..."

  change_admin_password

  if ! validate_credentials "${SONAR_ADMIN_LOGIN}" "${SONAR_ADMIN_PASSWORD}"; then
    log_error "Admin password change was attempted, but new credentials are not valid."
    exit 1
  fi

  log_done "SonarQube admin password has been changed."
}

ensure_token() {
  if [ -s "${SONAR_TOKEN_FILE}" ]; then
    existing_token="$(tr -d '\n' <"${SONAR_TOKEN_FILE}")"

    if [ -n "${existing_token}" ] && validate_token "${existing_token}"; then
      log_done "Existing SonarQube token is valid."
      return 0
    fi

    log_info "Existing SonarQube token is missing or invalid. Creating a new one..."
  else
    log_info "SonarQube token file does not exist. Creating a new token..."
  fi

  log_info "Revoking old token with the same name if it exists..."

  curl -sS \
    -u "${SONAR_ADMIN_LOGIN}:${SONAR_ADMIN_PASSWORD}" \
    -X POST \
    "${SONAR_HOST_URL}/api/user_tokens/revoke" \
    --data-urlencode "name=${SONAR_TOKEN_NAME}" >/dev/null || true

  log_info "Generating SonarQube user token..."

  response="$(
    curl -sS \
      -u "${SONAR_ADMIN_LOGIN}:${SONAR_ADMIN_PASSWORD}" \
      -X POST \
      "${SONAR_HOST_URL}/api/user_tokens/generate" \
      --data-urlencode "name=${SONAR_TOKEN_NAME}" \
      --data-urlencode "type=USER_TOKEN"
  )"

  token="$(printf '%s' "${response}" | jq -r '.token // empty')"

  if [ -z "${token}" ]; then
    log_info "Typed token generation failed or is unsupported. Trying legacy token generation..."

    response="$(
      curl -sS \
        -u "${SONAR_ADMIN_LOGIN}:${SONAR_ADMIN_PASSWORD}" \
        -X POST \
        "${SONAR_HOST_URL}/api/user_tokens/generate" \
        --data-urlencode "name=${SONAR_TOKEN_NAME}"
    )"

    token="$(printf '%s' "${response}" | jq -r '.token // empty')"
  fi

  if [ -z "${token}" ]; then
    log_error "Could not generate SonarQube token."
    log_error "SonarQube response:"
    printf '%s\n' "${response}" >&2
    exit 1
  fi

  printf '%s' "${token}" >"${SONAR_TOKEN_FILE}"
  chmod 600 "${SONAR_TOKEN_FILE}"

  if ! validate_token "${token}"; then
    log_error "Generated token was saved, but validation failed."
    exit 1
  fi

  log_done "SonarQube token saved to ${SONAR_TOKEN_FILE}"
}

main() {
  wait_for_sonarqube
  ensure_admin_password
  ensure_token
}

main "$@"
