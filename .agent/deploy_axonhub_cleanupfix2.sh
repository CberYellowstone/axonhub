#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/axonhub"
SERVICE="axonhub"
CONTAINER="axonhub-app"
HEALTH_URL="http://127.0.0.1:18090/health"
NEW_IMAGE="local/axonhub:e0d2a57a-cleanupfix2"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="${APP_DIR}/docker-compose.yml.before-cleanupfix2-${STAMP}"

cd "$APP_DIR"

get_compose_image() {
  awk '
    /^[[:space:]]{2}axonhub:[[:space:]]*$/ { svc=1; next }
    svc && /^[[:space:]]{2}[A-Za-z0-9_-]+:[[:space:]]*$/ { svc=0 }
    svc && /^[[:space:]]+image:[[:space:]]*/ { print $2; found=1; exit }
    END { if (!found) exit 42 }
  ' docker-compose.yml
}

set_compose_image() {
  local image="$1"
  local tmp
  tmp="$(mktemp)"
  awk -v image="$image" '
    /^[[:space:]]{2}axonhub:[[:space:]]*$/ { svc=1 }
    svc && /^[[:space:]]{2}[A-Za-z0-9_-]+:[[:space:]]*$/ && $0 !~ /^[[:space:]]{2}axonhub:[[:space:]]*$/ { svc=0 }
    svc && /^[[:space:]]+image:[[:space:]]*/ { $0="    image: " image; replaced++ }
    { print }
    END { if (replaced != 1) exit 42 }
  ' docker-compose.yml > "$tmp"
  sudo -n mv "$tmp" docker-compose.yml
  sudo -n chown --reference="$BACKUP" docker-compose.yml
  sudo -n chmod --reference="$BACKUP" docker-compose.yml
}

health_ok_for_image() {
  local expected_image="$1"
  local image status body
  image="$(sudo -n docker inspect -f '{{.Config.Image}}' "$CONTAINER" 2>/dev/null || true)"
  status="$(sudo -n docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$CONTAINER" 2>/dev/null || true)"
  body="$(curl -fsS --max-time 3 "$HEALTH_URL" 2>/dev/null || true)"

  if [[ "$image" == "$expected_image" ]] &&
     [[ "$status" == "healthy" ]] &&
     printf '%s' "$body" | grep -q '"status":"healthy"' &&
     printf '%s' "$body" | grep -q '"platform":"linux/arm64"'; then
    printf '%s\n' "$body"
    return 0
  fi
  return 1
}

wait_healthy() {
  local expected_image="$1"
  local i body
  for i in $(seq 1 60); do
    if body="$(health_ok_for_image "$expected_image")"; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep 2
  done
  return 1
}

rollback() {
  local old_image="$1"
  echo "ROLLBACK_START old_image=${old_image}"
  set_compose_image "$old_image"
  sudo -n docker compose config >/tmp/axonhub-compose-rollback.config
  sudo -n docker compose up -d "$SERVICE"
  if body="$(wait_healthy "$old_image")"; then
    echo "ROLLBACK_OK image=${old_image}"
    echo "ROLLBACK_HEALTH=${body}"
    return 0
  fi
  echo "ROLLBACK_HEALTH_FAILED image=${old_image}"
  sudo -n docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}' | grep -E 'axonhub'
  sudo -n docker logs --since 3m "$CONTAINER" | tail -120 || true
  return 1
}

OLD_IMAGE="$(get_compose_image)"
echo "DEPLOY_START old_image=${OLD_IMAGE} new_image=${NEW_IMAGE}"

sudo -n cp docker-compose.yml "$BACKUP"
sudo -n docker image inspect "$NEW_IMAGE" >/dev/null

set_compose_image "$NEW_IMAGE"
if ! sudo -n docker compose config >/tmp/axonhub-compose-cleanupfix2.config; then
  echo "COMPOSE_CONFIG_FAILED_RESTORING"
  set_compose_image "$OLD_IMAGE"
  exit 10
fi

if ! sudo -n docker compose up -d "$SERVICE"; then
  echo "COMPOSE_UP_FAILED"
  rollback "$OLD_IMAGE"
  exit 20
fi

if body="$(wait_healthy "$NEW_IMAGE")"; then
  container_image="$(sudo -n docker inspect -f '{{.Config.Image}}' "$CONTAINER")"
  container_health="$(sudo -n docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$CONTAINER")"
  echo "DEPLOY_OK old_image=${OLD_IMAGE} new_image=${NEW_IMAGE}"
  echo "BACKUP=${BACKUP}"
  echo "CONTAINER=${container_image} health=${container_health}"
  echo "HEALTH=${body}"
  exit 0
fi

echo "NEW_HEALTH_FAILED"
rollback "$OLD_IMAGE"
exit 30
