#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/opt/axonhub}"
SERVICE="${SERVICE:-axonhub}"
CONTAINER="${CONTAINER:-axonhub-app}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-axonhub-postgres}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:18090/health}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-/tmp/axonhub-deploy/runs}"
NEW_IMAGE="${NEW_IMAGE:?NEW_IMAGE is required}"
TAR_PATH="${TAR_PATH:?TAR_PATH is required}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_ROOT}/sticky-cleanup-${STAMP}"
SMOKE_NAME="axonhub-smoke-${STAMP}"
LOCK_FILE="${LOCK_FILE:-/tmp/axonhub-deploy.lock}"
ROLLBACK_NEEDED=0

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

die() {
  log "ERROR $*"
  exit 1
}

cd "$APP_DIR"
exec 9>"$LOCK_FILE"
flock -n 9 || die "another deployment is already running"

get_compose_image() {
  python3 - "$APP_DIR/docker-compose.yml" "$SERVICE" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
service = sys.argv[2]
lines = path.read_text(encoding="utf-8").splitlines()
in_service = False
for line in lines:
    stripped = line.strip()
    if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
        in_service = stripped[:-1] == service
        continue
    if in_service and line.startswith("    image:"):
        print(line.split(":", 1)[1].strip())
        raise SystemExit(0)
raise SystemExit(42)
PY
}

set_compose_image() {
  local image="$1"
  local tmp
  tmp="$(mktemp)"
  python3 - "$APP_DIR/docker-compose.yml" "$SERVICE" "$image" "$tmp" <<'PY'
import sys
from pathlib import Path

src = Path(sys.argv[1])
service = sys.argv[2]
image = sys.argv[3]
dst = Path(sys.argv[4])
lines = src.read_text(encoding="utf-8").splitlines(keepends=True)
out = []
in_service = False
replaced = 0
for line in lines:
    stripped = line.strip()
    if line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":"):
        in_service = stripped[:-1] == service
    if in_service and line.startswith("    image:"):
        newline = "\n" if line.endswith("\n") else ""
        out.append(f"    image: {image}{newline}")
        replaced += 1
        continue
    out.append(line)
if replaced != 1:
    raise SystemExit(f"expected to replace exactly one image line, replaced={replaced}")
dst.write_text("".join(out), encoding="utf-8")
PY
  sudo -n cp "$tmp" "$APP_DIR/docker-compose.yml"
  sudo -n chown --reference="${ARTIFACT_DIR}/docker-compose.yml.before" "$APP_DIR/docker-compose.yml" || true
  sudo -n chmod --reference="${ARTIFACT_DIR}/docker-compose.yml.before" "$APP_DIR/docker-compose.yml" || true
  rm -f "$tmp"
}

wait_healthy() {
  local expected_image="$1"
  local i image status body
  for i in $(seq 1 90); do
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
    sleep 2
  done
  return 1
}

cleanup_smoke() {
  sudo -n docker rm -f "$SMOKE_NAME" >/dev/null 2>&1 || true
}

rollback() {
  local rc=$?
  set +e
  log "ROLLBACK_START old_image=${OLD_IMAGE:-unknown} rc=${rc}"
  cleanup_smoke
  if [[ -n "${OLD_IMAGE:-}" ]]; then
    set_compose_image "$OLD_IMAGE"
    sudo -n docker compose config >"${ARTIFACT_DIR}/compose.rollback.config" 2>"${ARTIFACT_DIR}/compose.rollback.stderr"
    sudo -n docker compose up -d --no-deps "$SERVICE"
    if body="$(wait_healthy "$OLD_IMAGE")"; then
      log "ROLLBACK_OK old_image=${OLD_IMAGE}"
      printf '%s\n' "$body" >"${ARTIFACT_DIR}/rollback-health.json"
      exit "$rc"
    fi
  fi
  log "ROLLBACK_FAILED"
  sudo -n docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}' | grep -E 'axonhub' || true
  sudo -n docker logs --since 5m "$CONTAINER" >"${ARTIFACT_DIR}/rollback-container.log" 2>&1 || true
  exit "$rc"
}

on_exit() {
  local rc=$?
  if [[ "$rc" -ne 0 && "$ROLLBACK_NEEDED" == "1" ]]; then
    rollback
  fi
  cleanup_smoke
  exit "$rc"
}
trap on_exit EXIT INT TERM

[[ -f "$TAR_PATH" ]] || die "tar not found: $TAR_PATH"
sudo -n install -d -m 700 -o "$(id -u)" -g "$(id -g)" "$ARTIFACT_DIR"
sudo -n cp "$APP_DIR/docker-compose.yml" "$ARTIFACT_DIR/docker-compose.yml.before"
[[ -f "$APP_DIR/config.yml" ]] && sudo -n cp "$APP_DIR/config.yml" "$ARTIFACT_DIR/config.yml.before"

OLD_IMAGE="$(get_compose_image)"
OLD_CONTAINER_IMAGE_ID="$(sudo -n docker inspect -f '{{.Image}}' "$CONTAINER")"
NETWORK="$(sudo -n docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$CONTAINER" | head -n1)"

{
  echo "stamp=${STAMP}"
  echo "app_dir=${APP_DIR}"
  echo "service=${SERVICE}"
  echo "container=${CONTAINER}"
  echo "old_image=${OLD_IMAGE}"
  echo "old_container_image_id=${OLD_CONTAINER_IMAGE_ID}"
  echo "new_image=${NEW_IMAGE}"
  echo "tar_path=${TAR_PATH}"
  echo "network=${NETWORK}"
} | sudo -n tee "$ARTIFACT_DIR/manifest.txt" >/dev/null

log "PRECHECK old_image=${OLD_IMAGE} new_image=${NEW_IMAGE} artifacts=${ARTIFACT_DIR}"
sudo -n docker compose config >"$ARTIFACT_DIR/compose.before.config"
sudo -n docker image inspect "$OLD_IMAGE" >"$ARTIFACT_DIR/old-image.inspect.json"

log "LOAD_IMAGE_START tar=${TAR_PATH}"
sudo -n docker load -i "$TAR_PATH" | sudo -n tee "$ARTIFACT_DIR/docker-load.log" >/dev/null
sudo -n docker image inspect "$NEW_IMAGE" >"$ARTIFACT_DIR/new-image.inspect.json"
new_arch="$(sudo -n docker image inspect -f '{{.Os}}/{{.Architecture}}' "$NEW_IMAGE")"
[[ "$new_arch" == "linux/arm64" ]] || die "unexpected image platform: ${new_arch}"
log "LOAD_IMAGE_OK platform=${new_arch}"

log "SMOKE_START name=${SMOKE_NAME} network=${NETWORK}"
cleanup_smoke
DB_PASSWORD_VALUE="$(grep -E '^DB_PASSWORD=' "$APP_DIR/.env" | tail -n1 | cut -d= -f2-)"
DB_PASSWORD_VALUE="${DB_PASSWORD_VALUE%\"}"
DB_PASSWORD_VALUE="${DB_PASSWORD_VALUE#\"}"
[[ -n "$DB_PASSWORD_VALUE" ]] || die "DB_PASSWORD is missing from ${APP_DIR}/.env"
SMOKE_DB_DSN="postgres://axonhub:${DB_PASSWORD_VALUE}@postgres:5432/axonhub?sslmode=disable"
sudo -n docker run -d \
  --name "$SMOKE_NAME" \
  --network "$NETWORK" \
  --env-file "$APP_DIR/.env" \
  -e AXONHUB_DB_DIALECT=postgres \
  -e "AXONHUB_DB_DSN=${SMOKE_DB_DSN}" \
  -e AXONHUB_SERVER_HOST=0.0.0.0 \
  -e AXONHUB_SERVER_PORT=8090 \
  -e AXONHUB_LOG_LEVEL=info \
  -e AXONHUB_LOG_ENCODING=json \
  -e AXONHUB_LOG_OUTPUT=stdio \
  -e AXONHUB_CACHE_MODE=memory \
  -v "$APP_DIR/config.yml:/app/config.yml:ro" \
  "$NEW_IMAGE" >/dev/null

for _ in $(seq 1 60); do
  if body="$(sudo -n docker exec "$SMOKE_NAME" wget -qO- http://127.0.0.1:8090/health 2>/dev/null)"; then
    if printf '%s' "$body" | grep -q '"status":"healthy"' &&
       printf '%s' "$body" | grep -q '"platform":"linux/arm64"'; then
      printf '%s\n' "$body" >"$ARTIFACT_DIR/smoke-health.json"
      log "SMOKE_OK"
      break
    fi
  fi
  running="$(sudo -n docker inspect -f '{{.State.Running}}' "$SMOKE_NAME" 2>/dev/null || true)"
  if [[ "$running" != "true" ]]; then
    sudo -n docker logs "$SMOKE_NAME" >"$ARTIFACT_DIR/smoke.log" 2>&1 || true
    die "smoke container exited"
  fi
  sleep 2
done
[[ -s "$ARTIFACT_DIR/smoke-health.json" ]] || die "smoke health timeout"
sudo -n docker logs --since 2m "$SMOKE_NAME" >"$ARTIFACT_DIR/smoke.log" 2>&1 || true
cleanup_smoke

log "DEPLOY_SWITCH_START"
ROLLBACK_NEEDED=1
set_compose_image "$NEW_IMAGE"
sudo -n docker compose config >"$ARTIFACT_DIR/compose.new.config"
sudo -n docker compose up -d --no-deps "$SERVICE"

if body="$(wait_healthy "$NEW_IMAGE")"; then
  printf '%s\n' "$body" >"$ARTIFACT_DIR/deploy-health.json"
  sudo -n docker exec "$POSTGRES_CONTAINER" psql -U axonhub -d axonhub -Atc 'select 1' >"$ARTIFACT_DIR/db-postcheck.txt"
  sudo -n docker ps --format '{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}' | grep -E 'axonhub' >"$ARTIFACT_DIR/containers.after.txt" || true
  ROLLBACK_NEEDED=0
  log "DEPLOY_OK old_image=${OLD_IMAGE} new_image=${NEW_IMAGE} artifacts=${ARTIFACT_DIR}"
  log "HEALTH=${body}"
  exit 0
fi

sudo -n docker logs --since 5m "$CONTAINER" >"$ARTIFACT_DIR/new-container.log" 2>&1 || true
die "new container health timeout"
