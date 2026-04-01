#!/usr/bin/env bash
# ============================================================
# update.sh — WickedSync updater with automatic rollback
#
# Usage:
#   bash update.sh               # pull latest tag from GitHub
#   bash update.sh v1.2.3        # pull a specific version
#   bash update.sh --rollback    # revert to the previous version
#
# Mirrors the "vault" update pattern:
#   1. Record current image digest as PREVIOUS_VERSION
#   2. Pull new image
#   3. Recreate container with the new image
#   4. Health-check: hit /api/status with retries
#   5. On failure → auto-rollback to PREVIOUS_VERSION
#
# Run from the directory that contains your docker-compose.yml.
# ============================================================
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
CONTAINER="${CONTAINER:-wickedsync}"
REGISTRY="${REGISTRY:-ghcr.io}"
REPO="${REPO:-caseyi/wickedsync}"   # change to your GitHub username/repo
IMAGE="${REGISTRY}/${REPO}"

# File to persist the last known-good image digest
VERSION_FILE=".wickedsync_previous_version"
HEALTH_URL="http://localhost:8088/api/status"
HEALTH_RETRIES=12
HEALTH_DELAY=5

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}▶${NC} $*"; }
warn()  { echo -e "${YELLOW}⚠${NC} $*"; }
error() { echo -e "${RED}✗${NC} $*"; }

# ── Rollback mode ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--rollback" ]]; then
    if [[ ! -f "$VERSION_FILE" ]]; then
        error "No previous version recorded in $VERSION_FILE"
        exit 1
    fi
    PREV=$(cat "$VERSION_FILE")
    warn "Rolling back to: $PREV"
    export WICKEDSYNC_IMAGE="$PREV"
    # Rewrite docker-compose image reference
    sed -i.bak "s|image: .*|image: $PREV|" "$COMPOSE_FILE" || true
    docker compose -f "$COMPOSE_FILE" up -d --force-recreate "$CONTAINER"
    info "Rollback complete. Current version: $PREV"
    exit 0
fi

# ── Determine target version ──────────────────────────────────────────────────
TARGET="${1:-latest}"

# Fetch latest tag from GitHub if not specified
if [[ "$TARGET" == "latest" ]]; then
    info "Fetching latest release tag from GitHub..."
    LATEST=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" \
             | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\(.*\)".*/\1/')
    if [[ -z "$LATEST" ]]; then
        warn "Could not fetch latest tag, falling back to 'latest' image tag"
        TARGET="latest"
    else
        TARGET="$LATEST"
        info "Latest release: $TARGET"
    fi
fi

NEW_IMAGE="${IMAGE}:${TARGET}"

# ── Save current version for rollback ─────────────────────────────────────────
CURRENT_IMAGE=$(docker inspect --format='{{.Config.Image}}' "$CONTAINER" 2>/dev/null || echo "")
if [[ -n "$CURRENT_IMAGE" ]]; then
    echo "$CURRENT_IMAGE" > "$VERSION_FILE"
    info "Previous version saved: $CURRENT_IMAGE"
fi

# ── Pull new image ─────────────────────────────────────────────────────────────
info "Pulling $NEW_IMAGE..."
if ! docker pull "$NEW_IMAGE"; then
    error "Failed to pull $NEW_IMAGE"
    exit 1
fi

# ── Update docker-compose and restart ─────────────────────────────────────────
info "Restarting container with new image..."
# Update the image tag in docker-compose.yml so it's persistent
if grep -q "image:" "$COMPOSE_FILE" 2>/dev/null; then
    sed -i.bak "s|image: .*${REPO}.*|    image: ${NEW_IMAGE}|" "$COMPOSE_FILE"
else
    warn "No 'image:' line found in $COMPOSE_FILE; using docker pull only"
fi

docker compose -f "$COMPOSE_FILE" up -d --force-recreate "$CONTAINER"

# ── Health check ───────────────────────────────────────────────────────────────
info "Waiting for container to become healthy..."
HEALTHY=false
for i in $(seq 1 $HEALTH_RETRIES); do
    sleep $HEALTH_DELAY
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
    if [[ "$HTTP_CODE" == "200" ]]; then
        HEALTHY=true
        break
    fi
    echo "  Attempt $i/$HEALTH_RETRIES: HTTP $HTTP_CODE"
done

if $HEALTHY; then
    info "Update successful! Running: $NEW_IMAGE"
    # Update the saved version to the new one
    echo "$NEW_IMAGE" > "${VERSION_FILE}.new"
    mv "${VERSION_FILE}.new" "$VERSION_FILE"
    exit 0
else
    error "Health check failed after $((HEALTH_RETRIES * HEALTH_DELAY))s"

    # Auto-rollback
    if [[ -n "$CURRENT_IMAGE" ]]; then
        warn "Auto-rolling back to $CURRENT_IMAGE..."
        docker pull "$CURRENT_IMAGE" 2>/dev/null || true
        if grep -q "image:" "$COMPOSE_FILE" 2>/dev/null; then
            sed -i.bak "s|image: .*${REPO}.*|    image: ${CURRENT_IMAGE}|" "$COMPOSE_FILE"
        fi
        docker compose -f "$COMPOSE_FILE" up -d --force-recreate "$CONTAINER"

        # Verify rollback
        sleep 5
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
        if [[ "$HTTP_CODE" == "200" ]]; then
            warn "Rolled back successfully to $CURRENT_IMAGE"
        else
            error "Rollback also failed! Manual intervention required."
            echo "Run: docker logs $CONTAINER"
        fi
    fi

    exit 1
fi
