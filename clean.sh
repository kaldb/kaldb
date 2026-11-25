#!/usr/bin/env bash
set -e

SCRIPT_IMAGE_LABEL_KEY="kaldb.quick_start.managed"
SCRIPT_IMAGE_LABEL_VALUE="true"

echo "🧹 Cleaning KalDB demo containers, networks, and volumes..."
docker compose down -v --remove-orphans

echo "🔥 Optionally pruning demo images..."
docker image prune -af --filter "label=$SCRIPT_IMAGE_LABEL_KEY=$SCRIPT_IMAGE_LABEL_VALUE" || true

echo "✅ KalDB environment cleaned."
