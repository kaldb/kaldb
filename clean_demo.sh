#!/usr/bin/env bash
set -e

echo "🧹 Cleaning demo containers, networks, and volumes..."
docker compose down -v --remove-orphans

echo "🔥 Optionally pruning demo images..."
docker image prune -af --filter "label=astra-demo=true" || true

echo "✅ Demo environment cleaned."
