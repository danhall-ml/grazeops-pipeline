#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${PROJECT_NAME:-grazeops-smoke-$(date +%s)}"
COMPOSE_CMD=(docker compose -p "$PROJECT_NAME" -f "$ROOT_DIR/docker-compose.yml" -f "$ROOT_DIR/docker-compose.smoke.yml")

cleanup() {
  "${COMPOSE_CMD[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "Starting ephemeral stack: $PROJECT_NAME"
"${COMPOSE_CMD[@]}" up -d --build postgres-db model-registry scheduler calculation-service

echo "Seeding ingestion data"
"${COMPOSE_CMD[@]}" --profile manual run --rm ingestion-worker

echo "Running smoke test"
"${COMPOSE_CMD[@]}" run --rm smoke-runner

echo "Ephemeral smoke passed: $PROJECT_NAME"
