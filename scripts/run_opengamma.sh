#!/usr/bin/env bash
set -euo pipefail

# Start local risk service (stub-compatible endpoint).
# This is for local development and integration testing.

docker compose -f docker/docker-compose.opengamma.yml up
