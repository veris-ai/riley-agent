#!/bin/bash
set -e

export PORT="${PORT:-8008}"
uv run --no-sync uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
