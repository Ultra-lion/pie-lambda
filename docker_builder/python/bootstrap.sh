#!/bin/bash
# 1. Load the visible env vars from the staging folder
if [ -f .env ]; then
  echo "Poochie: Loading environment from .env file..."
  export $(grep -v '^#' .env | xargs)
fi

# 2. Hand off to the actual Lambda Runtime Interface Client (RIC)
# "$@" will pass through the handler you defined in CMD
PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "Error: Python not found in PATH"
  exit 1
fi

exec "$PYTHON_BIN" -m awslambdaric "$@"
