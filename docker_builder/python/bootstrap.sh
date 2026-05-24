#!/bin/bash

# 1. Load the visible env vars from the staging folder
if [ -f .env ]; then
  echo "Poochie: Loading environment from .env file..."
  export $(grep -v '^#' .env | xargs)
fi

# 2. THE FIX: Force /var/task to be a package to support relative imports
if [ ! -f /var/task/__init__.py ]; then
  touch /var/task/__init__.py
fi
export PYTHONPATH=$PYTHONPATH:/var

export AWS_LAMBDA_RUNTIME_API=127.0.0.1:8080

# 3. THE FIX: Prepend 'task.' to the handler argument
# This allows relative imports like 'from .module' to find the 'task' parent
ORIGINAL_HANDLER=$1
shift # remove original $1 from args
NEW_HANDLER="task.$ORIGINAL_HANDLER"

# 4. Hand off to the actual Lambda Runtime Interface Client (RIC)
# "$@" will pass through any remaining arguments
PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "Error: Python not found in PATH"
  exit 1
fi

exec "$PYTHON_BIN" -m awslambdaric "$NEW_HANDLER" "$@"
