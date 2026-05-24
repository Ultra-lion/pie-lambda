#!/bin/bash

# 1. Load the visible env vars from the staging folder
if [ -f .env ]; then
  echo "Poochie: Loading environment from .env file..."
  export $(grep -v '^#' .env | xargs)
fi

# 2. THE FIX: Force the task directory to be a package named after the function
# This supports BOTH absolute imports (from func_name.utils) and relative imports (.utils)
if [ ! -f /var/task/__init__.py ]; then
  touch /var/task/__init__.py
fi

# Create a symlink so python can find the package by its real name
if [ ! -L "/var/${LAMBDA_FUNC_NAME}" ]; then
  ln -s /var/task "/var/${LAMBDA_FUNC_NAME}"
fi

export PYTHONPATH=$PYTHONPATH:/var

# 3. THE FIX: Redirect the handler to the new package path
ORIGINAL_HANDLER=$1
shift 
NEW_HANDLER="${LAMBDA_FUNC_NAME}.${ORIGINAL_HANDLER}"

# 4. Hand off to the actual Lambda Runtime Interface Client (RIC)
# "$@" will pass through any remaining arguments
PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "Error: Python not found in PATH"
  exit 1
fi

exec "$PYTHON_BIN" -m awslambdaric "$NEW_HANDLER" "$@"
