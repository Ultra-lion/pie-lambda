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
if [ ! -e "${LAMBDA_TASK_ROOT}/${LAMBDA_FUNC_NAME}" ]; then
  ln -s . "${LAMBDA_TASK_ROOT}/${LAMBDA_FUNC_NAME}" || echo "Warning: Could not create symlink"
fi

# 3. THE FIX: Resolve the handler
# Priority 1: Environment variables set in Dockerfile
# Priority 2: Positional argument passed via CMD
if [ -n "$MAIN_HANDLER_FILE_NAME" ] && [ -n "$LAMBDA_HANDLER_FUNC_NAME" ]; then
  echo "Poochie: Using handler from ENV variables..."
  MODULE_NAME="${MAIN_HANDLER_FILE_NAME%.*}"
  NEW_HANDLER="${LAMBDA_FUNC_NAME}.${MODULE_NAME}.${LAMBDA_HANDLER_FUNC_NAME}"
  # Consume the CMD argument if it was passed so it doesn't pollute $@
  [ $# -gt 0 ] && shift
elif [ $# -gt 0 ]; then
  echo "Poochie: Falling back to handler from CMD argument..."
  NEW_HANDLER="${LAMBDA_FUNC_NAME}.${1}"
  [ $# -gt 0 ] && shift
fi

# 4. Hand off to the actual Lambda Runtime Interface Client (RIC)
# "$@" will pass through any remaining arguments
PYTHON_BIN=$(command -v python3 || command -v python)
if [ -z "$PYTHON_BIN" ]; then
  echo "Error: Python not found in PATH"
  exit 1
fi

# exec "$PYTHON_BIN" -m awslambdaric "$NEW_HANDLER" "$@"
exec /lambda-entrypoint.sh "$NEW_HANDLER" "$@"