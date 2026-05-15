#!/bin/bash
# 1. Load the visible env vars from the staging folder
if [ -f .env ]; then
  echo "Poochie: Loading environment from .env file..."
  export $(grep -v '^#' .env | xargs)
fi

# 2. Hand off to the actual Lambda Runtime Interface Client (RIC)
# "$@" will pass through the handler you defined in CMD
exec /usr/bin/python3 -m awslambdaric "$@"
