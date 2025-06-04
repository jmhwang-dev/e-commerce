#!/bin/bash

# Helper to load environment variables from a .env file located one directory
# above the calling script. The script's directory should be passed as the first
# argument.
load_env() {
  local caller_dir="$1"
  local env_file="$caller_dir/../.env"
  if [ -f "$env_file" ]; then
    set -a
    source "$env_file"
    set +a
  fi
}
