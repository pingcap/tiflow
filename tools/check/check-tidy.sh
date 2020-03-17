#!/usr/bin/env bash
set -euo pipefail

GO111MODULE=on go mod tidy

if ! git diff-index --quiet HEAD --; then
  echo "Please run \`go mod tidy\` to clean up"
  git status
  git --no-pager diff
  exit 1
fi
