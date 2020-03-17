#!/usr/bin/env bash
set -euo pipefail

GO111MODULE=on go mod tidy

if [ `git --no-pager diff | wc -c` -ne 0 ]; then
  echo "Please run \`go mod tidy\` to clean up"
  git diff-index HEAD --
  git status
  git --no-pager diff
  exit 1
fi
