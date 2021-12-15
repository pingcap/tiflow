#!/usr/bin/env bash
set -euo pipefail

GO111MODULE=on go mod tidy

if [ "$(git --no-pager diff go.mod go.sum | wc -c)" -ne 0 ]; then
	echo "Please run \`go mod tidy\` to clean up"
	git --no-pager diff go.mod go.sum
	exit 1
fi
