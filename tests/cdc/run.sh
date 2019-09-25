#!/bin/sh

set -e

cd "$(dirname "$0")"

echo "Run cdc test"

GO111MODULE=on go run cdc.go -config ./config.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1
