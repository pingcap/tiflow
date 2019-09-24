#!/bin/sh

set -e

cd "$(dirname "$0")"

echo "Run binlog test"

GO111MODULE=on go build -o out

./out -config ./config.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1


