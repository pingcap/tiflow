#!/usr/bin/env bash
set -euo pipefail

# go mod tidy do not support symlink
cd -P .

cp go.sum /tmp/go.sum.before
GO111MODULE=on go mod tidy
diff -q go.sum /tmp/go.sum.before
