#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

trap "cd $PWD" EXIT
cd $CUR
GO111MODULE=on go build gen.go && ./gen ../../pkg/terror/error_list.go && rm gen
GO111MODULE=on go run checker_generated.go
git diff --exit-code -- errors_release.txt
