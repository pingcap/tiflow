#!/bin/bash
set -e
# We use this repository temporaryly since mysql-tester will delete schema after every case
# ref: https://github.com/pingcap/mysql-tester/blob/0ecb03c9cf5c0e19318bc5d0a38bcc59c8aab56d/src/main.go#L245
# expected pr: https://github.com/pingcap/mysql-tester/pull/55
GOBIN=$PWD go install github.com/maxshuang/mysql-tester/src@v0.0.2
mv src mysql_test
echo -e "build mysql_test successfully"
