#!/bin/bash

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare

# make sure all test cases use database `test`
echo -e "mysql_test start\n"
TEST_BIN_PATH=./mysql_test
set -eu
./build.sh
echo "run all mysql test cases"
"$TEST_BIN_PATH" --host=${UP_TIDB_HOST} --port=${UP_TIDB_PORT} --log-level=error --all=true --reserve-schema=true

echo "mysqltest end"
