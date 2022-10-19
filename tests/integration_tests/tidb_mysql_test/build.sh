#!/bin/bash
set -e
# we use specify version to support `--reserve-schema` option
GOBIN=$PWD go install github.com/pingcap/mysql-tester/src@295e8dfd61f967b4eefcf07a2c0d2878ae28da99
mv src mysql_test
echo -e "build mysql_test successfully"
