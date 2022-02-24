#!/bin/bash
set -e
GOBIN=$PWD go get github.com/maxshuang/mysql-tester/src@v0.0.2
mv src mysql_test
echo -e "build mysql_test successfully"
