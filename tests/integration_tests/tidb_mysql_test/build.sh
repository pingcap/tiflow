#!/bin/bash
set -e
GOBIN=$PWD go get github.com/pingcap/mysql-tester/src@0ecb03c9cf5c0e19318bc5d0a38bcc59c8aab56d
mv src mysql_test
echo -e "build mysql_test successfully"
