## Reference
https://github.com/pingcap/tidb-test/tree/master/mysql_test
https://github.com/pingcap/mysql-tester

## Issue
https://github.com/pingcap/tiflow/issues/4674

## Description
Migrate from TiDB mysql-test

## Step
1. Setup upstream and downstream cluster
2. build mysql_test
3. run all cases
4. check data

## Add New Test
1. Add test and result file
2. ./build.sh
3. ./mysql_test or ./mysql_test xxx to check if test case works well locally (default host: 127.0.0.1:4000)
