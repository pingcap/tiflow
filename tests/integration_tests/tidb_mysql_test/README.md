## Description
Migrate from TiDB mysql-test, tracking issue: [#4674](https://github.com/pingcap/tiflow/issues/4674)

## Run Routine
1. Setup upstream and downstream cluster.
2. Get [tidb-mysql-test](https://github.com/pingcap/tidb-test/tree/master/mysql_test) by git. 
3. Copy converter.sh script to mysql_test dir.
4. Convert the oriented test cases to achieve e2e test and verification.
5. Build [mysql_test](https://github.com/pingcap/mysql-tester) binary
6. Run all oriented cases
7. Check if data consistent for upstream and downstream cluster

## How to Migate and Test a new test case locally
1. Get repository [tidb-mysql-test](https://github.com/pingcap/tidb-test/tree/master/mysql_test) locally.
2. Copy converter.sh script to mysql_test dir.
3. Cd mysql_test and run `converter.sh $test_case`($test_case is the case you want to migrate.
4. Set up a tidb to test locally by tiup(tiup playground v5.4.0 --db 1 --kv 3 --pd 1 --without-monitor).
5. run `./mysql_test $test_case` to verify if the test case can run correctly
6. run the whole tidb_mysql_test in docker, `CASE="tidb_mysql_test" docker-compose -f ./deployments/ticdc/docker-compose/docker-compose-mysql-integration.yml up --build`

ps:
for step 5, if mysql_test fail, you may need to adjust the converter.sh or change some TiDB parameters.
for step 5, if your tidb port is not 4000, use `./mysql_test --host=xxx --port=xxx $test_case` to specify the host and port.
for step 6, if you want to adjust the script and test again, just reuse the same container and debug in that,
 by `docker exec -it $container_id /bin/bash`
