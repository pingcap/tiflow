## Description
Migrate from TiDB mysql-test, tracking issue: [#4674](https://github.com/pingcap/tiflow/issues/4674)

## Run Routine
1. Get the entire [tidb-test](https://github.com/pingcap/tidb-test) repository by submodule
2. Setup upstream and downstream cluster.
3. Copy converter.sh script to `mysql_test` dir.
4. Convert the oriented test cases to achieve e2e test and verification.
5. Copy build.sh script to `mysql_test` dir.
5. Run and build [mysql_test](https://github.com/pingcap/mysql-tester) binary
6. Using mysql_test binary to Run all oriented cases
7. Check if data consistent for upstream and downstream cluster

## How to Migate and Test a new test case locally
1. git clone --recursive git@github.com:pingcap/tiflow.git  
2. comment the `trap stop_tidb_cluster EXIT` in `tests/integration_tests/tidb_mysql_test/run.sh` to avoid tidb cluster exit
```
# mysql test may suffer from duplicate DML for no pk/uk
# trap stop_tidb_cluster EXIT
```
3. run the `CASE="tidb_mysql_test" docker-compose -f ./deployments/ticdc/docker-compose/docker-compose-mysql-integration.yml up --build` to run the 
tidb_mysql_test in docker(only for the PingCAP intranet, [learn more](https://github.com/pingcap/tiflow/blob/master/tests/integration_tests/README.md))
4. after the original test finish or fail, we find out the container and login in to do some debug
```
sudo docker ps -a
sudo docker exec -it $integration_test_container_id /bin/bash
```
5. in container, replace the `cases` variables with your new case in `tests/integration_tests/tidb_mysql_test/run.sh`
6. in container, 
```
cd tests/integration_tests
./run.sh mysql tidb_mysql_test (restart the tidb_mysql_test)
```
