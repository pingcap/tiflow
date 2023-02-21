#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="sync_collation"
db="sync_collation"
db_increment="sync_collation_increment"
db_server_collation="sync_collation_server"
db2="sync_collation2"
db_increment2="sync_collation_increment2"
db_server_collation2="sync_collation_server2"
tb="t1"
tb2="t2"
tb_check="t_check"

function run() {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	echo "prepare data"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml "--remove-meta"

	echo "check full phase"
	# wait
	run_sql_tidb_with_retry " select count(1) from information_schema.tables where TABLE_SCHEMA='${db}' and TABLE_NAME = '${tb}';" "count(1): 1"
	run_sql_tidb_with_retry " select count(1) from information_schema.tables where TABLE_SCHEMA='${db2}' and TABLE_NAME = '${tb}';" "count(1): 1"

	# check table
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb} where name = 'aa';" "count(1): 2"

	# check column
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb2} where name = 'aa';" "count(1): 2"

	# check database by create table
	run_sql_file $cur/data/tidb.checktable.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb_check} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db2}.${tb_check} where name = 'aa';" "count(1): 2"

	echo "prepare incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check incremental phase"
	# wait
	run_sql_tidb_with_retry " select count(1) from information_schema.tables where TABLE_SCHEMA='${db_increment}' and TABLE_NAME = '${tb}';" "count(1): 1"
	run_sql_tidb_with_retry " select count(1) from information_schema.tables where TABLE_SCHEMA='${db_increment2}' and TABLE_NAME = '${tb}';" "count(1): 1"

	# check table
	run_sql_tidb_with_retry "select count(1) from ${db_increment}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db_increment2}.${tb} where name = 'aa';" "count(1): 2"

	# check column
	run_sql_tidb_with_retry "select count(1) from ${db_increment}.${tb2} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db_increment2}.${tb2} where name = 'aa';" "count(1): 2"

	# check database by create table
	run_sql_file $cur/data/tidb.checktable.increment.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_tidb_with_retry "select count(1) from ${db_increment}.${tb_check} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db_increment2}.${tb_check} where name = 'aa';" "count(1): 2"

	# check set server collation
	run_sql_tidb_with_retry "select count(1) from ${db_server_collation}.${tb} where name = 'aa';" "count(1): 2"
	run_sql_tidb_with_retry "select count(1) from ${db_server_collation2}.${tb} where name = 'aa';" "count(1): 2"

	dmctl_stop_task $TASK_NAME $MASTER_PORT

	echo "prepare data for full phase error test"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD
	run_sql_file $cur/data/db1.prepare_err.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare_err.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta"

	echo "check full phase error"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status ${TASK_NAME}" \
		"Error 1273 (HY000): Unsupported collation when new collation is enabled: 'latin1_swedish_ci'" 1 \
		"Error 1273 (HY000): Unsupported collation when new collation is enabled: 'utf8mb4_0900_ai_ci'" 1

	dmctl_stop_task $TASK_NAME $MASTER_PORT

	echo "prepare data for incremental phase error test"
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/clean_data.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_file $cur/data/clean_data.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	dmctl_start_task $WORK_DIR/dm-task.yaml "--remove-meta"

	run_sql_file $cur/data/db1.increment_err.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment_err.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check incremental phase error"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status ${TASK_NAME}" \
		"Error 1273 (HY000): Unsupported collation when new collation is enabled: 'latin1_swedish_ci'" 1 \
		"Error 1273 (HY000): Unsupported collation when new collation is enabled: 'utf8mb4_0900_ai_ci'" 1
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
