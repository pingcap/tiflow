#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

function prepare_for_standalone_test() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	cp $cur/conf/dm-task-standalone.yaml $WORK_DIR/dm-task.yaml
	dmctl_start_task_standalone $WORK_DIR/dm-task.yaml --remove-meta

	# sync-diff seems cannot handle float/double well, will skip it here
}

function run_standalone() {
	echo "--> normal case, check we validate different data types"
	prepare_for_standalone_test

	# key=6, mysql store as 1.2345679e+17, but in tidb it's 1.23457e17
	# so will fail in current compare rule
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"

	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name

	echo "--> check we can catch inconsistent rows"
	# skip incremental rows with id <= 5
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(5)'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 6\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 3"
}

cleanup_data $db_name
cleanup_process $*
run_standalone $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
