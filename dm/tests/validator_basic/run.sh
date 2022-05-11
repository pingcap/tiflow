#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

function prepare_for_standalone_test() {
	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name

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
	# note: key=1 has the same condition, but it's not validated, since it's migrated in full phase.
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"

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

	echo "--> check update pk(split into insert and delete)"
	run_sql_source1 "update $db_name.t1 set id=100 where id=7"
	run_sql_source1 "alter table $db_name.t1 comment 'a';" # force flush checkpoint
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 7\/1\/2\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 6\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 3"

	echo "--> check validator panic and we can catch it"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorPanic=panic("validator panic")'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator panic" 1

	echo "--> check validator worker panic and we can catch it"
	# panic 1 times
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ValidatorWorkerPanic=1*panic("validator worker panic")'
	prepare_for_standalone_test
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	sleep 5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator worker panic" 1

	# in real world panic, we cannot restart just like this, this case only makes sure worker panic doesn't
	# mess status of validator up
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 6\/1\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 1\/0\/0" 1
	run_sql "SELECT count(*) from $db_name.t1" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 6"
}

run_standalone $*
cleanup_process $*
cleanup_data $db_name
cleanup_data_upstream $db_name

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
