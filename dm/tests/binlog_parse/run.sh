#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

# skip one tale, sync another table
# mariadb10.0 timestamp(3) will panic before dm v6.4.0
function run() {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	echo "prepare data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "start task"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml --remove-meta"

	echo "check full phase"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1

	echo "prepare incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "check incremental phase"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30

	run_sql_tidb_with_retry "select count(1) from binlog_parse.t1;" "count(1): 4"

	# relay error in mariadb:10.0, success in mysql
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"worker\": \"worker1\"" 1
	# "TCPReader get relay event with error" 1

	echo "prepare incremental data 2"
	run_sql_file $cur/data/db1.increment1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1

	echo "check incremental phase 2"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
