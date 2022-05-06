#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	source_cfg=$1
	run_sql_file $cur/data/db.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/$source_cfg $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# start DM task only
	dmctl_start_task_standalone $cur/conf/dm-task.yaml
	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db.increment1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_tidb "show create table $TEST_NAME.t1;"
	check_not_contains "ignore_1"
	echo "increment1 check success"

	# a not ignored DDL to trigger a checkpoint flush
	run_sql_source1 "create table tracker_ignored_ddl.test (c int primary key);"

	# sleep 2 second, so the next insert will trigger check point flush since checkpoint-flush-interval=1
	sleep 2
	run_sql_file $cur/data/db.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Error 1054: Unknown column" 1

	# force a resume, the error is still there, but we want to check https://github.com/pingcap/tiflow/issues/5272#issuecomment-1109283279
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	# need operate tidb
	run_sql_tidb "alter table $TEST_NAME.t1 add column ignore_1 int;"

	# resume task
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 2
	sleep 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	run_sql_tidb "select count(1) from $TEST_NAME.t1;"
	check_contains "count(1): 2"
	echo "increment2 check success"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1" \
		"\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	dmctl_operate_source stop $WORK_DIR/source1.yaml $SOURCE_ID1
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process
run source1_gtid.yaml
run source1_pos.yaml
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
