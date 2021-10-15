#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	export GO_FAILPOINTS="github.com/pingcap/dm/syncer/checkCheckpointInMiddleOfTransaction=return"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 1 row affected'

	# run dm master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	# bound source1 to worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	# start a task in all mode
	dmctl_start_task_standalone $cur/conf/dm-task.yaml

	# check diff
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# wait transaction start
	# you can see why sleep in https://github.com/pingcap/dm/pull/1928#issuecomment-895820239
	sleep 2
	echo "pause task and check status"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 1
	# check the point is the middle of checkpoint
	num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	[[ $num -gt 0 ]]
	sed -e '/not receive xid job yet/d' $WORK_DIR/worker1/log/dm-worker.log >$WORK_DIR/worker1/log/dm-worker.log

	echo "start check pause diff"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "resume task and check status"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# wait transaction start
	# you can see why sleep in https://github.com/pingcap/dm/pull/1928#issuecomment-895820239
	sleep 2
	echo "stop task"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	# check the point is the middle of checkpoint
	num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	[[ $num -gt 0 ]]

	echo "start check stop diff"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	export GO_FAILPOINTS=""
}

cleanup_data checkpoint_transaction
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
