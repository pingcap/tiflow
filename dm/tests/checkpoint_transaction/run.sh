#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function check_worker_ungraceful_stop_with_retry() {
	for ((k = 0; k < 10; k++)); do
		sleep 1
		echo "start check_worker_ungraceful_stop_with_retry times: $k"

		num=$(grep "kill unit" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
		if [ $num -lt 1 ]; then
			continue
		fi
		num=$(grep "kill syncer without graceful" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
		if [ $num -lt 1 ]; then
			continue
		fi
		num=$(grep "received ungraceful exit ctx, exit now" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
		if [ $num -lt 1 ]; then
			continue
		fi
		echo "check_worker_ungraceful_stop_with_retry success after retry: $k"
		return 0
	done

	echo "check_worker_ungraceful_stop_with_retry failed after retry"
	exit 1
}

function run() {
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/checkCheckpointInMiddleOfTransaction=return"

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

	# test ungraceful stop, worker will not wait transaction finish
	run_sql_file $cur/data/db1.increment1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	sleep 2
	# kill dm-master 1 to make worker lost keep alive while a transaction is not finished
	echo "kill dm-master1"
	kill_dm_master
	check_master_port_offline 1
	sleep 1 # wait worker lost keep alive ttl is 1 second

	# check dm-worker will exit quickly without waiting for the transaction to finish
	check_worker_ungraceful_stop_with_retry

	# test data in tidb less than source
	dataCountSource=$(mysql -uroot -h$MYSQL_HOST1 -P$MYSQL_PORT1 -p$MYSQL_PASSWORD1 -se "select count(1) from checkpoint_transaction.t1")
	dataCountInTiDB=$(mysql -uroot -h127.0.0.1 -P4000 -se "select count(1) from checkpoint_transaction.t1")
	echo "after ungraceful exit data in source count: $dataCountSource data in tidb count: $dataCountInTiDB"
	if [ "$dataCountInTiDB" -lt "$dataCountSource" ]; then
		echo "ungraceful stop test success"
	else
		echo "ungraceful stop test failed"
		exit 1
	fi

	# start dm-master again task will be resume, and data will be synced
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	sleep 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_file $cur/data/db1.increment1.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# wait transaction start
	check_log_contain_with_retry "\[32,30,null\]" $WORK_DIR/worker1/log/dm-worker.log
	echo "pause task and check status"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 1
	# check the point is the middle of checkpoint
	num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)

	if [ "$num" -gt 0 ]; then
		echo "graceful pause test success"
	else
		echo "graceful pause test failed"
		exit 1
	fi

	echo "start check pause diff"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "resume task and check status"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	echo "kill dm-worker1"
	kill_process dm-worker1
	check_port_offline $WORKER1_PORT 20
	rm -rf $WORK_DIR/worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 1

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# wait transaction start
	check_log_contain_with_retry "\[62,null,30\]" $WORK_DIR/worker1/log/dm-worker.log
	echo "stop task"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	# check the point is the middle of checkpoint
	num=$(grep "not receive xid job yet" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [ "$num" -gt 0 ]; then
		echo "graceful stop test success"
	else
		echo "graceful stop test failed"
		exit 1
	fi

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
