#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
	run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE start_task.t$1(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "INSERT INTO start_task.t$1 VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function lazy_init_tracker() {
	run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "CREATE TABLE start_task.t$j(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql "INSERT INTO start_task.t$j VALUES (1,10001),(1,10011);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# only table 1-50 flush checkpoint
	for j in $(seq 50); do
		run_sql "INSERT INTO start_task.t$j VALUES (2,20002),(2,20022);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml"

	for j in $(seq 100); do
		run_sql "INSERT INTO start_task.t$j VALUES (3,30003),(3,30033);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql "INSERT INTO start_task.t$j VALUES (4,40004),(4,40044);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 20

	check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'lazy init table info.*t50' 1
	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log 'lazy init table info.*t51'

	cleanup_data start_task
	cleanup_process
}

function run() {
	lazy_init_tracker
	failpoints=(
		# 1152 is ErrAbortingConnection
		"github.com/pingcap/dm/pkg/utils/FetchTargetDoTablesFailed=return(1152)"
		"github.com/pingcap/dm/pkg/utils/FetchAllDoTablesFailed=return(1152)"
	)

	for ((i = 0; i < ${#failpoints[@]}; i++)); do
		WORK_DIR=$TEST_DIR/$TEST_NAME/$i

		echo "failpoint=${failpoints[i]}"
		export GO_FAILPOINTS=${failpoints[i]}

		# clear downstream env
		run_sql 'DROP DATABASE if exists dm_meta;' $TIDB_PORT $TIDB_PASSWORD
		run_sql 'DROP DATABASE if exists start_task;' $TIDB_PORT $TIDB_PASSWORD
		prepare_data $i

		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		# operate mysql config to worker
		cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
		sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
		dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

		echo "check un-accessible DM-worker exists"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status -s 127.0.0.1:8888" \
			"sources \[127.0.0.1:8888\] haven't been added" 1

		echo "start task and will failed"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": false" 1 \
			"ERROR" 1

		echo "reset go failpoints, and need restart dm-worker, then start task again"
		kill_dm_worker
		kill_dm_master

		export GO_FAILPOINTS=''
		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		sleep 5

		dmctl_start_task_standalone $task_conf

		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		cleanup_process
	done
}

cleanup_data start_task

cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
