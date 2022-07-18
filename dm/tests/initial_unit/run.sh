#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
	run_sql 'DROP DATABASE if exists initial_unit;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE initial_unit;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE initial_unit.t$1(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "INSERT INTO initial_unit.t$1 VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function run() {
	failpoints=(
		# 1152 is ErrAbortingConnection
		"github.com/pingcap/tiflow/dm/syncer/LoadCheckpointFailed=return(1152)"
		"github.com/pingcap/tiflow/dm/syncer/dbconn/GetMasterStatusFailed=return(1152)"
	)

	for ((i = 0; i < ${#failpoints[@]}; i++)); do
		WORK_DIR=$TEST_DIR/$TEST_NAME/$i

		# clear downstream env
		run_sql 'DROP DATABASE if exists dm_meta;' $TIDB_PORT $TIDB_PASSWORD
		run_sql 'DROP DATABASE if exists initial_unit;' $TIDB_PORT $TIDB_PASSWORD
		prepare_data $i

		echo "failpoint=${failpoints[i]}"
		export GO_FAILPOINTS=${failpoints[i]}

		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		# operate mysql config to worker
		cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
		sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
		dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

		echo "start task and query status, the sync unit will initial failed"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": true" 1 \
			"\"source\": \"$SOURCE_ID1\"" 1 \
			"fail to initialize unit Sync of subtask test" 1

		sleep 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"stage\": \"Paused\"" 1 \
			"\"unit\": \"InvalidUnit\"" 1 \
			"fail to initialize unit Sync of subtask test" 1

		echo "resume task will also initial failed"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task test" \
			"\"result\": true" 1 \
			"\"source\": \"$SOURCE_ID1\"" 1 \
			"fail to initialize unit Sync of subtask test" 1

		sleep 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"stage\": \"Paused\"" 1 \
			"\"unit\": \"InvalidUnit\"" 1 \
			"fail to initialize unit Sync of subtask test" 1

		echo "reset go failpoints, and need restart dm-worker"
		echo "then resume task, task will recover success"
		export GO_FAILPOINTS=''

		kill_dm_worker

		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		sleep 10

		echo "start task after restarted dm-worker, the sync unit will initial success"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": false" 1 \
			"subtasks with name test for sources \[mysql-replica-01\] already exist" 1
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		# resume-task takes no effect
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task test" \
			"\"result\": true" 2 \
			"\"source\": \"$SOURCE_ID1\"" 1

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"stage\": \"Running\"" 2

		cleanup_process
	done
}

cleanup_data initial_unit
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
