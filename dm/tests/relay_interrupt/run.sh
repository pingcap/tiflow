#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data1() {
	run_sql 'DROP DATABASE if exists relay_interrupt;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE relay_interrupt;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE relay_interrupt.t$1(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "INSERT INTO relay_interrupt.t$1 VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function prepare_data2() {
	run_sql "DELETE FROM relay_interrupt.t$1 limit 1;" $MYSQL_PORT1 $MYSQL_PASSWORD1
}

function run() {
	failpoints=(
		# 1152 is ErrAbortingConnection
		"github.com/pingcap/tiflow/dm/pkg/utils/GetGlobalVariableFailed=return(\"server_uuid,1152\")"
		"github.com/pingcap/tiflow/dm/pkg/utils/GetSessionVariableFailed=return(\"sql_mode,1152\")"
	)

	for ((i = 0; i < ${#failpoints[@]}; i++)); do
		WORK_DIR=$TEST_DIR/$TEST_NAME/$i

		echo "failpoint=${failpoints[i]}"
		export GO_FAILPOINTS=${failpoints[i]}

		# clear downstream env
		run_sql 'DROP DATABASE if exists dm_meta;' $TIDB_PORT $TIDB_PASSWORD
		run_sql 'DROP DATABASE if exists relay_interrupt;' $TIDB_PORT $TIDB_PASSWORD
		prepare_data1 $i

		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		# operate mysql config to worker
		cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
		sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
		dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

		echo "query status, relay log failed"
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status -s $SOURCE_ID1" \
			"no sub task started" 1 \
			"ERROR" 1

		echo "start task and query status, task and relay have error message"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": false" 1 \
			"\"source\": \"$SOURCE_ID1\"" 1

		echo "waiting for asynchronous relay and subtask to be started"
		sleep 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status -s $SOURCE_ID1" \
			"database driver error" 1 \
			"ERROR 1152" 1
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status -s $SOURCE_ID1" \
			"there aren't any data under relay log directory" 1 \
			"ERROR" 1
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status" \
			"\"taskName\": \"test\"" 1 \
			"\"taskStatus\": \"Error - Some error occurred in subtask. Please run \`query-status test\` to get more details.\"" 1

		echo "reset go failpoints, and need restart dm-worker"
		echo "then resume task, task will recover success"
		kill_dm_worker
		export GO_FAILPOINTS=''

		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

		sleep 8
		echo "start task after restarted dm-worker"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": false" 1 \
			"subtasks with name test for sources \[mysql-replica-01\] already exist" 1
		# wait relay unit up
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"binlogType\": \"local\"" 1

		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		echo "read binlog from relay log failed, and will use remote binlog"
		kill_dm_worker
		export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/relay/GetEventFromLocalFailed=return()"
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		prepare_data2 $i
		sleep 8
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"binlogType\": \"remote\"" 1

		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		export GO_FAILPOINTS=''
		cleanup_process
	done
}

cleanup_data relay_interrupt
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
