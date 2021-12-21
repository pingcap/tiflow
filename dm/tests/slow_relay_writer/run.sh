#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME
TABLE_NUM=10

function prepare_data() {
	run_sql 'DROP DATABASE if exists slow_relay_writer;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE slow_relay_writer;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	for i in $(seq $TABLE_NUM); do
		run_sql "CREATE TABLE slow_relay_writer.t$i(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function incremental_data() {
	for j in $(seq 2); do
		for i in $(seq $TABLE_NUM); do
			run_sql "BEGIN;INSERT INTO slow_relay_writer.t$i VALUES ($j,${j}000$j);INSERT INTO slow_relay_writer.t$i VALUES ($j,${j}001$j);COMMIT;" $MYSQL_PORT1 $MYSQL_PASSWORD1
		done
	done
}

function run() {
	prepare_data

	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/relay/SlowDownWriteDMLRelayLog=10*return();github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval=return(5)'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	dmctl_start_task_standalone
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "start incremental_data"
	incremental_data
	echo "finish incremental_data"

	# we injected 10*1s by SlowDownWriteDMLRelayLog, so sleep a longer time
	sleep 10

	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "enter SlowDownWriteDMLRelayLog"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	export GO_FAILPOINTS=''
}

cleanup_data slow_relay_writer
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
