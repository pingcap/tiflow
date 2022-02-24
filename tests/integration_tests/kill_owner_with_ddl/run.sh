#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=10

function check_capture_count() {
	pd=$1
	expected=$2
	count=$(cdc cli capture list --pd=$pd 2>&1 | jq '.|length')
	if [[ ! "$count" -eq "$expected" ]]; then
		echo "count: $count expected: $expected"
		exit 1
	fi
}

function kill_cdc_and_restart() {
	pd_addr=$1
	work_dir=$2
	cdc_binary=$3
	MAX_RETRIES=10
	status=$(curl -s http://127.0.0.1:8300/status)
	cdc_pid=$(echo "$status" | jq '.pid')

	kill $cdc_pid
	ensure $MAX_RETRIES check_capture_count $pd_addr 0
	run_cdc_server --workdir $work_dir --binary $cdc_binary --addr "127.0.0.1:8300" --pd $pd_addr
	ensure $MAX_RETRIES check_capture_count $pd_addr 1
}

export -f check_capture_count
export -f kill_cdc_and_restart

function run() {
	# kafka is not supported yet.
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI"
	run_sql "CREATE DATABASE kill_owner_with_ddl;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table kill_owner_with_ddl.t1 (id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "kill_owner_with_ddl.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/MySQLSinkExecDDLDelay=return(true);github.com/pingcap/tiflow/cdc/capture/ownerFlushIntervalInject=return(10)'
	kill_cdc_and_restart $pd_addr $WORK_DIR $CDC_BINARY

	for i in $(seq 2 3); do
		run_sql "CREATE table kill_owner_with_ddl.t$i (id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	for i in $(seq 1 3); do
		run_sql "INSERT INTO kill_owner_with_ddl.t$i VALUES (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# sleep to ensure processor has consumed the DDL and flushed checkpoint
	sleep 5

	for i in $(seq 1 3); do
		kill_cdc_and_restart $pd_addr $WORK_DIR $CDC_BINARY
		sleep 8
	done

	export GO_FAILPOINTS=''
	kill_cdc_and_restart $pd_addr $WORK_DIR $CDC_BINARY

	for i in $(seq 1 3); do
		check_table_exists "kill_owner_with_ddl.t$i" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
