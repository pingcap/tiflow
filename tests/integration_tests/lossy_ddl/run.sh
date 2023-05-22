#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=10

# Because we want the lossy DDL to not cause any data updates, so we can check the
# data in the blackhole sink to see if any row is updated.
function check_lossy_ddl() {
	# Check finish_mark is written to the log.
	is_finish_mark_exist=$(grep "BlackHoleSink: DDL Event" "$1/cdc.log" | grep -c "finish_mark")
	if [[ "$is_finish_mark_exist" -ne 1 ]]; then
		echo "can't found finish mark"
		exit 1
	fi

	row_logs=$(grep "BlackHoleSink: WriteEvents" "$1/cdc.log")
	echo $row_logs
	row_logs_count=$(grep "BlackHoleSink: WriteEvents" -c "$1/cdc.log")
	if [[ "$row_logs_count" -ne 22 ]]; then
		echo "can't found 22 row logs, got $row_logs_count"
		exit 1
	fi
}

export -f check_lossy_ddl

function run() {
	# Use blackhole sink to check if the DDL is lossy.
	# So no need to run this test for other sinks.
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="blackhole://"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI"

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	ensure $MAX_RETRIES check_lossy_ddl $WORK_DIR
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
