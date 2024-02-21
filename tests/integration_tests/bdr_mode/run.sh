#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# BDR mode only supports mysql sink
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# cdc server 1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# cdc server 2
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8400" --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}" --logsuffix "down"
	# down
	SINK_URI_1="mysql://root@127.0.0.1:3306"
	# up
	SINK_URI_2="mysql://root@127.0.0.1:4000"

	# up -> down
	run_cdc_cli changefeed create --sink-uri="$SINK_URI_1" -c "test-1" --config="$CUR/conf/up.toml"
	# down -> up
	run_cdc_cli changefeed create --sink-uri="$SINK_URI_2" -c "test-2" --server "http://127.0.0.1:8400" --config="$CUR/conf/down.toml"

	run_sql_file $CUR/data/up.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/down.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql_file $CUR/data/finished.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# syncpoint table should exists in secondary tidb, but does not exists in primary cluster
	check_table_exists "tidb_cdc.syncpoint_v1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_not_exists "tidb_cdc.syncpoint_v1" ${UP_TIDB_HOST} ${UP_TIDB_PORT} 60

	check_table_exists "bdr_mode.finish_mark" ${UP_TIDB_HOST} ${UP_TIDB_PORT} 60

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
