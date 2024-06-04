#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# function run_consumer() {
# 	SINK_URI=$1
# 	CONFIG_FILE=$2

# 	# run_sql "DROP DATABASE IF EXISTS test;create database `test`;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
# 	run_storage_consumer $WORK_DIR $SINK_URI1 $CUR/conf/changefeed1.toml ""
# 	sleep 8
# 	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100
# }

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI1="file://$WORK_DIR/storage_test/changefeed1?flush-interval=5s"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI1" --config=$CUR/conf/changefeed1.toml -c "changefeed1"
	SINK_URI2="file://$WORK_DIR/storage_test/changefeed2?flush-interval=5s"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI2" --config=$CUR/conf/changefeed2.toml -c "changefeed2"

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 10
	run_sql_file $CUR/data/run.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_storage_consumer $WORK_DIR $SINK_URI1 $CUR/conf/changefeed1.toml ""
	sleep 8
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	run_storage_consumer $WORK_DIR $SINK_URI2 $CUR/conf/changefeed2.toml ""
	sleep 8
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
