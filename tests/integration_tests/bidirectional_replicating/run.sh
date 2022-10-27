#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test case is used to test the bidirectional replication of TiCDC.
# The upstream and downstream are both TiDB clusters.
function run() {
	# Do not support Kafka sink.
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/prepare.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# deploy a cdc cluster that replicates data from downstream to upstream
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8400" --pd "${DOWN_PD_HOST}:${DOWN_PD_PORT}"

	# down tidb
	SINK_URI_DOWN="mysql://normal:123456@127.0.0.1:3306/"
	# up tidb
	SINK_URI_UP="mysql://normal:123456@127.0.0.1:4000/"

	# This changefeed replicates data from upstream to downstream.
	run_cdc_cli changefeed create --sink-uri="$SINK_URI_DOWN" --config="$CUR/conf/cf.toml"
	# This changefeed replicates data from downstream to upstream.
	run_cdc_cli changefeed create --sink-uri="$SINK_URI_UP" --config="$CUR/conf/cf.toml" --server="http://127.0.0.1:8400"

	# write data to upstream
	run_sql_file $CUR/data/test_up.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# write data to downstream
	run_sql_file $CUR/data/test_down.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists batch_add_table.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists batch_add_table.finish_mark ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# check data
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
