#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# use kafka-consumer with canal-json decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# upstream tidb cluster enable row level checksum
	run_sql "set global tidb_enable_row_level_checksum=true" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME="ticdc-simple-basic"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	if [ "$SINK_TYPE" == "kafka" ]; then
		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple"
	fi

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" -c "simple-basic"
	sleep 5 # wait for changefeed to start
	# determine the sink uri and run corresponding consumer
	# currently only kafka and pulsar are supported
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR $SINK_URI
	fi

	# pre execute some ddls
	run_sql_file $CUR/data/pre_ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark_for_ddl ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200

	# pause and resume changefeed makse sure changefeed sneding bootstrap events
	# when it is resumed, so the data after pause can be decoded correctly
	run_cdc_cli changefeed pause -c "simple-basic"
	run_cdc_cli changefeed resume -c "simple-basic"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
