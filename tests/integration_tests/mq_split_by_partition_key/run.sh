#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# use kafka-consumer with canal-json decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ] && [ "$SINK_TYPE" != "pulsar" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	TOPIC_NAME="ticdc-mq-split-by-partition-key"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	if [ "$SINK_TYPE" == "kafka" ]; then
		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&partition-num=6"
	fi

	if [ "$SINK_TYPE" == "pulsar" ]; then
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&partition-num=6"
	fi

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	changefeed_id="changefeed-kafka"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$CUR/conf/$changefeed_id.toml -c "$changefeed_id"
	sleep 5 # wait for changefeed to start
	# determine the sink uri and run corresponding consumer
	# currently only kafka and pulsar are supported
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR $SINK_URI
	fi

	if [ "$SINK_TYPE" == "pulsar" ]; then
		run_pulsar_consumer --upstream-uri $SINK_URI
	fi

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
