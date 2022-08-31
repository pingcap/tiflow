#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function test_compression() {
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	TOPIC_NAME="ticdc-kafka-compression-$1-test"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&kafka-version=${KAFKA_VERSION}&compression=$1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $1
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true"
	run_sql_file $CUR/data/$1_data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	compression_algorithm=$(grep "Kafka producer uses $1 compression algorithm" "$WORK_DIR/cdc.log")
	if [[ "$compression_algorithm" -ne 1 ]]; then
		echo "can't found producer compression algorithm"
		exit 1
	fi
	check_table_exists test.$1_finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_cdc_cli changefeed pause -c $1
	run_cdc_cli changefeed remove -c $1
}

function run() {
	if [ "$SINK_TYPE" == "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	test_compression gzip
	test_compression snappy
	test_compression lz4
	test_compression zstd

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
