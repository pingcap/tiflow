#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	DEFAULT_TOPIC_NAME="multi_topics"
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/$DEFAULT_TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&kafka-version=${KAFKA_VERSION}"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config $CUR/conf/changefeed.toml

	run_sql_file $CUR/data/step1.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# NOTICE: we need to wait for the kafka topic to be created.
	sleep 2m

	for i in $(seq 1 3); do
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/test_table${i}?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true" "" ${i}
	done

	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/workload?protocol=canal-json&enable-tidb-extension=true" ""

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	for i in $(seq 1 3); do
		check_table_exists test.table${i} ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	run_sql "rename table test.table1 to test.table10, test.table2 to test.table20" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.table10 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists test.table20 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	run_sql_file $CUR/data/step2.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 30
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/test_table10?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true" "" 10
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/test_table20?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true" "" 20
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/test_finish?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true" "" "finish"

	check_table_exists test.finish ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
