#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_rename_before_watermark() {
	local log_file=$1
	local topic=$2
	local attempt ddl_line ddl_ts ddl_offset line watermark offset found

	for attempt in $(seq 1 120); do
		ddl_line=$(grep -m1 'DDL message received.*RENAME TABLE' "$log_file" 2>/dev/null || true)
		if [ -z "$ddl_line" ]; then
			sleep 1
			continue
		fi

		[[ $ddl_line =~ \[commitTs=([0-9]+)\] ]]
		ddl_ts=${BASH_REMATCH[1]}
		[[ $ddl_line =~ \[offset=([0-9]+)\] ]]
		ddl_offset=${BASH_REMATCH[1]}
		found=0
		while IFS= read -r line; do
			[[ $line == *'"watermark received"'* && $line == *'[partition=0]'* ]] || continue
			[[ $line =~ \[watermark=([0-9]+)\] ]] || continue
			watermark=${BASH_REMATCH[1]}
			[[ $line =~ \[offset=([0-9]+)\] ]] || continue
			offset=${BASH_REMATCH[1]}
			if ((watermark >= ddl_ts)); then
				if ((offset <= ddl_offset)); then
					echo "$topic: watermark $watermark at offset $offset precedes RENAME at offset $ddl_offset (commitTs $ddl_ts)"
					return 1
				fi
				found=1
			fi
		done <"$log_file"
		if ((found)); then
			echo "$topic: RENAME at offset $ddl_offset precedes covering watermark"
			return 0
		fi
		sleep 1
	done

	echo "$topic: no partition-0 watermark covering RENAME commitTs in $log_file"
	return 1
}

function run() {
	# test kafka sink only in this case
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
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/test_table${i}?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true" "" "" ${i}
	done

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	for i in $(seq 1 3); do
		check_table_exists test.table${i} ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	run_sql "rename table test.table1 to test.table10, test.table2 to test.table20" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in 1 2; do
		check_rename_before_watermark "$WORK_DIR/cdc_kafka_consumer${i}.log" "test_table${i}"
	done
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
