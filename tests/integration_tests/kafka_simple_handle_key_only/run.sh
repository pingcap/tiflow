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

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="simple-handle-key-only"
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	changefeed_id="simple-handle-key-only"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c ${changefeed_id} --config="$CUR/conf/changefeed.toml"
	run_sql_file $CUR/data/ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 5

	run_cdc_cli changefeed pause -c ${changefeed_id}

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&max-message-bytes=700"
	run_cdc_cli changefeed update -c ${changefeed_id} --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" --no-confirm
	run_cdc_cli changefeed resume -c ${changefeed_id}
	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false" --upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" --config="$CUR/conf/changefeed.toml" 2>&1 &

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
