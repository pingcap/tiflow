#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

# use kafka-consumer with canal-json decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	TOPIC_NAME="dispatcher-test"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "info"

	changefeed_id="test"

	# create a table before get pd tso to reproduce https://github.com/pingcap/tiflow/issues/10707
	run_sql "DROP DATABASE if exists verify;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE verify;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE verify.t (a int primary key, b int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c ${changefeed_id} --config="$CUR/conf/changefeed.toml"

	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	run_sql "DROP DATABASE if exists dispatcher;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE dispatcher;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE dispatcher.index (a int primary key, b int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "INSERT INTO dispatcher.index values (1, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "failed" "ErrDispatcherFailed"

	run_cdc_cli changefeed update -c ${changefeed_id} --sink-uri="$SINK_URI" --config="$CUR/conf/new_changefeed.toml" --no-confirm

	run_cdc_cli changefeed resume -c ${changefeed_id}

	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false" --upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" --config="$CUR/conf/new_changefeed.toml" 2>&1 &

	run_sql "INSERT INTO dispatcher.index values (2, 3);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO dispatcher.index values (3, 4);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO dispatcher.index values (4, 5);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE dispatcher.index set b = 5 where a = 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE dispatcher.index set b = 6 where a = 2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DELETE FROM dispatcher.index where a = 3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "CREATE TABLE test.finish_mark (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
