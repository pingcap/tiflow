#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
DB_NAME=ddl_default_current_timestamp

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://normal:123456@127.0.0.1:3306/"

	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.t (id int primary key, v int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.t VALUES (1, 10), (2, 20), (3, 30);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "SET @@timestamp = 1000000000.123456; ALTER TABLE ${DB_NAME}.t ADD COLUMN c DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 120
	cleanup_process $CDC_BINARY
}

# Only MySQL sink applies the DDL directly with full column metadata.
if [ "$SINK_TYPE" == "mysql" ]; then
	trap stop_tidb_cluster EXIT
	run $*
	check_logs $WORK_DIR
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
