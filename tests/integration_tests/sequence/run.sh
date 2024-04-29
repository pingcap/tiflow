#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# No need to test kafka and storage sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config $CUR/conf/force_replicate.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first

	check_table_exists sequence_test.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "check table exists success"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	# TiCDC discards all SEQUENCE DDL for now.
	# See https://github.com/pingcap/tiflow/issues/4559
	! run_sql "SHOW CREATE SEQUENCE sequence_test.seq0;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "DROP SEQUENCE sequence_test.seq0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Make sure changefeed is normal.
	run_sql "CREATE table sequence_test.mark_table(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "sequence_test.mark_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
# FIXME: re-enable the case later.
# run $*
# check_logs $WORK_DIR
# echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
echo "case $TEST_NAME is skipped"
