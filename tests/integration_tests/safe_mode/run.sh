#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_sql_file $CUR/data/create_table.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/create_table.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# insert data into upstream but not downstream
	run_sql_file $CUR/data/insert.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-changefeed-dup-error-restart-test-$RANDOM"
	case $SINK_TYPE in
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?safe_mode=true" ;;
	esac
	run_cdc_cli changefeed create --sink-uri="$SINK_URI"

	# test update sql can be split into delete + replace at all times in safe mode
	# otherwise the update sql will fail to execute on downstream cluster.
	sleep 10
	run_sql_file $CUR/data/update.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "CREATE TABLE safe_mode.finish_mark (a int primary key);"
	sleep 30
	check_table_exists "safe_mode.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
