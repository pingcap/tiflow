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

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE changefeed_dup_error_restart;"
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_dup_error_restart
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sinkv2/eventsink/txn/mysql/MySQLDuplicateEntryError=5%return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-changefeed-dup-error-restart-test-$RANDOM"
	case $SINK_TYPE in
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	run_sql "CREATE TABLE changefeed_dup_error_restart.finish_mark_1 (a int primary key);"
	sleep 30
	check_table_exists "changefeed_dup_error_restart.finish_mark_1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_dup_error_restart
	run_sql "CREATE TABLE changefeed_dup_error_restart.finish_mark_2 (a int primary key);"
	sleep 30
	check_table_exists "changefeed_dup_error_restart.finish_mark_2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
