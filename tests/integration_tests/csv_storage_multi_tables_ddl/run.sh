#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# Now, we run the storage tests in mysql sink tests.
	# It's a temporary solution, we will move it to a new test pipeline later.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME_1="ticdc-multi-tables-ddl-test-normal-$RANDOM"
	TOPIC_NAME_2="ticdc-multi-tables-ddl-test-error-1-$RANDOM"
	TOPIC_NAME_3="ticdc-multi-tables-ddl-test-error-2-$RANDOM"

	cf_normal="test-normal"
	cf_err1="test-error-1"
	cf_err2="test-error-2"

	SINK_URI1="file://$WORK_DIR/storage_test/$TOPIC_NAME_1?flush-interval=5s&protocol=csv"
	cdc cli changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI1" --config="$CUR/conf/normal.toml"

	SINK_URI2="file://$WORK_DIR/storage_test/$TOPIC_NAME_2?flush-interval=5s&protocol=csv"
	cdc cli changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI2" --config="$CUR/conf/error-1.toml"

	SINK_URI3="file://$WORK_DIR/storage_test/$TOPIC_NAME_3?flush-interval=5s&protocol=csv"
	cdc cli changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI3" --config="$CUR/conf/error-2.toml"
	run_storage_consumer $WORK_DIR $SINK_URI1 "$CUR/conf/normal.toml" 1
	run_storage_consumer $WORK_DIR $SINK_URI2 "$CUR/conf/error-1.toml" 2
	run_storage_consumer $WORK_DIR $SINK_URI3 "$CUR/conf/error-2.toml" 3

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t55 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t66 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t7 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t88 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists multi_tables_ddl_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "check table exists success"

	# changefeed test-error will not report an error, "multi_tables_ddl_test.t555 to multi_tables_ddl_test.t55" patr will be skipped.
	run_sql "rename table multi_tables_ddl_test.t7 to multi_tables_ddl_test.t77, multi_tables_ddl_test.t555 to multi_tables_ddl_test.t55;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_normal "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err1 "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err2 "failed" "ErrSyncRenameTableFailed" ""

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
