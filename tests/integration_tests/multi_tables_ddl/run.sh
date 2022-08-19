#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-multi-tables-ddl-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	cf_normal="test-normal"
	cf_err1="test-error-1"
	cf_err2="test-error-2"

	cdc cli changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/normal.toml"
	cdc cli changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-1.toml"
	cdc cli changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-2.toml"

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t55 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t66 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t7 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t88 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists multi_tables_ddl_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "check table exists success"

	# changefeed test-error will report an error
	run_sql "rename table t7 to t77, t88 to t99;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_normal "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err1 "error" "ErrSyncRenameTablesFailed" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err2 "error" "ErrSyncRenameTableFailed" ""

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
