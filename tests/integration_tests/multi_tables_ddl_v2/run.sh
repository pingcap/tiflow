#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# test kafka sink only in this case
	if [ "$SINK_TYPE" != "kafka" ]; then
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

	case $SINK_TYPE in
	*) ;;
	esac

	cf_normal="test-normal"
	cf_err1="test-error-1"
	cf_err2="test-error-2"

	if [ "$SINK_TYPE" == "kafka" ]; then
		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_1?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc cli changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/normal.toml"

		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_2?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc cli changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-1.toml"

		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_3?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc cli changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-2.toml"

		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_1?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_2?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_3?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	else
		SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
		cdc cli changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/normal.toml"
		cdc cli changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-1.toml"
		cdc cli changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-2.toml"
	fi

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

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
