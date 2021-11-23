#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# TODO: remove after kafka-consumer/main.go is fixed
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE move_table;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=move_table
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "debug" --logsuffix "1" --addr 127.0.0.1:8300

	TOPIC_NAME="ticdc-move-table-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "debug" --logsuffix "2" --addr 127.0.0.1:8301
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "debug" --logsuffix "3" --addr 127.0.0.1:8302

	# Add a check table to reduce check time, or if we check data with sync diff
	# directly, there maybe a lot of diff data at first because of the incremental scan
	run_sql "CREATE table move_table.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "move_table.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cd $CUR
	GO111MODULE=on go run main.go 2>&1 | tee $WORK_DIR/tester.log &
	cd $WORK_DIR

	check_table_exists "move_table.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_sql "truncate table move_table.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_sql "CREATE table move_table.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "move_table.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
