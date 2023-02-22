#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function reset_and_prepare_data() {
	run_sql "DROP DATABASE IF EXISTS sorter;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DROP DATABASE IF EXISTS sorter;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "CREATE DATABASE IF NOT EXISTS sorter;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=sorter
}

function run_test() {
	# Add a check table to reduce check time, or if we check data with sync diff
	# directly, there maybe a lot of diff data at first because of the incremental scan
	run_sql "CREATE table sorter.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "sorter.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "sorter.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql "truncate table sorter.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_sql "CREATE table sorter.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "sorter.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=sorter
	run_sql "CREATE table sorter.check3(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "sorter.check3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql "create table sorter.usertable2 like sorter.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into sorter.usertable2 select * from sorter.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table sorter.check4(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "sorter.usertable2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "sorter.check4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# FIXME: pull-based sink does not support unified sorter.
	#	echo "test unified sorter"
	#
	#	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	#	reset_and_prepare_data
	#
	#	# Run cdc server with unified sorter.
	#	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --config $CUR/conf/ticdc_unified_sorter.toml
	#
	#	TOPIC_NAME="ticdc-unified-sorter-test-$RANDOM"
	#	CF_NAME=$TOPIC_NAME
	#	case $SINK_TYPE in
	#	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	#	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	#	esac
	#	run_cdc_cli changefeed create -c $CF_NAME --start-ts=$start_ts --sink-uri="$SINK_URI" --sort-engine="unified"
	#	if [ "$SINK_TYPE" == "kafka" ]; then
	#		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	#	fi
	#
	#	run_test
	#	cleanup_process $CDC_BINARY
	#	run_cdc_cli unsafe reset --no-confirm --pd=http://${UP_PD_HOST_1}:${UP_PD_PORT_1}

	echo "test leveldb sorter"

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	reset_and_prepare_data

	# Run cdc server with leveldb sorter.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-leveldb-sorter-test-$RANDOM"
	CF_NAME=$TOPIC_NAME
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	run_cdc_cli changefeed create -c $CF_NAME --start-ts=$start_ts --sink-uri="$SINK_URI" --sort-engine="unified"
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	run_test
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
