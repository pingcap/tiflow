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
	# test kafka sink only in this case
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-big-txn-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306?transaction-atomicity=none" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql "CREATE DATABASE big_txn;"
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=big_txn

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	esac

	check_table_exists "big_txn.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE big_txn.usertable1 LIKE big_txn.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO big_txn.usertable1 SELECT * FROM big_txn.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE big_txn.finish_mark (a int primary key);"
	sleep 120
	check_table_exists "big_txn.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
