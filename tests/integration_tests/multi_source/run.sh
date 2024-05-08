#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	stop_tidb_cluster

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-multi-source-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

trap stop_tidb_cluster EXIT
# storage is not supported yet.
# test without fast create table
if [ "$SINK_TYPE" != "storage" ]; then
	# TODO(dongmen): enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi
	prepare $*
	run_sql "set global tidb_enable_fast_create_table=off" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	cd "$(dirname "$0")"
	set -o pipefail
	GO111MODULE=on go run main.go -config ./config.toml 2>&1 | tee $WORK_DIR/tester.log
	check_table_exists mark.finish_mark_0 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_4 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_5 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/diff_config.toml
	cleanup_process $CDC_BINARY
	check_logs $WORK_DIR
fi
# test with fast create table
if [ "$SINK_TYPE" != "storage" ]; then
	# TODO(dongmen): enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi
	prepare $*
	run_sql "set global tidb_enable_fast_create_table=on" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	cd "$(dirname "$0")"
	set -o pipefail
	GO111MODULE=on go run main.go -config ./config.toml 2>&1 | tee $WORK_DIR/tester.log
	check_table_exists mark.finish_mark_0 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_4 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark_5 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists mark.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/diff_config.toml
	cleanup_process $CDC_BINARY
	check_logs $WORK_DIR
fi
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
