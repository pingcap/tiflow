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

	TOPIC_NAME="ticdc-clustered-index-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar) SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer $WORK_DIR $SINK_URI ;;
	esac
	run_sql "set global tidb_enable_clustered_index=1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# TiDB global variables cache 2 seconds at most
	sleep 2
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first

	check_table_exists clustered_index_test.t0 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists clustered_index_test.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists clustered_index_test.t2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "check table exists success"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	cleanup_process $CDC_BINARY
}

# kafka is not supported yet.
# ref to issue: https://github.com/pingcap/tiflow/issues/3421
# TODO: enable this test for kafka, storage and pulsar sink.
if [ "$SINK_TYPE" != "mysql" ]; then
	echo "[$(date)] <<<<<< skip test case $TEST_NAME for $SINK_TYPE! >>>>>>"
	exit 0
fi
trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
