#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function test_mq_sink_lost_callback() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_sql "DROP DATABASE if exists mq_sink_lost_callback;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE mq_sink_lost_callback;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE mq_sink_lost_callback.t (a int not null primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	cd $WORK_DIR
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/MQSinkGetPartitionError=2*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "info"

	TOPIC_NAME="ticdc-kafka-message-test-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=12582912"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI"

	run_sql "INSERT INTO mq_sink_lost_callback.t (a) values (1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_lost_callback.t (a) values (2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_lost_callback.t (a) values (3);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_lost_callback.t (a) values (4);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_lost_callback.t (a) values (5);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}"
	fi
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

function run() {
	# test kafka sink only in this case
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	test_mq_sink_lost_callback $*
}

trap stop_tidb_cluster EXIT
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
