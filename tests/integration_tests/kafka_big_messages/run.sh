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

	# Use a max-message-bytes parameter that is larger than the kafka topic max message bytes.
	# Test if TiCDC automatically uses the max-message-bytes of the topic.
	# See: https://github.com/PingCAP-QE/ci/blob/ddde195ebf4364a0028d53405d1194aa37a4d853/jenkins/pipelines/ci/ticdc/cdc_ghpr_kafka_integration_test.groovy#L178
	# Use a topic that has already been created.
	# See: https://github.com/PingCAP-QE/ci/blob/ddde195ebf4364a0028d53405d1194aa37a4d853/jenkins/pipelines/ci/ticdc/cdc_ghpr_kafka_integration_test.groovy#L180
	SINK_URI="kafka://127.0.0.1:9092/big-message-test?protocol=open-protocol&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=12582912"
	cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/big-message-test?protocol=open-protocol&partition-num=1&version=${KAFKA_VERSION}"

	echo "Starting generate kafka big messages..."
	cd $CUR/../../utils/gen_kafka_big_messages
	if [ ! -f ./gen_kafka_big_messages ]; then
		GO111MODULE=on go build
	fi
	# Generate data larger than kafka broker max.message.bytes. We can send this data correctly.
	./gen_kafka_big_messages --row-count=15 --sql-file-path=$CUR/test.sql

	run_sql_file $CUR/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	table="kafka_big_messages.test"
	check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
