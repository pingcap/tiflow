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

	changefeed_id="test"
	TOPIC_NAME="column-selector-test"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&partition-num=1&enable-tidb-extension=true"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c ${changefeed_id} --config="$CUR/conf/changefeed.toml"

	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false" --upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" --config="$CUR/conf/changefeed.toml" 2>&1 &

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	echo "Starting build checksum checker..."
	cd $CUR/../../utils/checksum_checker
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi

	check_table_exists "test1.finishmark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	./checksum_checker --upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" --downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" --databases "test,test1" --config="$CUR/conf/changefeed.toml"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
