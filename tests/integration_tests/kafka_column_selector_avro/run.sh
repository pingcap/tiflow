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

	echo 'Starting schema registry...'
	./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
	i=0
	while ! curl -o /dev/null -v -s "http://127.0.0.1:8088"; do
		i=$(($i + 1))
		if [ $i -gt 30 ]; then
			echo 'Failed to start schema registry'
			exit 1
		fi
		sleep 2
	done

	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' http://127.0.0.1:8088/config

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	changefeed_id="test"
	TOPIC_NAME="column-selector-avro-test"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=avro&enable-tidb-extension=true&avro-enable-watermark=true"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c ${changefeed_id} --config="$CUR/conf/changefeed.toml" --schema-registry=http://127.0.0.1:8088

	run_kafka_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml "http://127.0.0.1:8088"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	echo "Starting build checksum checker..."
	cd $CUR/../../utils/checksum_checker
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi

	check_table_exists "test.finishmark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	./checksum_checker --upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" --downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" --databases "test" --config="$CUR/conf/changefeed.toml"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
