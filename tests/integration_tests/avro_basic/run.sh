#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "kafka" ]; then
  return
fi

./schema-registry/bin/schema-registry-start -daemon etc/schema-registry/schema-registry.properties & SCHEMA_REGISTRY_PID=$!
i = 0
while ! curl -o /dev/null -v -s "http://127.0.0.1:8081/"; do
  i=$(($i + 1))
  if [ $i -gt 30 ]; then
    echo 'Failed to start schema registry'
    exit 1
  fi
  sleep 2
done

stop_schema_registry() {
  kill -2 $SCHEMA_REGISTRY_PID
}

stop() {
  stop_tidb_cluster
  stop_schema_registry
}

# use kafka-consumer with avro decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	TOPIC_NAME="ticdc-avro-test"
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=avro&enable-tidb-extension=true&kafka-version=${KAFKA_VERSION}"

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

  run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=avro&enable-tidb-extension=true"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
