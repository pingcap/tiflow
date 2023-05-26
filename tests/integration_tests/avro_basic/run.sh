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

echo 'Starting schema registry...'
./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
curl_status_cmd="curl -vsL --max-time 20 http://127.0.0.1:8081/"
for ((i = 0; i <= 50; i++)); do
	res=$($curl_status_cmd)
  if (! echo "$res" | grep -q "200 OK"); then
      break
  fi

	if [ "$i" -eq 50 ]; then
		echo 'Failed to start schema registry'
		exit 1
	fi
	sleep 2
done

mv bin/logs/* /tmp/tidb_cdc_test


# use kafka-consumer with avro decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# adjust schema registry compatibility level to the none, to allow the schema evolution caused by the TiDB DDL execution.
	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' http://127.0.0.1:8081/config

	# upstream tidb cluster enable row level checksum
	run_sql "set global tidb_enable_row_level_checksum=true" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME="ticdc-avro-test"
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=avro&enable-tidb-extension=true&avro-enable-watermark=true&avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" --schema-registry=http://127.0.0.1:8081

	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=avro&enable-tidb-extension=true&enable-row-checksum=true" "" "http://127.0.0.1:8081"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*

check_logs $WORK_DIR

echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
