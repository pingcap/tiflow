#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "kafka" ]; then
	exit 0
fi

stop_tidb_cluster
rm -rf $WORK_DIR && mkdir -p $WORK_DIR

cleanup() {
	stop_tidb_cluster
}

trap cleanup EXIT

curl -i -X POST \
	-H "Accept:application/json" \
	-H "Content-Type:application/json" \
	localhost:8083/connectors/ --data-binary @- <<EOF
{
  "name": "my-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "127.0.0.1",
    "database.port": "3310",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "default",
    "schema.history.internal.kafka.bootstrap.servers": "127.0.0.1:9092",
    "schema.history.internal.kafka.topic": "schemahistory.test",
    "transforms": "x",
    "transforms.x.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.x.regex": "(.*)",
    "transforms.x.replacement":"output_debezium",
    "binary.handling.mode": "base64",
    "decimal.handling.mode": "double"
  }
}
EOF

start_tidb_cluster --workdir $WORK_DIR
run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
run_cdc_cli changefeed create -c test --sink-uri="kafka://127.0.0.1:9092/output_ticdc?protocol=debezium&kafka-version=2.4.0" --config "$CUR/changefeed.toml"

cd $CUR
go run ./src

if [ $? -ne 0 ]; then
	exit 1
fi

echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
