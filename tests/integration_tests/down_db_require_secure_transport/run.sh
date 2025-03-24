#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)
CLIENT_SSL="--ssl-ca=$TLS_DIR/ca.pem --ssl-cert=$TLS_DIR/client.pem --ssl-key=$TLS_DIR/client-key.pem"
DB_NAME=down_db_require_secure_transport

function prepare() {
	# Start upstream and the downstream TiDB with TLS enabled.
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --down-db-tls-dir $TLS_DIR
	cd $WORK_DIR

	# Set the global variable `require_secure_transport` to `ON` in the downstream TiDB.
	# This is to ensure that the downstream TiDB only accepts connections from the upstream TiDB with TLS enabled.
	run_sql "set global require_secure_transport=on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} ${CLIENT_SSL}

	# Start the CDC synchronization task.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	sleep 2
	run_cdc_cli changefeed create \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?ssl-ca=${TLS_DIR}/ca.pem&ssl-key=${TLS_DIR}/client-key.pem&ssl-cert=${TLS_DIR}/client.pem" \
		--changefeed-id="down-db-require-secure-transport"
	sleep 2
}

function run() {
	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.finishmark (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "${DB_NAME}.finishmark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 30 ${CLIENT_SSL}
}

# No need to test kafka, pulsar and storage sink.
if [ "$SINK_TYPE" == "mysql" ]; then
	trap stop_tidb_cluster EXIT

	prepare $*
	run $*

	check_logs $WORK_DIR
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
