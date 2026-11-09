#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_partition_error() {
	run_cdc_cli changefeed query --changefeed-id=ordered --server="127.0.0.1:8300" >"$WORK_DIR/status.json"
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/status.json"
}

function check_update_stopped() {
	run_cdc_cli changefeed query --changefeed-id=update-order | jq -e '.state == "stopped"'
}

export -f check_partition_error
export -f check_update_stopped
export WORK_DIR

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	go build -o "$WORK_DIR/check_order" "$CUR/main.go"
	cd "$WORK_DIR"

	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql 'CREATE DATABASE mysql_txn_order;
CREATE TABLE mysql_txn_order.t (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO mysql_txn_order.t VALUES (1,0),(2,0);' 127.0.0.1 "$port"
	done
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --addr "127.0.0.1:8300"
	# Keep multiple workers and normal batching settings: the ordered table must
	# automatically get full single-table transactions committed in order.
	SINK_URI="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?worker-count=4&max-txn-row=256&safe-mode=false&batch-dml-enable=false"
	run_cdc_cli changefeed create --changefeed-id=ordered --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	"$WORK_DIR/check_order" --upstream "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/mysql_txn_order" \
		--downstream "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/mysql_txn_order"

	# Unmatched partitioned tables are still supported. Renaming one into the
	# ordered set must stop the changefeed before it can replicate ordered DML.
	run_sql 'CREATE TABLE mysql_txn_order.unordered_p (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists mysql_txn_order.unordered_p "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql 'RENAME TABLE mysql_txn_order.unordered_p TO mysql_txn_order.ordered_p;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 30 check_partition_error

	# Creation must reject an existing matching partitioned table, too.
	if run_cdc_cli changefeed create --changefeed-id=invalid-partition --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" >"$WORK_DIR/partition-create.log" 2>&1; then
		echo 'partitioned ordered table was accepted'
		exit 1
	fi
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/partition-create.log"

	# Enabling ordering on a paused changefeed must validate the current tables.
	sed '/txn-order-tables/d' "$CUR/conf/changefeed.toml" >"$WORK_DIR/unordered.toml"
	run_cdc_cli changefeed create --changefeed-id=update-order --sink-uri="$SINK_URI" --config="$WORK_DIR/unordered.toml"
	run_cdc_cli changefeed pause --changefeed-id=update-order
	ensure 10 check_update_stopped
	if run_cdc_cli changefeed update --changefeed-id=update-order --no-confirm --config="$CUR/conf/changefeed.toml" >"$WORK_DIR/partition-update.log" 2>&1; then
		echo 'partitioned ordered table was accepted during update'
		exit 1
	fi
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/partition-update.log"
	cleanup_process "$CDC_BINARY"
}

trap stop_tidb_cluster EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
