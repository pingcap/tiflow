#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# check_partition_error checks that a feed reports the ordered-partition restriction.
function check_partition_error() {
	local id=${1:-ordered}
	run_cdc_cli changefeed query --changefeed-id="$id" --server="127.0.0.1:8300" >"$WORK_DIR/$id-status.json"
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/$id-status.json"
}

# check_update_stopped checks that the feed is paused before updating its config.
function check_update_stopped() {
	local info
	info=$(curl -fsS --max-time 30 'http://127.0.0.1:8300/api/v2/changefeeds/update-order') || return 1
	jq -e '.state == "stopped"' <<<"$info"
}

# check_mysql_scheduler verifies that MySQL sinks disable replication across nodes.
function check_mysql_scheduler() {
	local info
	info=$(curl -fsS --max-time 30 'http://127.0.0.1:8300/api/v2/changefeeds/ordered') || return 1
	jq -e '.config.scheduler.enable_table_across_nodes == false' <<<"$info"
}

# check_table_row_count compares a downstream table's size with the expected count.
function check_table_row_count() {
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM $1")" = "$2" ]
}

export -f check_partition_error
export -f check_update_stopped
export -f check_mysql_scheduler check_table_row_count
export WORK_DIR
export DOWN_TIDB_HOST DOWN_TIDB_PORT

# check_partition_ddl rejects partition creation, conversion, and index-enabled replication.
function check_partition_ddl() {
	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql 'CREATE DATABASE mysql_txn_order_ddl;' 127.0.0.1 "$port"
	done
	sed 's/mysql_txn_order/mysql_txn_order_ddl/g' "$CUR/conf/changefeed.toml" >"$WORK_DIR/partition-ddl.toml"

	# A matching partitioned table created after the feed starts must be rejected
	# before its DDL is applied downstream.
	run_cdc_cli changefeed create --changefeed-id=partition-ddl-create --sink-uri="$SINK_URI" --config="$WORK_DIR/partition-ddl.toml"
	run_sql 'CREATE TABLE mysql_txn_order_ddl.ordered_p (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 30 check_partition_error partition-ddl-create
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='mysql_txn_order_ddl' AND table_name='ordered_p'")" = 0 ]
	run_cdc_cli changefeed remove --changefeed-id=partition-ddl-create
	run_sql 'DROP TABLE mysql_txn_order_ddl.ordered_p;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	# Turning an ordered regular table into a partitioned table must be rejected,
	# too, while the downstream keeps its original nonpartitioned table.
	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql 'CREATE TABLE mysql_txn_order_ddl.ordered_t (id INT PRIMARY KEY);' 127.0.0.1 "$port"
	done
	run_cdc_cli changefeed create --changefeed-id=partition-ddl-alter --sink-uri="$SINK_URI" --config="$WORK_DIR/partition-ddl.toml"
	run_sql 'ALTER TABLE mysql_txn_order_ddl.ordered_t PARTITION BY HASH(id) PARTITIONS 2;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 30 check_partition_error partition-ddl-alter
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM information_schema.partitions WHERE table_schema='mysql_txn_order_ddl' AND table_name='ordered_t' AND partition_name IS NOT NULL")" = 0 ]
	run_cdc_cli changefeed remove --changefeed-id=partition-ddl-alter
	run_sql 'DROP TABLE mysql_txn_order_ddl.ordered_t;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	# A usable unique index starts replication of an otherwise skipped partition.
	run_cdc_cli changefeed create --changefeed-id=partition-ddl-unique --sink-uri="$SINK_URI" --config="$WORK_DIR/partition-ddl.toml"
	run_sql 'CREATE TABLE mysql_txn_order_ddl.ordered_no_key (id INT NOT NULL) PARTITION BY HASH(id) PARTITIONS 2;
CREATE TABLE mysql_txn_order_ddl.marker (id INT PRIMARY KEY);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists mysql_txn_order_ddl.marker "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql 'ALTER TABLE mysql_txn_order_ddl.ordered_no_key ADD UNIQUE KEY (id);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 30 check_partition_error partition-ddl-unique
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='mysql_txn_order_ddl' AND table_name='ordered_no_key'")" = 0 ]
	run_cdc_cli changefeed remove --changefeed-id=partition-ddl-unique
	echo 'partition CREATE, ALTER, and unique-index DDL checks passed'
}

# check_redo_order exercises the redo apply command with ordered transaction settings.
function check_redo_order() {
	mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -e '
CREATE DATABASE mysql_txn_order_redo;
CREATE TABLE mysql_txn_order_redo.t (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO mysql_txn_order_redo.t VALUES (1,0),(2,0);'
	"$WORK_DIR/check_redo" --downstream "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/mysql_txn_order_redo" \
		--sink-uri "mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
		--work-dir "$WORK_DIR/redo" --config "$CUR/conf/redo.toml" --cdc-binary "$CDC_BINARY"
}

# run exercises API validation and transaction ordering against a MySQL-compatible sink.
function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	go build -o "$WORK_DIR/check_order" "$CUR/main.go"
	go build -o "$WORK_DIR/check_redo" "$CUR/redo"
	cd "$WORK_DIR"

	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql 'CREATE DATABASE mysql_txn_order;
CREATE TABLE mysql_txn_order.t (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO mysql_txn_order.t VALUES (1,0),(2,0);' 127.0.0.1 "$port"
	done
	# Put the hot and independent rows in different TiKV regions. This does not
	# enable TiCDC span replication; the effective scheduler config is checked below.
	run_sql 'SPLIT TABLE mysql_txn_order.t BY (2);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --addr "127.0.0.1:8300"
	# Keep multiple workers and normal batching settings: the ordered table must
	# automatically get full single-table transactions committed in order.
	SINK_URI="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?worker-count=4&max-txn-row=256&safe-mode=false&batch-dml-enable=false"
	bash "$CUR/api.sh" "$SINK_URI"
	run_cdc_cli changefeed create --changefeed-id=ordered --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	ensure 10 check_mysql_scheduler

	"$WORK_DIR/check_order" --upstream "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/mysql_txn_order" \
		--downstream "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/mysql_txn_order"

	# Unmatched partitioned tables are still supported. Renaming one into the
	# ordered set must stop the changefeed before it can replicate ordered DML.
	run_sql 'CREATE TABLE mysql_txn_order.unordered_p (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists mysql_txn_order.unordered_p "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql 'INSERT INTO mysql_txn_order.unordered_p VALUES (1),(2);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 10 check_table_row_count mysql_txn_order.unordered_p 2
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
	check_partition_ddl
	check_redo_order
	cleanup_process "$CDC_BINARY"
}

trap stop_tidb_cluster EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
