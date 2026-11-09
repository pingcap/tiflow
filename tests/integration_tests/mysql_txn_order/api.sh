#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
SINK_URI=$1

function api_request() {
	local expected=$1 method=$2 path=$3 body=${4:-} status
	status=$(curl -sS --max-time 30 -X "$method" "http://127.0.0.1:8300/api/$path" \
		-H 'Content-Type: application/json' -d "$body" -o "$WORK_DIR/api-response.json" -w '%{http_code}')
	echo "$method /api/$path: $status"
	if [ "$status" != "$expected" ]; then
		cat "$WORK_DIR/api-response.json"
		echo "expected HTTP $expected, got $status"
		exit 1
	fi
}

function check_api_state() {
	curl -fsS --max-time 30 "http://127.0.0.1:8300/api/v2/changefeeds/$1" |
		jq -e --arg state "$2" '.state == $state'
}

function check_api_checkpoint() {
	curl -fsS --max-time 30 "http://127.0.0.1:8300/api/v2/changefeeds/$1" |
		jq -e --argjson ts "$2" '.checkpoint_ts >= $ts'
}

function check_api_row() {
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM $1.regular WHERE id=1")" = 1 ]
}

export -f check_api_state check_api_checkpoint check_api_row
export DOWN_TIDB_HOST DOWN_TIDB_PORT

function save_api_config() {
	api_request 200 GET "v2/changefeeds/$id"
	jq -S '{sink_uri, config}' "$WORK_DIR/api-response.json" >"$WORK_DIR/api-before.json"
}

function ordering_update() {
	local rules=$1 filters=$2
	if [ "$version" = v1 ]; then
		jq -cn --argjson rules "$rules" --argjson filters "$filters" \
			'{sink_config: {"transaction-atomicity": "none", "mysql-config": {"txn-order-tables": $rules}}, filter_rules: $filters}'
	else
		jq -c --argjson rules "$rules" --argjson filters "$filters" \
			'{replica_config: .config} | .replica_config.sink.mysql_config.txn_order_tables = $rules | .replica_config.filter.rules = $filters' \
			"$WORK_DIR/api-before.json"
	fi
}

function reject_api_update() {
	api_request 400 PUT "$version/changefeeds/$id" "$1"
	jq -e --arg message "$2" \
		'.error_code == "CDC:ErrChangefeedUpdateRefused" and (.error_msg | contains($message))' "$WORK_DIR/api-response.json"
	api_request 200 GET "v2/changefeeds/$id"
	jq -S '{sink_uri, config}' "$WORK_DIR/api-response.json" >"$WORK_DIR/api-after.json"
	diff -u "$WORK_DIR/api-before.json" "$WORK_DIR/api-after.json"
}

for version in v1 v2; do
	id="api-order-$version"
	schema="mysql_txn_order_api_$version"
	filters=$(jq -cn --arg schema "$schema" '[$schema + ".*"]')
	regular=$(jq -cn --arg schema "$schema" '[$schema + ".regular"]')
	future=$(jq -cn --arg schema "$schema" '[$schema + ".future"]')
	run_sql "CREATE DATABASE $schema; CREATE TABLE $schema.regular (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE $schema; CREATE TABLE $schema.regular (id INT PRIMARY KEY);" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	if [ "$version" = v1 ]; then
		success=202
		payload=$(jq -cn --arg id "$id" --arg uri "$SINK_URI" --argjson filters "$filters" --argjson rules "$future" \
			'{changefeed_id: $id, sink_uri: $uri, filter_rules: $filters, sink_config: {"transaction-atomicity": "none", "mysql-config": {"txn-order-tables": $rules}}}')
	else
		success=200
		payload=$(jq -cn --arg id "$id" --arg uri "$SINK_URI" --argjson filters "$filters" --argjson rules "$future" \
			'{changefeed_id: $id, sink_uri: $uri, replica_config: {filter: {rules: $filters}, sink: {transaction_atomicity: "none", mysql_config: {txn_order_tables: $rules}}}}')
	fi
	api_request "$success" POST "$version/changefeeds" "$payload"
	ensure 10 check_api_state "$id" normal
	save_api_config
	jq -e --argjson rules "$future" '.config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"

	# Create the partitioned table after start-ts and wait for its DDL to finish,
	# so an update must use the checkpoint snapshot rather than the start snapshot.
	run_sql "CREATE TABLE $schema.partitioned (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "$schema.partitioned" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	checkpoint=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 10 check_api_checkpoint "$id" "$checkpoint"
	api_request "$success" POST "$version/changefeeds/$id/pause"
	ensure 10 check_api_state "$id" stopped
	save_api_config
	reject_api_update "$(ordering_update "$filters" "$filters")" 'txn-order-tables does not support partitioned table'

	# Enable ordering for the regular table, and verify the stored API config.
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update "$regular" "$filters")"
	save_api_config
	jq -e --argjson rules "$regular" '.config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"
	reject_api_update '{"sink_uri":"blackhole://"}' 'txn-order-tables is only supported by MySQL sinks'

	# A broad ordering rule is valid if the table filter excludes partitions.
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update "$filters" "$regular")"
	save_api_config
	jq -e --argjson rules "$filters" --argjson filters "$regular" \
		'.config.sink.mysql_config.txn_order_tables == $rules and .config.filter.rules == $filters' "$WORK_DIR/api-before.json"
	if [ "$version" = v1 ]; then
		payload=$(jq -cn --argjson filters "$filters" '{filter_rules: $filters}')
	else
		payload=$(jq -c --argjson filters "$filters" '{replica_config: .config} | .replica_config.filter.rules = $filters' "$WORK_DIR/api-before.json")
	fi
	reject_api_update "$payload" 'txn-order-tables does not support partitioned table'

	# A rejected update must leave the feed resumable with its valid config.
	api_request "$success" POST "$version/changefeeds/$id/resume" '{}'
	ensure 10 check_api_state "$id" normal
	run_sql "INSERT INTO $schema.regular VALUES (1);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 10 check_api_row "$schema"
	api_request "$success" POST "$version/changefeeds/$id/pause"
	ensure 10 check_api_state "$id" stopped
	save_api_config

	# Clearing the option permits including partitioned tables again.
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update '[]' "$filters")"
	save_api_config
	jq -e --argjson filters "$filters" \
		'(.config.sink.mysql_config.txn_order_tables // []) == [] and .config.filter.rules == $filters' "$WORK_DIR/api-before.json"
	api_request "$success" DELETE "$version/changefeeds/$id"
	echo "txn-order-tables $version API checks passed"
done
