#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
SINK_URI=$1

# api_request sends a request and checks its HTTP status, retaining the response body.
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

# check_api_state checks the current changefeed lifecycle state.
function check_api_state() {
	curl -fsS --max-time 30 "http://127.0.0.1:8300/api/v2/changefeeds/$1" |
		jq -e --arg state "$2" '.state == $state'
}

# check_api_checkpoint checks that replication has reached the requested timestamp.
function check_api_checkpoint() {
	curl -fsS --max-time 30 "http://127.0.0.1:8300/api/v2/changefeeds/$1" |
		jq -e --argjson ts "$2" '.checkpoint_ts >= $ts'
}

# check_api_row verifies that the regular table's test row reached downstream.
function check_api_row() {
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM $1.regular WHERE id=1")" = 1 ]
}

# check_api_config checks ordering-rule persistence and the configured sink scheme.
function check_api_config() {
	curl -fsS --max-time 30 "http://127.0.0.1:8300/api/v2/changefeeds/$1" |
		jq -e --argjson rules "$API_EXPECTED_RULES" --arg scheme "$API_EXPECTED_SCHEME" \
			'(.config.sink.mysql_config.txn_order_tables // []) == $rules and ($scheme == "" or (.sink_uri | startswith($scheme + ":")))'
}

export -f check_api_state check_api_checkpoint check_api_row check_api_config
export DOWN_TIDB_HOST DOWN_TIDB_PORT

# save_api_config waits for expected settings and saves a normalized config snapshot.
function save_api_config() {
	if [ "$#" -gt 0 ]; then
		API_EXPECTED_RULES="$1" API_EXPECTED_SCHEME="${2:-}" ensure 10 check_api_config "$id"
	fi
	api_request 200 GET "v2/changefeeds/$id"
	jq -S '{sink_uri, config}' "$WORK_DIR/api-response.json" >"$WORK_DIR/api-before.json"
}

# ordering_update builds a version-specific payload for ordering and table-filter changes.
function ordering_update() {
	local rules=$1 filters=$2
	if [ "$version" = v1 ]; then
		jq -cn --argjson rules "$rules" --argjson filters "$filters" \
			'{sink_config: {"transaction-atomicity": "none", "mysql-config": {"txn-order-tables": $rules}}, filter_rules: $filters}'
	else
		jq -c --argjson rules "$rules" --argjson filters "$filters" \
			'{replica_config: .config} | .replica_config.sink.mysql_config.txn_order_tables = $rules | .replica_config.filter.rules = $filters | .replica_config.scheduler.enable_table_across_nodes = true' \
			"$WORK_DIR/api-before.json"
	fi
}

# reject_api_update verifies the refusal reason and that the stored config stayed unchanged.
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
	filters=$(jq -cn --arg schema "$schema" '[$schema + ".*", "!" + $schema + ".future_excluded"]')
	regular=$(jq -cn --arg schema "$schema" '[$schema + ".regular"]')
	future=$(jq -cn --arg schema "$schema" '[$schema + ".future*"]')
	run_sql "CREATE DATABASE $schema;
CREATE TABLE $schema.regular (id INT PRIMARY KEY);
CREATE TABLE $schema.future_no_key (id INT) PARTITION BY HASH(id) PARTITIONS 2;
CREATE TABLE $schema.future_excluded (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE $schema; CREATE TABLE $schema.regular (id INT PRIMARY KEY);" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	if [ "$version" = v1 ]; then
		success=202
		payload=$(jq -cn --arg id "$id" --arg uri "$SINK_URI" --argjson filters "$filters" --argjson rules "$future" \
			'{changefeed_id: $id, sink_uri: $uri, ignore_ineligible_table: true, filter_rules: $filters, sink_config: {"transaction-atomicity": "none", "mysql-config": {"txn-order-tables": $rules}}}')
	else
		success=200
		payload=$(jq -cn --arg id "$id" --arg uri "$SINK_URI" --argjson filters "$filters" --argjson rules "$future" \
			'{changefeed_id: $id, sink_uri: $uri, replica_config: {ignore_ineligible_table: true, filter: {rules: $filters}, sink: {transaction_atomicity: "none", mysql_config: {txn_order_tables: $rules}}, scheduler: {enable_table_across_nodes: true, region_threshold: 1}}}')
	fi
	# Without force-replicate, matching ineligible partitions are skipped. With
	# force-replicate they would be copied, so ordering must reject them.
	force_payload=$(echo "$payload" | jq -c --arg version "$version" \
		'.changefeed_id += "-force" | if $version == "v1" then .force_replicate = true else .replica_config.force_replicate = true end')
	api_request 500 POST "$version/changefeeds" "$force_payload"
	jq -e '.error_code == "CDC:ErrInvalidReplicaConfig" and (.error_msg | contains("txn-order-tables does not support partitioned table"))' "$WORK_DIR/api-response.json"
	api_request "$success" POST "$version/changefeeds" "$payload"
	ensure 10 check_api_state "$id" normal
	save_api_config "$future" mysql
	jq -e --argjson rules "$future" '.config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"
	jq -e '.config.scheduler.enable_table_across_nodes == false' "$WORK_DIR/api-before.json"

	# Exercise the puller's ineligible-table DDL path, then recreate the owner and
	# processor snapshots from a checkpoint that already contains the skipped table.
	run_sql "CREATE TABLE $schema.future_runtime (id INT) PARTITION BY HASH(id) PARTITIONS 2;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	checkpoint=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 10 check_api_checkpoint "$id" "$checkpoint"
	api_request "$success" POST "$version/changefeeds/$id/pause"
	ensure 10 check_api_state "$id" stopped
	save_api_config
	rules=$(jq -cn --argjson future "$future" --argjson regular "$regular" '$future + $regular')
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update "$rules" "$filters")"
	save_api_config "$rules" mysql
	api_request "$success" POST "$version/changefeeds/$id/resume" '{}'
	ensure 10 check_api_state "$id" normal
	checkpoint=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 10 check_api_checkpoint "$id" "$checkpoint"
	[ "$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$schema' AND table_name LIKE 'future%'")" = 0 ]
	# Dropping a skipped partition must not fail ordering validation, either.
	run_sql "DROP TABLE $schema.future_runtime;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	checkpoint=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 10 check_api_checkpoint "$id" "$checkpoint"

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
	save_api_config "$regular" mysql
	jq -e --argjson rules "$regular" '.config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"
	jq -e '.config.scheduler.enable_table_across_nodes == false' "$WORK_DIR/api-before.json"

	# Non-MySQL sinks ignore the ordering rules, including matches on partitions.
	payload=$(ordering_update "$filters" "$filters" | jq -c '.sink_uri = "blackhole://"')
	api_request "$success" PUT "$version/changefeeds/$id" "$payload"
	save_api_config "$filters" blackhole
	jq -e --argjson rules "$filters" \
		'(.sink_uri | startswith("blackhole:")) and .config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"
	if [ "$version" = v2 ]; then
		jq -e '.config.scheduler.enable_table_across_nodes == true' "$WORK_DIR/api-before.json"
	fi

	# Invalid rules are retained without being parsed or enabled on non-MySQL sinks.
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update '["invalid"]' "$filters")"
	save_api_config '["invalid"]' blackhole
	api_request "$success" POST "$version/changefeeds/$id/resume" '{}'
	ensure 10 check_api_state "$id" normal
	checkpoint=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 10 check_api_checkpoint "$id" "$checkpoint"
	api_request "$success" POST "$version/changefeeds/$id/pause"
	ensure 10 check_api_state "$id" stopped
	save_api_config '["invalid"]' blackhole
	payload=$(jq -cn --arg uri "$SINK_URI" '{sink_uri: $uri}')
	reject_api_update "$payload" 'invalid sink.mysql-config.txn-order-tables'

	payload=$(ordering_update "$regular" "$filters" | jq -c --arg uri "$SINK_URI" '.sink_uri = $uri')
	api_request "$success" PUT "$version/changefeeds/$id" "$payload"
	save_api_config "$regular" mysql
	jq -e --argjson rules "$regular" '.config.sink.mysql_config.txn_order_tables == $rules' "$WORK_DIR/api-before.json"
	jq -e '.config.scheduler.enable_table_across_nodes == false' "$WORK_DIR/api-before.json"

	# A broad ordering rule is valid if the table filter excludes partitions.
	api_request "$success" PUT "$version/changefeeds/$id" "$(ordering_update "$filters" "$regular")"
	save_api_config "$filters" mysql
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
	save_api_config '[]' mysql
	jq -e --argjson filters "$filters" \
		'(.config.sink.mysql_config.txn_order_tables // []) == [] and .config.filter.rules == $filters' "$WORK_DIR/api-before.json"
	api_request "$success" DELETE "$version/changefeeds/$id"
	echo "txn-order-tables $version API checks passed"
done
