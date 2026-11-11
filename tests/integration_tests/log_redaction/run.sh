#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This case checks the MySQL sink's parameterized statements and row arguments.
if [ "$SINK_TYPE" != "mysql" ]; then
	exit 0
fi

function wait_for_row() {
	local table=$1
	local i
	for ((i = 0; i < 60; i++)); do
		if run_sql "SELECT COUNT(*) AS matches FROM log_redaction.$table WHERE id = 1 AND val = 'redaction-secret' AND marked = 'redaction-marker-secret‹sensitive›';" \
			"$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" &&
			grep -Fq "matches: 1" "$OUT_DIR/sql_res.$TEST_NAME.txt"; then
			return
		fi
		sleep 1
	done
	echo "The original row values were not replicated for mode $table"
	return 1
}

function check_mode() {
	local mode=$1
	local config_value=$2
	local table=rows_$mode
	local server_config=$WORK_DIR/server_$mode.toml
	local log_file=$WORK_DIR/cdc_$mode.log
	local exec_log=$WORK_DIR/exec_$mode.log
	local start_ts
	printf '[security]\nredact-info-log = %s\n' "$config_value" >"$server_config"
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --config "$server_config" --logsuffix "_$mode"
	run_cdc_cli changefeed create --changefeed-id="redact-$mode" --start-ts="$start_ts" \
		--sink-uri="mysql://normal:123456@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/" \
		--config="$CUR/conf/changefeed.toml"

	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE log_redaction.$table (id INT PRIMARY KEY, val VARCHAR(64), marked VARCHAR(64)) CHARSET utf8mb4;" \
		"$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO log_redaction.$table VALUES (1, 'redaction-secret', 'redaction-marker-secret‹sensitive›');" \
		"$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "log_redaction.$table" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	wait_for_row "$table"

	run_cdc_cli changefeed remove --changefeed-id="redact-$mode"
	cleanup_process "$CDC_BINARY"

	# Require a real DML entry before testing absence of values. The template
	# remains useful for troubleshooting in every mode.
	grep -F "exec row" "$log_file" | grep -F "log_redaction" >"$exec_log"
	grep -Fq " VALUES " "$exec_log"
	grep -Fq "?" "$exec_log"
	case "$mode" in
	off)
		grep -Fq "redaction-secret" "$exec_log"
		grep -Fq "redaction-marker-secret‹sensitive›" "$exec_log"
		;;
	on)
		grep -Fq "[args=?]" "$exec_log"
		if grep -Eq "redaction-secret|redaction-marker-secret" "$log_file"; then
			echo "Sensitive row values leaked in ON mode"
			return 1
		fi
		;;
	marker)
		grep -Fq '[args="‹[' "$exec_log"
		grep -Fq ']›"]' "$exec_log"
		grep -Fq "redaction-secret" "$exec_log"
		grep -Fq "redaction-marker-secret‹‹sensitive››" "$exec_log"
		;;
	esac
}

trap stop_tidb_cluster EXIT
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
start_tidb_cluster --workdir "$WORK_DIR"
check_mode off false
check_mode on true
check_mode marker '"MARKER"'
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
