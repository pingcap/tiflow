#!/bin/bash

# [DESCRIPTION]:
#   This test verifies that redo log replay works correctly with schema routing.
#   It tests the following scenario:
#   1. Create a changefeed with redo log enabled AND schema routing (source_db -> target_db)
#   2. Insert data into source_db.t1 in upstream
#   3. Verify data flows to target_db.t1 in downstream (via schema routing)
#   4. Restart CDC with MySQLSinkHangLongTime failpoint to block sink writes
#   5. Insert more data (goes to redo logs but NOT to downstream due to failpoint)
#   6. Wait for redo logs to catch up, then stop CDC
#   7. Use `cdc redo apply` to replay redo logs
#   8. Verify all data is replayed to target_db.t1 (schema routing preserved in redo logs)
#   9. Restart CDC without failpoint and verify normal replication resumes

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

REDO_DIR="/tmp/tidb_cdc_test/redo_apply_schema_routing/redo"
SQL_RES_FILE="$OUT_DIR/sql_res.$TEST_NAME.txt"

function cleanup_redo_dir() {
	rm -rf $REDO_DIR
	mkdir -p $REDO_DIR
}

# Helper function to get count from SQL result file
function get_sql_count() {
	grep -oE 'cnt: [0-9]+' "$SQL_RES_FILE" | grep -oE '[0-9]+' || echo "0"
}

function run() {
	# Only run for mysql sink type since redo apply targets MySQL downstream
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip redo apply test for non-mysql sink"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	cleanup_redo_dir

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Create SOURCE database and table in upstream (where data originates)
	run_sql "CREATE DATABASE IF NOT EXISTS source_db;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE source_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Create TARGET database and table in downstream (where data should be routed to)
	# The schema routing rule routes source_db.* -> target_db.*
	run_sql "CREATE DATABASE IF NOT EXISTS target_db;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE target_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Also create source_db in downstream to verify data does NOT go there
	run_sql "CREATE DATABASE IF NOT EXISTS source_db;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE source_db.t1 (id INT PRIMARY KEY, val VARCHAR(255));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	# Start CDC server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root@127.0.0.1:3306/"
	changefeedid="redo-schema-routing-test"

	# Create changefeed with redo log AND schema routing enabled
	run_cdc_cli changefeed create \
		--start-ts=$start_ts \
		--sink-uri="$SINK_URI" \
		--changefeed-id=$changefeedid \
		--config="$CUR/conf/changefeed.toml"

	# Insert data into SOURCE database in upstream
	echo "Inserting data into source_db.t1..."
	for i in $(seq 1 50); do
		run_sql "INSERT INTO source_db.t1 VALUES ($i, 'value_$i');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# Wait for data to be replicated
	sleep 10

	# Verify data arrived in TARGET database (not source database) via schema routing
	echo "Verifying schema routing during normal replication..."
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	upstream_count=$(get_sql_count)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	target_count=$(get_sql_count)
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	source_count=$(get_sql_count)

	echo "Upstream source_db.t1 count: $upstream_count"
	echo "Downstream target_db.t1 count: $target_count"
	echo "Downstream source_db.t1 count: $source_count"

	if [ "$target_count" != "$upstream_count" ]; then
		echo "ERROR: Schema routing not working - target_count ($target_count) != upstream_count ($upstream_count)"
		exit 1
	fi
	if [ "$source_count" -gt 0 ]; then
		echo "ERROR: Schema routing not working - data incorrectly in source_db.t1"
		exit 1
	fi
	echo "Schema routing verified: all $target_count rows correctly routed to target_db.t1"

	# Record the count before injecting failpoint
	pre_failpoint_count=$target_count

	# Now inject the failpoint to prevent sink execution, but the global resolved can be moved forward.
	# Then we can apply redo log to reach an eventual consistent state in downstream.
	echo "Restarting CDC with MySQLSinkHangLongTime failpoint..."
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql/MySQLSinkHangLongTime=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Insert more data - this will go to redo logs but NOT to downstream (due to failpoint)
	echo "Inserting additional data (will be captured in redo but blocked from sink)..."
	for i in $(seq 51 100); do
		run_sql "INSERT INTO source_db.t1 VALUES ($i, 'value_$i');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# Wait for redo logs to be written (data is replicated to CDC but blocked at sink)
	sleep 20

	# Get current TSO and wait for redo logs to catch up
	storage_path="file://$REDO_DIR"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeedid
	current_tso=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	echo "Waiting for redo logs to reach TSO: $current_tso"
	ensure 50 check_redo_resolved_ts $changefeedid $current_tso $storage_path $tmp_download_path/meta

	# Stop CDC server
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS=''

	# Verify downstream still has only pre-failpoint data (failpoint blocked additional inserts)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	target_count_before_redo=$(get_sql_count)
	echo "Downstream target_db.t1 count before redo apply: $target_count_before_redo"
	echo "Expected (pre-failpoint count): $pre_failpoint_count"

	if [ "$target_count_before_redo" != "$pre_failpoint_count" ]; then
		echo "ERROR: Downstream has unexpected count (expected $pre_failpoint_count, got $target_count_before_redo)"
		echo "The failpoint should have blocked additional inserts from reaching downstream"
		exit 1
	fi

	# Check redo log files
	echo "Checking redo log files..."
	ls -la $REDO_DIR || echo "No redo files found"

	# Apply redo logs to downstream using cdc redo apply
	# The key test: redo logs should contain TargetSchema/TargetTable routing info
	echo "Applying redo logs to downstream..."
	$CDC_BINARY redo apply \
		--tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://root@127.0.0.1:3306/?safe-mode=true" \
		2>&1 | tee $WORK_DIR/redo_apply.log || {
		echo "Redo apply failed, checking logs..."
		cat $WORK_DIR/redo_apply.log
		exit 1
	}

	echo "Redo apply completed"

	# Verify schema routing was preserved through redo replay
	echo "Verifying schema routing after redo replay..."

	# Get expected count from upstream (should be 100 now: 50 original + 50 during failpoint)
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	expected_count=$(get_sql_count)

	# Verify data was replayed to target_db.t1
	run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	target_count_after_replay=$(get_sql_count)

	# Verify data did NOT go to source database in downstream
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	source_count_after_replay=$(get_sql_count)

	echo "After redo replay - target_db.t1 count: $target_count_after_replay"
	echo "After redo replay - source_db.t1 count (downstream): $source_count_after_replay"
	echo "Expected count (from upstream): $expected_count"

	if [ "$target_count_after_replay" != "$expected_count" ]; then
		echo "ERROR: Redo replay count mismatch - target_count ($target_count_after_replay) != expected_count ($expected_count)"
		echo "This indicates TargetSchema/TargetTable were not preserved in redo logs"
		exit 1
	fi

	if [ "$source_count_after_replay" -gt 0 ]; then
		echo "ERROR: Redo replay used wrong schema - data in source_db.t1 instead of target_db.t1"
		echo "This indicates TargetSchema/TargetTable were not used during redo apply"
		exit 1
	fi

	echo "SUCCESS: Schema routing preserved through redo log replay!"
	echo "  - All $target_count_after_replay rows correctly replayed to target_db.t1"
	echo "  - source_db.t1 correctly empty in downstream ($source_count_after_replay rows)"

	# Verify changefeed resumes normally after redo apply (without failpoint)
	echo "Verifying changefeed resumes normally after redo apply..."
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Insert more data to verify normal replication still works
	echo "Inserting additional data to verify normal replication..."
	for i in $(seq 101 150); do
		run_sql "INSERT INTO source_db.t1 VALUES ($i, 'value_$i');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# Wait for replication
	sleep 10

	# Verify all data arrived in target_db.t1
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	final_upstream_count=$(get_sql_count)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	final_target_count=$(get_sql_count)
	run_sql "SELECT COUNT(*) as cnt FROM source_db.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	final_source_count=$(get_sql_count)

	echo "Final upstream source_db.t1 count: $final_upstream_count"
	echo "Final downstream target_db.t1 count: $final_target_count"
	echo "Final downstream source_db.t1 count: $final_source_count"

	if [ "$final_target_count" != "$final_upstream_count" ]; then
		echo "ERROR: Final count mismatch after normal replication - target_count ($final_target_count) != upstream_count ($final_upstream_count)"
		exit 1
	fi

	if [ "$final_source_count" -gt 0 ]; then
		echo "ERROR: Data incorrectly routed to source_db.t1 after redo apply"
		exit 1
	fi

	echo "SUCCESS: Changefeed resumes normally after redo apply!"
	echo "  - All $final_target_count rows correctly in target_db.t1"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
