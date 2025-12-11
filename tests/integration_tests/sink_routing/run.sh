#!/bin/bash

# Integration test for sink routing feature
# This test verifies that schema and table routing works correctly for MySQL sinks
# Source: source_db.* -> Target: target_db.*_routed

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test only works with MySQL sink
if [ "$SINK_TYPE" != "mysql" ]; then
	echo "Skipping sink_routing test for non-MySQL sink type: $SINK_TYPE"
	exit 0
fi

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Create the target database in downstream (routing will route to this db)
	run_sql "DROP DATABASE IF EXISTS target_db" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE DATABASE target_db" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	# Run the prepare SQL to create source tables and insert initial data
	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Run the test SQL to perform more operations
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for the finish marker table to appear in downstream (with routed name)
	# source_db.finish_mark -> target_db.finish_mark_routed
	echo "Waiting for routing to complete..."
	check_table_exists target_db.finish_mark_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Verify schema routing: tables should be in target_db, not source_db
	echo "Verifying schema routing..."
	check_table_exists target_db.users_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists target_db.orders_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists target_db.products_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Verify source_db tables do NOT exist in downstream
	check_table_not_exists source_db.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_not_exists source_db.orders ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Verify data was replicated correctly
	echo "Verifying data replication..."

	# Check users table data
	run_sql "SELECT COUNT(*) as cnt FROM target_db.users_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 3"

	# Check the updated email
	run_sql "SELECT email FROM target_db.users_routed WHERE id = 1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "alice_updated@example.com"

	# Check orders table data
	run_sql "SELECT COUNT(*) as cnt FROM target_db.orders_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 3"

	# Check products table data
	run_sql "SELECT COUNT(*) as cnt FROM target_db.products_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 2"

	# Verify DDL was applied correctly (the ALTER TABLE ADD COLUMN)
	echo "Verifying DDL routing..."
	run_sql "SHOW COLUMNS FROM target_db.users_routed LIKE 'created_at'" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "created_at"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
