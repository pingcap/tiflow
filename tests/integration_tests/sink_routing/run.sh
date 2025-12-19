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

	# ============================================
	# Verify schema routing: tables should be in target_db, not source_db
	# ============================================
	echo "Verifying schema routing..."
	check_table_exists target_db.users_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists target_db.orders_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists target_db.products_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Verify source_db tables do NOT exist in downstream
	check_table_not_exists source_db.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_not_exists source_db.orders ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# ============================================
	# Verify DDL: CREATE TABLE
	# ============================================
	echo "Verifying CREATE TABLE routing..."
	check_table_exists target_db.products_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# ============================================
	# Verify DDL: CREATE TABLE LIKE
	# ============================================
	echo "Verifying CREATE TABLE LIKE routing..."
	check_table_exists target_db.products_backup_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "SELECT COUNT(*) as cnt FROM target_db.products_backup_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 1"

	# ============================================
	# Verify DDL: RENAME TABLE
	# ============================================
	echo "Verifying RENAME TABLE routing..."
	# temp_table was renamed to renamed_table, so only renamed_table_routed should exist
	check_table_not_exists target_db.temp_table_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists target_db.renamed_table_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# Verify DML on renamed table worked
	run_sql "SELECT COUNT(*) as cnt FROM target_db.renamed_table_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 2"
	run_sql "SELECT value FROM target_db.renamed_table_routed WHERE id = 1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "updated"

	# ============================================
	# Verify DDL: TRUNCATE TABLE
	# ============================================
	echo "Verifying TRUNCATE TABLE routing..."
	check_table_exists target_db.truncate_test_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# After truncate, only 1 row should exist (inserted after truncate)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.truncate_test_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 1"
	run_sql "SELECT id FROM target_db.truncate_test_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "id: 10"

	# ============================================
	# Verify DDL: ALTER TABLE ADD/DROP COLUMN
	# ============================================
	echo "Verifying ALTER TABLE routing..."
	# created_at column was added then dropped, so it should NOT exist
	run_sql "SHOW COLUMNS FROM target_db.users_routed LIKE 'created_at'" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_not_contains "created_at"

	# ============================================
	# Verify DDL: ALTER TABLE ADD INDEX
	# ============================================
	echo "Verifying ADD INDEX routing..."
	run_sql "SHOW INDEX FROM target_db.orders_routed WHERE Key_name = 'idx_user_id'" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "idx_user_id"

	# ============================================
	# Verify DDL: DROP TABLE
	# ============================================
	echo "Verifying DROP TABLE routing..."
	check_table_not_exists target_db.to_be_dropped_routed ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# ============================================
	# Verify DML: INSERT, UPDATE, DELETE on users
	# ============================================
	echo "Verifying DML operations on users table..."
	# After all operations:
	# - Started with id 1,2 from prepare.sql
	# - Added id 3,4,5 in test.sql
	# - Deleted id 5
	# Final count should be 4 (ids: 1, 2, 3, 4)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.users_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 4"

	# Check UPDATE worked (email updated for id=1)
	run_sql "SELECT email FROM target_db.users_routed WHERE id = 1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "alice_updated@example.com"

	# Check batch UPDATE worked (names updated for ids 3,4)
	run_sql "SELECT name FROM target_db.users_routed WHERE id = 3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "Charlie_v2"
	run_sql "SELECT name FROM target_db.users_routed WHERE id = 4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "Diana_v2"

	# ============================================
	# Verify DML: INSERT, UPDATE, DELETE on orders
	# ============================================
	echo "Verifying DML operations on orders table..."
	# Started with id 1,2, added id 3, deleted id 2
	# Final count should be 2 (ids: 1, 3)
	run_sql "SELECT COUNT(*) as cnt FROM target_db.orders_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 2"

	# Check UPDATE worked (amount updated for id=1)
	run_sql "SELECT amount FROM target_db.orders_routed WHERE id = 1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "150.00"

	# ============================================
	# Verify DML: INSERT, UPDATE, DELETE on products
	# ============================================
	echo "Verifying DML operations on products table..."
	# Started with ids 1,2 (prices 9.99 and 19.99)
	# Updated id 1 (price to 12.99)
	# Deleted where price < 15.00 (deletes id=1 with 12.99, keeps id=2 with 19.99)
	# Final count should be 1
	run_sql "SELECT COUNT(*) as cnt FROM target_db.products_routed" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "cnt: 1"

	echo "All routing verifications passed!"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
