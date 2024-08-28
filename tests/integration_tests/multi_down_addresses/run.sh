#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	# Start upstream and downstream TiDB cluster
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --downstream_db 0
	cd $WORK_DIR
}

function prepare_create() {
	# Start downstream TiDB instances
	start_downstream_tidb_instances --db 3 --out_dir $WORK_DIR
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	echo "Started downstream TiDB instances with PIDs: ${down_tidb_pids[@]}"

	# Start the CDC synchronization task.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_cdc_cli changefeed create \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_1},${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_2},${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_3}/" \
		--changefeed-id="multi-down-addresses"
	sleep 5

	# Prepare tables for test `cdc cli changefeed create` and `cdc cli changefeed update`
	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists multi_down_addresses.changefeed_create ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT_1} 60
	check_table_exists multi_down_addresses.changefeed_update ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT_1} 60
}

# Test after `cdc cli changefeed create`
function run_create() {
	pid1=${down_tidb_pids[0]}
	pid2=${down_tidb_pids[1]}
	pid3=${down_tidb_pids[2]}

	# Round 1
	# shutdown tidb 1 -> insert -> insert -> check_sync_diff
	# tidb 2 should works
	kill $pid1
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(11, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_2.toml

	# Round 2
	# insert -> shutdown tidb 2 -> update -> update -> check_sync_diff
	# tidb 3 should works
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(2, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	kill $pid2
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=2 WHERE round=2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=3 WHERE round=2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_3.toml

	# Round 3
	# insert -> update -> recover tidb 1 -> shutdown tidb 3 -> update -> check_sync_diff
	# tidb 1 should works
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(3, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=2 WHERE round=3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 1
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	pid1=${down_tidb_pids[0]}
	kill $pid3
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=3 WHERE round=3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_1.toml

	# Round 4
	# insert -> insert -> recover tidb 2 -> shutdown tidb 1 -> update -> delete -> check_sync_diff
	# tidb 2 should works
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(4, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(41, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 2
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	pid2=${down_tidb_pids[0]}
	kill $pid1
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=2 WHERE round=4;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DELETE FROM multi_down_addresses.changefeed_create WHERE round=41;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_2.toml

	# Round 5
	# insert -> update -> recover tidb 3 -> shutdown tidb 2 -> insert -> delete -> check_sync_diff
	# tidb 3 should works
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(5, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE multi_down_addresses.changefeed_create SET val=2 WHERE round=5;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 3
	pid3=${down_tidb_pids[0]}
	kill $pid2
	run_sql "INSERT INTO multi_down_addresses.changefeed_create VALUES(51, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DELETE FROM multi_down_addresses.changefeed_create WHERE round=51;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_3.toml

	kill $pid3
}

function prepare_update() {
	# Start downstream TiDB instances
	start_downstream_tidb_instances --db 2 --out_dir $WORK_DIR
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	echo "Started downstream TiDB instances with PIDs: ${down_tidb_pids[@]}"

	run_cdc_cli changefeed pause -c "multi-down-addresses"
	run_cdc_cli changefeed update \
		-c "multi-down-addresses" \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_1},${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_2}/"
	run_cdc_cli changefeed resume -c "multi-down-addresses"
	sleep 5
}

# Test after `cdc cli changefeed update`
function run_update() {
	pid1=${down_tidb_pids[0]}
	pid2=${down_tidb_pids[1]}

	# Round 1
	# shutdown tidb 1 -> insert -> insert -> insert -> check_sync_diff
	# tidb 2 should works
	kill $pid1
	run_sql "INSERT INTO multi_down_addresses.changefeed_update VALUES(1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.changefeed_update VALUES(11, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.changefeed_update VALUES(111, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_2.toml

	# Round 2
	# create -> insert -> recover tidb 1 -> shutdown tidb 2 -> drop -> check_sync_diff
	# tidb 1 should works
	run_sql "CREATE TABLE multi_down_addresses.ddl1 (round INT PRIMARY KEY)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.ddl1 VALUES(2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 1
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	pid1=${down_tidb_pids[0]}
	kill $pid2
	run_sql "DROP TABLE multi_down_addresses.ddl1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_1.toml

	# Round 3
	# create -> create -> recover tidb 2 -> shutdown tidb 1 -> create -> drop -> check_sync_diff
	# tidb 2 should works
	run_sql "CREATE TABLE multi_down_addresses.ddl3 (round INT PRIMARY KEY)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE multi_down_addresses.ddl33 (round INT PRIMARY KEY)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 2
	mapfile -t down_tidb_pids <"$WORK_DIR/downstream_tidb_instances_pids.log"
	pid2=${down_tidb_pids[0]}
	kill $pid1
	run_sql "CREATE TABLE multi_down_addresses.ddl333 (round INT PRIMARY KEY)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DROP TABLE multi_down_addresses.ddl3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_2.toml
}

# No need to support kafka and storage sink.
if [ "$SINK_TYPE" == "mysql" ]; then
	trap stop_tidb_cluster EXIT

	prepare $*
	prepare_create $*
	run_create $*
	prepare_update $*
	run_update $*
	
	check_logs $WORK_DIR
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
