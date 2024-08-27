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

	# Start downstream TiDB instances
	start_downstream_tidb_instances --db 3 --out_dir $WORK_DIR
	mapfile -t down_tidb_pids < "$WORK_DIR/downstream_tidb_instances_pids.log"
	echo "Started downstream TiDB instances with PIDs: ${down_tidb_pids[@]}"

	# Start the CDC synchronization task.
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	run_cdc_cli changefeed create --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_1},${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_2},${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT_3}/"
	sleep 5

	# Prepare a table
	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists multi_down_addresses.round ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT_1} 60
}

function run() {
	pid1=${down_tidb_pids[0]}
	pid2=${down_tidb_pids[1]}
	pid3=${down_tidb_pids[2]}

	# Round 1
	# shutdown tidb 1 -> begin -> insert -> commit -> check_sync_diff
	# tidb 2 should works
	kill $pid1
	run_sql "BEGIN;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.round VALUES(1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config_2.toml

	# Round 2
	# begin -> shutdown tidb 2 -> insert -> commit -> check_sync_diff
	# tidb 3 should works
	run_sql "BEGIN;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	kill $pid2
	run_sql "INSERT INTO multi_down_addresses.round VALUES(2, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config_3.toml

	# Round 3
	# begin -> insert -> recover tidb 1 -> shutdown tidb 3 -> commit -> check_sync_diff
	# tidb 1 should works
	run_sql "BEGIN;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.round VALUES(3, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 1
	mapfile -t down_tidb_pids < "$WORK_DIR/downstream_tidb_instances_pids.log"
	pid1=${down_tidb_pids[0]}
	kill $pid3
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config_1.toml

	# Round 4
	# begin -> insert -> recover tidb 2 -> shutdown tidb 1 -> insert -> commit -> check_sync_diff
	# tidb 2 should works
	run_sql "BEGIN;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.round VALUES(4, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 2
	mapfile -t down_tidb_pids < "$WORK_DIR/downstream_tidb_instances_pids.log"
	pid2=${down_tidb_pids[0]}
	kill $pid1
	run_sql "INSERT INTO multi_down_addresses.round VALUES(4, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config_2.toml

	# Round 5
	# begin -> insert -> commit -> recover tidb 3 -> shutdown tidb 2 -> begin -> insert -> commit -> check_sync_diff
	# tidb 3 should works
	run_sql "BEGIN;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_down_addresses.round VALUES(5, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 5
	start_downstream_tidb_instances --db 1 --out_dir $WORK_DIR --suffix 3
	kill $pid2
	run_sql "INSERT INTO multi_down_addresses.round VALUES(5, 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config_3.toml
}

# No need to support kafka and storage sink.
if [ "$SINK_TYPE" == "mysql" ]; then
	trap stop_tidb_cluster EXIT
	prepare $*
	run $*
	check_logs $WORK_DIR
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
