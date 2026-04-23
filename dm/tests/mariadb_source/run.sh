#!/bin/bash

set -eu

# The next-gen CI pod template does not include a MariaDB sidecar yet, so
# skip the test until MARIADB_PORT is wired up for next-gen. Keeps the rest
# of the G10 group runnable.
if [ "${NEXT_GEN:-}" = "1" ]; then
	echo "NEXT_GEN=1: skipping mariadb_source test (no MariaDB sidecar in next-gen CI pod)"
	exit 0
fi

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export RESET_MASTER=false
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

MARIADB_HOST=${MARIADB_HOST1:-127.0.0.1}
MARIADB_PORT=${MARIADB_PORT1:-3308}
MARIADB_PASSWORD=${MARIADB_PASSWORD1:-123456}

function prepare_source_cfg() {
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "s|host: 127.0.0.1|host: ${MARIADB_HOST}|" $WORK_DIR/source1.yaml
	sed -i "s|port: 3308|port: ${MARIADB_PORT}|" $WORK_DIR/source1.yaml
	sed -i "s|password: '123456'|password: '${MARIADB_PASSWORD}'|" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
}

function check_full_data() {
	run_sql_tidb_with_retry "select count(*) from mariadb_source.t1" "count(*): 2"
	run_sql_tidb "select name from mariadb_source.t1 where id = 1"
	check_contains "alpha"
}

function check_incremental_data() {
	run_sql_tidb_with_retry "select count(*) from mariadb_source.t1" "count(*): 3"
	run_sql_tidb "show create table mariadb_source.t1"
	check_contains "\`note\` varchar(32)"
	run_sql_tidb "select name, note from mariadb_source.t1 where id = 2"
	check_contains "beta_updated"
	check_contains "updated"
	run_sql_tidb "select name from mariadb_source.t1 where id = 3"
	check_contains "gamma"
}

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MARIADB_HOST $MARIADB_PORT $MARIADB_PASSWORD

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	prepare_source_cfg
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1 \
		"\"stage\": \"Running\"" 2

	check_full_data

	run_sql_file $cur/data/db1.increment.sql $MARIADB_HOST $MARIADB_PORT $MARIADB_PASSWORD

	# Diagnostic: check if syncer has errors processing MariaDB binlog
	sleep 5
	echo "=== diagnostic: query-status after increment ==="
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test"
	echo "=== diagnostic: worker log errors ==="
	grep -i "error\|panic\|fail" $WORK_DIR/worker1/log/dm-worker.log | tail -20 || echo "no errors in worker log"

	check_incremental_data
}

cleanup_data mariadb_source
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
