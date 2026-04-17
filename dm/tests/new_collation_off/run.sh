#!/bin/bash

set -eu

# Next-gen TiDB bakes `new_collations_enabled_on_first_bootstrap = true` in at
# keyspace bootstrap and has no way to turn the new collation framework off, so
# there is no "new collation off" state to test. The upstream schema in this
# case also uses `utf8mb4_0900_as_cs`, which new collation explicitly rejects
# (ERROR 1273). Skip the case on next-gen until an equivalent scenario is
# defined for that architecture.
if [ "${NEXT_GEN:-}" = "1" ]; then
	echo "NEXT_GEN=1: skipping new_collation_off (next-gen cannot disable new collation)"
	exit 0
fi

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

API_VERSION="v1alpha1"

# this case will change downstream TiDB not to use new collation. Following cases
# should turn on new collation if they need.
function run() {
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server

	# clean unistore data
	rm -rf /tmp/tidb

	# start a TiDB with off new-collation
	run_tidb_server 4000 $TIDB_PASSWORD $cur/conf/tidb-config.toml
	sleep 2

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 1 row affected'

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml" \
		"\"result\": true" 2

	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data new_collation_off
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
