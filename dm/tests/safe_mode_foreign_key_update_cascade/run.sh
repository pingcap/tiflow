#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_tidb "set @@global.foreign_key_checks=1;"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml --remove-meta" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"stage\": \"Running\"" 2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"stage\": \"Running\"" 2

	# parent PK updated
	run_sql_source1 "SELECT payload FROM fk_update_demo.parent WHERE parent_id=101;"
	check_contains "payload: p1_v2"
	run_sql_tidb_with_retry "SELECT payload FROM fk_update_demo.parent WHERE parent_id=101;" "payload: p1_v2"

	# child FK cascaded
	run_sql_source1 "SELECT COUNT(*) FROM fk_update_demo.child WHERE parent_id=101;"
	check_contains "COUNT(*): 2"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_update_demo.child WHERE parent_id=101;" "COUNT(*): 2"

	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_update_demo.child c LEFT JOIN fk_update_demo.parent p ON c.parent_id=p.parent_id WHERE p.parent_id IS NULL;" "COUNT(*): 0"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data fk_update_demo
cleanup_data_upstream fk_update_demo
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"

