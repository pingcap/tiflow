#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_tidb "set @@global.foreign_key_checks=1;"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/tidb.prepare.sql $TIDB_HOST $TIDB_PORT $TIDB_PASSWORD

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml --remove-meta" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"stage\": \"Running\"" 1

	# This case validates route + worker_count=1 as the single-worker FK escape
	# hatch. The downstream schema is prepared in advance, while the full-load
	# schema metadata still keeps the upstream FK reference name, so structure
	# diff is not the signal we want to assert here.
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.parent_r;" "COUNT(*): 3"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.child_r;" "COUNT(*): 4"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.child_r c LEFT JOIN fk_route_dst.parent_r p ON c.parent_id=p.parent_id WHERE p.parent_id IS NULL;" "COUNT(*): 0"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"stage\": \"Running\"" 1

	run_sql_tidb_with_retry "SELECT note FROM fk_route_dst.parent_r WHERE parent_id=1;" "note: n1_v2"
	run_sql_tidb_with_retry "SELECT child_data FROM fk_route_dst.child_r WHERE child_id=20;" "child_data: c20_v2"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.parent_r WHERE parent_id=3;" "COUNT(*): 0"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.child_r WHERE parent_id=3;" "COUNT(*): 0"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.child_r WHERE child_id=21;" "COUNT(*): 1"
	run_sql_tidb_with_retry "SELECT COUNT(*) FROM fk_route_dst.child_r c LEFT JOIN fk_route_dst.parent_r p ON c.parent_id=p.parent_id WHERE p.parent_id IS NULL;" "COUNT(*): 0"
}

cleanup_data fk_route_dst
cleanup_data_upstream fk_route_src
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
