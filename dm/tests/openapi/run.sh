#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
export PATH=$PATH:$cur/client/
WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_database() {
	run_sql_source1 'DROP DATABASE if exists openapi;'
	run_sql_source1 'CREATE DATABASE openapi;'

	run_sql_source2 'DROP DATABASE if exists openapi;'
	run_sql_source2 'CREATE DATABASE openapi;'
}

function init_noshard_data() {

	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (3, 4);"
}

function init_shard_data() {
	run_sql_source1 "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);"

	run_sql_source1 "INSERT INTO openapi.t(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t(i,j) VALUES (3, 4);"
}

function test_source() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SOURCE"
	prepare_database
	# create source succesfully
	openapi_source_check "create_source1_success"

	# recreate source will failed
	openapi_source_check "create_source_failed"

	# get source list success
	openapi_source_check "list_source_success" 1

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"

	# after delete source, source list should be empty
	openapi_source_check "list_source_success" 0

	# re delete source failed
	openapi_source_check "delete_source_failed" "mysql-01"

	# send request to not leader node
	openapi_source_check "list_source_with_redirect" 0

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SOURCE SUCCESS"
}

function test_relay() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: RELAY"
	prepare_database
	# create source succesfully
	openapi_source_check "create_source1_success"

	# we need make sure that source is bounded by worker1 because we will start relay on worker1
	# todo: use openapi to transfer source
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source mysql-01 worker1" \
		"\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1

	# start relay failed
	openapi_source_check "start_relay_failed" "mysql-01" "no-worker"

	# start relay success
	openapi_source_check "start_relay_success" "mysql-01" "worker1"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"relayCatchUpMaster\": true" 1

	# get source status failed
	openapi_source_check "get_source_status_failed" "no-mysql"

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"
	openapi_source_check "get_source_status_success_with_relay" "mysql-01"

	# stop relay failed: not pass worker name
	openapi_source_check "stop_relay_failed" "mysql-01" "no-worker"

	# stop relay success
	openapi_source_check "stop_relay_success" "mysql-01" "worker1"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"relayStatus\": null" 1

	openapi_source_check "get_source_status_success_no_relay" "mysql-01"

	openapi_source_check "start_relay_success_with_two_worker" "mysql-01" "worker1" "worker2"
	openapi_source_check "get_source_status_success" "mysql-01" 2            # have two source status
	openapi_source_check "get_source_status_success_with_relay" "mysql-01" 0 # check worker1 relay status
	openapi_source_check "get_source_status_success_with_relay" "mysql-01" 1 # check worker2 relay status

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"worker\": \"worker2\"" 1

	# stop relay on two worker success
	openapi_source_check "stop_relay_success" "mysql-01" "worker1"
	openapi_source_check "stop_relay_success" "mysql-01" "worker2"

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"

	# TODO add transfer-source test when openapi support this

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: RELAY SUCCESS"

}

function test_shard_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SHARD TASK"
	prepare_database

	task_name="test-shard"

	# create source succesfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1
	# get source status success
	# openapi_source_check "get_source_status_success" "mysql-01"

	# create source succesfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2
	# get source status success
	# openapi_source_check "get_source_status_success" "mysql-02"

	# start task success: not vaild task create request
	openapi_task_check "start_task_failed"

	# start shard task success
	openapi_task_check "start_shard_task_success"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	init_shard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_shard.toml

	# test binlog event filter, this delete will ignored in source-1
	run_sql_source1 "DELETE FROM openapi.t;"
	run_sql_tidb_with_retry "select count(1) from openapi.t;" "count(1): 2"

	# test binlog event filter, this ddl will ignored in source-2
	run_sql "alter table openapi.t add column aaa int;" $MYSQL_PORT2 $MYSQL_PASSWORD2
	# ddl will be ignored, so no ddl locks and the task will work normally.
	run_sql "INSERT INTO openapi.t(i,j) VALUES (5, 5);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_tidb_with_retry "select count(1) from openapi.t;" "count(1): 3"

	# stop task success
	openapi_task_check "stop_task_success" "$task_name"

	# stop task failed
	openapi_task_check "stop_task_failed" "$task_name"

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"
	openapi_source_check "delete_source_success" "mysql-02"
	openapi_source_check "list_source_success" 0
	run_sql_tidb "DROP DATABASE if exists openapi;"
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SHARD TASK SUCCESS"

}

function test_noshard_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: NO SHARD TASK"
	prepare_database

	task_name="test-no-shard"

	# create source succesfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1
	# get source status success
	# openapi_source_check "get_source_status_success" "mysql-01"

	# create source succesfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2
	# get source status success
	# openapi_source_check "get_source_status_success" "mysql-02"

	# start task success: not vaild task create request
	openapi_task_check "start_task_failed"

	# start no shard task success
	openapi_task_check "start_noshard_task_success"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	init_noshard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_no_shard.toml

	# stop task success
	openapi_task_check "stop_task_success" "$task_name"

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"
	openapi_source_check "delete_source_success" "mysql-02"
	openapi_source_check "list_source_success" 0
	run_sql_tidb "DROP DATABASE if exists openapi;"
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: NO SHARD TASK SUCCESS"
}

function run() {
	make install_test_python_dep

	# run dm-master1
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
	# join master2
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $cur/conf/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2
	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	test_source
	test_relay

	test_shard_task
	test_noshard_task
}

cleanup_data openapi
cleanup_process

run

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
