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
	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (1, 2);"

	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (3, 4);"
}

function init_shard_data() {
	run_sql_source1 "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);"

	run_sql_source1 "INSERT INTO openapi.t(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t(i,j) VALUES (3, 4);"
}

function init_data_with_auto_id() {
	run_sql_source1 "CREATE TABLE openapi.t(id bigint primary key auto_increment, i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t(id bigint primary key auto_increment, i TINYINT, j INT UNIQUE KEY);"

	run_sql_source1 "INSERT INTO openapi.t(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t(i,j) VALUES (3, 4);"
}

function init_data_with_diff_column() {
	run_sql_source1 "CREATE TABLE openapi.t(id bigint, i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t(i TINYINT, j INT UNIQUE KEY);"

	run_sql_source1 "INSERT INTO openapi.t(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t(i,j) VALUES (3, 4);"
}

function clean_cluster_sources_and_tasks() {
	openapi_source_check "delete_source_with_force_success" "mysql-01"
	openapi_source_check "delete_source_with_force_success" "mysql-02"
	openapi_source_check "list_source_success" 0
	openapi_task_check "get_task_list" 0
	run_sql_tidb "DROP DATABASE if exists openapi;"
}

function test_source() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SOURCE"
	prepare_database
	# create source successfully
	openapi_source_check "create_source1_success"

	# recreate source will failed
	openapi_source_check "create_source_failed"

	# update source1 without password success
	openapi_source_check "update_source1_without_password_success"

	# get source list success
	openapi_source_check "list_source_success" 1

	# get source list with status
	openapi_source_check "list_source_with_status_success" 1 1

	# transfer source
	openapi_source_check "transfer_source_success" "mysql-01" "worker2"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker2\"" 1

	init_noshard_data # init table in database openapi for test get schema and table

	# test get source schemas and tables
	openapi_source_check "get_source_schemas_and_tables_success" "mysql-01" "openapi" "t1"

	# test the db name that must be quoted are working properly
	must_quote_db_name="\`db-name\`"
	run_sql_source1 "create database if not exists $must_quote_db_name"
	run_sql_source1 "create table $must_quote_db_name.t1 (i TINYINT, j INT UNIQUE KEY)"
	openapi_source_check "get_source_schemas_and_tables_success" "mysql-01" "db-name" "t1"
	run_sql_source1 "drop database $must_quote_db_name"

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"

	# after delete source, source list should be empty
	openapi_source_check "list_source_success" 0

	# re delete source failed
	openapi_source_check "delete_source_failed" "mysql-01"

	# send request to not leader node
	openapi_source_check "list_source_with_reverse" 0

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SOURCE SUCCESS"
}

function test_relay() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: RELAY"
	prepare_database
	# create source successfully
	openapi_source_check "create_source1_success"

	# we need make sure that source is bound by worker1 because we will start relay on worker1
	openapi_source_check "transfer_source_success" "mysql-01" "worker1"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1

	# start relay failed
	openapi_source_check "enable_relay_failed" "mysql-01" "no-worker"

	# enable relay success
	openapi_source_check "enable_relay_success" "mysql-01" "worker1"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"relayCatchUpMaster\": true" 1

	# get source status failed
	openapi_source_check "get_source_status_failed" "no-mysql"

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"
	openapi_source_check "get_source_status_success_with_relay" "mysql-01"

	# disable relay failed: not pass worker name
	openapi_source_check "disable_relay_failed" "mysql-01" "no-worker"

	# purge relay success
	openapi_source_check "purge_relay_success" "mysql-01" "mysql-bin.000001"

	# disable relay success
	openapi_source_check "disable_relay_success" "mysql-01" "worker1"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"relayStatus\": null" 1

	openapi_source_check "get_source_status_success_no_relay" "mysql-01"

	openapi_source_check "enable_relay_success_with_two_worker" "mysql-01" "worker1" "worker2"
	openapi_source_check "list_source_with_status_success" 1 2               # source 1 status_list will have two items
	openapi_source_check "get_source_status_success" "mysql-01" 2            # have two source status
	openapi_source_check "get_source_status_success_with_relay" "mysql-01" 0 # check worker1 relay status
	openapi_source_check "get_source_status_success_with_relay" "mysql-01" 1 # check worker2 relay status

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s mysql-01" \
		"\"worker\": \"worker1\"" 1 \
		"\"worker\": \"worker2\"" 1

	# stop relay on two worker success
	openapi_source_check "disable_relay_success" "mysql-01" "worker1"
	openapi_source_check "disable_relay_success" "mysql-01" "worker2"

	# delete source success
	openapi_source_check "delete_source_success" "mysql-01"

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: RELAY SUCCESS"

}

function test_shard_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: SHARD TASK"
	prepare_database

	task_name="test-shard"

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1
	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"

	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2
	# get source status success
	openapi_source_check "get_source_status_success" "mysql-02"

	# create task success: not valid task create request
	openapi_task_check "create_task_failed"

	# create success
	openapi_task_check "create_shard_task_success"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2

	# start success
	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	init_shard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_shard.toml

	# test binlog event filter, this delete will ignored in source-1
	run_sql_source1 "DELETE FROM openapi.t;"
	run_sql_tidb_with_retry "select count(1) from openapi.t;" "count(1): 2"

	# test binlog event filter, this ddl will be ignored in source-2
	run_sql "alter table openapi.t add column aaa int;" $MYSQL_PORT2 $MYSQL_PASSWORD2
	# ddl will be ignored, so no ddl locks and the task will work normally.
	run_sql "INSERT INTO openapi.t(i,j) VALUES (5, 5);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_tidb_with_retry "select count(1) from openapi.t;" "count(1): 3"

	# get task status failed
	openapi_task_check "get_task_status_failed" "not a task name"

	# get illegal char task_status failed
	openapi_task_check get_illegal_char_task_status_failed

	# get task status success
	openapi_task_check "get_task_status_success" "$task_name" 2

	# get task list
	openapi_task_check "get_task_list" 1

	# stop task success
	openapi_task_check "stop_task_success" "$task_name" ""

	openapi_task_check "get_task_list" 1

	# delete task success
	openapi_task_check "delete_task_success" "$task_name"

	# get task list
	openapi_task_check "get_task_list" 0

	# delete source success
	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: SHARD TASK SUCCESS"

}

function test_noshard_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: NO SHARD TASK"
	prepare_database

	task_name="test-no-shard"
	target_table_name="" # empty means no route

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"

	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-02"

	# create no shard task success
	openapi_task_check "create_noshard_task_success" $task_name $target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2

	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2
	init_noshard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_no_shard.toml

	# get task status failed
	openapi_task_check "get_task_status_failed" "not a task name"

	# get task status success
	openapi_task_check "get_task_status_success" "$task_name" 2

	# delete source with force
	openapi_source_check "delete_source_with_force_success" "mysql-01"

	# after delete source-1, there is only one subtask status
	openapi_task_check "get_task_status_success" "$task_name" 1

	# get task list
	openapi_task_check "get_task_list" 1

	# stop task first for operate schema
	openapi_task_check "stop_task_success" "$task_name" "mysql-02"

	# operate schema
	openapi_task_check "operate_schema_and_table_success" "$task_name" "mysql-02" "openapi" "t2"

	# start task again
	openapi_task_check "start_task_success" "$task_name" "mysql-02"

	# delete task failed because there is a running task
	openapi_task_check "delete_task_failed" "$task_name"

	# delete task success with force
	openapi_task_check "delete_task_with_force_success" "$task_name"

	openapi_task_check "get_task_list" 0

	# delete source success
	openapi_source_check "delete_source_success" "mysql-02"
	openapi_source_check "list_source_success" 0
	run_sql_tidb "DROP DATABASE if exists openapi;"
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: NO SHARD TASK SUCCESS"
}

function test_complex_operations_of_source_and_task() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: COMPLEX OPERATION"
	prepare_database

	task_name="test-complex"
	target_table_name="" # empty means no route

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1
	openapi_source_check "create_source2_success"
	openapi_source_check "list_source_success" 2

	# create and check task
	openapi_task_check "create_noshard_task_success" $task_name $target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	openapi_task_check "get_task_list" 1

	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2
	init_noshard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_no_shard.toml
	openapi_task_check "get_task_status_success" "$task_name" 2

	# do some complex operations
	openapi_task_check "do_complex_operations" "$task_name"

	# incr more data
	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (3,4);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (5,6);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config_no_shard.toml

	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: COMPLEX OPERATION SUCCESS"
}

function test_multi_tasks() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: MULTI TASK"
	prepare_database

	task1="test-1"
	task1_target_table_name="task1_target_table"

	task2="test-2"
	task2_target_table_name="task2_target_table"

	# create and check source
	openapi_source_check "create_source1_success"
	openapi_source_check "create_source2_success"
	openapi_source_check "list_source_success" 2

	init_noshard_data

	openapi_task_check "create_noshard_task_success" $task1 $task1_target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task1" \
		"\"stage\": \"Stopped\"" 2

	openapi_task_check "start_task_success" $task1 ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task1" \
		"\"stage\": \"Running\"" 2

	# test get task list with status, now we have 1 task with two status
	openapi_task_check "get_task_list_with_status" 1 $task1 2

	openapi_task_check "create_noshard_task_success" $task2 $task2_target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task2" \
		"\"stage\": \"Stopped\"" 2

	openapi_task_check "start_task_success" $task2 ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task2" \
		"\"stage\": \"Running\"" 2

	# now we have 2 task and each one has two status
	openapi_task_check "get_task_list_with_status" 2 $task2 2

	# delete source success and clean data for other test
	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: MULTI TASK SUCCESS"

}

function test_task_templates() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: TASK TEMPLATES"
	prepare_database

	task_name="test-1"

	# create and check source
	openapi_source_check "create_source1_success"
	openapi_source_check "create_source2_success"
	openapi_source_check "list_source_success" 2

	# crud task template
	openapi_task_check "create_task_template_failed"
	openapi_task_check "create_task_template_success" $task_name
	openapi_task_check "list_task_template" 1
	openapi_task_check "get_task_template" $task_name
	openapi_task_check "update_task_template_success" $task_name "full"
	openapi_task_check "delete_task_template" $task_name
	openapi_task_check "list_task_template" 0

	# import from tasks and get from dmctl
	init_noshard_data

	openapi_task_check "create_noshard_task_success" $task_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	openapi_task_check "import_task_template" 1 0
	openapi_task_check "list_task_template" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config task $task_name --path $WORK_DIR/get_task_from_task.yaml" \
		"\"result\": true" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config task-template $task_name --path $WORK_DIR/get_task_from_task_template.yaml" \
		"\"result\": true" 1

	diff $WORK_DIR/get_task_from_task.yaml $WORK_DIR/get_task_from_task_template.yaml || exit 1

	# delete source success and clean data for other test
	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: TASK TEMPLATES"
}

function test_noshard_task_dump_status() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: NO SHARD TASK DUMP STATUS"

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/dumpling/dumpUnitProcessForever=return()"
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	prepare_database

	task_name="test-no-shard-dump-status"
	target_table_name="" # empty means no route

	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1
	openapi_source_check "get_source_status_success" "mysql-01"
	openapi_source_check "create_source2_success"
	openapi_source_check "list_source_success" 2
	openapi_source_check "get_source_status_success" "mysql-02"

	openapi_task_check "create_noshard_task_success" $task_name $target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"unit\": \"Dump\"" 2 \
		"\"stage\": \"Running\"" 2

	# check noshard task dump status success
	openapi_task_check "check_noshard_task_dump_status_success" "$task_name" 0

	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	openapi_task_check "get_task_status_success_but_worker_meet_error" "$task_name" 2

	export GO_FAILPOINTS=""
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	openapi_source_check "list_source_success" 2
	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: NO SHARD TASK DUMP STATUS SUCCESS"
}

function test_task_with_ignore_check_items() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: TEST TASK WITH IGNORE CHECK ITEMS"
	prepare_database
	init_shard_data

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"

	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-02"

	# no ignore precheck and no warn or error
	task_name="test-no-ignore-no-error"
	ignore_check=""
	is_success="success"
	check_res="pre-check is passed"
	openapi_task_check "create_task_with_precheck" "$task_name" "$ignore_check" "$is_success" "$check_res"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	# delete task
	openapi_task_check "delete_task_success" "$task_name"
	openapi_task_check "get_task_list" 0
	run_sql_tidb "DROP DATABASE if exists dm_mata;"

	# no ignore precheck and has warn
	prepare_database
	init_data_with_auto_id
	task_name="test-no-ignore-has-warn"
	ignore_check=""
	is_success="success"
	check_res="have auto-increment key"
	openapi_task_check "create_task_with_precheck" "$task_name" "$ignore_check" "$is_success" "$check_res"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	# delete task
	openapi_task_check "delete_task_success" "$task_name"
	openapi_task_check "get_task_list" 0
	run_sql_tidb "DROP DATABASE if exists dm_mata;"

	# no ignore precheck and has error
	prepare_database
	init_data_with_diff_column
	task_name="test-no-ignore-has-error"
	ignore_check=""
	is_success="failed"
	check_res="column length mismatch"
	openapi_task_check "create_task_with_precheck" "$task_name" "$ignore_check" "$is_success" "$check_res"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"result\": false" 1
	run_sql_tidb "DROP DATABASE if exists dm_mata;"

	# # has ignore precheck and no error
	prepare_database
	init_data_with_diff_column
	task_name="test-has-ignore-without-error"
	ignore_check="schema_of_shard_tables"
	is_success="success"
	check_res="pre-check is passed"
	openapi_task_check "create_task_with_precheck" "$task_name" "$ignore_check" "$is_success" "$check_res"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	# delete task
	openapi_task_check "delete_task_success" "$task_name"
	openapi_task_check "get_task_list" 0

	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: TEST TASK WITH IGNORE CHECK ITEMS SUCCESS"
}

function test_delete_task_with_stopped_downstream() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: DELETE TASK WITH STOPPED DOWNSTREAM"
	prepare_database

	task_name="test-no-shard"
	target_table_name="" # empty means no route

	# create source successfully
	openapi_source_check "create_source1_success"

	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2

	# create no shard task success
	openapi_task_check "create_noshard_task_success" $task_name $target_table_name
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2

	# stop downstream
	cleanup_tidb_server

	# delete task failed because downstream is stopped.
	openapi_task_check "delete_task_failed" "$task_name"

	# delete task success with force
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# restart downstream
	run_tidb_server 4000 $TIDB_PASSWORD
	sleep 2
	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: DELETE TASK WITH STOPPED DOWNSTREAM SUCCESS"
}

function test_start_task_with_condition() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: START TASK WITH CONDITION"
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"
	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-02"

	# incremental task no source meta and start time, still error
	task_name="incremental_task_no_source_meta"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	openapi_task_check "create_incremental_task_with_gitd_success" $task_name "" "" "" "" "" ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2

	check_result="must set meta for task-mode incremental"
	openapi_task_check "start_task_failed" $task_name "" "$check_result"
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# incremental task use gtid
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"
	task_name="incremental_task_use_gtid"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	master_status1=($(get_master_status $MYSQL_HOST1 $MYSQL_PORT1))
	master_status2=($(get_master_status $MYSQL_HOST2 $MYSQL_PORT2))
	openapi_task_check "create_incremental_task_with_gitd_success" $task_name ${master_status1[0]} ${master_status1[1]} ${master_status1[2]} ${master_status2[0]} ${master_status2[1]} ${master_status2[2]}
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	run_sql_tidb 'CREATE DATABASE openapi;'
	run_sql_source1 "CREATE TABLE openapi.t3(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t4(i TINYINT, j INT UNIQUE KEY);"

	run_sql_tidb_with_retry "show tables in openapi;" "t3"
	run_sql_tidb_with_retry "show tables in openapi;" "t4"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'openapi';" "count(1): 2"

	openapi_task_check "stop_task_success" "$task_name" ""
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# incremental task use start_time
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"
	task_name="incremental_task_use_start_time"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	openapi_task_check "create_incremental_task_with_gitd_success" $task_name "" "" "" "" "" ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	sleep 2
	start_time=$(TZ='UTC' date '+%Y-%m-%d %T') # mysql 3306 and 3307 is in UTC
	sleep 2
	duration=""
	is_success="success"
	check_result=""
	run_sql_tidb 'CREATE DATABASE openapi;'
	run_sql_source1 "CREATE TABLE openapi.t3(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t4(i TINYINT, j INT UNIQUE KEY);"
	openapi_task_check "start_task_with_condition" $task_name "$start_time" "$duration" "$is_success" "$check_result"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	run_sql_tidb_with_retry "show tables in openapi;" "t3"
	run_sql_tidb_with_retry "show tables in openapi;" "t4"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'openapi';" "count(1): 2"

	openapi_task_check "stop_task_success" "$task_name" ""
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# incremental task use start_time, but time is after create table
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"
	task_name="incremental_task_use_start_time_after_create"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	openapi_task_check "create_incremental_task_with_gitd_success" $task_name "" "" "" "" "" ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	sleep 2
	start_time=$(TZ='UTC' date '+%Y-%m-%d %T') # mysql 3306 and 3307 is in UTC
	sleep 2
	duration=""
	is_success="success"
	check_result=""
	run_sql_tidb 'CREATE DATABASE openapi;'
	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (3, 4);"
	openapi_task_check "start_task_with_condition" $task_name "$start_time" "$duration" "$is_success" "$check_result"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"Table 'openapi.*' doesn't exist" 2

	openapi_task_check "stop_task_success" "$task_name" ""
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# incremental task both gtid and start_time, start_time first
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"
	task_name="incremental_task_both_gtid_start_time"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"
	master_status1=($(get_master_status $MYSQL_HOST1 $MYSQL_PORT1))
	master_status2=($(get_master_status $MYSQL_HOST2 $MYSQL_PORT2))
	openapi_task_check "create_incremental_task_with_gitd_success" $task_name ${master_status1[0]} ${master_status1[1]} ${master_status1[2]} ${master_status2[0]} ${master_status2[1]} ${master_status2[2]}
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	run_sql_source1 "CREATE TABLE openapi.t3(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t4(i TINYINT, j INT UNIQUE KEY);"
	sleep 2
	start_time=$(TZ='UTC' date '+%Y-%m-%d %T') # mysql 3306 and 3307 is in UTC
	sleep 2
	duration=""
	is_success="success"
	check_result=""
	run_sql_tidb 'CREATE DATABASE openapi;'
	run_sql_source1 "CREATE TABLE openapi.t5(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t6(i TINYINT, j INT UNIQUE KEY);"
	openapi_task_check "start_task_with_condition" $task_name "$start_time" "$duration" "$is_success" "$check_result"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2

	run_sql_tidb_with_retry "show tables in openapi;" "t5"
	run_sql_tidb_with_retry "show tables in openapi;" "t6"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'openapi';" "count(1): 2"

	openapi_task_check "stop_task_success" "$task_name" ""
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	# incremental task no duration has error
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(\"0s\")"
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	openapi_source_check "list_source_success" 2

	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"
	task_name="incremental_task_no_duration_but_error"
	run_sql_source1 "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_source2 "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"

	sleep 2
	start_time=$(TZ='UTC' date '+%Y-%m-%d %T') # mysql 3306 and 3307 is in UTC
	sleep 2
	duration=""
	is_success="success"
	check_result=""

	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (1, 2);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (1, 2);"
	run_sql_source1 "INSERT INTO openapi.t1(i,j) VALUES (3, 4);"
	run_sql_source2 "INSERT INTO openapi.t2(i,j) VALUES (3, 4);"
	# mock already sync data to downstream
	run_sql_tidb 'CREATE DATABASE openapi;'
	run_sql_tidb "CREATE TABLE openapi.t1(i TINYINT, j INT UNIQUE KEY);"
	run_sql_tidb "CREATE TABLE openapi.t2(i TINYINT, j INT UNIQUE KEY);"
	run_sql_tidb "INSERT INTO openapi.t1(i,j) VALUES (1, 2);"
	run_sql_tidb "INSERT INTO openapi.t2(i,j) VALUES (1, 2);"

	openapi_task_check "create_incremental_task_with_gitd_success" $task_name "" "" "" "" "" ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2
	openapi_task_check "start_task_with_condition" $task_name "$start_time" "$duration" "$is_success" "$check_result"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"Duplicate entry" 2

	# set duration and start again
	openapi_task_check "stop_task_success" "$task_name" ""
	duration="600s"
	openapi_task_check "start_task_with_condition" $task_name "$start_time" "$duration" "$is_success" "$check_result"

	run_sql_tidb_with_retry "SELECT count(1) FROM openapi.t1;" "count(1): 2"
	run_sql_tidb_with_retry "SELECT count(1) FROM openapi.t2;" "count(1): 2"

	openapi_task_check "stop_task_success" "$task_name" ""
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: START TASK WITH CONDITION SUCCESS"
}

function test_stop_task_with_condition() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: STOP TASK WITH CONDITION"
	prepare_database
	run_sql_tidb "DROP DATABASE if exists openapi;"

	# create source successfully
	openapi_source_check "create_source1_success"
	openapi_source_check "list_source_success" 1

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-01"
	# create source successfully
	openapi_source_check "create_source2_success"
	# get source list success
	openapi_source_check "list_source_success" 2

	# get source status success
	openapi_source_check "get_source_status_success" "mysql-02"

	# test wait_time_on_stop
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/recordAndIgnorePrepareTime=return();github.com/pingcap/tiflow/dm/syncer/checkWaitDuration=return("200s")'
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	task_name="test_wait_time_on_stop"
	# create no shard task success
	openapi_task_check "create_noshard_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Stopped\"" 2

	timeout_duration="200s"

	openapi_task_check "start_task_success" $task_name ""
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_name" \
		"\"stage\": \"Running\"" 2
	init_noshard_data
	check_sync_diff $WORK_DIR $cur/conf/diff_config_no_shard.toml
	openapi_task_check "stop_task_with_condition" "$task_name" "" "$timeout_duration"
	echo "error check"
	check_log_contain_with_retry 'panic: success check wait_time_on_stop !!!' $WORK_DIR/worker1/log/stdout.log
	check_log_contain_with_retry 'panic: success check wait_time_on_stop !!!' $WORK_DIR/worker2/log/stdout.log

	# clean
	export GO_FAILPOINTS=''
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	openapi_task_check "delete_task_with_force_success" "$task_name"
	openapi_task_check "get_task_list" 0

	clean_cluster_sources_and_tasks
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: START TASK WITH CONDITION SUCCESS"
}

function test_reverse_https() {
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>START TEST OPENAPI: REVERSE HTTPS"
	cleanup_data openapi
	cleanup_process

	cp $cur/tls_conf/dm-master1.toml $WORK_DIR/
	cp $cur/tls_conf/dm-master2.toml $WORK_DIR/
	cp $cur/tls_conf/dm-worker1.toml $WORK_DIR/
	cp $cur/tls_conf/dm-worker2.toml $WORK_DIR/
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-master1.toml
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-master2.toml
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-worker1.toml
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-worker2.toml

	# run dm-master1
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $WORK_DIR/dm-master1.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1 "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	# join master2
	run_dm_master $WORK_DIR/master2 $MASTER_PORT2 $WORK_DIR/dm-master2.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT2 "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"

	prepare_database
	# create source successfully
	openapi_source_check "create_source_success_https" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"

	# get source list success
	openapi_source_check "list_source_success_https" 1 "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"

	# send request to not leader node
	openapi_source_check "list_source_with_reverse_https" 1 "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"

	cleanup_data openapi
	cleanup_process

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

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>TEST OPENAPI: REVERSE HTTPS"
}

function test_cluster() {
	# list master and worker node
	openapi_cluster_check "list_master_success" 2

	openapi_cluster_check "list_worker_success" 2

	# delete master node
	openapi_cluster_check "delete_master_with_retry_success" "master2"
	openapi_cluster_check "list_master_success" 1

	# delete worker node failed because of worker is still online
	openapi_cluster_check "delete_worker_failed" "worker1"
	kill_dm_worker
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20

	openapi_cluster_check "delete_worker_with_retry_success" "worker1"
	openapi_cluster_check "list_worker_success" 1
}

function run() {
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

	test_relay
	test_source

	test_shard_task
	test_multi_tasks
	test_noshard_task
	test_task_templates
	test_noshard_task_dump_status
	test_complex_operations_of_source_and_task
	test_task_with_ignore_check_items
	test_delete_task_with_stopped_downstream
	test_start_task_with_condition
	test_stop_task_with_condition
	test_reverse_https

	# NOTE: this test case MUST running at last, because it will offline some members of cluster
	test_cluster
}

cleanup_data openapi
cleanup_process

run

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
