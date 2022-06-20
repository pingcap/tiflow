#!/bin/bash
set -eu
cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

TABLE_NUM=20

function prepare_incompatible_tables() {
	run_sql_both_source "drop database if exists checktask"
	run_sql_both_source "create database if not exists checktask"
	for i in $(seq $TABLE_NUM); do
		run_sql_both_source "create table checktask.test${i}(id int, b varchar(10))" # no primary key
	done
}

function prepare() {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1
}

function test_check_task_fail_no_block() {
	prepare_incompatible_tables
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-noshard.yaml" \
		"\"state\": \"fail\"" 1
}

function test_check_task_fail_no_block_forsharding() {
	run_sql_both_source "drop database if exists \`check-task\`"
	run_sql_both_source "create database if not exists \`check-task\`"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-sharding.yaml" \
		"\"state\": \"fail\"" 1
}

function run() {
	prepare
	test_check_task_fail_no_block
	test_check_task_fail_no_block_forsharding
}

cleanup_data check-task
cleanup_data checktask
cleanup_process $*
run $*
cleanup_process $*
cleanup_data checktask
cleanup_data check-task
