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

function test_check_task_warn_no_block() {
	prepare_incompatible_tables
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-noshard.yaml" \
		"\"state\": \"warn\"" 1
}

function test_check_task_fail_no_block_forsharding() {
	run_sql_both_source "drop database if exists \`check-task\`"
	run_sql_both_source "create database if not exists \`check-task\`"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-sharding.yaml" \
		"\"state\": \"fail\"" 1
}

# this test case ensures that the privileges we require fully meet the demands of both syncer and loader,
# and dm can still make progress if the downstream user lacks `super` privilege.
function test_privileges_can_migrate() {
	# cleanup data if last test failed
	echo "--> start test_privileges_can_migrate..."
	run_sql_source1 "drop database if exists \`checktask1\`"
	run_sql_source1 "create database if not exists \`checktask1\`"
	run_sql_tidb "drop user if exists 'test1'@'%';"
	run_sql_tidb "drop database if exists \`checktask1\`"
	run_sql_source1 "create table checktask1.test_privilege(id int primary key, b varchar(10))"
	run_sql_source1 "insert into checktask1.test_privilege values (1, 'a'),(2, 'b');"
	run_sql_tidb "create user 'test1'@'%' identified by '123456';"
	run_sql_tidb "grant select, create, insert, update, delete, alter, drop, index, config, create view on *.* to 'test1'@'%';"
	run_sql_tidb "flush privileges;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/task-priv.yaml --remove-meta" \
		"\"state\": \"fail\"" 0
	sleep 1 # wait full migration finish
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_source1 "insert into checktask1.test_privilege values (3, 'c'),(4, 'd');"      # simple dml
	run_sql_source1 "alter table checktask1.test_privilege add column c int default 0;"    # ddl
	run_sql_source1 "create table checktask1.test_ddl(id int primary key, b varchar(10));" # create table
	run_sql_source1 "create table checktask1.test_ddl2(id int primary key, b varchar(10));"
	run_sql_source1 "drop table checktask1.test_ddl2;"                                      # drop table
	run_sql_source1 "create index idx on checktask1.test_privilege(b);"                     # create index
	run_sql_source1 "insert into checktask1.test_privilege values (5, 'e', 5),(6, 'f', 6);" # dml with ddl
	run_sql_source1 "insert into checktask1.test_ddl values (1, 'a'),(2, 'b');"             # simple dml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"Running\"" 2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	# cleanup
	run_sql_tidb "drop user 'test1'@'%';"
	run_sql_tidb "drop database if exists \`checktask1\`;"
	echo "pass test_privileges_can_migrate"
}

function test_privilege_precheck() {
	echo "--> start test_privilege_precheck..."
	# fail: missing privilege
	run_sql_tidb "drop user if exists 'test1'@'%';"
	run_sql_tidb "create user 'test1'@'%' identified by '123456';"
	run_sql_tidb "grant select, create, insert, delete, alter, drop, index on *.* to 'test1'@'%';"
	run_sql_tidb "flush privileges;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-priv.yaml" \
		"\"warning\": 1" 1 \
		"lack of Update global (*.*) privilege" 1
	run_sql_tidb "grant update on *.* to 'test1'@'%';"
	run_sql_tidb "flush privileges;"
	# success: fulfill privileges
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-priv.yaml" \
		"\"msg\": \"pre-check is passed. \"" 1
	run_sql_tidb "drop user 'test1'@'%';"

	# success: all privileges
	run_sql_tidb "create user 'test1'@'%' identified by '123456';"
	run_sql_tidb "grant all privileges on *.* to 'test1'@'%';"
	run_sql_tidb "flush privileges;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/task-priv.yaml" \
		"\"msg\": \"pre-check is passed. \"" 1
	run_sql_tidb "drop user 'test1'@'%';"
	echo "pass test_privilege_precheck"
}

function run() {
	prepare
	test_check_task_warn_no_block
	test_check_task_fail_no_block_forsharding
	test_privileges_can_migrate
	test_privilege_precheck
}

cleanup_data check-task
cleanup_data checktask
cleanup_process $*
run $*
cleanup_process $*
cleanup_data checktask
cleanup_data check-task
