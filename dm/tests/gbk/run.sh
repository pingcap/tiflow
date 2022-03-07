#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="gbk"

function test_from_tidb() {
	cleanup_data_upstream gbk3

	# check table can inherited charset from database
	run_sql_source1 "use gbk; create table ddl1(b char(20));"
	run_sql_tidb_with_retry "show create table gbk.ddl1;" "CHARSET=gbk"

	# can't test "create table as select", because it is not supported in GTID mode

	# test create table like
	run_sql_source1 "use gbk; create table ddl2(a char(20) charset gbk, b char(20) charset utf8mb4);"
	run_sql_source1 "use gbk; create table ddl2_copy like ddl2; insert into ddl2_copy values('一二三', '一二三');"
	run_sql_tidb_with_retry "select hex(a) from gbk.ddl2_copy;" "D2BBB6FEC8FD"
	run_sql_tidb_with_retry "select hex(b) from gbk.ddl2_copy;" "E4B880E4BA8CE4B889"

	# test create partition table
	run_sql_source1 "use gbk; create table ddl3(id int, a char(20) charset gbk) partition by hash(id) partitions 4;"
	run_sql_tidb_with_retry "show create table gbk.ddl3;" "CHARSET=gbk"
	run_sql_tidb_with_retry "show create table gbk.ddl3;" "ARTITION BY HASH (\`id\`) PARTITIONS 4"

	# test alter database charset
	run_sql_source1 "create database gbk3 charset utf8; alter database gbk3 charset gbk;"
	run_sql_tidb_with_retry "show create database gbk3;" "CHARACTER SET gbk"

	# test client use GBK encoding
	run_sql "create table gbk.ddl4 (c int primary key comment '你好');" $MYSQL_PORT1 $MYSQL_PASSWORD1 "gbk"
	run_sql_tidb_with_retry "show create table gbk.ddl4;" "COMMENT '你好'"
}

function run() {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	echo "prepare data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	dmctl_start_task $cur/conf/dm-task.yaml "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "prepare incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check incremental phase"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	test_from_tidb
}

cleanup_data gbk gbk2 gbk3
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
