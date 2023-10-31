#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function incompatible_ddl() {
	cleanup_data incompatible_ddl_changes

	run_dm_master $WORK_DIR/master $MASTER_PORT1 $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	dmctl_start_task_standalone $cur/conf/$1

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"synced\": true" 1

	# value range decrease
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_mediumint smallint(6);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event value range decrease" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_mediumint mediumint(7);"

	# precision decrease
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_decimal decimal(7,1);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event precision decrease" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_decimal decimal(7,2);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event precision decrease" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify column
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_mediumint varchar(10);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify column" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify column c_mediumint mediumint(7);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify column" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# rename column
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change c_mediumint c_mediumint_new mediumint(7);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event rename column" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change c_mediumint_new c_mediumint mediumint(7);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event rename column" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop column
	run_sql_source1 "alter table incompatible_ddl_changes.t1 drop column c_json;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop column" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.t1 add column c_json json;"

	# remove auto increment
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change id id int(11)"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event remove auto increment" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop pk
	run_sql_source1 "alter table incompatible_ddl_changes.t1 drop primary key"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop primary key" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify default value
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change c_int c_int int default 1"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify default value" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify constaints
	run_sql_source1 "alter table incompatible_ddl_changes.t1 add constraint c_int_unique unique(c_int)"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify constaint" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop uk
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change c_int c_int int"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"drop unique key" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	# drop index
	run_sql_source1 "alter table incompatible_ddl_changes.t1 drop index c_int_unique"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop index" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify column order
	run_sql_source1 "alter table incompatible_ddl_changes.t1 change c_smallint c_smallint smallint(6) after c_json"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify columns order" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify charset
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify c_char char(4) CHARACTER SET latin1;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify charset" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify collation
	run_sql_source1 "alter table incompatible_ddl_changes.t1 modify c_text text COLLATE utf8mb4_unicode_ci;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify collation" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# modify storage engine
	run_sql_source1 "alter table incompatible_ddl_changes.t1 engine = MyISAM;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify storage engine" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# reorganize partition
	run_sql_source1 "alter table incompatible_ddl_changes.t1 partition by range(id) (partition p0 values less than (100000))"
	run_sql_source1 "alter table incompatible_ddl_changes.t1 reorganize partition p0 into ( partition n0 values less than (5), partition n1 values less than (100000));"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event reorganize table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# truncate partition
	run_sql_source1 "alter table incompatible_ddl_changes.t1 truncate partition n0"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event truncate table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# rebuild partition
	run_sql_source1 "alter table incompatible_ddl_changes.t1 rebuild partition n0,n1;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event rebuild table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# exchange partition
	run_sql_source1 "create table incompatible_ddl_changes.tb1(id int) partition by range(id)(partition p0 values less than (100000), partition p1 values less than(1000000));"
	run_sql_source1 "create table incompatible_ddl_changes.tb2(id int);"
	run_sql_source1 "alter table incompatible_ddl_changes.tb1 exchange partition p0 with table incompatible_ddl_changes.tb2"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event exchange table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# coalesce partition
	run_sql_source1 "create table incompatible_ddl_changes.tb3(id int) partition by hash(id) partitions 6;"
	run_sql_source1 "alter table incompatible_ddl_changes.tb3 coalesce partition 2;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event coalesce table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# rename index
	run_sql_source1 "alter table incompatible_ddl_changes.tb3 add index idx(id);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event modify constaints" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2
	run_sql_source1 "alter table incompatible_ddl_changes.tb3 rename index idx to idx1;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event rename index" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop partition
	run_sql_source1 "alter table incompatible_ddl_changes.tb1 drop partition p0;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop table partition" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# rename table
	run_sql_source1 "rename table incompatible_ddl_changes.tb3 to incompatible_ddl_changes.tb4;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event rename table" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# truncate table
	run_sql_source1 "truncate table incompatible_ddl_changes.tb4;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event truncate table" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop table
	run_sql_source1 "drop table incompatible_ddl_changes.tb4;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop table" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	# drop database
	run_sql_source1 "drop database incompatible_ddl_changes;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"event drop database" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test"

	cleanup_process $*
	cleanup_data incompatible_ddl_changes
}

function run() {
	incompatible_ddl dm-task.yaml
	incompatible_ddl dm-task1.yaml
}

cleanup_data incompatible_ddl_changes
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
