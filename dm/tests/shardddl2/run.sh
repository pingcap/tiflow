#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_036_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int after a;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii',9);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
}

function DM_036() {
	# currently not support pessimistic
	# run_case 036 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\";" \
	# "clean_table" "pessimistic"

	run_case 036 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\";" \
		"clean_table" "optimistic"
}

function DM_037_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int default 0;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_037() {
	run_case 037 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 037 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_038_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	# TODO: remove sleep after we support detect ASAP in optimistic mode
	sleep 1
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values (4);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 datetime default now();"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values (5);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 datetime default now();"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values (6);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
}

function DM_038() {
	run_case 038 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 038 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_039_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_039() {
	run_case 039 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 039 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_040_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8 collate utf8_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 varchar(10) character set utf8mb4 collate utf8mb4_bin;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_040() {
	run_case 040 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 040 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_041_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_041() {
	run_case 041 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 041 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_043_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int as (id+2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int as (id+2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"because schema conflict detected" 1
	fi
}

function DM_043() {
	run_case 043 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 043 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_INIT_SCHEMA_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*DROP COLUMN' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_INIT_SCHEMA() {
	run_case INIT_SCHEMA "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function restart_worker() {
	echo "restart dm-worker" $1
	if [[ "$1" = "1" ]]; then
		ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
		check_port_offline $WORKER1_PORT 20
	else
		ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
		check_port_offline $WORKER2_PORT 20
	fi
	export GO_FAILPOINTS=$2

	if [[ "$1" = "1" ]]; then
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	else
		run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	fi
}

function DM_DROP_COLUMN_EXEC_ERROR_CASE() {
	# get worker of source1
	w="1"
	got=$(grep "mysql-replica-01" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [[ "$got" -eq 0 ]]; then
		w="2"
	fi

	restart_worker $w "github.com/pingcap/dm/syncer/ExecDDLError=return()"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"

	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb1 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb2 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"execute .* error" 1

	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b varchar(10);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	restart_worker $w ""
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b varchar(10);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b varchar(10);"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DROP_COLUMN_EXEC_ERROR() {
	run_case DROP_COLUMN_EXEC_ERROR "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_DROP_COLUMN_ALL_DONE_CASE() {
	# get worker of source1
	w="1"
	got=$(grep "mysql-replica-01" $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [[ "$got" -eq 0 ]]; then
		w="2"
	fi

	restart_worker $w "github.com/pingcap/dm/syncer/ExecDDLError=return()"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb1 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"execute .* error" 1

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode.*tb2 drop column' \
		$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b varchar(10);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	restart_worker $w ""
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b varchar(10);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b varchar(10);"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'fff');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DROP_COLUMN_ALL_DONE() {
	run_case DROP_COLUMN_ALL_DONE "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_RECOVER_LOCK_CASE() {
	# tb1(a,b) tb2(a,b)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(2,2);"

	# tb1(a,b,c); tb2(a,b)
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c varchar(10);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3,'aaa');"
	check_log_contain_with_retry "putted a shard DDL.*tb1.*ALTER TABLE .* ADD COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# tb1(a,b,c); tb2(a)
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	check_log_contain_with_retry "putted a shard DDL.*tb2.*ALTER TABLE .* DROP COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	restart_master

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
	check_log_contain_with_retry "putted a shard DDL.*tb1.*ALTER TABLE .* DROP COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# tb1(a,c); tb2(a,b)
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,'ccc');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(7,7);"
	check_log_contain_with_retry "putted a shard DDL.*tb2.*ALTER TABLE .* ADD COLUMN" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# recover lock, tb1's info: (a,b,c)->(a,c); tb2's info: (a)->(a,b)
	# joined(a,b,c); tb1(a,b,c); tb2(a)
	# TrySync tb1: joined(a,b,c); tb1(a,c); tb2(a)
	# TrySync tb2: joined(a,c); tb1(a,c); tb2(a,b)
	restart_master

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(8,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column c varchar(10) after a;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,'fff',10);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(11,'ggg',11);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"no DDL lock exists" 1
}

function DM_RECOVER_LOCK() {
	run_case RECOVER_LOCK "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int) DEFAULT CHARSET=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int) DEFAULT CHARSET=latin1;\"" \
		"clean_table" "optimistic"
}

function restart_master_on_pos() {
	if [ "$1" = "$2" ]; then
		restart_master
	fi
}

function DM_DropAddColumn_CASE() {
	reset=$2

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"

	restart_master_on_pos $reset "1"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"

	restart_master_on_pos $reset "2"

	# make sure column c is fully dropped in the downstream
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'finish to handle ddls in optimistic shard mode' $WORK_DIR/worker2/log/dm-worker.log

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"no DDL lock exists" 1

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5);"

	restart_master_on_pos $reset "3"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(6,6);"

	restart_master_on_pos $reset "4"

	# make sure task to step in "Sync" stage
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int after a;"

	restart_master_on_pos $reset "5"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column b that wasn't fully dropped in downstream" 1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 'fail'
	# no ddl error but have un-synced ddl
	check_metric_not_contains $MASTER_PORT "dm_master_shard_ddl_error" 3
	# 9223372036854775807 is 2**63 -1
	check_metric $MASTER_PORT 'dm_master_ddl_state_number{task="test",type="Un-synced"}' 3 0 9223372036854775807

	# skip this error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	# after we skip ADD COLUMN, we should fix the table structure
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 3

	echo 'CREATE TABLE `tb1` ( `a` int(11) NOT NULL, `b` int(11) DEFAULT NULL, `c` int(11) DEFAULT NULL, PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin' >${WORK_DIR}/schema.sql
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test ${shardddl1} ${tb1} ${WORK_DIR}/schema.sql -s mysql-replica-01" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	run_sql_source1 "update ${shardddl1}.${tb1} set b=1 where a=1;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=3 where a=3;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=4 where a=4;"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=6 where a=6;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c int"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,7,7);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DropAddColumn() {
	for i in $(seq 0 5); do
		echo "run DM_DropAddColumn case #${i}"
		run_case DropAddColumn "double-source-optimistic" \
			"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"; \
            run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int, c int);\"" \
			"clean_table" "optimistic" "$i"
	done
}

function run() {
	init_cluster
	init_database
	DM_DROP_COLUMN_EXEC_ERROR
	DM_INIT_SCHEMA
	DM_DROP_COLUMN_ALL_DONE
	DM_RECOVER_LOCK
	DM_DropAddColumn
	start=36
	end=45
	except=(042 044 045)
	for i in $(seq -f "%03g" ${start} ${end}); do
		if [[ ${except[@]} =~ $i ]]; then
			continue
		fi
		DM_${i}
		sleep 1
	done
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
