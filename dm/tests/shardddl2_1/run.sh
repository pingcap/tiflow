#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_046_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'bbb');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_046() {
	run_case 046 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 046 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_047_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_047() {
	run_case 047 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c int as (a+1) stored);\"" \
		"clean_table" "pessimistic"
	run_case 047 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) stored);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c int as (a+1) stored);\"" \
		"clean_table" "optimistic"
}

function DM_048_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (a,b) values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (a,b) values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_048() {
	run_case 048 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c int as (a+1) virtual);\"" \
		"clean_table" "pessimistic"
	run_case 048 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10), c int as (a+1) virtual);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c int as (a+1) virtual);\"" \
		"clean_table" "optimistic"
}

function DM_049_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	# insert 3 recorde to make sure optimistic mode sharding resolve can finish fast
	sleep 3
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,'jjj');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'kkk');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'lll');"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_049() {
	run_case 049 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 049 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_050_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"
	if [[ "$1" = "optimistic" ]]; then
		relate_worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-01" |
			grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
		# make sure in skip and wait redirect
		check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `a` `c` INT' $WORK_DIR/$relate_worker/log/dm-worker.log
	fi
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		# can't make sure DDL of which source comes first
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` CHANGE COLUMN' 1 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb1}\`\"" 1
	fi
}

function DM_050() {
	run_case 050 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 050 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_051_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	if [[ "$1" = "optimistic" ]]; then
		relate_worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-01" |
			grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
		# make sure in skip and wait redirect
		check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `a` `c` INT' $WORK_DIR/$relate_worker/log/dm-worker.log
	fi

	run_sql_source2 "alter table ${shardddl1}.${tb1} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` CHANGE COLUMN `b` `c` INT' 1 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb1}\`\"" 1
	fi
}

function DM_051() {
	run_case 051 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_case 051 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_056_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,101);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,102);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,103);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,104);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,105);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,106);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,107);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,108);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,109);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'Running' 3
		run_sql_source1 "alter table ${shardddl1}.${tb1} change c c int first;"
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,110);"
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,111);"
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,112);"

		sleep 3
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,113);"
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,114);"
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,115);"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	fi
}

function DM_056() {
	run_case 056 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_case 056 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_057_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change id new_col int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_057() {
	run_case 057 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	# currently not support optimistic
	# run_case 057 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_058_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} change id new_col int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	if [[ "$1" = "optimistic" ]]; then
		relate_worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-01" |
			grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
		# make sure in skip and wait redirect
		check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `id` `new_col`' $WORK_DIR/$relate_worker/log/dm-worker.log
	fi
	run_sql_source2 "alter table ${shardddl1}.${tb1} change id new_col int default 2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change id new_col int default 2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` CHANGE COLUMN `id` `new_col` INT DEFAULT 2' 1 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb1}\`\"" 1
	fi
}

function DM_058() {
	run_case 058 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 058 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_059_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change a new_col datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(6);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a new_col datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(9);"
	sleep 1
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a new_col datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} (id) values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} (id) values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} (id) values(12);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_059() {
	run_case 059 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a datetime);\"" \
		"clean_table" "pessimistic"
	run_case 059 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a datetime);\"" \
		"clean_table" "optimistic"
}

function DM_062_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id bigint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id bigint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id bigint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_062() {
	run_case 062 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 062 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_063_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id mediumint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	if [[ "$1" = "optimistic" ]]; then
		# make sure alter column mediumint exec before bigint
		# otherwise will report "Unsupported modify column length is less than origin"
		run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 3"
	fi

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id bigint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id bigint;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		# TODO: should detect schema conflict in optimistic mode
		run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
	fi
}

function DM_063() {
	run_case 063 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id smallint primary key);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id smallint primary key);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id smallint primary key);\"" \
		"clean_table" "pessimistic"
	run_case 063 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id smallint primary key);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (id smallint primary key);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (id smallint primary key);\"" \
		"clean_table" "optimistic"
}

function DM_064_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int(30);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int(30);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int(30);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_064() {
	run_case 064 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 064 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_065_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify a bigint after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify a bigint first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify a bigint first;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
	fi
}

function DM_065() {
	run_case 065 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_case 065 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_066_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_066() {
	run_case 066 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 066 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_067_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id int default 1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	if [[ "$1" = "optimistic" ]]; then
		relate_worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-01" |
			grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
		# make sure in skip and wait redirect
		check_log_contain_with_retry 'got a shard DDL lock operation.*MODIFY COLUMN `id` INT' $WORK_DIR/$relate_worker/log/dm-worker.log
	fi
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id int default 2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id int default 2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` MODIFY COLUMN `id` INT DEFAULT 2' 1 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb1}\`\"" 1
	fi
}

function DM_067() {
	run_case 067 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 067 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_068_CASE {
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify id datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify id datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify id datetime default now();"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,now());"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_068() {
	run_case 068 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "pessimistic"
	run_case 068 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "optimistic"
}

function restart_worker() {
	echo "restart dm-worker" $1
	if [[ "$1" = "1" ]]; then
		kill_process dm-worker1
		check_port_offline $WORKER1_PORT 20
	else
		kill_process dm-worker2
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

function run() {
	init_cluster
	init_database
	start=46
	end=70
	except=(052 053 054 055 060 061 069 070)
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
