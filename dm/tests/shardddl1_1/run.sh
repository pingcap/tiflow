#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_006_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_006() {
	run_case 006 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_007_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_007() {
	run_case 007 "double-source-pessimistic" "init_table 111 212" "clean_table" ""
}

function DM_008_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source2 "alter table ${shardddl2}.${tb1} add column new_col1 int;"
	run_sql_source2 "insert into ${shardddl2}.${tb1} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_008() {
	run_case 008 "double-source-pessimistic" "init_table 111 221" "clean_table" ""
}

function DM_009_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source2 "alter table ${shardddl2}.${tb2} add column new_col1 int;"
	run_sql_source2 "insert into ${shardddl2}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_009() {
	run_case 009 "double-source-pessimistic" "init_table 111 222" "clean_table" ""
}

function DM_010_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_010() {
	run_case 010 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_011_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 float;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3.0)"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 float;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4.0,4.0)"
	check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_011() {
	run_case 011 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}

function DM_012_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (3,3)"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values (4,4,4)"
	check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
}

function DM_012() {
	run_case 012 "double-source-pessimistic" "init_table 111 211" "clean_table" ""
}
function DM_013_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_013() {
	run_case 013 "single-source-optimistic" "init_table 111 112" "clean_table" ""
}

function DM_014_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (2,2,2)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (3,3)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (4,4,4)"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 4"
}

function DM_014() {
	run_case 014 "single-source-optimistic" "init_table 111 112" "clean_table" ""
}

function DM_015_CASE() {
	run_sql_source1 "drop database ${shardddl1};"
	check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "create database ${shardddl1};"
	check_log_contain_with_retry "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_015() {
	run_case 015 "single-source-pessimistic" "init_table 111" "clean_table 111" ""
}

function DM_016_CASE() {
	run_sql_source1 "drop database ${shardddl1};"
	check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "create database if not exists ${shardddl1};"
	check_log_contain_with_retry "CREATE DATABASE IF NOT EXISTS \`${shardddl1}\`" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_016() {
	run_case 016 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_017_CASE() {
	run_sql_source1 "drop table ${shardddl1}.${tb1};"
	check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "create table ${shardddl1}.${tb1}(id int);"
	check_log_contain_with_retry "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_017() {
	run_case 017 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_018_CASE() {
	run_sql_source1 "drop table ${shardddl1}.${tb1};"
	check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "create table if not exists ${shardddl1}.${tb1}(id int);"
	check_log_contain_with_retry "CREATE TABLE IF NOT EXISTS \`${shardddl1}\`.\`${tb1}\` (\`id\` INT)" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_018() {
	run_case 018 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_019_CASE() {
	run_sql_source1 "truncate table ${shardddl1}.${tb1};"
	check_log_contain_with_retry "skip event, need handled ddls is empty" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_019() {
	run_case 019 "single-source-pessimistic" "init_table 111" "clean_table" ""
	run_case 019 "single-source-optimistic" "init_table 111" "clean_table" ""
}

function DM_020_CASE() {
	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (1);"
	run_sql_tidb_with_retry "show tables from ${shardddl1};" "${tb2}"
	run_sql_tidb_with_retry "select count(1) from ${shardddl1}.${tb2};" "count(1): 1"
}

function DM_020() {
	run_case 020 "single-source-no-routes" "init_table 111" "clean_table;run_sql_tidb \"drop database ${shardddl1};\"" ""
}

function DM_021_CASE() {
	# same as "rename ${shardddl}.${tb} to ${shardddl}.${tb};"
	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
	check_log_contain_with_retry "Table '${shardddl}.${tb}' already exist" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_021() {
	run_case 021 "single-source-pessimistic" "init_table 111" "clean_table" ""
}

function DM_022_CASE() {
	run_sql_tidb "create table ${shardddl1}.${tb2} (id int);"
	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb2};"
	check_log_contain_with_retry "Table '${shardddl1}.${tb2}' already exists" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_022() {
	run_case 022 "single-source-no-routes" "init_table 111" "clean_table;run_sql_tidb \"drop database ${shardddl1};\"" ""
}

function DM_023_CASE() {
	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3}, ${shardddl1}.${tb2} to ${shardddl1}.${tb4};"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"rename table .* not supported" 1
}

function DM_023() {
	run_case 023 "double-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_026_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
	run_sql_source1 "create table ${shardddl1}.${tb3}(id int primary key);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
	run_sql_source1 "insert into ${shardddl1}.${tb3} values (5)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_026() {
	run_case 026 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_027_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1)"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2)"
	run_sql_source1 "create table ${shardddl1}.${tb3}(id int,val int);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (3)"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (4)"
	run_sql_source1 "insert into ${shardddl1}.${tb3} values (5,6)"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Error 1054: Unknown column 'val' in 'field list'" 1
}

function DM_027() {
	run_case 027 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_028_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported drop primary key" 1
}

function DM_028() {
	run_sql_tidb "create table ${shardddl}.${tb} (a varchar(10), primary key(a) clustered);"
	run_case 028 "single-source-pessimistic" "run_sql_source1 \"create table ${shardddl1}.${tb1} (a varchar(10), PRIMARY KEY (a));\"" "clean_table" ""
}

function DM_030_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_030() {
	run_case 030 "double-source-pessimistic" "init_table 111 211" "clean_table" "pessimistic"
	run_case 030 "double-source-optimistic" "init_table 111 211" "clean_table" "optimistic"
}

function DM_031_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 varchar(10);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,'dkfj');"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `new_col1`' 1 \
			"\`${shardddl1}\`.\`${tb1}\`\"" 1
	fi
}

function DM_031() {
	run_case 031 "double-source-pessimistic" "init_table 111 211" "clean_table" "pessimistic"
	run_case 031 "double-source-optimistic" "init_table 111 211" "clean_table" "optimistic"
}

function DM_032_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col2 float;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3.0);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col2 float;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col2 float;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_032() {
	# currently not support pessimistic
	# run_case 032 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 032 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_033_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,null);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5,null);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int not null;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_033() {
	run_case 033 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\";" \
		"clean_table" "pessimistic"
	# currently not support optimistic
	# run_case 033 "double-source-optimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\";" \
	# "clean_table" "optimistic"
}

function DM_034_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int unique auto_increment;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(5);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int unique auto_increment;"

	if [[ "$1" = "pessimistic" ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column 'new_col1' constraint UNIQUE KEY when altering" 1
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"there will be conflicts" 2
	fi

}

function DM_034() {
	run_case 034 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 034 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_035_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col2 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col2 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col2 int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 9"
}

function DM_035() {
	# currently not support pessimistic
	# run_case 035 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 035 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_SAME_DDL_TWICE_CASE() {
	# source1.tb1 add column
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	# source2.tb1 add column
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6);"
	# source2.tb1 drop column
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9);"
	# source2.tb1 add column back
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12);"
	# source2.tb1 drop column again
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15);"
	# source2.tb1 add column back again
	run_sql_source2 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18);"

	# source2.tb2 add column
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19,19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20,20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21,21);"
	# source2.tb2 drop column
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(22,22);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,23);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(24);"
	# source2.tb2 add column back
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(25,25);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(26,26);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(27,27);"
	# source2.tb2 drop column again
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(28,28);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(29,29);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30);"
	# source2.tb2 add column back again
	run_sql_source2 "alter table ${shardddl1}.${tb2} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,31);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,33);"

	# source1.tb1 drop column
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,36);"
	# source1.tb1 add column back
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,37);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,39);"
	# source1.tb1 drop column again
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop new_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(40);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(41,41);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(42,42);"
	# source1.tb1 add column back again
	run_sql_source1 "alter table ${shardddl1}.${tb1} add new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(43,43);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(44,44);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(45,45);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_SAME_DDL_TWICE() {
	run_case SAME_DDL_TWICE "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
}

function DM_BINARY_COLUMN_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values(1,0xBF500C00A2034521B819D6EB7065D200)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_source1 "update ${shardddl1}.${tb1} set a=2 where b=0xBF500C00A2034521B819D6EB7065D200"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_source1 "delete from ${shardddl1}.${tb1} where b=0xBF500C00A2034521B819D6EB7065D200"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# padding by mysql
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values(1,0xBF500C00A2034521B819D6EB7065D2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_source1 "update ${shardddl1}.${tb1} set a=2 where b=0xBF500C00A2034521B819D6EB7065D200"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_source1 "delete from ${shardddl1}.${tb1} where b=0xBF500C00A2034521B819D6EB7065D200"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 0"
}

function DM_BINARY_COLUMN() {
	run_case BINARY_COLUMN "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b binary(16) primary key);\"" \
		"clean_table" ""
}

function run() {
	init_cluster
	init_database
	DM_SAME_DDL_TWICE
	DM_BINARY_COLUMN
	start=6
	end=35
	except=(024 025 029)
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
