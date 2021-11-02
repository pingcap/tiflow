#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_131_CASE() {
	# Test rollback NULL to NOT NULL.
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Test rollback NOT NULL to NULL
	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int not null;"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,15);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,18);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Modify nullable and then rollback.
function DM_131 {
	# run_case 131 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
	# "clean_table" "pessimistic"
	run_case 131 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_132_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: DM should report an error to user that data constraints become smaller and may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Expand the primary key field.
function DM_132 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 132 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 132 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_133_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Shrink the primary key field.
function DM_133 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a,b) nonclustered);"
	run_case 133 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a,b));\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a,b) nonclustered);"
	run_case 133 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a,b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a,b));\"" \
		"clean_table" "optimistic"
}

function DM_134_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop primary key, add primary key(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: dm-master should give warnings to users that constraint is changed.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change the primary key field.
function DM_134 {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 134 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a));\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int, b int, primary key(a) nonclustered);"
	run_case 134 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int, b int, primary key(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int, b int, primary key(a));\"" \
		"clean_table" "optimistic"
}

function DM_135_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	if ! run_sql_source1 "alter table ${shardddl1}.${tb1} drop primary key, add primary key(b);" 2>&1 |
		grep "Incorrect table definition; there can be only one auto column and it must be defined as a key" >/dev/null; then
		echo "sql should be failed because there can be only one auto column and it must be defined as a key" >&2
		return 255
	fi
}

function DM_135() {
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int auto_increment, b int, primary key(a) nonclustered);"
	run_case 135 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int auto_increment primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_sql_tidb "create database if not exists ${shardddl}; create table ${shardddl}.${tb} (a int auto_increment, b int, primary key(a) nonclustered);"
	run_case 135 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (a int auto_increment primary key, b int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (a int auto_increment primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_136_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk, add unique key uk(a, b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	# FIXME: DM should report an error to user that data constraints become smaller and may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Expand the unique key field.
function DM_136 {
	# run_case 136 "double-source-pessimistic" \
	#     "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a));\"" \
	#     "clean_table" "pessimistic"

	run_case 136 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a));\"" \
		"clean_table" "optimistic"
}

function DM_137_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk, add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Shrink the unique key field.
function DM_137 {
	# run_case 137 "double-source-pessimistic" \
	#     "run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a, b));\"" \
	#     "clean_table" "pessimistic"

	run_case 137 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, b int, unique key uk(a, b));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, b int, unique key uk(a, b));\"" \
		"clean_table" "optimistic"
}

function DM_138_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique key uk(a);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add the unique key.
function DM_138 {
	run_case 138 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"

	run_case 138 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_139_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uk;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	# FIXME: DM should report an error to user that this operation may not be able to rollback.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop the unique key.
function DM_139 {
	run_case 139 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, unique key uk(a));\"" \
		"clean_table" "pessimistic"

	run_case 139 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int, unique key uk(a));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int, unique key uk(a));\"" \
		"clean_table" "optimistic"
}

function DM_140_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} partition by range(id)(partition p0 values less than (106));"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` PARTITION BY RANGE (\`id\`) (PARTITION \`p0\` VALUES LESS THAN (106))" 1 \
		"alter table partition is unsupported" 1
}

# Add partitioning
function DM_140 {
	run_case 140 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 140 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_141_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} remove partitioning"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` REMOVE PARTITIONING" 1 \
		"Unsupported remove partitioning" 1
}

# Remove partitioning.
function DM_141 {
	run_case 141 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "pessimistic"
	run_case 141 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "optimistic"
}

function DM_142_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add partition (partition p1 values less than (200));"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add partition (partition p1 values less than (200));"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add partition (partition p1 values less than (200));"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(130),(131),(132),(133),(134),(135);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add new partition.
function DM_142 {
	run_case 142 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "pessimistic"

	run_case 142 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
		"clean_table" "optimistic"
}

function DM_143_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15),(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25),(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35),(130),(131),(132),(133),(134),(135);"

	run_sql_source1 "delete from ${shardddl1}.${tb1} where id >= 100;"
	run_sql_source2 "delete from ${shardddl1}.${tb1} where id >= 100;"
	run_sql_source2 "delete from ${shardddl1}.${tb2} where id >= 100;"
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop partition p1;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop partition p1;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop partition p1;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Remove partition.
function DM_143 {
	run_case 143 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "pessimistic"
	# optimistic sharding doesn't support partition
	# run_case 143 "double-source-optimistic" \
	# 	"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
	#     (partition p0 values less than (100), partition p1 values less than (200));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
	#      (partition p0 values less than (100), partition p1 values less than (200));\"; \
	#      run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
	#      (partition p0 values less than (100), partition p1 values less than (200));\"" \
	# 	"clean_table" "optimistic"
}

function DM_144_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10),(11),(12),(13),(14),(15),(110),(111),(112),(113),(114),(115);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20),(21),(22),(23),(24),(25),(120),(121),(122),(123),(124),(125);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30),(31),(32),(33),(34),(35),(130),(131),(132),(133),(134),(135);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} reorganize partition p0,p1 into (partition p0 values less than (200))"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"ALTER TABLE \`${shardddl1}\`.\`${tb1}\` REORGANIZE PARTITION \`p0\`,\`p1\` INTO (PARTITION \`p0\` VALUES LESS THAN (200))" 1 \
		"Unsupported reorganize partition" 1
}

# Reorganize partition.
function DM_144 {
	run_case 144 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "pessimistic"
	run_case 144 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
        (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id) \
         (partition p0 values less than (100), partition p1 values less than (200));\"" \
		"clean_table" "optimistic"
}

function DM_145_CASE {
	shardmode=$1
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(100),(101),(102),(103),(104),(105);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(200),(201),(202),(203),(204),(205);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(300),(301),(302),(303),(304),(305);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} engine=innodb;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} engine=innodb;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} engine=innodb;"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(400),(401),(402),(403),(404),(405);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(500),(501),(502),(503),(504),(505);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(600),(601),(602),(603),(604),(605);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Defragment.
function DM_145 {
	run_case 145 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 145 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_146_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(100),(101),(102),(103),(104),(105);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(200),(201),(202),(203),(204),(205);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(300),(301),(302),(303),(304),(305);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} ROW_FORMAT=COMPACT;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"This type of ALTER TABLE is currently unsupported" 1
}

# Modify row format.
function DM_146 {
	run_case 146 "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case 146 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_147_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int, drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column c that wasn't fully dropped in downstream" 1

	# try to fix data
	echo 'create table tbl(a int primary key, b int, c int) engine=innodb default charset=latin1;' >${WORK_DIR}/schema.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test ${shardddl1} ${tb1} ${WORK_DIR}/schema.sql -s mysql-replica-01" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${shardddl1}.${tb1} drop column b\"" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_tidb "update ${shardddl}.${tb} set c=null where a=1;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add and Drop multiple fields and then rollback.
function DM_147 {
	run_case 147 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int) engine=innodb default charset=latin1;\"" \
		"clean_table" "optimistic"
}

function DM_148_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column b int after id, add column c int after b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple fields in a specific order.
function DM_148 {
	run_case 148 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"
	run_case 148 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_149_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,\"ccccccc\");"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,\"ccccccc\");"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,\"bbbbbbbbbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,\"ccccccc\");"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify column a varchar(20);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,\"aaaaaaaaaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,\"bbbbbbbbbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,\"cccccccccccccc\");"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Increase field length.
function DM_149 {
	run_case 149 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case 149 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(10));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_150_CASE {
	shardmode=$1
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,\"ccccccc\");"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a varchar(10);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,\"aaaaaaa\");"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,\"bbbbbbb\");"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,\"ccccccc\");"

	if [[ "$shardmode" == "pessimistic" ]]; then
		# ddl: "modify column a varchar(10);" passes in worker1, but in pessimistic mode is still waiting for the other worker in the sharding group to be executed with the same ddl.
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'"ALTER TABLE `shardddl`.`tb` MODIFY COLUMN `a` VARCHAR(10)"' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'ALTER TABLE `shardddl`.`tb` MODIFY COLUMN `a` VARCHAR(10)"' 1

		# we alter database in source2 and the ddl lock will be resolved
		run_sql_source2 "alter table ${shardddl1}.${tb1} modify column a varchar(10);"
		run_sql_source2 "alter table ${shardddl1}.${tb2} modify column a varchar(10);"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	else
		# ddl: "modify column a varchar(10)" is passed in optimistic mode and will be executed downstream.
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'"stage": "Running"' 2
	fi

}

# Increase field length.
function DM_150 {
	run_case 150 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(20));\"" \
		"clean_table" "pessimistic"
	run_case 150 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a varchar(20));\"; \
         run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a varchar(20));\"" \
		"clean_table" "optimistic"
}

function DM_151_CASE {
	shardmode=$1
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify column a double;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	if [[ "$shardmode" == "pessimistic" ]]; then
		# ddl: "modify column a double;" passes in worker1, but in pessimistic mode is still waiting for the other worker in the sharding group to be executed with the same ddl.
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'"ALTER TABLE `shardddl`.`tb` MODIFY COLUMN `a` DOUBLE"' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"show-ddl-locks" \
			'"ALTER TABLE `shardddl`.`tb` MODIFY COLUMN `a` DOUBLE"' 1

		# we alter database in source2 and the ddl lock will be resolved
		run_sql_source2 "alter table ${shardddl1}.${tb1} modify column a double;"
		run_sql_source2 "alter table ${shardddl1}.${tb2} modify column a double;"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	else
		# ddl: "modify column a double" is passed in optimistic mode and will be executed downstream.
		# but changing the int column to a double column is not allowed, so task is paused
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'"stage": "Paused"' 1 \
			"incompatible mysql type" 1
	fi

}

function DM_151 {
	run_case 151 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "pessimistic"

	run_case 151 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key, a int);\"; \
	     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key, a int);\"" \
		"clean_table" "optimistic"
}

function DM_152_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	# Add multiple fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column a int, add column b varchar(20), add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column a int, add column b varchar(20), add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column a int, add column b varchar(20), add column c double;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,\"aaaa\",4.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,\"bbbb\",5.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,\"cccc\",6.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add multiple indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add unique index uni_a(a), add index idx_b(b);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,\"aaaa\",7.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,\"bbbb\",8.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,\"cccc\",9.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add and drop indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx_b, add index idx_c(c);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx_b, add index idx_c(c);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx_b, add index idx_c(c);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,\"aaaa\",10.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,\"bbbb\",11.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,\"cccc\",12.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Add and drop fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b, add column d int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b, add column d int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b, add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13,13.0,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,14,14.0,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,15,15.0,15);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# Drop all indexes.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index uni_a, drop index idx_c;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index uni_a, drop index idx_c;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index uni_a, drop index idx_c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16,16.0,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17,17.0,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,18,18.0,18);"

	# Drop all fields.
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column a, drop column c, drop column d;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column a, drop column c, drop column d;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column a, drop column c, drop column d;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_152 {
	run_case 152 "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	start=131
	end=152
	for i in $(seq -f "%03g" ${start} ${end}); do
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
