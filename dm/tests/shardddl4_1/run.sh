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

	# Note: not support optimistic partition yet
	#run_case 142 "double-source-optimistic" \
	#	"run_sql_source1 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
	#     run_sql_source2 \"create table ${shardddl1}.${tb1} (id int primary key) partition by range(id)(partition p0 values less than (100));\"; \
	#     run_sql_source2 \"create table ${shardddl1}.${tb2} (id int primary key) partition by range(id)(partition p0 values less than (100));\"" \
	#	"clean_table" "optimistic"
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
	echo 'create table tbl(a int primary key, b int, c int) engine=innodb default charset=latin1 collate=latin1_bin;' >${WORK_DIR}/schema.sql
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
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"" \
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
			'"stage": "Running"' 3
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
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Running" 3
	fi

	# we alter database in source2 and the ddl lock will be resolved
	run_sql_source2 "alter table ${shardddl1}.${tb1} modify column a double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} modify column a double;"

	# insert 3 recorde to make sure optimistic mode sharding resolve can finish fast
	sleep 3
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8.0);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9.0);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
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

function DM_153_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int, drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column c that wasn't fully dropped in downstream" 1

	# try to fix data
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test ${shardddl1} ${tb1} -s mysql-replica-01 --from-target" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${shardddl1}.${tb1} drop column b\"" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_tidb "update ${shardddl}.${tb} set c=null where a=1;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add fix sharding error by use target schema
function DM_153 {
	run_case 153 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"" \
		"clean_table" "optimistic"
}

function DM_154_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column b int, drop column c;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c int, drop column b;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1 \
		"add column c that wasn't fully dropped in downstream" 1

	# try to fix data
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update test ${shardddl1} ${tb1} -s mysql-replica-01 --from-source" \
		"\"result\": true" 2

	run_sql_tidb "alter table ${shardddl}.${tb} drop column b;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2 \
		"\"source 'mysql-replica-02' has no error\"" 1

	run_sql_tidb "update ${shardddl}.${tb} set c=null where a=1;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add fix sharding error by use source schema
function DM_154 {
	run_case 154 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int) engine=innodb default charset=latin1 collate=latin1_bin;\"" \
		"clean_table" "optimistic"
}

function DM_155_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change c b int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column g int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change d f int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column e int not null after f;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13,13,13,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,15,15);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} change c b int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,16,16,16,16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,18,18);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} change d f int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19,19,19,19,19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20,20,20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21,21,21);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column g int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(22,22,22,22,22);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,23,23,23);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(24,24,24);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column e int not null after f;"
	sleep 1
	random_restart 3

	# make sure source1 has received conflict ddl and handled
	check_log_contain_with_retry "skip conflict ddls in optimistic shard mode.*ALTER TABLE \`${shardddl1}\`.\`${tb1}\` CHANGE COLUMN \`c\` \`b\` INT" \
		$WORK_DIR/worker1/log/dm-worker.log
	run_sql_source2 "alter table ${shardddl1}.${tb2} change c b int;"

	# make sure source2 has handled tb2 ddl and trigger redirection
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb2} change c b int" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(25,25,25,25,25);"

	# make sure source1,2 has received and done redirection
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} change c b int" \
		$WORK_DIR/worker1/log/dm-worker.log
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(26,26,26,26,26);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(27,27,27);"
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} change c b int" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(28,28,28,28,28);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(29,29,29,29,29);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(30,30,30);"

	random_restart 3
	# make sure source1 has received conflict ddl and handled
	check_log_contain_with_retry "skip conflict ddls in optimistic shard mode.*ALTER TABLE \`${shardddl1}\`.\`${tb1}\` CHANGE COLUMN \`d\` \`f\` INT" \
		$WORK_DIR/worker1/log/dm-worker.log

	# make sure source2 has handled tb2 ddl and trigger redirection
	run_sql_source2 "alter table ${shardddl1}.${tb2} change d f int;"
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb2} change d f int" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,31,31,31,31);"

	# make sure source1,2 has received and done redirection
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} change d f int" \
		$WORK_DIR/worker1/log/dm-worker.log
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,32,32,32,32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,33,33);"
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} change d f int" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34,34,34,34,34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,35,35,35,35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,36,36);"

	random_restart 3

	# make sure source1 has received conflict ddl and handled
	check_log_contain_with_retry "skip conflict ddls in optimistic shard mode.*ALTER TABLE \`${shardddl1}\`.\`${tb1}\` ADD COLUMN \`e\` INT" \
		$WORK_DIR/worker1/log/dm-worker.log

	# make sure source2 has handled tb2 ddl and trigger redirection
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column e int not null after f;"
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb2} add column e int not null after f" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,37,37,37,37);"

	# make sure source1,2 has received and done redirection
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} add column e int not null after f" \
		$WORK_DIR/worker1/log/dm-worker.log
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,38,38,38,38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,39,39,39);"
	check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} add column e int not null after f" \
		$WORK_DIR/worker2/log/dm-worker.log
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(40,40,40,40,40);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(41,41,41,41,41);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(42,42,42,42);"

	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(43,43,43,43,43);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(44,44,44,44,44);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(45,45,45,45);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column g int;"
	sleep 1
	random_restart 3

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(46,46,46,46,46);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(47,47,47,47,47);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(48,48,48,48,48);"

	# sleep 15 seconds to make sure both dm-workers have reached their final event
	# then insert some dmls to avoid dm-worker get blocked at getting heart event which may cause 30s
	# this part is used to handle case like:
	# worker1 receives skip and wait redirect, and finishes all the events and start waiting to for heartbeat event
	# worker2 resolves this lock, and finishes all its dmls, but worker1 is blocked at receiving heartbeat event(because there is no new data written)
	for ((k = 100; k < 145; k++)); do
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(${k},${k},${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(${k},${k},${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(${k},${k},${k},${k},${k});"
		sleep 1
	done

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add syncing optimistic conflict sequence DDLs case
function DM_155 {
	run_case 155 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c int, d int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c int, d int) engine=innodb default charset=latin1 collate=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c int, d int) engine=innodb default charset=latin1 collate=latin1_bin;\";" \
		"clean_table" "optimistic"
}

function DM_TABLE_CHECKPOINT_BACKWARD_CASE() {
	run_sql_source1 "alter table shardddl1.tb1 change b c int;"
	for i in $(seq 1 1000); do
		run_sql_source1 "insert into shardddl1.tb1(a,c) values($i,$i)"
	done
	run_sql_source1 "alter table shardddl1.t_1 change b c int;"
	for i in $(seq 1001 2000); do
		run_sql_source1 "insert into shardddl1.tb1(a,c) values($i,$i)"
	done
	for i in $(seq 2001 3000); do
		run_sql_source1 "insert into shardddl1.t_1(a,c) values($i,$i)"
	done
	run_sql_source2 "alter table shardddl1.tb1 change b c int;"
	for i in $(seq 3001 3100); do
		run_sql_source1 "insert into shardddl1.tb1(a,c) values($i,$i)"
	done
	run_sql_source2 "alter table shardddl1.t_1 change b c int;"
	for i in $(seq 3101 3200); do
		run_sql_source1 "insert into shardddl1.tb1(a,c) values($i,$i)"
	done
	cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
	sed -i "s/\[routes.rule1\]/[routes.rule2]\nschema-pattern = \"shardddl[1-2]\"\ntable-pattern = \"t_1\"\ntarget-schema = \"shardddl\"\ntarget-table = \"t_1\"\n\[routes.rule1\]/g" $WORK_DIR/diff_config.toml
	sed -i "s/route-rules = \[\"rule1\"\]/route-rules = \[\"rule1\"\,\"rule2\"]/g" $WORK_DIR/diff_config.toml
	sed -i "s/target-check-tables = \[\"shardddl.tb\"\]/target-check-tables = \[\"shardddl.tb\",\"shardddl.t_1\"\]/g" $WORK_DIR/diff_config.toml
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30
}

function DM_TABLE_CHECKPOINT_BACKWARD() {
	run_case TABLE_CHECKPOINT_BACKWARD "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${shardddl1}.t_1 (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.t_1 (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_RESYNC_NOT_FLUSHED_CASE() {
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ReSyncExit=return(true)'
	restart_worker1
	restart_worker2
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(6,6);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,8);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column d int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(9,9,9);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"

	run_sql_source1 "alter table ${shardddl1}.${tb2} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13,13);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,15);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,16);"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column d int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,17,17);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(18,18,18);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(19,19);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(20,20);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,21,21);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(22,22,22);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,23);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(24,24);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column d int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(25,25,25);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(26,26,26);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(27,27,27);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(28,28);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} change b c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(29,29,29);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(30,30,30);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(31,31,31);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(32,32);"

	for ((k = 100; k < 120; k++)); do
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source1 "insert into ${shardddl1}.${tb2} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(${k},${k});"
		sleep 1
	done

	check_log_contain_with_retry "receive redirection operation from master" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "receive redirection operation from master" $WORK_DIR/worker2/log/dm-worker.log
	for ((k = 140; k < 160; k++)); do
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source1 "insert into ${shardddl1}.${tb2} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(${k},${k});"
		sleep 1
	done

	# lock finished at first time, both workers should exit
	check_process_exit worker1 20
	check_process_exit worker2 20
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/FakeRedirect=1*return("`shardddl`.`tb`")'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column d int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(33,33,33);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(34,34,34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,35,35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,36,36);"

	for ((k = 200; k < 280; k++)); do
		run_sql_source1 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source1 "insert into ${shardddl1}.${tb2} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb1} values(${k},${k},${k});"
		k=$((k + 1))
		run_sql_source2 "insert into ${shardddl1}.${tb2} values(${k},${k},${k});"
		sleep 1
	done

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	export GO_FAILPOINTS=''
	restart_worker1
	restart_worker2
}

function DM_RESYNC_NOT_FLUSHED() {
	run_case RESYNC_NOT_FLUSHED "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
		 run_sql_source1 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"; \
		 run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_RESYNC_TXN_INTERRUPT_CASE() {
	# continue at the middle of a dml transaction
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SleepInTxn=return(20)'
	restart_worker1
	restart_worker2

	run_sql_source2 "alter table shardddl1.tb2 change b c int;"
	run_sql_with_txn "shardddl1.tb2" 2 1 10 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_with_txn "shardddl1.t_1" 2 11 50 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	source2worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-02" |
		grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
	# make sure source2 in skip and wait redirect
	check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `b` `c` INT' $WORK_DIR/$source2worker/log/dm-worker.log
	run_sql_source1 "alter table shardddl1.tb1 change b c int;"
	run_sql_with_txn "shardddl1.tb1" 2 51 60 $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_with_txn "shardddl1.t_1" 2 61 70 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
	sed -i "s/\[routes.rule1\]/[routes.rule2]\nschema-pattern = \"shardddl[1-2]\"\ntable-pattern = \"t_1\"\ntarget-schema = \"shardddl\"\ntarget-table = \"t_1\"\n\[routes.rule1\]/g" $WORK_DIR/diff_config.toml
	sed -i "s/route-rules = \[\"rule1\"\]/route-rules = \[\"rule1\"\,\"rule2\"]/g" $WORK_DIR/diff_config.toml
	sed -i "s/target-check-tables = \[\"shardddl.tb\"\]/target-check-tables = \[\"shardddl.tb\",\"shardddl.t_1\"\]/g" $WORK_DIR/diff_config.toml
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30

	# continue after gtid event but before query event
	export GO_FAILPOINTS=""
	restart_worker1
	restart_worker2

	run_sql_source1 "alter table shardddl1.tb1 change c d int;"
	source1worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-01" |
		grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
	# make sure source2 in skip and wait redirect
	check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `c` `d` INT' $WORK_DIR/$source1worker/log/dm-worker.log
	run_sql_source2 "alter table shardddl1.tb2 change c d int;"
	run_sql_with_txn "shardddl1.tb2" 2 101 110 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_with_txn "shardddl1.tb1" 2 111 120 $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_with_txn "shardddl1.tb2" 2 121 130 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_with_txn "shardddl1.t_1" 2 131 140 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30

	run_sql_source2 "alter table shardddl1.tb2 change d e int;"
	# make sure source2 in skip and wait redirect
	source2worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT1" operate-source show -s "mysql-replica-02" |
		grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
	check_log_contain_with_retry 'got a shard DDL lock operation.*CHANGE COLUMN `d` `e` INT' $WORK_DIR/$source2worker/log/dm-worker.log
	run_sql_source1 "alter table shardddl1.tb1 change d e int;"
	run_sql_with_txn "shardddl1.tb2" 2 201 210 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_with_txn "shardddl1.tb1" 2 211 220 $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_with_txn "shardddl1.tb2" 2 221 230 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_sql_with_txn "shardddl1.t_1" 2 231 240 $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30
}

function DM_RESYNC_TXN_INTERRUPT() {
	run_case RESYNC_TXN_INTERRUPT "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.t_1 (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_STRICT_OPTIMISTIC_SINGLE_SOURCE_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(2,2);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add c int not null default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3,3);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add c varchar(30) not null default '10';"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"stage": "Paused"' 1 \
		"because schema conflict detected" 1
}

function DM_STRICT_OPTIMISTIC_SINGLE_SOURCE() {
	run_case STRICT_OPTIMISTIC_SINGLE_SOURCE "single-source-strict-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_STRICT_OPTIMISTIC_DOUBLE_SOURCE_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(3,3,3);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(4,4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add d int not null;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1
}

function DM_STRICT_OPTIMISTIC_DOUBLE_SOURCE() {
	run_case STRICT_OPTIMISTIC_DOUBLE_SOURCE "double-source-strict-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	DM_TABLE_CHECKPOINT_BACKWARD
	DM_RESYNC_NOT_FLUSHED
	DM_RESYNC_TXN_INTERRUPT
	DM_STRICT_OPTIMISTIC_SINGLE_SOURCE
	DM_STRICT_OPTIMISTIC_DOUBLE_SOURCE
	start=131
	end=155
	except=(140 141 144)
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
