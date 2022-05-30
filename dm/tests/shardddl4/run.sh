#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_119_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx(c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx(c1,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9);"

	# FIXME: DM should detect conflicts and give human readable error messages.
	# For example:
	# if [[ "$1" = "pessimistic" ]]; then
	#     check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	# else
	#     run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	#         "query-status test" \
	#         "because schema conflict detected" 1
	# fi
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10 "fail"
}

# Add index with the same name but with different fields.
function DM_119 {
	run_case 119 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int);\"" \
		"clean_table" "pessimistic"
	run_case 119 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int);\"" \
		"clean_table" "optimistic"
}

function DM_120_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple indexes to a single table.
function DM_120 {
	run_case 120 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "pessimistic"
	run_case 120 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_121_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx(c1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add index idx(c1,c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	# FIXME: DM should detect conflicts and give human readable error messages.
	# For example:
	# if [[ "$1" = "pessimistic" ]]; then
	#     check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	# else
	#     run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	#         "query-status test" \
	#         "because schema conflict detected" 1
	# fi
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10 "fail"
}

# Add index with the same name but with different fields.
function DM_121 {
	run_case 121 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "pessimistic"
	run_case 121 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_122_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Drop multiple indexes from a single table.
function DM_122 {
	run_case 122 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "pessimistic"
	run_case 122 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "optimistic"
}

function DM_123_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10,10,10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11,11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12,12,12,12);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Adjust multiple indexes combination.
function DM_123 {
	run_case 123 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "pessimistic"
	run_case 123 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "optimistic"
}

function DM_124_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Add multiple indexes and then rollback.
function DM_124 {
	# run_case 124 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
	# "clean_table" "pessimistic"
	run_case 124 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int);\"" \
		"clean_table" "optimistic"
}

function DM_125_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), add index idx2(c2);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Drop multiple indexes and then rollback.
function DM_125 {
	# run_case 125 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
	# "clean_table" "pessimistic"
	run_case 125 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1), index idx2(c2));\"" \
		"clean_table" "optimistic"
}

function DM_126_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c2,c1), drop index idx2, add index idx2(c4,c3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop index idx1, add index idx1(c1,c2), drop index idx2, add index idx2(c3,c4);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Ajust multiple indexes combination and then rollback.
function DM_126 {
	# run_case 126 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
	# "clean_table" "pessimistic"
	run_case 126 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, c3 int, c4 int, index idx1(c1, c2), index idx2(c3, c4));\"" \
		"clean_table" "optimistic"
}

function DM_127_CASE {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx2(c2), drop index idx1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add index idx1(c1), drop index idx2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	# FIXME: dm-master should remove this lock after all shards are synced.
	run_dm_ctl_with_rematch $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"unsynced\": \[[\n ]*\]" 1
}

# Add and drop index at the same time and then rollback.
function DM_127 {
	# run_case 127 "double-source-pessimistic" \
	# "run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
	#  run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1));\"" \
	# "clean_table" "pessimistic"
	run_case 127 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c1 int, c2 int, index idx1(c1));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, c1 int, c2 int, index idx1(c1));\"" \
		"clean_table" "optimistic"
}

function DM_128_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int not null;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,12);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change NULL to NOT NULL.
function DM_128() {
	run_case 128 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"
	run_case 128 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

function DM_129_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int default 10;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2}(a) values(12);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

# Change NOT NULL to NULL with the same default value.
function DM_129() {
	run_case 129 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "pessimistic"
	run_case 129 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "optimistic"
}

function DM_130_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} modify b int default 0;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	if [[ "$1" = "optimistic" ]]; then
		check_log_contain_with_retry "finish to handle ddls in optimistic shard mode.*alter table ${shardddl1}.${tb1} modify b int default 0" \
			$WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	fi

	run_sql_source2 "alter table ${shardddl1}.${tb1} modify b int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9);"

	run_sql_source2 "alter table ${shardddl1}.${tb2} modify b int default -1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(10);"
	run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(11);"
	run_sql_source2 "insert into ${shardddl1}.${tb2}(a) values(12);"
	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "is different with" $WORK_DIR/master/log/dm-master.log
	else
		# skip first ddl
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"blockingDDLs" 2 \
			"ALTER TABLE \`${shardddl}\`.\`${tb}\` MODIFY COLUMN \`b\` INT DEFAULT -1" 1 \
			"blockDDLOwner" 2 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb1}\`\"" 1
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock unlock test-\`${shardddl}\`.\`${tb}\` -s ${SOURCE_ID2} -d ${shardddl1} -t ${tb1} --action skip" \
			"\"result\": true" 1
		# skip second ddl
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"blockingDDLs" 2 \
			"ALTER TABLE \`${shardddl}\`.\`${tb}\` MODIFY COLUMN \`b\` INT DEFAULT -1" 1 \
			"blockDDLOwner" 2 \
			"\"${SOURCE_ID2}-\`${shardddl1}\`.\`${tb2}\`\"" 1
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock unlock test-\`${shardddl}\`.\`${tb}\` -s ${SOURCE_ID2} -d ${shardddl1} -t ${tb2} --action skip" \
			"\"result\": true" 1
		# check whether both locks are unlocked
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\"result\": true" 3 \
			"\"blockDDLOwner\": \"\"" 2
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a) values(13);"
		run_sql_source2 "insert into ${shardddl1}.${tb1}(a) values(14);"
		run_sql_source2 "insert into ${shardddl1}.${tb2}(a) values(15);"
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	fi
}

# Change NOT NULL to NULL with the different default value.
function DM_130() {
	run_case 130 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "pessimistic"
	run_case 130 "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b int not null);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b int not null);\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database
	start=119
	end=130
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
