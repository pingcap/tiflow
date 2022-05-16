#!/bin/bash

set -eux

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
source $cur/../_utils/handle_error_lib.sh
WORK_DIR=$TEST_DIR/$TEST_NAME

# skip modify column, two sources, no sharding
function DM_SKIP_ERROR_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb2} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} add column new_col1 int;"
	run_sql_source2 "alter table ${db}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${db}.${tb2} values(4,4);"

	# not support in TiDB
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb2} modify id varchar(10);"
	run_sql_source1 "insert into ${db}.${tb1} values('aaa',5);"
	run_sql_source2 "insert into ${db}.${tb2} values('bbb',6);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 2

	# begin to handle error
	# skip all sources
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 3

	# insert fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Error .*: Incorrect int value" 2

	# skip one source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s mysql-replica-01" \
		"only support to handle ddl error currently" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 2

	# skip all sources
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"only support to handle ddl error currently" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 2
}

function DM_SKIP_ERROR() {
	run_case SKIP_ERROR "double-source-no-sharding" "init_table 11 22" "clean_table" ""
}

# skip modify column, two sources, 4 tables, sharding
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
# source2: tb1 first ddl -> tb2 first ddl -> tb1 second ddl -> tb2 second ddl
function DM_SKIP_ERROR_SHARDING_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "insert into ${db}.${tb2} values(2);"
	run_sql_source2 "insert into ${db}.${tb1} values(3);"
	run_sql_source2 "insert into ${db}.${tb2} values(4);"

	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
	run_sql_source2 "alter table ${db}.${tb1} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
	# 11 second ddl
	run_sql_source1 "alter table ${db}.${tb1} add column new_col1 varchar(10);"
	run_sql_source1 "insert into ${db}.${tb1} values(5,'aaa');"
	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
	run_sql_source2 "alter table ${db}.${tb2} CHARACTER SET latin1 COLLATE latin1_danish_ci;"
	# 21 second ddl
	run_sql_source2 "alter table ${db}.${tb1} add column new_col1 varchar(10);"
	run_sql_source2 "insert into ${db}.${tb1} values(6,'bbb');"
	# 12/22 second ddl
	run_sql_source1 "alter table ${db}.${tb2} add column new_col1 varchar(10);"
	run_sql_source1 "insert into ${db}.${tb2} values(7,'ccc');"
	run_sql_source2 "alter table ${db}.${tb2} add column new_col1 varchar(10);"
	run_sql_source2 "insert into ${db}.${tb2} values(8,'ddd');"

	# begin to handle error
	# 11/21 first ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify change collate from latin1_bin to latin1_danish_ci" 2

	# skip 11/21 first ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 3

	if [[ "$1" = "pessimistic" ]]; then
		# 11 second ddl bypass, 12 first ddl: detect conflict
		# 22 first ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 1 \
			"Unsupported modify change collate from latin1_bin to latin1_danish_ci" 1
	else
		# 12/22 first ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Unsupported modify change collate from latin1_bin to latin1_danish_ci" 2
	fi

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s mysql-replica-01,mysql-replica-02" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

function DM_SKIP_ERROR_SHARDING() {
	run_case SKIP_ERROR_SHARDING "double-source-pessimistic" "init_table 11 12 21 22" "clean_table" "pessimistic"
	run_case SKIP_ERROR_SHARDING "double-source-optimistic" "init_table 11 12 21 22" "clean_table" "optimistic"
}

# replace add column unique
# one source, one table, no sharding
function DM_REPLACE_ERROR_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1,1);"

	# error in TiDB
	run_sql_source1 "alter table ${db}.${tb1} add column new_col text, add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(2,2,'haha',2);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 1 \
		"origin SQL: \[alter table ${db}.${tb1} add column new_col text, add column c int unique\]" 1

	# replace sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column new_col text, add column c int; alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3,'hihi',3);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb1};" "count(1): 3"
}

function DM_REPLACE_ERROR() {
	run_case REPLACE_ERROR "double-source-no-sharding" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"" \
		"clean_table" ""
}

# replace add column unique twice
# two source, 4 tables
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
# source2: tb1 first ddl -> tb2 first ddl -> tb1 second ddl -> tb2 second ddl
function DM_REPLACE_ERROR_SHARDING_CASE() {
	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

	# 11 second ddl
	run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3,3);"

	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
	run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

	# 21 second ddl
	run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6,6,6);"

	# 12/22 second ddl
	run_sql_source1 "alter table ${db}.${tb2} add column d int unique;"
	run_sql_source2 "alter table ${db}.${tb2} add column d int unique;"
	run_sql_source1 "insert into ${db}.${tb2} values(7,7,7,7);"
	run_sql_source2 "insert into ${db}.${tb2} values(8,8,8,8);"

	# 11/21 first ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# begin to handle error
	# split 11/21 first ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c)" \
		"\"result\": true" 3

	if [[ "$1" = "pessimistic" ]]; then
		# 11 second ddl bypass, 12 first ddl detect conflict
		# 22 first ddl: detect conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# split 12,22 first ddl into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01,mysql-replica-02 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 3

		# 11/21 second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# split 11/21 second ddl into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3

		# 12/22 second ddl: detect conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# split 11/21 second ddl into two ddls one by one
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
			"\"result\": true" 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
			"\"result\": true" 2
	else
		# 11 second ddl, 22 first ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 11 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 2

		# replace 22 first ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 2

		# 12 first ddl, 21 second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 12 first ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 2

		# replace 21 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 2

		# 12 first ddl, 22 second ddl: unspport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 12/22 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test alter table ${db}.${tb2} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3

	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

function DM_REPLACE_ERROR_SHARDING() {
	run_case REPLACE_ERROR_SHARDING "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"

	run_case REPLACE_ERROR_SHARDING "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

# two source, 4 tables
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
# source2: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
function DM_CROSS_DDL_SHARDING_CASE() {
	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} add column c int;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source1 "insert into ${db}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${db}.${tb1} values(22,22,22);"

	# 11/21 second ddl
	run_sql_source1 "alter table ${db}.${tb1} add column d int;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3,3);"
	run_sql_source2 "alter table ${db}.${tb1} add column d int;"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6,6,6);"

	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} add column c int;"
	run_sql_source2 "alter table ${db}.${tb2} add column c int;"
	run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
	run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

	# 12/22 second ddl
	run_sql_source1 "alter table ${db}.${tb2} add column d int;"
	run_sql_source2 "alter table ${db}.${tb2} add column d int;"
	run_sql_source1 "insert into ${db}.${tb2} values(7,7,7,7);"
	run_sql_source2 "insert into ${db}.${tb2} values(8,8,8,8);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 10"
}

function DM_CROSS_DDL_SHARDING() {
	run_case CROSS_DDL_SHARDING "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"

	run_case CROSS_DDL_SHARDING "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

# replace add column unique twice
# two source, 4 tables
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
# source2: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
function DM_CROSS_DDL_SHARDING_WITH_REPLACE_ERROR_CASE() {
	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source1 "insert into ${db}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${db}.${tb1} values(22,22,22);"

	# 11/21 second ddl
	run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3,3);"
	run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6,6,6);"

	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
	run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

	# 12/22 second ddl
	run_sql_source1 "alter table ${db}.${tb2} add column d int unique;"
	run_sql_source2 "alter table ${db}.${tb2} add column d int unique;"
	run_sql_source1 "insert into ${db}.${tb2} values(7,7,7,7);"
	run_sql_source2 "insert into ${db}.${tb2} values(8,8,8,8);"

	# 11/21 first ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# begin to handle error
	# split 11/21 first ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c)" \
		"\"result\": true" 3

	if [[ "$1" = "pessimistic" ]]; then
		# 11 second ddl bypass, 12 first ddl detect conflict
		# 22 first ddl: detect conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# split 12,22 first ddl into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01,mysql-replica-02 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 3

		# 11/21 second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# split 11/21 second ddl into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3

		# 12/22 second ddl: detect conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# split 11/21 second ddl into two ddls one by one
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
			"\"result\": true" 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column d int;alter table ${db}.${tb2} add unique(d);" \
			"\"result\": true" 2
	else
		# 11 second ddl, 22 first ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 11 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 2

		# replace 21 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 2

		# 12 first ddl, 21 second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 12 first ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 2

		# replace 22 first ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column c int;alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 2

		# 12 first ddl, 22 second ddl: unspport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 12/22 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test alter table ${db}.${tb2} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3

	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 10"
}

function DM_CROSS_DDL_SHARDING_WITH_REPLACE_ERROR() {
	run_case CROSS_DDL_SHARDING_WITH_REPLACE_ERROR "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "pessimistic"

	run_case CROSS_DDL_SHARDING_WITH_REPLACE_ERROR "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" "optimistic"
}

# one source, one table, no sharding
function DM_INJECT_DDL_ERROR_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${db}.${tb2} values(1,1);"

	# error in TiDB
	run_sql_source1 "alter table ${db}.${tb1} add column c int default 100 unique not null;"
	run_sql_source1 "insert into ${db}.${tb1} values(2,2,2);"
	run_sql_source2 "alter table ${db}.${tb2} add column c int default 100 unique not null;"
	run_sql_source2 "insert into ${db}.${tb2} values(2,2,2);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# replace sql but there has a mistake which is use 'c' as pk
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -s mysql-replica-01 alter table ${db}.${tb1} add column c int default 100; alter table ${db}.${tb1} add primary key (c);" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column c int default 100; alter table ${db}.${tb2} add primary key (c);" \
		"\"result\": true" 2

	# error in TiDB
	run_sql_source1 "alter table ${db}.${tb1} modify column c double;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3.5);"
	run_sql_source2 "alter table ${db}.${tb2} modify column c double;"
	run_sql_source2 "insert into ${db}.${tb2} values(3,3,3.5);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	# inject sqls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-01 alter table ${db}.${tb1} drop primary key; alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-02 alter table ${db}.${tb2} drop primary key; alter table ${db}.${tb2} add unique(c);" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb1} where c = 3.5;" "count(1): 1"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where c = 3.5;" "count(1): 1"
}

function DM_INJECT_DDL_ERROR_SHARDING_BASE_CASE() {
	run_sql_source1 "alter table ${db}.${tb1} add column c int default 100 unique not null;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int default 100 unique not null;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

	# first ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# begin to handle error
	# replace first ddl into two ddls, but add c as pk
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int default 100; alter table ${db}.${tb1} add primary key (c);" \
		"\"result\": true" 3

	if [[ "$1" = "pessimistic" ]]; then
		run_sql_source1 "alter table ${db}.${tb1} modify column c double;"
		run_sql_source1 "insert into ${db}.${tb1} values(3,3,3.5);"

		run_sql_source2 "alter table ${db}.${tb1} modify column c double;"
		run_sql_source2 "insert into ${db}.${tb1} values(4,4,4.5);"

		# second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Unsupported modify column: this column has primary key flag" 2

		# inject into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test alter table ${db}.${tb1} drop primary key; alter table ${db}.${tb1} add unique(c);" \
			"\"result\": true" 3
	else
		run_sql_source1 "alter table ${db}.${tb1} drop column c;"
		run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

		run_sql_source2 "alter table ${db}.${tb1} drop column c;"
		run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

		# second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"can't drop column .* with composite index covered or Primary Key covered now" 2

		# inject into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test alter table ${db}.${tb1} drop primary key;" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 4"
}

# two source, 4 tables
function DM_INJECT_DDL_ERROR_SHARDING_BASE2_CASE() {
	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} add column c int default 100 unique not null;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int default 100 unique not null;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} add column c int default 100 unique not null;"
	run_sql_source2 "alter table ${db}.${tb2} add column c int default 100 unique not null;"
	run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
	run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

	# 11/21 first ddl error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# replace 11/21 first ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int default 100; alter table ${db}.${tb1} add primary key (c);" \
		"\"result\": true" 3

	if [[ "$1" = "pessimistic" ]]; then
		# 11/21 second ddl
		run_sql_source1 "alter table ${db}.${tb1} modify column c double;"
		run_sql_source2 "alter table ${db}.${tb1} modify column c double;"
		run_sql_source1 "insert into ${db}.${tb1} values(3,3,3.5);"
		run_sql_source2 "insert into ${db}.${tb1} values(6,6,6.6);"

		# 12/22 second ddl
		run_sql_source1 "alter table ${db}.${tb2} modify column c double;"
		run_sql_source2 "alter table ${db}.${tb2} modify column c double;"
		run_sql_source1 "insert into ${db}.${tb2} values(7,7,7.7);"
		run_sql_source2 "insert into ${db}.${tb2} values(8,8,8.8);"

		# 11/21 inject with 12/22 first ddl conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# replace 12/22 first ddl into two ddls
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -s mysql-replica-01,mysql-replica-02 alter table ${db}.${tb2} add column c int default 100; alter table ${db}.${tb2} add primary key (c);" \
			"\"result\": true" 3

		# 11/21 second ddl: unsupport error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Unsupported modify column: this column has primary key flag" 2

		# inject 11/21 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test alter table ${db}.${tb1} drop primary key; alter table ${db}.${tb1} add unique(c);" \
			"\"result\": true" 3

		# 11/21 inject ddl with 12/22 second ddl: detect conflict
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"detect inconsistent DDL sequence from source" 2

		# inject 12/22 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test -s mysql-replica-01 alter table ${db}.${tb2} drop primary key; alter table ${db}.${tb2} add unique(c);" \
			"\"result\": true" 2
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test -s mysql-replica-02 alter table ${db}.${tb2} drop primary key; alter table ${db}.${tb2} add unique(c);;" \
			"\"result\": true" 2
	else
		# 11/21 second ddl
		run_sql_source1 "alter table ${db}.${tb1} drop column c;"
		run_sql_source2 "alter table ${db}.${tb1} drop column c;"
		run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
		run_sql_source2 "insert into ${db}.${tb1} values(6,6);"

		# 12/22 second ddl
		run_sql_source1 "alter table ${db}.${tb2} drop column c;"
		run_sql_source2 "alter table ${db}.${tb2} drop column c;"
		run_sql_source1 "insert into ${db}.${tb2} values(7,7);"
		run_sql_source2 "insert into ${db}.${tb2} values(8,8);"

		# 12/22 first ddl error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column .* constraint UNIQUE KEY" 2

		# replace 12/22 first ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test alter table ${db}.${tb2} add column c int default 100; alter table ${db}.${tb2} add primary key (c);" \
			"\"result\": true" 3

		#  11/21 second ddl error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"can't drop column .* with composite index covered or Primary Key covered now" 2

		# inject 11/21 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test alter table ${db}.${tb1} drop primary key;" \
			"\"result\": true" 3

		# 12/22 second ddl error
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"can't drop column .* with composite index covered or Primary Key covered now" 2

		# inject 12/22 second ddl
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog inject test alter table ${db}.${tb2} drop primary key;" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

# two source, 4 tables, cross ddls
# source1: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
# source2: tb1 first ddl -> tb1 second ddl -> tb2 first ddl -> tb2 second ddl
function DM_INJECT_DDL_ERROR_SHARDING_CROSS_CASE() {
	# 11/21 first ddl
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

	# 11/21 first ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# 11/21 second ddl
	run_sql_source1 "alter table ${db}.${tb1} modify column c double;"
	run_sql_source2 "alter table ${db}.${tb1} modify column c double;"
	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3.5);"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6,6.6);"

	# 12/22 first ddl
	run_sql_source1 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb2} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb2} values(4,4,4);"
	run_sql_source2 "insert into ${db}.${tb2} values(5,5,5);"

	# 12/22 second ddl
	run_sql_source1 "alter table ${db}.${tb2} modify column c double;"
	run_sql_source2 "alter table ${db}.${tb2} modify column c double;"
	run_sql_source1 "insert into ${db}.${tb2} values(7,7,7.7);"
	run_sql_source2 "insert into ${db}.${tb2} values(8,8,8.8);"

	# begin to handle error
	# split 11/21 first ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int; alter table ${db}.${tb1} add primary key (c);" \
		"\"result\": true" 3

	# 12/22 first ddl with 11/21 inject replace ddl conflict
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"detect inconsistent DDL sequence from source" 2

	# split 12,22 first ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -s mysql-replica-01,mysql-replica-02 alter table ${db}.${tb2} add column c int; alter table ${db}.${tb2} add primary key (c);" \
		"\"result\": true" 3

	# 11/21 second ddl: unsupport error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	# inject 11/21 second ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test alter table ${db}.${tb1} drop primary key; alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 3

	# 12/22 second ddl with 11/21 inject ddl conflict
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"detect inconsistent DDL sequence from source" 2

	# inject 12/22 second ddl into two ddls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-01 alter table ${db}.${tb2} drop primary key; alter table ${db}.${tb2} add unique(c);" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-02 alter table ${db}.${tb2} drop primary key; alter table ${db}.${tb2} add unique(c);;" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 8"
}

# test inject fail on second ddl
# two sources, two tables
function DM_INJECT_ERROR_MULTIPLE_CASE() {
	run_sql_source1 "alter table ${db}.${tb1} add primary key (id);"
	run_sql_source2 "alter table ${db}.${tb1} add primary key (id);"
	run_sql_source1 "alter table ${db}.${tb1} drop column id;"
	run_sql_source2 "alter table ${db}.${tb1} drop column id;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2);"

	# 11, 21 unspported error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"can't drop column id with composite index covered or Primary Key covered now" 2

	# inject and have an error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test \"alter table ${db}.${tb1} add unique(b); alter table ${db}.${tb1} add primary key (a);\"" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Multiple primary key defined" 2

	# revert
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"can't drop column id with composite index covered or Primary Key covered now" 2

	# drop pk
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test \"alter table ${db}.${tb1} drop primary key;\"" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_INJECT_DDL_ERROR() {
	run_case INJECT_DDL_ERROR "double-source-no-sharding" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int unique, b int);\"" \
		"clean_table" ""

	run_case INJECT_DDL_ERROR_SHARDING_BASE "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int unique, b int);\"" \
		"clean_table" "pessimistic"
	run_case INJECT_DDL_ERROR_SHARDING_BASE "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int unique, b int);\"" \
		"clean_table" "optimistic"

	run_case INJECT_DDL_ERROR_SHARDING_BASE2 "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int unique, b int);\"" \
		"clean_table" "pessimistic"
	run_case INJECT_DDL_ERROR_SHARDING_BASE2 "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source1 \"create table ${db}.${tb2} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (a int unique, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int unique, b int);\"" \
		"clean_table" "optimistic"

	run_case INJECT_DDL_ERROR_SHARDING_CROSS "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"; \
	 run_sql_source1 \"create table ${db}.${tb2} (a int unique, b int);\"; \
	 run_sql_source2 \"create table ${db}.${tb1} (a int unique, b int);\"; \
	 run_sql_source2 \"create table ${db}.${tb2} (a int unique, b int);\"" \
		"clean_table" ""

	run_case INJECT_ERROR_MULTIPLE "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (id int unique, a int, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb1} (id int unique, a int, b int);\"" \
		"clean_table" ""
}

function DM_INJECT_DML_ERROR_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1,1);"
	run_sql_source2 "insert into ${db}.${tb2} values(1,1);"

	# error in TiDB
	run_sql_source1 "alter table ${db}.${tb1} add column c varchar(10) unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(2,2,'22');"
	run_sql_source2 "alter table ${db}.${tb2} add column c varchar(10) unique;"
	run_sql_source2 "insert into ${db}.${tb2} values(2,2,'22');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 2

	# replace sql but there has a mistake which is add unque to column 'b'
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -s mysql-replica-01 alter table ${db}.${tb1} add column c varchar(10); alter table ${db}.${tb1} add unique (b);" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add column c varchar(10); alter table ${db}.${tb2} add unique (b);" \
		"\"result\": true" 2

	# error in TiDB, the second dml will be error
	run_sql_source1 "start transaction;insert into ${db}.${tb1} values(3,3,'33');insert into ${db}.${tb1} values(4,2,'44');insert into ${db}.${tb1} values(5,2,'55');commit;"
	run_sql_source2 "start transaction;insert into ${db}.${tb2} values(3,3,'33');insert into ${db}.${tb2} values(4,2,'44');insert into ${db}.${tb2} values(5,2,'55');commit;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Duplicate entry '2' for key 'b'" 4

	# inject sqls
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-01 alter table ${db}.${tb1} drop index b;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -s mysql-replica-02 alter table ${db}.${tb2} drop index b;alter table ${db}.${tb2} add unique(c);" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb1} where b = 2;" "count(1): 3"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb2} where b = 2;" "count(1): 3"
}

# TODO inject at dml will support at other pr
function DM_INJECT_DML_ERROR() {
	run_case INJECT_DML_ERROR "double-source-no-sharding" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int primary key, b int);\"; \
     run_sql_source2 \"create table ${db}.${tb2} (a int primary key, b int);\"" \
		"clean_table" ""
}

function DM_LIST_ERROR_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1, 1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int default 100; alter table ${db}.${tb1} add primary key (c)"
	run_sql_source1 "alter table ${db}.${tb1} modify c varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify c varchar(20);"
	run_sql_source1 "alter table ${db}.${tb1} modify c double;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog list test" \
		"\"msg\": \"\[\]\"" 1

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	third_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $second_pos1)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $first_name1:$second_pos1" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog list test" \
		'\"msg\": \"\[{\\\"op\\\":1,\\\"task\\\":\\\"test\\\",\\\"binlogPos\\\":\\\"('${first_name1}', '${second_pos1}')\\\"}\]\"' 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog inject test -b $first_name1:$third_pos1 alter table ${db}.${tb1} drop primary key; alter table ${db}.${tb1} add unique (c);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog list test" \
		'\"msg\": \"\[{\\\"op\\\":1,\\\"task\\\":\\\"test\\\",\\\"binlogPos\\\":\\\"('${first_name1}', '${second_pos1}')\\\"},{\\\"op\\\":4,\\\"task\\\":\\\"test\\\",\\\"binlogPos\\\":\\\"('${first_name1}', '${third_pos1}')\\\",\\\"sqls\\\":\[\\\"alter table handle_error.tb1 drop primary key;\\\",\\\" alter table handle_error.tb1 add unique (c);\\\"\]}\]\"' 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2,1,2.2);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where c = 2.2;" "count(1): 1"
}

function DM_LIST_ERROR() {
	run_case LIST_ERROR "single-source-no-sharding" \
		"run_sql_source1 \"create table ${db}.${tb1} (a int unique, b int);\"" \
		"clean_table" ""
}

# test handle_error fail on second replace ddl
# two sources, two tables
function DM_REPLACE_ERROR_MULTIPLE_CASE() {
	run_sql_source1 "alter table ${db}.${tb1} add column a int unique, add column b int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column a int unique, add column b int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"

	# 11, 21 unspported error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'a' constraint UNIQUE KEY" 2

	# begin to handle error
	# replace 11/21 ddl, wrong on second replace ddl
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${db}.${tb1} add column a int; alter table ${db}.${tb1} add column b int unique;\"" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'b' constraint UNIQUE KEY" 2

	# now we change the second replace ddl, but first replace ddl will error because it has been executed in TiDB ...
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${db}.${tb1} add column a int; alter table ${db}.${tb1} add column b int;\"" \
		"\"result\": true" 3

	# 11, 21 first replace ddl error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Duplicate column name 'a'" 2

	# test handle-error revert
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'a' constraint UNIQUE KEY" 2

	# now we only replace with ddl2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${db}.${tb1} add column b int;\"" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 4

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_REPLACE_ERROR_MULTIPLE() {
	run_case REPLACE_ERROR_MULTIPLE "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case REPLACE_ERROR_MULTIPLE "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_EXEC_ERROR_SKIP_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1,1,1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2,2,2);"
	run_sql_tidb "insert into ${db}.${tb} values(3,1,1);"
	run_sql_tidb "insert into ${db}.${tb} values(4,2,2);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"

	run_sql_source1 "alter table ${db}.${tb1} add unique index ua(a);"
	run_sql_source2 "alter table ${db}.${tb1} add unique index ua(a);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Error 1062: Duplicate " 1

	run_sql_tidb "insert into ${db}.${tb} values(5,3,3);"
	run_sql_tidb "insert into ${db}.${tb} values(6,4,4);"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 6"
}

function DM_EXEC_ERROR_SKIP() {
	run_case EXEC_ERROR_SKIP "double-source-pessimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"; \
      run_sql_source2 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"" \
		"clean_table" "pessimistic"
	run_case EXEC_ERROR_SKIP "double-source-optimistic" \
		"run_sql_source1 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"; \
      run_sql_source2 \"create table ${db}.${tb1} (id int primary key, a int, b int);\"" \
		"clean_table" "optimistic"
}

function DM_SKIP_INCOMPATIBLE_DDL_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"

	run_sql_source1 "CREATE FUNCTION ${db}.hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	run_sql_source1 "/*!50003 drop function ${db}.hello*/;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"drop function $(hello)" 2 \
		"Please confirm your DDL statement is correct and needed." 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2);"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_SKIP_INCOMPATIBLE_DDL() {
	run_case SKIP_INCOMPATIBLE_DDL "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# FIXME: uncomment this after support replace sql for ddl locks
#function DM_REPLACE_DEFAULT_VALUE_CASE() {
#	run_sql_source1 "insert into ${db}.${tb1} values(1);"
#	run_sql_source2 "insert into ${db}.${tb1} values(2);"
#	run_sql_source2 "insert into ${db}.${tb2} values(3);"
#
#	run_sql_source1 "alter table ${db}.${tb1} add new_col1 int default 1;"
#	run_sql_source1 "insert into ${db}.${tb1} values(4,4);"
#	run_sql_source2 "insert into ${db}.${tb1} values(5);"
#	run_sql_source2 "insert into ${db}.${tb2} values(6);"
#
#	# make sure order is source1.table1, source2.table1, source2.table2
#	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 6"
#
#	run_sql_source2 "alter table ${db}.${tb1} add new_col1 int default 2;"
#	run_sql_source1 "insert into ${db}.${tb1} values(7,7);"
#	run_sql_source2 "insert into ${db}.${tb1} values(8,8);"
#	run_sql_source2 "insert into ${db}.${tb2} values(9);"
#	run_sql_source2 "alter table ${db}.${tb2} add new_col1 int default 3;"
#	run_sql_source1 "insert into ${db}.${tb1} values(10,10);"
#	run_sql_source2 "insert into ${db}.${tb1} values(11,11);"
#	run_sql_source2 "insert into ${db}.${tb2} values(12,12);"
#
#	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
#		"query-status test" \
#		"because schema conflict detected" 1
#
#	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
#		"binlog replace test -s mysql-replica-02 alter table ${db}.${tb1} add new_col1 int default 1;" \
#		"\"result\": true" 2
#
#	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
#		"query-status test" \
#		"because schema conflict detected" 1
#
#	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
#		"binlog replace test -s mysql-replica-02 alter table ${db}.${tb2} add new_col1 int default 1;" \
#		"\"result\": true" 2
#
#	run_sql_source1 "alter table ${db}.${tb1} add new_col2 int;"
#	run_sql_source2 "alter table ${db}.${tb1} add new_col2 int;"
#	run_sql_source2 "alter table ${db}.${tb2} add new_col2 int;"
#	run_sql_source1 "insert into ${db}.${tb1} values(13,13,13);"
#	run_sql_source2 "insert into ${db}.${tb1} values(14,14,14);"
#	run_sql_source2 "insert into ${db}.${tb2} values(15,15,15);"
#
#	# WARN: some data different
#	# all the value before alter table in TiDB will be 1, while upstream table is 1, 2 or 3
#	run_sql_tidb_with_retry "select count(1) from ${db}.${tb}" "count(1): 15"
#
#	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
#		"query-status test" \
#		"\"result\": true" 3
#}

function DM_REPLACE_DEFAULT_VALUE() {
	run_case REPLACE_DEFAULT_VALUE "double-source-optimistic" "init_table 11 21 22" "clean_table" ""
}

function DM_4202_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 1

	start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $start_location" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4202() {
	run_case 4202 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function DM_4204_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4204() {
	run_case 4204 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4206, 4208
function DM_4206_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $first_name1:$second_pos1 -s $source1" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $first_name1:$first_pos1 -s $source1" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $first_name2:$first_pos2 -s $source2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $first_name2:$second_pos2 -s $source2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source2 "insert into ${db}.${tb1} values(4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4206() {
	run_case 4206 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4206 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4207_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 2

	start_location1=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
	start_location2=$(get_start_location 127.0.0.1:$MASTER_PORT $source2)

	if [ "$start_location1" = "$start_location2" ]; then
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog skip test -b $start_location1" \
			"\"result\": true" 3
	else
		# WARN: may skip unknown event like later insert, test will fail
		# It hasn't happened yet.
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog skip test -b $start_location1" \
			"\"result\": true" 3

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"Unsupported modify column" 1

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog skip test -b $start_location2" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source1 "insert into ${db}.${tb1} values(3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 4"
}

function DM_4207() {
	run_case 4207 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4207 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"

	# test different error locations
	run_case 4207 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
	run_case 4207 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

function DM_4209_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source1" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source2" \
		"\"result\": true" 2

	run_sql_source2 "insert into ${db}.${tb1} values(4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4209() {
	run_case 4209 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4209 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4211_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 1

	run_sql_source1 "insert into ${db}.${tb1} values(2,2);"

	start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test --binlog-pos $start_location alter table ${db}.${tb1} add column c int;" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4211() {
	run_case 4211 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function DM_4213_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column .* constraint UNIQUE KEY" 1

	run_sql_source1 "insert into ${db}.${tb1} values(2,2);"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4213() {
	run_case 4213 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function run() {
	init_cluster
	init_database

	DM_INJECT_DDL_ERROR
	# TODO inject at dml will support at other pr
	# DM_INJECT_DML_ERROR
	DM_LIST_ERROR
	DM_SKIP_ERROR
	DM_SKIP_ERROR_SHARDING
	DM_REPLACE_ERROR
	DM_CROSS_DDL_SHARDING
	DM_CROSS_DDL_SHARDING_WITH_REPLACE_ERROR
	DM_REPLACE_ERROR_SHARDING
	DM_REPLACE_ERROR_MULTIPLE
	DM_EXEC_ERROR_SKIP
	DM_SKIP_INCOMPATIBLE_DDL
	# FIXME: uncomment this after support replace sql for ddl locks
	#	DM_REPLACE_DEFAULT_VALUE

	implement=(4202 4204 4206 4207 4209 4211 4213)
	for i in ${implement[@]}; do
		DM_${i}
		sleep 1
	done
}

cleanup_data $db
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
