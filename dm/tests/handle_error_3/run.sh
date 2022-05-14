#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
source $cur/../_utils/handle_error_lib.sh
WORK_DIR=$TEST_DIR/$TEST_NAME

# 4189, 4190, 4191, 4192, 4214, 4218
function DM_4189_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column c int unique;"

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4,4);"

	run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"
	run_sql_source2 "alter table ${db}.${tb1} add column d int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)

	if [ "$second_pos1" = "$second_pos2" ]; then
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -b $first_name1:$second_pos1 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3
	else
		# WARN: may replace unknown event like later insert, test will fail
		# It hasn't happened yet.
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -b $first_name1:$second_pos1 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"unsupported add column 'c' constraint UNIQUE KEY" 2

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"binlog replace test -b $first_name2:$second_pos2 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
			"\"result\": true" 3
	fi

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source1 "insert into ${db}.${tb1} values(5,5,5);"
	run_sql_source2 "insert into ${db}.${tb1} values(6,6,6);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 6"
}

function DM_4189() {
	run_case 4189 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4189 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"

	# test different error locations
	run_case 4189 "double-source-pessimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "pessimistic"
	run_case 4189 "double-source-optimistic" "init_table 11 21;run_sql_source1 \"insert into ${db}.${tb1} values(100);\"" "clean_table" "optimistic"
}

# 4210, 4212
function DM_4210_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"
	run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
	run_sql_source1 "alter table ${db}.${tb1} add column d int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test -b $first_name1:$second_pos1 alter table ${db}.${tb1} add column d int;alter table ${db}.${tb1} add unique(d);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column e int unique;" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'e' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3,3);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb} where id<100;" "count(1): 3"
}

function DM_4210() {
	run_case 4210 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4193, 4221, 4225, 4227, 4228
function DM_4193_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source2 "insert into ${db}.${tb1} values(2);"

	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(30);"

	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(10);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(20);"
	run_sql_source2 "alter table ${db}.${tb1} modify id varchar(30);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	first_pos1=$(get_start_pos 127.0.0.1:$MASTER_PORT $source1)
	first_pos2=$(get_start_pos 127.0.0.1:$MASTER_PORT $source2)
	first_name1=$(get_start_name 127.0.0.1:$MASTER_PORT $source1)
	first_name2=$(get_start_name 127.0.0.1:$MASTER_PORT $source2)

	temp_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $first_pos1)
	temp_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $first_pos2)
	second_pos1=$(get_next_query_pos $MYSQL_PORT1 $MYSQL_PASSWORD1 $temp_pos1)
	second_pos2=$(get_next_query_pos $MYSQL_PORT2 $MYSQL_PASSWORD2 $temp_pos2)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source1 -b $first_name1:$first_pos1" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source2 -b $first_name2:$first_pos2" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test -s $source1 -b $first_name1:$first_pos1" \
		"operator not exist" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test -s $source2 -b $first_name2:$first_pos2" \
		"operator not exist" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source1 -b $first_name1:$second_pos1" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test -b $first_name1:$second_pos1" \
		"operator not exist" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s $source1 -b $first_name1:$second_pos1" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test -s $source2 -b $first_name2:$second_pos2" \
		"operator not exist" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"stage": "Paused"' 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column: this column has primary key flag" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	run_sql_source1 "insert into ${db}.${tb1} values(3);"
	run_sql_source2 "insert into ${db}.${tb1} values(4);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 4"
}

function DM_4193() {
	run_case 4193 "double-source-pessimistic" "init_table 11 21" "clean_table" "pessimistic"
	run_case 4193 "double-source-optimistic" "init_table 11 21" "clean_table" "optimistic"
}

function DM_4230_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_sql_source1 "insert into ${db}.${tb1} values(2,2);"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column d int unique;" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'd' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int;" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(3,3);"

	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 3"
}

function DM_4230() {
	run_case 4230 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

# 4177, 4178, 4179, 4181, 4183, 4188, 4180, 4182
function DM_4177_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} modify id varchar(10);"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 1

	start_location=$(get_start_location 127.0.0.1:$MASTER_PORT $source1)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip wrong-task" \
		"\"result\": false" 1 \
		"task wrong-task.*not exist" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip" \
		"Usage" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b $start_location -s wrong-source" \
		"source wrong-source.*not found" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b mysql-bin|1111 -s wrong-source" \
		"invalid --binlog-pos" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unsupported modify column" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": false" 1 \
		"source.*has no error" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${db}.${tb1} add column c int;\"" \
		"\"result\": false" 1 \
		"source.*has no error" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -b mysql-bin.000000:00000" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog aaa test" \
		"Available Commands" 1

	run_sql_source1 "insert into ${db}.${tb1} values(2);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4177() {
	run_case 4177 "single-source-no-sharding" "init_table 11" "clean_table" ""
	# 4184
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"task.*not exist" 1
	# 4223
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add column c int" \
		"task.*not exist" 1
	# 4197
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test" \
		"task.*not exist" 1
}

function DM_4231_CASE() {
	run_sql_source1 "insert into ${db}.${tb1} values(1);"
	run_sql_source1 "alter table ${db}.${tb1} add column c int unique;"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test \"alter table ${db}.${tb1} add column c int; alter table ${db}.${tb1} add column d int unique;\"" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'd' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog revert test" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"unsupported add column 'c' constraint UNIQUE KEY" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog replace test alter table ${db}.${tb1} add unique(c);" \
		"\"result\": true" 2

	run_sql_source1 "insert into ${db}.${tb1} values(2,2);"
	run_sql_tidb_with_retry "select count(1) from ${db}.${tb};" "count(1): 2"
}

function DM_4231() {
	run_case 4231 "single-source-no-sharding" "init_table 11" "clean_table" ""
}

function run() {
	init_cluster
	init_database

	implement=(4189 4210 4193 4230 4177 4231)
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
