#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_DIFFERENT_SCHEMA_FULL_CASE() {
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 4"
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(5);"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values(6,'6');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,'77');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,'8','88');"

    run_sql_source1 "alter table ${shardddl1}.${tb1} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,b,c)
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(9,'999');"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values(10,'1010');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'111111');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'1212','121212');"

    run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,c)
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,'131313');"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values(14,'1414');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,'151515');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,'161616');"

    run_sql_source1 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a); source2.tb1(a,c); source2.tb2(a,c)
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,'171717');"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values(18);"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(19,'191919');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(20,'202020');"

    run_sql_source1 "alter table ${shardddl1}.${tb2} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,c); source2.tb1(a,c); source2.tb2(a,c)
    run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,'212121');"
    run_sql_source1 "insert into ${shardddl1}.${tb2} values(22,'222222');"
    run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,'232323');"
    run_sql_source2 "insert into ${shardddl1}.${tb2} values(24,'242424');"

	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 24"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DIFFERENT_SCHEMA_FULL() {
	# create table with different schema, init data, and create table in downstream manually
	run_case DIFFERENT_SCHEMA_FULL "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key);\"; \
     run_sql_source1 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c text);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c text);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb1} values(1);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb2} values(2,'22');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb1} values(3,'333');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb2} values(4,'44','444');\"; \
     run_sql_tidb \"create database if not exists ${shardddl};\"; \
     run_sql_tidb \"create table ${shardddl}.${tb} (a int primary key, b varchar(10), c text);\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database

	DM_DIFFERENT_SCHEMA_FULL
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
