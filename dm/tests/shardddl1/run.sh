#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_001_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	# schema tracker could track per table without error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"synced\": true" 1
	# only downstream sees a duplicate error, but currently ignored by DM
	check_log_contain_with_retry "Duplicate column name 'new_col1'" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
}

function DM_001() {
	run_case 001 "single-source-no-sharding" "init_table 111 112" "clean_table" ""
}

function DM_002_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_002() {
	run_case 002 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_003_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 3 "fail"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_003() {
	run_case 003 "single-source-pessimistic" "init_table 111 112" "clean_table" "pessimistic"
}

function DM_004_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 1"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_004() {
	run_case 004 "single-source-optimistic" "init_table 111 112" "clean_table" "optimistic"
}

function DM_005_CASE() {
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values (1,1)"
	run_sql_source1 "alter table ${shardddl1}.${tb2} add column new_col1 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values (2,2)"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_005() {
	run_case 005 "single-source-pessimistic" "init_table 111 112" "clean_table" ""
}

function DM_RENAME_TABLE_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column a int;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column a int;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column a int;"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6);"

	run_sql_source1 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3};"
	run_sql_source2 "rename table ${shardddl1}.${tb1} to ${shardddl1}.${tb3};"
	run_sql_source2 "rename table ${shardddl1}.${tb2} to ${shardddl1}.${tb4};"

	run_sql_source1 "insert into ${shardddl1}.${tb3} values(7,7)"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb4} values(9,9);"

	run_sql_source1 "alter table ${shardddl1}.${tb3} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb3} add column b int;"
	run_sql_source2 "alter table ${shardddl1}.${tb4} add column b int;"

	run_sql_source1 "insert into ${shardddl1}.${tb3} values(10,10,10)"
	run_sql_source2 "insert into ${shardddl1}.${tb3} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb4} values(12,12,12);"

	if [[ "$1" = "pessimistic" ]]; then
		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			"\`RENAME TABLE\` statement not supported in $1 mode" 2
	fi
}

function DM_RENAME_TABLE() {
	run_case RENAME_TABLE "double-source-pessimistic" "init_table 111 211 212" "clean_table" "pessimistic"
	run_case RENAME_TABLE "double-source-optimistic" "init_table 111 211 212" "clean_table" "optimistic"
}

function DM_RENAME_COLUMN_OPTIMISTIC_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,'ddd');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,'eee');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,'fff');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,'ggg');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,'hhh');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,'iii');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} change a c int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(10,'jjj');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'kkk');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'lll');"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 2

	# first, execute sql in downstream TiDB
	run_sql_tidb "alter table ${shardddl}.${tb} change a c int;"

	# second, skip the unsupported ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test" \
		"\"result\": true" 3

	# dmls fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Paused" 1 \
		"Unknown column 'a' in 'field list'" 1

	# third, set schema to be same with upstream
	echo 'CREATE TABLE `tb1` ( `c` int NOT NULL, `b` varchar(10) DEFAULT NULL, PRIMARY KEY (`c`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin' >${WORK_DIR}/schema1.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update -s mysql-replica-02 test ${shardddl1} ${tb1} ${WORK_DIR}/schema1.sql --flush --sync" \
		"\"result\": true" 2

	# fourth, resume-task. don't check "result: true" here, because worker may run quickly and meet the error from tb2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test"

	# WARN: if it's sequence_sharding, the other tables will not be fixed
	# source2.table2's dml fails
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unknown column 'a' in 'field list'" 1

	# WARN: set schema of source2.table2
	# Actually it should be tb2(a,b), dml is {a: 9, b: 'iii'}
	# Now we set it to tb2(c,b), dml become {c: 9, b: 'iii'}
	# This may only work for a "rename ddl"
	echo 'CREATE TABLE `tb2` ( `c` int NOT NULL, `b` varchar(10) DEFAULT NULL, PRIMARY KEY (`c`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin' >${WORK_DIR}/schema2.sql
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog-schema update -s mysql-replica-02 test ${shardddl1} ${tb2} ${WORK_DIR}/schema2.sql --flush --sync" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test -s mysql-replica-02"

	# source2.table2's ddl fails
	# Unknown column 'a' in 'tb2'
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Unknown column 'a' in 'tb2'" 1

	# skip source2.table2's ddl
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"binlog skip test -s mysql-replica-02" \
		"\"result\": true" 2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3

	# now, it works as normal
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,'mmm',13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14,'nnn');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(15,'ooo');"

	run_sql_source2 "alter table ${shardddl1}.${tb1} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(16,'ppp',16);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(17,'qqq',17);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(18,'rrr');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} add column d int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(19,'sss',19);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(20,'ttt',20);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(21,'uuu',21);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"no DDL lock exists" 1
}

# workaround of rename column in optimistic mode currently until we support it
# maybe also work for some other unsupported ddls in optimistic mode
function DM_RENAME_COLUMN_OPTIMISTIC() {
	run_case RENAME_COLUMN_OPTIMISTIC "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;\"" \
		"clean_table" "optimistic"
}

function DM_RemoveLock_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,'ccc');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column c double;"
	check_log_contain_with_retry "wait new ddl info putted into etcd in ${1}" $WORK_DIR/master/log/dm-master.log
	check_metric_not_contains $MASTER_PORT "dm_master_shard_ddl_error" 3
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column b;"

	if [[ "$1" = "pessimistic" ]]; then
		check_log_contain_with_retry "found new DDL info" $WORK_DIR/master/log/dm-master.log
	else
		check_log_contain_with_retry "fail to delete shard DDL infos and lock operations" $WORK_DIR/master/log/dm-master.log
	fi

	run_sql_source1 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} change a a bigint default 10;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	run_sql_source2 "alter table ${shardddl1}.${tb2} change a a bigint default 10;"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_RemoveLock() {
	ps aux | grep dm-master | awk '{print $2}' | xargs kill || true
	check_master_port_offline 1
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/dm/master/shardddl/SleepWhenRemoveLock=return(30)"
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w" \
		"bound" 2

	run_case RemoveLock "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"
	run_case RemoveLock "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"

	export GO_FAILPOINTS=""
	ps aux | grep dm-master | awk '{print $2}' | xargs kill || true
	check_master_port_offline 1
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -w" \
		"bound" 2
}

function DM_RestartMaster_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,'aaa');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,'bbb');"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c double;"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column c text;"

	if [[ "$1" = "pessimistic" ]]; then
		# count of 2: `blockingDDLs` and `unresolvedGroups`
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` DOUBLE' 2 \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` TEXT' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c`' 1
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'because schema conflict detected' 1
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock" \
			'mysql-replica-01-`shardddl1`.`tb1`' 1 \
			'mysql-replica-02-`shardddl1`.`tb1`' 1
	fi

	restart_master

	if [[ "$1" = "pessimistic" ]]; then
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` DOUBLE' 2 \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c` TEXT' 2
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock" \
			'ALTER TABLE `shardddl`.`tb` ADD COLUMN `c`' 1
	else
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status test" \
			'because schema conflict detected' 1
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"shard-ddl-lock" \
			'mysql-replica-01-`shardddl1`.`tb1`' 1 \
			'mysql-replica-02-`shardddl1`.`tb1`' 1
	fi
}

function DM_RestartMaster() {
	run_case RestartMaster "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" \
		"clean_table" "pessimistic"

	run_case RestartMaster "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, b varchar(10));\"" \
		"clean_table" "optimistic"
}

function DM_UpdateBARule_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1);"
	run_sql_source1 "insert into ${shardddl2}.${tb1} values(2);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(3);"
	run_sql_source2 "insert into ${shardddl2}.${tb1} values(4);"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col1 int"
	run_sql_source1 "alter table ${shardddl2}.${tb1} add column new_col1 int"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col1 int"
	run_sql_source2 "alter table ${shardddl2}.${tb1} add column new_col1 int"

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5,5);"
	run_sql_source1 "insert into ${shardddl2}.${tb1} values(6,6);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,7);"
	run_sql_source2 "insert into ${shardddl2}.${tb1} values(8,8);"

	# source1 db2.tb1 add column and then drop column
	run_sql_source1 "alter table ${shardddl2}.${tb1} add column new_col2 int"
	run_sql_source1 "insert into ${shardddl2}.${tb1} values(9,9,9);"
	run_sql_source1 "alter table ${shardddl2}.${tb1} drop column new_col2"
	run_sql_source1 "insert into ${shardddl2}.${tb1} values(10,10);"

	# source1 db1.tb1 add column
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column new_col3 int"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(11,11,11);"

	# source2 db1.tb1 drop column
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column new_col1"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(12);"

	# source2 db2.tb1 do a unsupported DDL
	run_sql_source2 "alter table ${shardddl2}.${tb1} rename column id to new_id;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"because schema conflict detected" 1

	# user found error and then change block-allow-list, restart task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3

	cp $cur/conf/double-source-optimistic.yaml $WORK_DIR/task.yaml
	sed -i 's/do-dbs: \["shardddl1","shardddl2"\]/do-dbs: \["shardddl1"\]/g' $WORK_DIR/task.yaml
	echo 'ignore-checking-items: ["schema_of_shard_tables"]' >>$WORK_DIR/task.yaml

	# source1: db1.tb1(id,new_col1,new_col3)
	# source2: db1.tb1(id)
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/task.yaml" \
		"\"result\": true" 3

	# no lock exist when task begin
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1

	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,13,13);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(14);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 14"

	restart_master

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1

	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column new_col1"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column new_col3 int"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(15,15);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(16,16);"
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb};" "count(1): 16"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"no DDL lock exists" 1
}

function DM_UpdateBARule() {
	run_case UpdateBARule "double-source-optimistic" "init_table 111 121 211 221" "clean_table" "optimistic"
}

function DM_ADD_DROP_COLUMNS_CASE() {
	# add cols
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,now(),1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,now());"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,now(),4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,now(),5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,now());"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col1 int, add column col2 int, add column col3 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,now(),7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,now(),8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,now(),9,9,9);"

	# drop cols
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(11,now(),11);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(12,now(),12,12,12);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(13,now(),13,13,13);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(14,now(),14);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,now(),15);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,now(),16,16,16);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col1, drop column col2;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,now(),17);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(18,now(),18);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(19,now(),19);"

	# add and drop
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,now(),21);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(22,now(),22);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(23,now(),23);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(24,now(),24);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(25,now(),25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(26,now(),26);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col4 int, drop column col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(27,now(),27);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(28,now(),28);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(29,now(),29);"

	# drop and add
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,now(),31);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,now(),32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,now(),33);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34,now(),34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,now(),35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,now(),36);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col4, add column col5 int;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,now(),37);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,now(),38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,now(),39);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_ADD_DROP_COLUMNS() {
	run_case ADD_DROP_COLUMNS "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "pessimistic"
	run_case ADD_DROP_COLUMNS "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, id datetime);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, id datetime);\"" \
		"clean_table" "optimistic"
}

function DM_COLUMN_INDEX_CASE() {
	# add col and index
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,1,1,1);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(2,2,2);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(3,3,3);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(4,4,4,4);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(5,5,5,5);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(6,6,6);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col3 int, add index idx_col1(col1);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(7,7,7,7);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(8,8,8,8);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(9,9,9,9);"

	# drop col and index
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(11,11,11);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(12,12,12,12);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(13,13,13,13);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(14,14,14);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,15,15);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,16,16,16);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col2, drop index idx_col1;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,17,17);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(18,18,18);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(19,19,19);"

	# drop col, add index
	run_sql_source1 "alter table ${shardddl1}.${tb1} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,21);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(22,22,22);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(23,23,23);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(24,24);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(25,25);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(26,26,26);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column col1, add index idx_col3(col3);"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(27,27);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(28,28);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(29,29);"

	# add col, drop index
	run_sql_source1 "alter table ${shardddl1}.${tb1} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(31,31,31);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(32,32);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(33,33);"
	run_sql_source2 "alter table ${shardddl1}.${tb1} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(34,34,34);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(35,35,35);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(36,36);"
	run_sql_source2 "alter table ${shardddl1}.${tb2} add column col4 int, drop index idx_col3;"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(37,37,37);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(38,38,38);"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(39,39,39);"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_COLUMN_INDEX() {
	run_case COLUMN_INDEX "double-source-pessimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, col1 int, col2 int);\"" \
		"clean_table" "pessimistic"
	run_case COLUMN_INDEX "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, col1 int, col2 int);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, col1 int, col2 int);\"" \
		"clean_table" "optimistic"
}

function DM_COMPACT_CASE() {
	END=100
	for i in $(seq 1 $END); do
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values($i,$i)"
		run_sql_source1 "update ${shardddl1}.${tb1} set c=1 where a=$i"
		run_sql_source1 "update ${shardddl1}.${tb1} set c=c+1 where a=$i"
		run_sql_source1 "update ${shardddl1}.${tb1} set b=b+1 where a=$i"
		run_sql_source1 "update ${shardddl1}.${tb1} set a=a+100 where a=$i"
		run_sql_source1 "delete from ${shardddl1}.${tb1} where a=$((i + 100))"
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values($i,$i)"
	done
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30
	compactCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep "finish to compact" | wc -l)
	if [[ "$compactCnt" -le 100 ]]; then
		echo "compact $compactCnt dmls which is less than 100"
		exit 1
	fi
}

function DM_COMPACT() {
	# mock downstream has a high latency and upstream has a high workload
	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/BlockExecuteSQLs=return(1);github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(5)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_case COMPACT "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int unique, c int);\"" \
		"clean_table" ""
}

function DM_COMPACT_USE_DOWNSTREAM_SCHEMA_CASE() {
	END=10
	# As this kind of sql is no use, like "update tb1 set c=1 where a=100" which is behind of "insert into tb1(a,b,c) values(100,1,1)"
	# We should avoid this kind of sql to make sure the count of dmls
	for i in $(seq 0 $END); do
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b,c) values($((i + 100)),$i,$i)"
		run_sql_source1 "update ${shardddl1}.${tb1} set c=20 where a=$((i + 100))"
		run_sql_source1 "update ${shardddl1}.${tb1} set c=c+1 where a=$((i + 100))"
		# Use downstream uk 'b' as key and this sql which modifiies 'b' will be splited to two job(delete+insert)
		run_sql_source1 "update ${shardddl1}.${tb1} set b=b+1 where a=$((i + 100))"
		run_sql_source1 "update ${shardddl1}.${tb1} set a=a+100 where a=$((i + 100))"
		run_sql_source1 "delete from ${shardddl1}.${tb1} where a=$((i + 200))"
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b,c) values($((i + 100)),$i,$i)"
	done
	run_sql_tidb_with_retry_times "select count(1) from ${shardddl}.${tb};" "count(1): 11" 30
	run_sql_tidb "create table ${shardddl}.${tb}_temp (a int primary key auto_increment, b int unique not null, c int) auto_increment = 100; 
		insert into ${shardddl}.${tb}_temp (a, b, c) select a, b, c from ${shardddl}.${tb}; 
		drop table ${shardddl}.${tb}; rename table ${shardddl}.${tb}_temp to ${shardddl}.${tb};"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30
	compactCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep "finish to compact" | wc -l)
	# As compact is affected by "j.tp == flush", the check count of compact use "-le 50"
	if [[ "$compactCnt" -le 50 ]]; then
		echo "compact $compactCnt dmls which is less than 50"
		exit 1
	fi
}

function DM_COMPACT_USE_DOWNSTREAM_SCHEMA() {
	# downstream pk/uk/column is diffrent with upstream, compact use downstream schema.
	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	# DownstreamIdentifyKeyCheckInCompact=return(20) will check whether the key value in compact is less than 20, if false, it will be panic.
	# This goal is check whether it use downstream schema in compator.
	# if use downstream schema, key will be 'b' with value less than 20.
	# If use upstream schema, key will be 'a' with value greater than 100.
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipFlushCompactor=return();github.com/pingcap/tiflow/dm/syncer/DownstreamIdentifyKeyCheckInCompact=return(20)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_case COMPACT_USE_DOWNSTREAM_SCHEMA "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int unique not null, c int);\"; 
		run_sql_tidb \"drop database if exists ${shardddl}; create database ${shardddl}; create table ${shardddl}.${tb} (a int, b int unique not null, c int, d int primary key auto_increment) auto_increment = 100;\"" \
		"clean_table" ""
}

function DM_MULTIPLE_ROWS_CASE() {
	END=100
	for i in $(seq 1 10 $END); do
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values($i,$i),($((i + 1)),$((i + 1))),($((i + 2)),$((i + 2))),($((i + 3)),$((i + 3))),($((i + 4)),$((i + 4))),\
		($((i + 5)),$((i + 5))),($((i + 6)),$((i + 6))),($((i + 7)),$((i + 7))),($((i + 8)),$((i + 8))),($((i + 9)),$((i + 9)))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set c=1 where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set b = 0 - b where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set a = 0 - a where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "delete from ${shardddl1}.${tb1} where a<=$((0 - i)) and a>$((-10 - i))"
	done

	# wait safemode exit
	check_log_contain_with_retry "disable safe-mode after task initialization finished" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	# insert again without safmode
	for i in $(seq 1 10 $END); do
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values($i,$i),($((i + 1)),$((i + 1))),($((i + 2)),$((i + 2))),($((i + 3)),$((i + 3))),($((i + 4)),$((i + 4))),\
		($((i + 5)),$((i + 5))),($((i + 6)),$((i + 6))),($((i + 7)),$((i + 7))),($((i + 8)),$((i + 8))),($((i + 9)),$((i + 9)))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set c=1 where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set b = 0 - b where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "update ${shardddl1}.${tb1} set a = 0 - a where a>=$i and a<$((i + 10))"
	done
	for i in $(seq 1 10 $END); do
		run_sql_source1 "delete from ${shardddl1}.${tb1} where a<=$((0 - i)) and a>$((-10 - i))"
	done

	# insert new values, otherwise there may not be any data in downstream in middle stage and check_sync_diff return true immediately
	for i in $(seq 101 10 200); do
		run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values($i,$i),($((i + 1)),$((i + 1))),($((i + 2)),$((i + 2))),($((i + 3)),$((i + 3))),($((i + 4)),$((i + 4))),\
		($((i + 5)),$((i + 5))),($((i + 6)),$((i + 6))),($((i + 7)),$((i + 7))),($((i + 8)),$((i + 8))),($((i + 9)),$((i + 9)))"
	done

	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb} where a>100 and a<=200;" "count(1): 100"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 30
	insertMergeCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep '\[op=insert\]' | wc -l)
	replaceMergeCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep '\[op=replace\]' | wc -l)
	updateMergeCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep '\[op="insert on duplicate update"\]' | wc -l)
	deleteMergeCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep '\[op=delete\]' | wc -l)
	echo $insertMergeCnt $replaceMergeCnt $updateMergeCnt $deleteMergeCnt
	if [[ "$insertMergeCnt" -le 5 || "$updateMergeCnt" -le 5 || "$deleteMergeCnt" -le 5 || "$replaceMergeCnt" -le 5 ]]; then
		echo "merge dmls less than 5, insertMergeCnt: $insertMergeCnt, replaceMergeCnt: $replaceMergeCnt, updateMergeCnt: $updateMergeCnt, deleteMergeCnt: $deleteMergeCnt"
		exit 1
	fi
}

function DM_MULTIPLE_ROWS() {
	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/BlockExecuteSQLs=return(1);github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(5)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_case MULTIPLE_ROWS "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int unique, c int);\"" \
		"clean_table" ""

	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	export GO_FAILPOINTS=''
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
}

function DM_CAUSALITY_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,2)"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,3)"
	run_sql_source1 "update ${shardddl1}.${tb1} set a=3, b=4 where b=3"
	run_sql_source1 "delete from ${shardddl1}.${tb1} where a=1"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,3)"

	check_log_contain_with_retry "meet causality key, will generate a conflict job to flush all sqls" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_CAUSALITY() {
	run_case CAUSALITY "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int unique);\"" \
		"clean_table" ""
}

function DM_CAUSALITY_USE_DOWNSTREAM_SCHEMA_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,2)"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(2,3)"
	run_sql_source1 "update ${shardddl1}.${tb1} set a=3, b=4 where b=3"
	run_sql_source1 "delete from ${shardddl1}.${tb1} where a=1"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(1,3)"

	run_sql_tidb_with_retry_times "select count(1) from ${shardddl}.${tb} where a =1 and b=3;" "count(1): 1" 30
	run_sql_tidb "create table ${shardddl}.${tb}_temp (a int primary key, b int unique); 
		insert into ${shardddl}.${tb}_temp (a, b) select a, b from ${shardddl}.${tb};
		drop table ${shardddl}.${tb}; rename table ${shardddl}.${tb}_temp to ${shardddl}.${tb};"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	causalityCnt=$(cat $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log | grep "meet causality key, will generate a conflict job to flush all sqls" | wc -l)
	if [[ "$causalityCnt" -ne 0 ]]; then
		echo "causalityCnt is $causalityCnt, but it should be 0"
		exit 1
	fi
}

function DM_CAUSALITY_USE_DOWNSTREAM_SCHEMA() {
	# downstream pk/uk/column is diffrent with upstream, causality use downstream schema.
	run_case CAUSALITY_USE_DOWNSTREAM_SCHEMA "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int unique);\"; 
		run_sql_tidb \"drop database if exists ${shardddl}; create database ${shardddl}; create table ${shardddl}.${tb} (a int, b int unique, c int primary key auto_increment) auto_increment = 100;\"" \
		"clean_table" ""
}

function DM_DML_EXECUTE_ERROR_CASE() {
	run_sql_source1 "insert into ${shardddl1}.${tb1}(a,b) values(1,1)"
	run_sql_source1 "update ${shardddl1}.${tb1} set b=b+1 where a=1"

	check_log_contain_with_retry "length of queries not equals length of jobs" $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"RawCause\": \"ErrorOnLastDML\"" 1 \
		"Paused" 1
}

function DM_DML_EXECUTE_ERROR() {
	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	check_port_offline $WORKER2_PORT 20
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/ErrorOnLastDML=return()'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_case DML_EXECUTE_ERROR "single-source-no-sharding" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key, b int);\"" \
		"clean_table" ""
}

function run() {
	init_cluster
	init_database

	DM_COMPACT
	DM_COMPACT_USE_DOWNSTREAM_SCHEMA
	DM_MULTIPLE_ROWS
	DM_CAUSALITY
	DM_CAUSALITY_USE_DOWNSTREAM_SCHEMA
	DM_UpdateBARule
	DM_RENAME_TABLE
	DM_RENAME_COLUMN_OPTIMISTIC
	DM_RemoveLock
	DM_RestartMaster
	DM_ADD_DROP_COLUMNS
	DM_COLUMN_INDEX
	DM_DML_EXECUTE_ERROR
	start=1
	end=5
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
