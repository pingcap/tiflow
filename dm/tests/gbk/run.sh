#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="gbk"

function test_from_tidb() {
	cleanup_data_upstream gbk3

	# check table can inherited charset from database
	run_sql_source1 "use gbk; create table ddl1(b char(20));"
	run_sql_tidb_with_retry "show create table gbk.ddl1;" "CHARSET=gbk"

	# can't test "create table as select", because it is not supported in GTID mode

	# test create table like
	run_sql_source1 "use gbk; create table ddl2(a char(20) charset gbk, b char(20) charset utf8mb4);"
	run_sql_source1 "use gbk; create table ddl2_copy like ddl2; insert into ddl2_copy values('一二三', '一二三');"
	run_sql_tidb_with_retry "select hex(a) from gbk.ddl2_copy;" "D2BBB6FEC8FD"
	run_sql_tidb_with_retry "select hex(b) from gbk.ddl2_copy;" "E4B880E4BA8CE4B889"

	# test create partition table
	run_sql_source1 "use gbk; create table ddl3(id int, a char(20) charset gbk) partition by hash(id) partitions 4;"
	run_sql_tidb_with_retry "show create table gbk.ddl3;" "CHARSET=gbk"
	run_sql_tidb_with_retry "show create table gbk.ddl3;" "ARTITION BY HASH (\`id\`) PARTITIONS 4"

	# test alter database charset
	run_sql_source1 "create database gbk3 charset utf8; alter database gbk3 charset gbk;"
	run_sql_tidb_with_retry "show create database gbk3;" "CHARACTER SET gbk"

	# test client use GBK encoding
	run_sql "create table gbk.ddl4 (c int primary key comment '你好');" $MYSQL_PORT1 $MYSQL_PASSWORD1 "gbk"
	run_sql_tidb_with_retry "show create table gbk.ddl4;" "COMMENT '你好'"
}

function run() {
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	echo "prepare data"
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "start task"
	dmctl_start_task $cur/conf/dm-task.yaml "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "prepare incremental data"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "check incremental phase"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	test_from_tidb

	echo "prepare data for invalid connection test"
	run_sql_source1 "CREATE TABLE IF NOT EXISTS gbk.invalid_conn_test1(i TINYINT, j INT UNIQUE KEY, k TINYINT, m TINYINT, n TINYINT, h TINYINT)"
	run_sql_source1 "CREATE TABLE IF NOT EXISTS gbk.invalid_conn_test2(i TINYINT, j INT UNIQUE KEY, k TINYINT, m TINYINT, n TINYINT, h TINYINT)"
	check_log_contain_with_retry 'finish to handle ddls in normal mode.*CREATE TABLE IF NOT EXISTS gbk.invalid_conn_test2' $WORK_DIR/worker1/log/dm-worker.log $WORK_DIR/worker2/log/dm-worker.log

	kill_dm_worker

	# test invalid connection with status running
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"running\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status running"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN j SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN j SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` SMALLINT(4) NOT NULL DEFAULT '0'"
	echo "check count"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` SMALLINT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status running successfully"

	kill_dm_worker

	# test invalid connection with status queueing
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"queueing\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status queueing"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN m SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN m SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`m\` SMALLINT(4) NOT NULL DEFAULT '0'"
	echo "check count"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`m\` SMALLINT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status queueing successfully"

	kill_dm_worker

	# test invalid connection with status none
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"none\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status none"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN n SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN n SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`n\` SMALLINT(4) NOT NULL DEFAULT '0'"
	echo "check count"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`n\` SMALLINT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status none successfully"

	kill_dm_worker

	# test inserting data after invalid connection
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test inserting data after invalid connection"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN h SMALLINT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (1,1,1,1,1,1)"
	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (2,2,2,2,2,2)"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`h\` SMALLINT(4) NOT NULL DEFAULT '0'"
	echo "check count"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`h\` SMALLINT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test inserting data after invalid connection successfully"

	kill_dm_worker

	# test adding UNIQUE on column with duplicate data
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test adding UNIQUE on column with duplicate data"

	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (3,3,3,3,3,3)"
	run_sql_tidb "INSERT INTO gbk.invalid_conn_test1 VALUES (3,4,4,4,4,4)"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 ADD UNIQUE(i)"

	echo "check cancelled error"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status gbk" \
		"origin SQL: \[ALTER TABLE gbk.invalid_conn_test1 ADD UNIQUE(i)\]: DDL ALTER TABLE \`gbk\`.\`invalid_conn_test1\` ADD UNIQUE(\`i\`) executed in background and met error" 1
	# manually synchronize upstream and downstream
	run_sql_tidb "DELETE FROM gbk.invalid_conn_test1 WHERE j=4"
	# manually resume
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task gbk" \
		"\"result\": true" 3
	echo "check test adding UNIQUE on column with duplicate data successfully"

	kill_dm_worker

	# multi-schema change tests
	# test invalid connection with status running (multi-schema change)
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"running\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status running (multi-schema change)"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN i INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN j INT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN i INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN j INT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`i\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 1"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`i\` INT(4) NOT NULL DEFAULT '0'" 1
	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 2"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` INT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status running (multi-schema change) successfully"

	kill_dm_worker

	# test invalid connection with status queueing (multi-schema change)
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"queueing\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status queueing (multi-schema change)"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN k INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN m INT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN k INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN m INT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`k\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 1"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`k\` INT(4) NOT NULL DEFAULT '0'" 1
	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`m\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 2"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`m\` INT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status queueing (multi-schema change) successfully"

	kill_dm_worker

	# test invalid connection with status none (multi-schema change)
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/TestStatus=1*return(\"none\");github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test invalid connection with status none (multi-schema change)"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN n INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN h INT(4) NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test2 MODIFY COLUMN n INT(4) NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN h INT(4) NOT NULL DEFAULT _UTF8MB4'0'"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`n\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 1"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`n\` INT(4) NOT NULL DEFAULT '0'" 1
	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`h\` INT(4) NOT NULL DEFAULT '0'"
	echo "check count 2"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`h\` INT(4) NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test invalid connection with status none (multi-schema change) successfully"

	kill_dm_worker

	# test inserting data after invalid connection (multi-schema change)
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test inserting data after invalid connection (multi-schema change)"

	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 MODIFY COLUMN i TINYINT NOT NULL DEFAULT _UTF8MB4'0', MODIFY COLUMN j TINYINT NOT NULL DEFAULT _UTF8MB4'0'"
	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (5,5,5,5,5,5)"
	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (6,6,6,6,6,6)"

	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`i\` TINYINT NOT NULL DEFAULT '0'"
	echo "check count 1"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`i\` TINYINT NOT NULL DEFAULT '0'" 1
	run_sql_tidb_with_retry "ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET 0" "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` TINYINT NOT NULL DEFAULT '0'"
	echo "check count 2"
	check_count "ALTER TABLE \`gbk\`.\`invalid_conn_test1\` MODIFY COLUMN \`j\` TINYINT NOT NULL DEFAULT '0'" 1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "check test inserting data after invalid connection (multi-schema change) successfully"

	kill_dm_worker

	# test adding UNIQUE on column with duplicate data (multi-schema change)
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/TestHandleSpecialDDLError=return();github.com/pingcap/tiflow/dm/syncer/ChangeDuration=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "start test adding UNIQUE on column with duplicate data (multi-schema change)"

	run_sql_source1 "INSERT INTO gbk.invalid_conn_test1 VALUES (7,7,7,7,7,7)"
	run_sql_tidb "INSERT INTO gbk.invalid_conn_test1 VALUES (8,8,7,7,8,8)"
	run_sql_source1 "ALTER TABLE gbk.invalid_conn_test1 ADD UNIQUE(k), ADD UNIQUE(m)"

	echo "check cancelled error"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status gbk" \
		"origin SQL: \[ALTER TABLE gbk.invalid_conn_test1 ADD UNIQUE(k), ADD UNIQUE(m)\]: DDL ALTER TABLE \`gbk\`.\`invalid_conn_test1\` ADD UNIQUE(\`k\`) executed in background and met error" 1
	echo "check test adding UNIQUE on column with duplicate data (multi-schema change) successfully"
}

cleanup_data gbk gbk2 gbk3
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
