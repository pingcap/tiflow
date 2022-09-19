#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# init full data in different timezone and sql mode
	run_sql_source1 "SET @@GLOBAL.SQL_MODE='PIPES_AS_CONCAT,IGNORE_SPACE,ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT'"
	run_sql_source2 "SET @@GLOBAL.SQL_MODE=''"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	# start DM task only
	dmctl_start_task $cur/conf/dm-task.yaml
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# change timezone and incr data
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/timezone.Asia-Shanghai.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql_file $cur/data/timezone.America-Phoenix.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source2 "SET @@GLOBAL.SQL_MODE='NO_BACKSLASH_ESCAPES'"
	run_sql_source2 'create table sql_mode.t0(id bigint primary key, a text,b text as ((regexp_replace(a, "^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\.[x|X][m|M][l|L]$","aaaaa"))),  c text as ((regexp_substr(a, "^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\.[x|X][m|M][l|L]$"))), d text as ((regexp_like(a, "^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\.[x|X][m|M][l|L]$"))), e text as ((regexp_instr(a, "^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\.[x|X][m|M][l|L]$"))));'
	run_sql_tidb_with_retry "show create table sql_mode.t0" $'CREATE TABLE `t0` (
  `id` bigint(20) NOT NULL,
  `a` text DEFAULT NULL,
  `b` text GENERATED ALWAYS AS ((regexp_replace(`a`, _utf8mb4\'^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\\\.[x|X][m|M][l|L]$\', _utf8mb4\'aaaaa\'))) VIRTUAL,
  `c` text GENERATED ALWAYS AS ((regexp_substr(`a`, _utf8mb4\'^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\\\.[x|X][m|M][l|L]$\'))) VIRTUAL,
  `d` text GENERATED ALWAYS AS ((regexp_like(`a`, _utf8mb4\'^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\\\.[x|X][m|M][l|L]$\'))) VIRTUAL,
  `e` text GENERATED ALWAYS AS ((regexp_instr(`a`, _utf8mb4\'^([a-zA-Z]+-?)+[a-zA-Z0-9]+\\\\.[x|X][m|M][l|L]$\'))) VIRTUAL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
