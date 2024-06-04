#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function fail_acquire_global_lock() {
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/worker/TaskCheckInterval=return(\"500ms\")"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	cp $cur/data/db1.prepare.user.sql $WORK_DIR/db1.prepare.user.sql
	sed -i "/revoke create temporary/i\revoke reload on *.* from 'dm_full'@'%';" $WORK_DIR/db1.prepare.user.sql
	run_sql_file $WORK_DIR/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_count 'Query OK, 0 rows affected' 8
	cp $cur/data/db2.prepare.user.sql $WORK_DIR/db2.prepare.user.sql
	sed -i "/revoke create temporary/i\revoke reload on *.* from 'dm_full'@'%';" $WORK_DIR/db2.prepare.user.sql
	run_sql_file $WORK_DIR/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_count 'Query OK, 0 rows affected' 11

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i '/heartbeat-report-interval/i\ignore-checking-items: ["dump_privilege"]' $WORK_DIR/dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta"

	# TaskCheckInterval set to 500ms
	sleep 1

	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "you need (at least one of) the RELOAD privilege(s) for this operation"
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "error is not resumable"
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "you need (at least one of) the RELOAD privilege(s) for this operation"
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "error is not resumable"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 2 \
		"LOCK TABLES \`full_mode\`.\`t1\` READ: Error 1044 (42000): Access denied" 1 \
		"LOCK TABLES \`full_mode\`.\`t2\` READ: Error 1044 (42000): Access denied" 1

	cleanup_process $*
	cleanup_data full_mode
}

function escape_schema() {
	cp $cur/data/db1.prepare.sql $WORK_DIR/db1.prepare.sql
	cp $cur/data/db2.prepare.sql $WORK_DIR/db2.prepare.sql
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	cp $cur/conf/diff_config.toml $WORK_DIR/diff_config.toml
	sed -i "s/full_mode/full\\\\\\\\\/mode/g" $WORK_DIR/diff_config.toml
	sed -i "s/full_mode/full\/mode/g" $WORK_DIR/db1.prepare.sql $WORK_DIR/db2.prepare.sql $WORK_DIR/dm-task.yaml

	run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $WORK_DIR/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# test load data with `/` in the table name
	run_sql_source1 "create table \`full/mode\`.\`tb\/1\` (id int, name varchar(10), primary key(\`id\`));"
	run_sql_source1 "insert into \`full/mode\`.\`tb\/1\` values(1,'haha');"
	run_sql_source1 "insert into \`full/mode\`.\`tb\/1\` values(2,'hihi');"

	run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_count 'Query OK, 0 rows affected' 7
	run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_count 'Query OK, 0 rows affected' 10

	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/dumpling/SleepBeforeDumplingClose=return(3)'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	check_metric $WORKER1_PORT 'dumpling_dump_finished_tables' 3 0 3
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml

	check_log_contain_with_retry 'clean dump files' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'clean dump files' $WORK_DIR/worker2/log/dm-worker.log

	echo "check dump files have been cleaned"
	ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"
	ls $WORK_DIR/worker2/dumped_data.test && exit 1 || echo "worker2 auto removed dump files"
	export GO_FAILPOINTS=''

	cleanup_process $*
	cleanup_data full/mode
}

function empty_data() {
	run_sql_both_source "drop database if exists full_mode;"
	run_sql_both_source "create database full_mode;"
	run_sql_source1 "create table full_mode.t1 (id int, name varchar(20), primary key(id));"
	run_sql_source2 "create table full_mode.t2 (id int, name varchar(20), primary key(id));"

	init_cluster

	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	sleep 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Finished\"" 2 \
		"\"totalBytes\": \"0\"" 2 \
		"\"progress\": \"100.00 %\"" 2

	check_log_contains $WORK_DIR/worker1/log/dm-worker.log "progress=\"100.00 %\""
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "progress=\"100.00 %\""

	cleanup_process $*
	cleanup_data full_mode
}

function only_route_schema() {
	run_sql_source1 "drop database if exists full_mode;"
	run_sql_source1 "create database full_mode;"
	init_cluster

	dmctl_start_task_standalone "$cur/conf/dm-task-2.yaml" "--remove-meta"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Finished\"" 1

	run_sql_tidb_with_retry "SHOW DATABASES LIKE 'full_mode_test';" ": full_mode_test"
	cleanup_process $*
	cleanup_data full_mode
}

function run() {
	fail_acquire_global_lock
	escape_schema
	empty_data
	only_route_schema

	run_sql_both_source "SET @@GLOBAL.SQL_MODE='NO_BACKSLASH_ESCAPES'"
	run_sql_source1 "SET @@global.time_zone = '+01:00';"
	run_sql_source2 "SET @@global.time_zone = '+02:00';"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# test load data with `"` in the table name
	run_sql_source1 "create table full_mode.\`tb\"1\` (id int,name varchar(10), primary key(\`id\`));"
	run_sql_source1 "insert into full_mode.\`tb\"1\` values(1,'haha');"
	run_sql_source1 "insert into full_mode.\`tb\"1\` values(2,'hihi');"
	# write different data in downstream, to test on-duplicate-logical = ignore
	run_sql_tidb "create database if not exists full_mode;"
	run_sql_tidb "create table full_mode.\`tb\"1\` (id int,name varchar(10), primary key(\`id\`));"
	run_sql_tidb "insert into full_mode.\`tb\"1\` values(1,'hoho');"

	run_sql_file $cur/data/db1.prepare.user.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_count 'Query OK, 0 rows affected' 7
	run_sql_file $cur/data/db2.prepare.user.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_count 'Query OK, 0 rows affected' 10

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# check dm-master metrics
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Finished\"" 2

	run_sql_tidb "SELECT name FROM full_mode.\`tb\"1\` WHERE id = 1;"
	check_contains "hoho"
	run_sql_source1 "REPLACE INTO full_mode.\`tb\"1\` values(1,'hoho');"

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "check dump files have been cleaned"
	ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"
	ls $WORK_DIR/worker2/dumped_data.test && exit 1 || echo "worker2 auto removed dump files"
	# check task finished and metric cleaned
	check_metric_not_contains $WORKER1_PORT 'dm_worker_task_state{source_id="mysql-replica-01",task="test",worker=\"worker1\"}' 3
	check_metric_not_contains $WORKER2_PORT 'dm_worker_task_state{source_id="mysql-replica-02",task="test",worker=\"worker2\"}' 3
	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	run_sql_both_source "SET @@GLOBAL.TIME_ZONE='SYSTEM';"
}

cleanup_data full_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
