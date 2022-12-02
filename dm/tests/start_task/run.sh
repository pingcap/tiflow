#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function prepare_data() {
	run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE start_task.t$1(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "INSERT INTO start_task.t$1 VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function init_tracker_test() {
	run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 100); do
		run_sql "CREATE TABLE start_task.t$j(i TINYINT, j INT UNIQUE KEY);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql "INSERT INTO start_task.t$j VALUES (1,10001),(1,10011);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	dmctl_start_task_standalone "$cur/conf/dm-task.yaml" "--remove-meta"

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# only table 1-50 flush checkpoint
	for j in $(seq 50); do
		run_sql "INSERT INTO start_task.t$j VALUES (2,20002),(2,20022);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	dmctl_start_task_standalone "$cur/conf/dm-task.yaml"

	for j in $(seq 100); do
		run_sql "INSERT INTO start_task.t$j VALUES (3,30003),(3,30033);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql "INSERT INTO start_task.t$j VALUES (4,40004),(4,40044);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 20

	# now syncer will save all table structure from dump files at Init, so all tables
	# should be loaded into schema tracker.
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'init table info.*t50' 1
	check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'init table info.*t51' 1

	cleanup_process
	cleanup_data start_task
}

function restore_timezone() {
	run_sql_source1 "set global time_zone = SYSTEM"
	run_sql_tidb "set global time_zone = SYSTEM"
}

function start_task_by_time() {
	run_sql_source1 "set global time_zone = '+02:00'"
	run_sql_source1 "SELECT cast(TIMEDIFF(NOW(6), UTC_TIMESTAMP(6)) as time) time"
	check_contains "time: 02:00:00"
	run_sql_tidb "set global time_zone = '+06:00'"
	run_sql_tidb "SELECT cast(TIMEDIFF(NOW(6), UTC_TIMESTAMP(6)) as time) time"
	check_contains "time: 06:00:00"
	trap restore_timezone EXIT

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/SafeModeInitPhaseSeconds=return(\"10ms\")"
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_sql_source1 'DROP DATABASE if exists start_task;'
	run_sql_source1 'CREATE DATABASE start_task;'
	run_sql_source1 'CREATE TABLE start_task.t1 (c INT PRIMARY KEY);'

	sleep 2
	start_time=$(TZ='UTC-2' date '+%Y-%m-%d %T') # TZ=UTC-2 means +02:00
	sleep 2

	run_sql_source1 'CREATE TABLE start_task.t2 (c INT PRIMARY KEY);'
	run_sql_source1 'INSERT INTO start_task.t2 VALUES (1), (2);INSERT INTO start_task.t2 VALUES (3), (4);'

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/task-mode: all/task-mode: incremental/g" $WORK_DIR/dm-task.yaml

	# test with relay

	run_sql_tidb 'DROP DATABASE if exists start_task;CREATE DATABASE start_task;'
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --start-time '$start_time'" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "show tables in start_task;" "t2"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'start_task';" "count(1): 1"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	# test without relay and safe mode

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1" \
		"\"result\": true" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"relayStatus\": null" 1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source $SOURCE_ID1 worker2" \
		"\"result\": true" 1

	run_sql_tidb 'DROP DATABASE if exists start_task;CREATE DATABASE start_task;'
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --start-time '$start_time'" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "show tables in start_task;" "t2"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'start_task';" "count(1): 1"

	# no duplicate entry error
	check_log_contain_with_retry "enable safe-mode for safe mode exit point, will exit at" $WORK_DIR/worker2/log/dm-worker.log
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	# test too early

	run_sql_tidb 'DROP DATABASE if exists start_task;CREATE DATABASE start_task;'
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --start-time '1995-03-07 01:02:03'" \
		"\"result\": true" 2

	run_sql_tidb_with_retry "show tables in start_task;" "t1"
	run_sql_tidb_with_retry "SELECT count(1) FROM information_schema.tables WHERE table_schema = 'start_task';" "count(1): 2"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	# test too late

	run_sql_tidb 'DROP DATABASE if exists start_task;CREATE DATABASE start_task;'
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --start-time '2037-12-12 01:02:03'"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Paused\"" 1 \
		"no binlog location matches it" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2

	export GO_FAILPOINTS=''
	cleanup_process
	cleanup_data start_task
}

function run() {
	start_task_by_time
	init_tracker_test
	failpoints=(
		# 1152 is ErrAbortingConnection
		"github.com/pingcap/tiflow/dm/pkg/utils/FetchTargetDoTablesFailed=return(1152)"
		"github.com/pingcap/tiflow/dm/pkg/utils/FetchAllDoTablesFailed=return(1152)"
	)

	for ((i = 0; i < ${#failpoints[@]}; i++)); do
		WORK_DIR=$TEST_DIR/$TEST_NAME/$i

		echo "failpoint=${failpoints[i]}"
		export GO_FAILPOINTS=${failpoints[i]}

		# clear downstream env
		run_sql 'DROP DATABASE if exists dm_meta;' $TIDB_PORT $TIDB_PASSWORD
		run_sql 'DROP DATABASE if exists start_task;' $TIDB_PORT $TIDB_PASSWORD
		prepare_data $i

		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		# operate mysql config to worker
		cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
		sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
		dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

		echo "check un-accessible DM-worker exists"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status -s 127.0.0.1:8888" \
			"sources \[127.0.0.1:8888\] haven't been added" 1

		echo "start task and will failed"
		task_conf="$cur/conf/dm-task.yaml"
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $task_conf" \
			"\"result\": false" 1 \
			"ERROR" 1

		echo "reset go failpoints, and need restart dm-worker, then start task again"
		kill_dm_worker
		kill_dm_master

		export GO_FAILPOINTS=''
		run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
		check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
		run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
		check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
		sleep 5

		dmctl_start_task_standalone $task_conf

		check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

		cleanup_process
	done

	test_COMMIT_in_QueryEvent
}

function prepare_data_MyISAM() {
	run_sql 'DROP DATABASE if exists start_task;' $TIDB_PORT $TIDB_PASSWORD
	run_sql 'DROP DATABASE if exists start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE start_task;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "CREATE TABLE start_task.t1(i TINYINT, j INT UNIQUE KEY) engine=MyISAM;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	for j in $(seq 10); do
		run_sql "INSERT INTO start_task.t1 VALUES ($j,${j}000$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function test_COMMIT_in_QueryEvent() {
	echo "[$(date)] <<<<<< start test_COMMIT_in_QueryEvent >>>>>>"
	cleanup_process
	cleanup_data start_task
	prepare_data_MyISAM

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/dm-master.toml $WORK_DIR/
	cp $cur/conf/dm-worker1.toml $WORK_DIR/
	cp $cur/conf/dm-task.yaml $WORK_DIR/

	# start DM worker and master
	run_dm_master $WORK_DIR/master $MASTER_PORT $WORK_DIR/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source1.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1

	echo "check master alive"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"alive\": true" 1

	echo "start task and check stage"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml --remove-meta=true" \
		"\"result\": true" 2

	run_sql "CREATE TABLE start_task.t2(i TINYINT, j INT UNIQUE KEY) engine=MyISAM;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'INSERT INTO start_task.t1 VALUES (99,9999);' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'INSERT INTO start_task.t2 VALUES (99,9999);' $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1 \
		"\"stage\": \"Running\"" 2

	echo "check data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log "originSQL: COMMIT"

	echo "<<<<<< test_COMMIT_in_QueryEvent success! >>>>>>"
}

cleanup_data start_task

cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
