#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME
TABLE_NUM=500

function prepare_data() {
	run_sql 'DROP DATABASE if exists many_tables_db;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql 'CREATE DATABASE many_tables_db;' $MYSQL_PORT1 $MYSQL_PASSWORD1
	for i in $(seq $TABLE_NUM); do
		run_sql "CREATE TABLE many_tables_db.t$i(i TINYINT, j INT UNIQUE KEY, c1 VARCHAR(20), c2 VARCHAR(20), c3 VARCHAR(20), c4 VARCHAR(20), c5 VARCHAR(20), c6 VARCHAR(20), c7 VARCHAR(20), c8 VARCHAR(20), c9 VARCHAR(20), c10 VARCHAR(20), c11 VARCHAR(20), c12 VARCHAR(20), c13 VARCHAR(20));" $MYSQL_PORT1 $MYSQL_PASSWORD1
		for j in $(seq 2); do
			run_sql "INSERT INTO many_tables_db.t$i(i,j) VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		done
		# to make the tables have odd number of lines before 'ALTER TABLE' command, for check_sync_diff to work correctly
		run_sql "INSERT INTO many_tables_db.t$i(i,j) VALUES (9, 90009);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function incremental_data() {
	for j in $(seq 3 5); do
		for i in $(seq $TABLE_NUM); do
			run_sql "INSERT INTO many_tables_db.t$i(i,j) VALUES ($j,${j}000$j),($j,${j}001$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
		done
	done
}

function incremental_data_2() {
	j=6
	for i in $(seq $TABLE_NUM); do
		run_sql "INSERT INTO many_tables_db.t$i (i, j) VALUES ($j,${j}000$j);" $MYSQL_PORT1 $MYSQL_PASSWORD1
	done
}

function run() {
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server

	# clean unistore data
	rm -rf /tmp/tidb

	# start a TiDB with small txn-total-size-limit
	run_tidb_server 4000 $TIDB_PASSWORD $cur/conf/tidb-config-small-txn.toml
	sleep 2

	echo "start prepare_data"
	prepare_data
	echo "finish prepare_data"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	dmctl_start_task_standalone
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"totalTables\": \"500\"" 1 \
		"\"completedTables\"" 1 \
		"\"finishedBytes\"" 1 \
		"\"finishedRows\"" 1 \
		"\"estimateTotalRows\"" 1
	wait_until_sync $WORK_DIR "127.0.0.1:$MASTER_PORT"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_tidb_with_retry_times "select count(*) from dm_meta.test_syncer_checkpoint" "count(*): 501" 60

	check_log_contains $WORK_DIR/worker1/log/dm-worker.log 'Error 8004: Transaction is too large'

	# check https://github.com/pingcap/tiflow/issues/5063
	check_time=20
	sleep 5
	while [ $check_time -gt 0 ]; do
		syncer_recv_event_num=$(grep '"receive binlog event"' $WORK_DIR/worker1/log/dm-worker.log | wc -l)
		if [ $syncer_recv_event_num -eq 3 ]; then
			break
		fi
		echo "syncer_recv_event_num: $syncer_recv_event_num, will retry later"
		sleep 1
		((check_time--))
	done

	if [ $syncer_recv_event_num -ne 3 ]; then
		exit 1
	fi

	echo "start incremental_data"
	incremental_data
	echo "finish incremental_data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test https://github.com/pingcap/tiflow/issues/5344
	kill_dm_worker
	# let some binlog event save table checkpoint before meet downstream error
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/BlockExecuteSQLs=return(1)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_sql_source1 "CREATE TABLE many_tables_db.flush (c INT PRIMARY KEY);"
	sleep 5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"synced": true' 1

	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server
	# now worker will process some binlog events, save table checkpoint and meet downstream error
	incremental_data_2
	sleep 30

	resume_num=$(grep 'unit process error' $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	echo "resume_num: $resume_num"
	# because we check auto resume every 5 seconds...
	[ $resume_num -ge 4 ]
	folder_size=$(du -d0 $WORK_DIR/worker1/ --exclude="$WORK_DIR/worker1/log" | cut -f1)
	echo "folder_size: $folder_size"
	# less than 10M
	[ $folder_size -lt 10000 ]

	export GO_FAILPOINTS=''
}

cleanup_data many_tables_db
cleanup_process

run $*

cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
