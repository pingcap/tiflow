#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

COUNT=200
function prepare_datafile() {
	for i in $(seq 2); do
		data_file="$WORK_DIR/db$i.prepare.sql"
		echo 'DROP DATABASE if exists load_interrupt;' >>$data_file
		echo 'CREATE DATABASE load_interrupt;' >>$data_file
		echo 'USE load_interrupt;' >>$data_file
		echo "CREATE TABLE t$i(i TINYINT, j INT UNIQUE KEY);" >>$data_file
		for j in $(seq $COUNT); do
			echo "INSERT INTO t$i VALUES ($i,${j}000$i),($i,${j}001$i);" >>$data_file
		done
	done
}

function check_row_count() {
	index=$1
	lines=$(($(wc -l $WORK_DIR/db$index.prepare.sql | awk '{print $1}') - 4))
	# each line has two insert values
	lines=$((lines * 2))
	run_sql "SELECT FLOOR(offset / end_pos * $lines) as cnt from dm_meta.test_loader_checkpoint where cp_table = 't$index'" $TIDB_PORT $TIDB_PASSWORD
	estimate=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
	run_sql "SELECT count(1) as cnt from $TEST_NAME.t$index" $TIDB_PORT $TIDB_PASSWORD
	row_count=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt")
	echo "estimate row count: $estimate, real row count: $row_count"
	[ "$estimate" == "$row_count" ]
}

function test_save_checkpoint_failed() {
	prepare_datafile
	run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/loaderCPUpdateOffsetError=return()"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml"

	# load task should Paused because job file is not right
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Paused" 1

	# check dump files are generated before worker down
	ls $WORK_DIR/worker1/dumped_data.test

	echo "test_save_checkpoint_failed SUCCESS!"
	cleanup_process $*
	cleanup_data load_interrupt
}

function run() {
	test_save_checkpoint_failed

	prepare_datafile
	run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	THRESHOLD=1024
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/loader/LoadExceedOffsetExit=return($THRESHOLD)"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# don't check result, because worker may meet failpoint `LoadExceedOffsetExit` before correct response.
	# let following check (port offline, dump data) ensure task has been started
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml"

	check_port_offline $WORKER1_PORT 20

	# check dump files are generated before worker down
	ls $WORK_DIR/worker1/dumped_data.test

	run_sql "SELECT count(*) from dm_meta.test_loader_checkpoint where cp_schema = '$TEST_NAME' and offset < $THRESHOLD" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 1"
	# TODO: block for dumpling temporarily
	# check_row_count 1

	# only failed at the first two time, will retry later and success
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/loader/LoadExecCreateTableFailed=3*return("1213")' # ER_LOCK_DEADLOCK, retryable error code
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	sleep 8
	echo "check sync diff after restarted dm-worker"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# LoadExecCreateTableFailed error return twice
	sleep 8
	err_cnt=$(grep LoadExecCreateTableFailed $WORK_DIR/worker1/log/dm-worker.log | wc -l)
	if [ $err_cnt -ne 2 ]; then
		echo "error LoadExecCreateTableFailed's count is not 2"
		exit 2
	fi

	# strange, TiDB (at least with mockTiKV) needs a long time to see the update of `test_loader_checkpoint`,
	# and even later txn may see the older state than the earlier txn.
	sleep 8
	run_sql "SELECT count(*) from dm_meta.test_loader_checkpoint where cp_schema = '$TEST_NAME' and offset = end_pos" $TIDB_PORT $TIDB_PASSWORD
	check_contains "count(*): 1"

	export GO_FAILPOINTS=''
	ls $WORK_DIR/worker1/dumped_data.test && exit 1 || echo "worker1 auto removed dump files"
}

cleanup_data load_interrupt
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
