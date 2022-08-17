#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

# only use one DM-worker instance to avoid re-schedule after restart process.

COUNT=200
function prepare_datafile() {
	data_file="$WORK_DIR/db1.prepare.sql"
	echo 'DROP DATABASE if exists import_goroutine_leak;' >>$data_file
	echo 'CREATE DATABASE import_goroutine_leak;' >>$data_file
	echo 'USE import_goroutine_leak;' >>$data_file
	echo "CREATE TABLE t1(i TINYINT, j INT UNIQUE KEY);" >>$data_file
	for j in $(seq $COUNT); do
		echo "INSERT INTO t1 VALUES (1,${j}0001),(1,${j}0011);" >>$data_file
	done
}

function run() {
	prepare_datafile

	run_sql_file $WORK_DIR/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	echo "dm-worker panic, doJob of import unit workers don't exit"
	# send to closed `runFatalChan`
	inject_points=("github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(1000)"
		"github.com/pingcap/tiflow/dm/loader/dispatchError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/executeSQLError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/returnDoJobError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/workerCantClose=return(1)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml" \
		"\"source\": \"$SOURCE_ID1\"" 1

	check_port_offline $WORKER1_PORT 20

	# dm-worker1 panics
	err_cnt=$(grep "panic" $WORK_DIR/worker1/log/stdout.log | wc -l)
	if [ $err_cnt -ne 1 ]; then
		echo "dm-worker1 doesn't panic, panic count ${err_cnt}"
		exit 2
	fi

	echo "dm-worker panic again, workers of import unit don't exit"
	# send to closed `runFatalChan`
	inject_points=("github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(1000)"
		"github.com/pingcap/tiflow/dm/loader/dispatchError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/executeSQLError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/returnDoJobError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/dontWaitWorkerExit=return(1)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml

	check_port_offline $WORKER1_PORT 20
	sleep 2

	# dm-worker1 panics
	err_cnt=$(grep "panic" $WORK_DIR/worker1/log/stdout.log | wc -l)
	# there may be more panic
	if [ $err_cnt -lt 2 ]; then
		echo "dm-worker1 doesn't panic again, panic count ${err_cnt}"
		exit 2
	fi

	echo "restart dm-workers with errros to pause"
	# paused with injected error
	inject_points=("github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(1000)"
		"github.com/pingcap/tiflow/dm/loader/dispatchError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/executeSQLError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/returnDoJobError=return(1)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"inject failpoint dispatchError" 1

	echo "restart dm-workers block in sending to chan"
	kill_process dm-worker
	check_port_offline $WORKER1_PORT 20

	# use a small job chan size to block the sender
	inject_points=("github.com/pingcap/tiflow/dm/loader/LoadDataSlowDown=sleep(1000)"
		"github.com/pingcap/tiflow/dm/loader/executeSQLError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/returnDoJobError=return(1)"
		"github.com/pingcap/tiflow/dm/loader/workerChanSize=return(10)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# wait until the task running
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"stage": "Running"' 1
	sleep 2 # wait to be blocked

	# check to be blocked
	curl -X POST 127.0.0.1:$WORKER1_PORT/debug/pprof/goroutine?debug=2 >$WORK_DIR/goroutine.worker1
	check_log_contains $WORK_DIR/goroutine.worker1 "chan send"

	ps aux | grep dm-worker | awk '{print $2}' | xargs kill || true
	sleep 5
	worker_cnt=$(ps aux | grep dm-worker | grep -v "grep" | wc -l)
	if [ $worker_cnt -lt 1 ]; then
		echo "some dm-workers exit, remain count ${worker_cnt}"
		exit 2
	fi

	echo "force to restart dm-workers without errors"
	ps aux | grep dm-worker | grep -v "grep" | awk '{print $2}' | xargs kill -9 || true

	export GO_FAILPOINTS=''
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		'"stage": "Finished"' 1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

cleanup_data import_goroutine_leak
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
