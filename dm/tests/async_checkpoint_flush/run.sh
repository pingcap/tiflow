#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

function run_sql_silent() {
	TIDB_PORT=4000
	user="root"
	if [[ "$2" = $TIDB_PORT ]]; then
		user="test"
	fi
	mysql -u$user -h127.0.0.1 -P$2 -p$3 --default-character-set utf8 -E -e "$1" >>/dev/null
}

function insert_data() {
	i=1

	while true; do
		run_sql_silent "insert into async_checkpoint_flush.t1 values ($(($i * 2 + 1)));" $MYSQL_PORT1 $MYSQL_PASSWORD1
		((i++))
	done
}

function run() {
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/AsyncCheckpointFlushThrowError=return(true)"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 1 row affected'

	# run dm master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	# copy config file
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml

	# bound source1 to worker1, source2 to worker2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# check dm-workers metrics unit: relay file index must be 1.
	check_metric $WORKER1_PORT "dm_relay_binlog_file" 3 0 2

	# start a task in all mode, and when enter incremental mode, we only execute DML
	dmctl_start_task_standalone $cur/conf/dm-task.yaml

	# check task has started state=2 running
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$TASK_NAME\",worker=\"worker1\"}" 10 1 3

	# check diff
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	insert_data &
	pid=$!
	echo "PID of insert_data is $pid"

	sleep 30

	kill $pid
	check_log_contain_with_retry 'async flush checkpoint snapshot failed, ignore this error' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'sync flush checkpoint snapshot successfully' $WORK_DIR/worker1/log/dm-worker.log
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	export GO_FAILPOINTS=""
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
