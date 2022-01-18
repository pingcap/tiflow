#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

function purge_relay_success() {
	binlog_file=$1
	source_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay --filename $binlog_file -s $source_id" \
		"\"result\": true" 2
}

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
		sleep 1
		run_sql_silent "insert into only_dml.t1 values ($(($i * 2 + 1)));" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql_silent "insert into only_dml.t2 values ($(($i * 2 + 2)));" $MYSQL_PORT2 $MYSQL_PASSWORD2
		((i++))
		run_sql_silent "insert into only_dml.t1 values ($(($i * 2 + 1)));" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql_silent "insert into only_dml.t2 values ($(($i * 2 + 2)));" $MYSQL_PORT2 $MYSQL_PASSWORD2
		((i++))
		run_sql_silent "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
		run_sql_silent "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2
	done
}

function run() {
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval=return(1);github.com/pingcap/tiflow/dm/syncer/syncDMLBatchNotFull=return(true)"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 1 row affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 1 row affected'

	# run dm master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2

	# copy config file
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml

	# bound source1 to worker1, source2 to worker2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# check dm-workers metrics unit: relay file index must be 1.
	check_metric $WORKER1_PORT "dm_relay_binlog_file" 3 0 2
	check_metric $WORKER2_PORT "dm_relay_binlog_file" 3 0 2

	# start a task in all mode, and when enter incremental mode, we only execute DML
	dmctl_start_task $cur/conf/dm-task.yaml

	# check task has started state=2 running
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$TASK_NAME\",worker=\"worker1\"}" 10 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$TASK_NAME\",worker=\"worker2\"}" 10 1 3

	# check diff
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	insert_data &
	pid=$!
	echo "PID of insert_data is $pid"

	# check twice, make sure update active relay log could work for first time and later
	for i in {1..2}; do
		for ((k = 1; k < 10; k++)); do
			server_uuid1=$(tail -n 1 $WORK_DIR/worker1/relay-dir/server-uuid.index)
			run_sql_source1 "show binary logs\G"
			max_binlog_name=$(grep Log_name "$SQL_RESULT_FILE" | tail -n 1 | awk -F":" '{print $NF}')
			earliest_relay_log1=$(ls $WORK_DIR/worker1/relay-dir/$server_uuid1 | grep -v 'relay.meta' | sort | head -n 1)
			purge_relay_success $max_binlog_name $SOURCE_ID1
			earliest_relay_log2=$(ls $WORK_DIR/worker1/relay-dir/$server_uuid1 | grep -v 'relay.meta' | sort | head -n 1)
			echo "earliest_relay_log1: $earliest_relay_log1 earliest_relay_log2: $earliest_relay_log2"
			if [ "$earliest_relay_log1" != "$earliest_relay_log2" ]; then
				break
			fi
			echo "purge relay log failed $k-th time, retry later"
			sleep 1
		done

		for ((k = 1; k < 10; k++)); do
			server_uuid2=$(tail -n 1 $WORK_DIR/worker2/relay-dir/server-uuid.index)
			run_sql_source2 "show binary logs\G"
			max_binlog_name=$(grep Log_name "$SQL_RESULT_FILE" | tail -n 1 | awk -F":" '{print $NF}')
			earliest_relay_log1=$(ls $WORK_DIR/worker2/relay-dir/$server_uuid2 | grep -v 'relay.meta' | sort | head -n 1)
			purge_relay_success $max_binlog_name $SOURCE_ID2
			earliest_relay_log2=$(ls $WORK_DIR/worker2/relay-dir/$server_uuid2 | grep -v 'relay.meta' | sort | head -n 1)
			echo "earliest_relay_log1: $earliest_relay_log1 earliest_relay_log2: $earliest_relay_log2"
			if [ "$earliest_relay_log1" != "$earliest_relay_log2" ]; then
				break
			fi
			echo "purge relay log failed $k-th time, retry later"
			sleep 1
		done
	done

	kill $pid
	check_log_contain_with_retry 'execute not full job queue' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry 'execute not full job queue' $WORK_DIR/worker2/log/dm-worker.log
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	export GO_FAILPOINTS=""
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
