#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run_with_prepared_source_config() {
	# 1. test sync fetch binlog met error and reset binlog streamer with remote binlog

	# with a 5 rows insert txn: 1 * FormatDesc + 1 * PreviousGTID + 1 * GTID + 1 * BEGIN + 5 * (Table_map + Write_rows) + 1 * XID
	# here we fail at the third write rows event, sync should retry and auto recover without any duplicate event
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/syncer/GetEventErrorInTxn=return(3)"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml --remove-meta"
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"test\",worker=\"worker1\"}" 10 1 3

	# wait safe-mode pass
	check_log_contain_with_retry "disable safe-mode after task initialization finished" $WORK_DIR/worker1/log/dm-worker.log

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	check_log_contain_with_retry "reset replication binlog puller" $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry "discard event already consumed" $WORK_DIR/worker1/log/dm-worker.log
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# 2. test relay log retry relay with GTID

	# with a 5 rows insert txn: 1 * FormatDesc + 1 * PreviousGTID + 1 * GTID + 1 * BEGIN + 5 * (Table_map + Write_rows) + 1 * XID
	# here we fail at the third write rows event, sync should retry and auto recover without any duplicate event
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/relay/RelayGetEventFailedAt=return(3);github.com/pingcap/tiflow/dm/relay/RelayAllowRetry=return"

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 2 rows affected'

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task-relay.yaml --remove-meta"
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"test_relay\",worker=\"worker2\"}" 10 1 3

	check_sync_diff $WORK_DIR $cur/conf/diff_relay_config.toml

	run_sql_source2 "flush logs;"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID2" \
		"\"relayCatchUpMaster\": true" 1

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	check_sync_diff $WORK_DIR $cur/conf/diff_relay_config.toml

	# check relay log binlog file size is the same as master size
	run_sql_source2 "show master status;"
	binlog_file=$(grep "File" $TEST_DIR/sql_res.$TEST_NAME.txt | awk -F: '{print $2}' | xargs)
	binlog_pos=$(grep "Position" $TEST_DIR/sql_res.$TEST_NAME.txt | awk -F: '{print $2}' | xargs)
	server_uuid=$(tail -n 1 $WORK_DIR/worker2/relay-dir/server-uuid.index)

	succ=0
	for ((k = 1; k < 6; k++)); do
		relay_log_size=$(ls -al $WORK_DIR/worker2/relay-dir/$server_uuid/$binlog_file | awk '{print $5}')
		echo "binlog_pos: $binlog_pos relay_log_size: $relay_log_size"
		if [[ "$binlog_pos" -eq "$relay_log_size" ]]; then
			succ=1
			break
		fi
		sleep 1
	done
	if [[ $succ -eq 0 ]]; then
		echo "binlog_pos is not equal to relay_log_size"
		exit 1
	fi

	echo "============== run_with_prepared_source_config success ==================="
}

function run() {
	# test source enable gtid
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml

	run_with_prepared_source_config

	cleanup_process $*
	cleanup_data dup_event1 dup_event_relay

	# test source disable gtid
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "s/enable-gtid: true/enable-gtid: false/g" $WORK_DIR/source1.yaml
	sed -i "s/enable-gtid: true/enable-gtid: false/g" $WORK_DIR/source2.yaml

	run_with_prepared_source_config
}

# also cleanup dm processes in case of last run failed
cleanup_process $*
cleanup_data dup_event1 dup_event_relay
run
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
