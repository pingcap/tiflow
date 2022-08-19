#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_NAME="test"
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

API_VERSION="v1alpha1"

function test_restart_relay_status() {
	cleanup_process
	cleanup_data $TEST_NAME
	export GO_FAILPOINTS=""

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"will be deprecated soon" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID2" \
		"\"result\": true" 2 \
		"\"worker\": \"worker2\"" 1

	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker3"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"worker\": \"worker2\"" 1 \
		"\"worker\": \"worker3\"" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member -n worker3" \
		"relay" 1

	kill_dm_worker
	kill_dm_master

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"worker\": \"worker2\"" 1 \
		"\"worker\": \"worker3\"" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --worker" \
		"relay" 1 \
		"bound" 2

	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_restart_relay_status passed"
}

function test_relay_leak() {
	cleanup_process
	cleanup_data $TEST_NAME
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/relay/RelayGetEventFailed=return()"

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/check-enable: false/d" $WORK_DIR/source1.yaml
	sed -i "/checker:/d" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"RelayGetEventFailed" 1

	check_log_contain_with_retry 'dispatch auto resume relay' $WORK_DIR/worker1/log/dm-worker.log

	count=$(curl "http://127.0.0.1:8262/debug/pprof/goroutine?debug=2" 2>/dev/null | grep -c doIntervalOps || true)
	if [ $count -gt 1 ]; then
		echo "relay goroutine leak detected, count expect 1 but got $count"
		exit 1
	fi
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_relay_leak passed"
}

function test_cant_dail_upstream() {
	cleanup_process
	cleanup_data $TEST_NAME
	export GO_FAILPOINTS=""

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2

	echo "kill dm-worker1"
	kill_process dm-worker1
	check_port_offline $WORKER1_PORT 20

	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/pkg/conn/failDBPing=return()"
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# make sure DM-worker doesn't exit
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"injected error" 1
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_cant_dail_upstream passed"
}

function test_cant_dail_downstream() {
	cleanup_process
	cleanup_data $TEST_NAME
	export GO_FAILPOINTS=""

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2
	dmctl_start_task_standalone $cur/conf/dm-task.yaml "--remove-meta"

	echo "kill dm-worker1"
	kill_process dm-worker1
	check_port_offline $WORKER1_PORT 20
	# kill tidb
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"relayCatchUpMaster\": true" 1 \
		"dial tcp 127.0.0.1:4000: connect: connection refused" 1

	# restart tidb
	run_tidb_server 4000 $TIDB_PASSWORD
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_cant_dail_downstream passed"
}

function test_kill_dump_connection() {
	cleanup_process
	cleanup_data $TEST_NAME

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		"\"worker\": \"worker1\"" 1
	run_sql_source1 "show processlist"

	# kill dump connection to test whether relay will auto reconnect db
	dump_conn_id=$(cat $TEST_DIR/sql_res.$TEST_NAME.txt | grep Binlog -B 4 | grep Id | cut -d : -f2)
	run_sql_source1 "kill ${dump_conn_id}"

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"relayCatchUpMaster\": true" 1
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_kill_dump_connection passed"
}

function test_relay_operations() {
	cleanup_process
	cleanup_data $TEST_NAME
	export GO_FAILPOINTS="github.com/pingcap/tiflow/dm/relay/ReportRelayLogSpaceInBackground=return(1)"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1 worker2" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source $SOURCE_ID1 worker1" \
		"\"result\": true" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 3 \
		"\"worker\": \"worker1\"" 1 \
		"\"worker\": \"worker2\"" 1

	# worker1 and worker2 has one relay job and worker3 have none.
	check_metric $WORKER1_PORT "dm_relay_binlog_file{node=\"relay\"}" 3 0 2
	check_metric $WORKER1_PORT "dm_relay_exit_with_error_count" 3 -1 1
	check_metric $WORKER2_PORT "dm_relay_binlog_file{node=\"relay\"}" 3 0 2
	check_metric $WORKER2_PORT "dm_relay_exit_with_error_count" 3 -1 1
	check_metric_not_contains $WORKER3_PORT "dm_relay_binlog_file" 3

	dmctl_start_task_standalone $cur/conf/dm-task.yaml "--remove-meta"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	# wait syncer begin to sync so it has deleted load task etcd KV.
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# relay task transfer to worker1 with no error.
	check_metric $WORKER1_PORT "dm_relay_data_corruption" 3 -1 1
	check_metric $WORKER1_PORT "dm_relay_read_error_count" 3 -1 1
	check_metric $WORKER1_PORT "dm_relay_write_error_count" 3 -1 1
	# check worker relay space great than 0 9223372036854775807 is 2**63 -1
	check_metric $WORKER1_PORT 'dm_relay_space{type="available"}' 5 0 9223372036854775807

	# subtask is preferred to scheduled to another relay worker
	echo "kill dm-worker1"
	kill_process dm-worker1
	check_port_offline $WORKER1_PORT 20
	# worker1 is down, worker2 has running relay and sync unit
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"connect: connection refused" 1 \
		"\"stage\": \"Running\"" 2

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# after restarting, worker will purge relay log directory because checkpoint is newer than relay.meta
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_sql_file $cur/data/db1.increment3.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 3 \
		"\"worker\": \"worker1\"" 1 \
		"\"worker\": \"worker2\"" 1

	# test purge-relay for all relay workers
	run_sql_source1 "show binary logs\G"
	max_binlog_name=$(grep Log_name "$SQL_RESULT_FILE" | tail -n 1 | awk -F":" '{print $NF}')
	server_uuid_1=$(tail -n 1 $WORK_DIR/worker1/relay-dir/server-uuid.index)
	relay_log_count_1=$(($(ls $WORK_DIR/worker1/relay-dir/$server_uuid_1 | wc -l) - 1))
	server_uuid_2=$(tail -n 1 $WORK_DIR/worker2/relay-dir/server-uuid.index)
	relay_log_count_2=$(($(ls $WORK_DIR/worker2/relay-dir/$server_uuid_2 | wc -l) - 1))
	[ "$relay_log_count_1" -ne 1 ]
	[ "$relay_log_count_2" -ne 1 ]
	sleep 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay --filename $max_binlog_name -s $SOURCE_ID1" \
		"\"result\": true" 3
	new_relay_log_count_1=$(($(ls $WORK_DIR/worker1/relay-dir/$server_uuid_1 | wc -l) - 1))
	new_relay_log_count_2=$(($(ls $WORK_DIR/worker2/relay-dir/$server_uuid_2 | wc -l) - 1))
	[ "$new_relay_log_count_1" -eq 1 ]
	[ "$new_relay_log_count_2" -eq 1 ]

	echo "kill dm-worker1"
	kill_process dm-worker1
	check_port_offline $WORKER1_PORT 20
	echo "kill dm-worker2"
	kill_process dm-worker2
	check_port_offline $WORKER1_PORT 20
	# if all relay workers are offline, relay-not-enabled worker should continue to sync
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		"\"worker\": \"worker3\"" 1

	run_sql_file $cur/data/db1.increment4.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# config export
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config export -p /tmp/configs" \
		"export configs to directory .* succeed" 1

	# check configs
	sed '/password/d' /tmp/configs/tasks/test.yaml | diff $cur/configs/tasks/test.yaml - || exit 1
	sed '/password/d' /tmp/configs/sources/mysql-replica-01.yaml | diff -I '^case-sensitive' $cur/configs/sources/mysql-replica-01.yaml - || exit 1
	diff <(jq --sort-keys . /tmp/configs/relay_workers.json) <(jq --sort-keys . $cur/configs/relay_workers.json) || exit 1

	# destroy cluster
	cleanup_process $*
	cleanup_data $TEST_NAME

	# insert new data
	run_sql_file $cur/data/db1.increment5.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# deploy new cluster
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# import configs
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config import -p /tmp/configs" \
		"creating sources" 1 \
		"creating tasks" 1 \
		"The original relay workers have been exported to" 1 \
		"Currently DM doesn't support recover relay workers.*transfer-source.*start-relay" 1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source show" \
		"mysql-replica-01" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1" \
		"\"result\": true" 2

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>test test_relay_operations passed"
}

function run() {
	test_relay_leak
	test_relay_operations
	test_cant_dail_upstream
	test_restart_relay_status
	test_cant_dail_downstream
	test_kill_dump_connection
}

cleanup_data $TEST_NAME
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
