#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/../_utils/ha_cases_lib.sh

function test_multi_task_running() {
	echo "[$(date)] <<<<<< start test_multi_task_running >>>>>>"
	cleanup
	prepare_sql_multi_task
	start_multi_tasks_cluster

	# make sure task to step in "Sync" stage
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test2" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "apply increment data before restart dm-worker to ensure entering increment phase"
	run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
	run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test2
	run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test2

	sleep 5 # wait for flush checkpoint
	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 50 || print_debug_status
	check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml 50 || print_debug_status
	echo "[$(date)] <<<<<< finish test_multi_task_running >>>>>>"
}

function test_stop_task() {
	echo "[$(date)] <<<<<< start test_stop_task >>>>>>"
	test_multi_task_running

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	echo "start dumping SQLs into source"
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

	task_name=(test test2)
	task_config=(dm-task.yaml dm-task2.yaml)
	for name in ${task_name[@]}; do
		echo "stop tasks $name"
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"stop-task $name" \
			"\"result\": true" 3

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status $name" \
			"\"result\": false" 1
	done

	sleep 1

	run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker" '"stage": "bound",' 2

	for idx in $(seq 0 1); do
		echo "start tasks $cur/conf/${task_config[$idx]}"
		# if relay hasn't catch up with current sync pos(such as when workload too high), start-task may start with an error
		# so we don't check "\"result\": true" here, and check it using query-status
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"start-task $cur/conf/${task_config[$idx]}" \
			"\"source\": \"$SOURCE_ID1\"" 1 \
			"\"source\": \"$SOURCE_ID2\"" 1

		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status ${task_name[$idx]}" \
			"\"stage\": \"Running\"" 4
	done

	# waiting for syncing
	wait
	sleep 1

	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
	echo "[$(date)] <<<<<< finish test_stop_task >>>>>>"
}

function run() {
	test_stop_task # TICASE-991, 984
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
