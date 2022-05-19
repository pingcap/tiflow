#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/../_utils/ha_cases_lib.sh

function print_debug_status() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"fail me!" 1 &&
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
			"query-status test2" \
			"fail me!" 1 && exit 1
}

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

function test_pause_task() {
	echo "[$(date)] <<<<<< start test_pause_task >>>>>>"
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
	for name in ${task_name[@]}; do
		echo "pause tasks $name"

		# because some SQL may running (often remove checkpoint record), pause will cause that SQL failed
		# thus `result` is not true
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"pause-task $name"

		# pause twice, just used to test pause by the way
		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"pause-task $name"

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status $name" \
			"\"stage\": \"Paused\"" 2
	done

	sleep 1

	for name in ${task_name[@]}; do
		echo "resume tasks $name"
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task $name" \
			"\"result\": true" 3

		# resume twice, just used to test resume by the way
		run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"resume-task $name" \
			"\"result\": true" 3

		run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
			"query-status $name" \
			"\"stage\": \"Running\"" 4
	done

	# waiting for syncing
	wait
	sleep 1

	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
	echo "[$(date)] <<<<<< finish test_pause_task >>>>>>"
}

function test_multi_task_reduce_and_restart_worker() {
	echo "[$(date)] <<<<<< start test_multi_task_reduce_and_restart_worker >>>>>>"
	test_multi_task_running

	echo "start dumping SQLs into source"
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" $ha_test2 &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" $ha_test2 &
	worker_ports=($WORKER1_PORT $WORKER2_PORT $WORKER3_PORT $WORKER4_PORT $WORKER5_PORT)

	# find which worker is in use
	task_name=(test test2)
	worker_inuse=("") # such as ("worker1" "worker4")
	status=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test |
		grep 'worker' | awk -F: '{print $2}')
	echo $status
	for w in ${status[@]}; do
		worker_inuse=(${worker_inuse[*]} ${w:0-9:7})
		echo "find workers: ${w:0-9:7} for task: test"
	done
	echo "find all workers: ${worker_inuse[@]} (total: ${#worker_inuse[@]})"

	for idx in $(seq 1 5); do
		if [[ ! " ${worker_inuse[@]} " =~ " worker${idx} " ]]; then
			echo "restart unuse worker${idx}"

			echo "try to kill worker port ${worker_ports[$(($idx - 1))]}"
			kill_process dm-worker${idx}
			run_dm_ctl_with_retry $WORK_DIR 127.0.0.1:$MASTER_PORT2 "list-member --worker --name=worker$idx" '"stage": "offline"' 1

			echo "start dm-worker${idx}"
			run_dm_worker $WORK_DIR/worker${idx} ${worker_ports[$(($idx - 1))]} $cur/conf/dm-worker${idx}.toml
			check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:${worker_ports[$(($idx - 1))]}
		fi
	done

	for ((i = 0; i < ${#worker_inuse[@]}; i++)); do
		wk=${worker_inuse[$i]:0-1:1}                                 # get worker id, such as ("1", "4")
		echo "try to kill worker port ${worker_ports[$(($wk - 1))]}" # get relative worker port
		kill_process dm-${worker_inuse[$i]}
		check_port_offline ${worker_ports[$(($wk - 1))]} 20
		# just one worker was killed should be safe
		echo "${worker_inuse[$i]} was killed"
		for name in ${task_name[@]}; do
			run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
				"query-status $name" \
				"\"stage\": \"Running\"" 2
		done
		if [ $i = 0 ]; then
			# waiting for syncing
			wait
			sleep 2
			echo "use sync_diff_inspector to check increment data"
			check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
			check_sync_diff $WORK_DIR $cur/conf/diff_config_multi_task.toml
			echo "data checked after one worker was killed"
		fi
	done
	echo "[$(date)] <<<<<< finish test_multi_task_reduce_and_restart_worker >>>>>>"
}

function run() {
	test_pause_task                           # TICASE-990
	test_multi_task_reduce_and_restart_worker # TICASE-968, 994, 995, 964, 966, 979, 981, 982, 985, 986, 989, 993
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
