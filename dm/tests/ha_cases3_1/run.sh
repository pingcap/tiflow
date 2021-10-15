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

function test_isolate_master_and_worker() {
	echo "[$(date)] <<<<<< start test_isolate_master_and_worker >>>>>>"

	test_multi_task_running

	# join master4 and master5
	run_dm_master $WORK_DIR/master-join4 $MASTER_PORT4 $cur/conf/dm-master-join4.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT4
	sleep 5
	run_dm_master $WORK_DIR/master-join5 $MASTER_PORT5 $cur/conf/dm-master-join5.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT5
	sleep 5

	master_ports=($MASTER_PORT1 $MASTER_PORT2 $MASTER_PORT3 $MASTER_PORT4 $MASTER_PORT5)
	alive=(1 2 3 4 5)

	leader=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[${alive[0]} - 1]}))
	leader_idx=${leader:6}
	echo "try to isolate leader dm-master$leader_idx"
	isolate_master $leader_idx "isolate"
	for idx in "${!alive[@]}"; do
		if [[ ${alive[idx]} = $leader_idx ]]; then
			unset 'alive[idx]'
		fi
	done
	alive=("${alive[@]}")

	new_leader=($(get_leader $WORK_DIR 127.0.0.1:${master_ports[${alive[0]} - 1]}))
	new_leader_idx=${new_leader:6}
	new_leader_port=${master_ports[$new_leader_idx - 1]}
	follower_idx=${alive[0]}
	if [[ $follower_idx = $new_leader_idx ]]; then
		follower_idx=${alive[1]}
	fi

	echo "try to isolate follower dm-master$follower_idx"
	isolate_master $follower_idx "isolate"
	for idx in "${!alive[@]}"; do
		if [[ ${alive[idx]} = $follower_idx ]]; then
			unset 'alive[idx]'
		fi
	done
	alive=("${alive[@]}")

	# find which worker is in use
	task_name=(test test2)
	worker_inuse=("") # such as ("worker1" "worker4")
	status=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$new_leader_port" query-status test |
		grep 'worker' | awk -F: '{print $2}')
	echo $status
	for w in ${status[@]}; do
		worker_inuse=(${worker_inuse[*]} ${w:0-9:7})
		echo "find workers: ${w:0-9:7} for task: test"
	done
	echo "find all workers: ${worker_inuse[@]} (total: ${#worker_inuse[@]})"

	for ((i = 0; i < ${#worker_inuse[@]}; i++)); do
		wk=${worker_inuse[$i]:0-1:1} # get worker id, such as ("1", "4")
		echo "try to isolate dm-worker$wk"
		isolate_worker $wk "isolate"
	done

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$new_leader_port" \
		"pause-task test" \
		"\"result\": true" 3

	run_sql "DROP TABLE if exists $ha_test.ta;" $TIDB_PORT $TIDB_PASSWORD
	run_sql "DROP TABLE if exists $ha_test.tb;" $TIDB_PORT $TIDB_PASSWORD
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$new_leader_port" \
		"resume-task test" \
		"\"result\": true" 3 # wait for load data wait
	wait
	sleep 3

	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "[$(date)] <<<<<< finish test_isolate_master_and_worker >>>>>>"
}

function run() {
	test_isolate_master_and_worker # TICASE-934, 935, 936, 987, 992, 998, 999
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
