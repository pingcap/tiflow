#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
# import helper functions
source $cur/../_utils/ha_cases_lib.sh

function test_running() {
	echo "[$(date)] <<<<<< start test_running >>>>>>"
	cleanup
	prepare_sql
	start_cluster

	# make sure task to step in "Sync" stage
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2

	echo "use sync_diff_inspector to check full dump loader"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	echo "flush logs to force rotate binlog file"
	run_sql "flush logs;" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "flush logs;" $MYSQL_PORT2 $MYSQL_PASSWORD2

	echo "apply increment data before restart dm-worker to ensure entering increment phase"
	run_sql_file_withdb $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql_file_withdb $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test

	sleep 3 # wait for flush checkpoint
	echo "use sync_diff_inspector to check increment data"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_running >>>>>>"
}

# usage: test_kill_master_in_sync leader
# or: test_kill_master_in_sync follower (default)
function test_kill_master_in_sync() {
	echo "[$(date)] <<<<<< start test_kill_master_in_sync >>>>>>"
	test_running

	echo "start dumping SQLs into source"
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

	ps aux | grep dm-master1 | awk '{print $2}' | xargs kill || true
	check_master_port_offline 1

	echo "wait and check task running"
	sleep 1
	check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"stage": "Running"' 10
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	# waiting for syncing
	wait

	echo "wait for dm to sync"
	sleep 1
	echo "use sync_diff_inspector to check data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_kill_master_in_sync >>>>>>"
}

function test_kill_worker_in_sync() {
	echo "[$(date)] <<<<<< start test_kill_worker_in_sync >>>>>>"
	test_running

	echo "start dumping SQLs into source"
	load_data $MYSQL_PORT1 $MYSQL_PASSWORD1 "a" &
	load_data $MYSQL_PORT2 $MYSQL_PASSWORD2 "b" &

	echo "kill dm-worker1"
	ps aux | grep dm-worker1 | awk '{print $2}' | xargs kill || true
	echo "start worker3"
	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	# start-relay halfway
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker3" \
		"\"result\": true" 1

	echo "kill dm-worker2"
	ps aux | grep dm-worker2 | awk '{print $2}' | xargs kill || true
	echo "start worker4"
	run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

	echo "wait and check task running"
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

	echo "query-status from all dm-master"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	# waiting for syncing
	wait

	echo "wait for dm to sync"
	sleep 1

	echo "use sync_diff_inspector to check data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_kill_worker_in_sync >>>>>>"
}

function run() {
	test_kill_master_in_sync
	test_kill_worker_in_sync
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
