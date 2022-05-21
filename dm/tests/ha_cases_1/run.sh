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

function test_kill_master() {
	echo "[$(date)] <<<<<< start test_kill_master >>>>>>"
	test_running

	echo "kill dm-master1"
	kill_process dm-master1
	check_master_port_offline 1
	rm -rf $WORK_DIR/master1/default.master1

	echo "waiting 5 seconds"
	sleep 5
	echo "check task is running"
	check_http_alive 127.0.0.1:$MASTER_PORT2/apis/${API_VERSION}/status/test '"stage": "Running"' 10

	echo "check master2,3 are running"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT2" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT3" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
	sleep 2

	echo "use sync_diff_inspector to check increment2 data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_kill_master >>>>>>"
}

function test_kill_and_isolate_worker() {
	inject_points=("github.com/pingcap/tiflow/dm/dm/worker/defaultKeepAliveTTL=return(1)"
		"github.com/pingcap/tiflow/dm/dm/worker/defaultRelayKeepAliveTTL=return(2)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"
	echo "[$(date)] <<<<<< start test_kill_and_isolate_worker >>>>>>"
	test_running

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2

	echo "kill dm-worker2"
	kill_process dm-worker2
	check_port_offline $WORKER2_PORT 20
	rm -rf $WORK_DIR/worker2/relay_log
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"result\": false" 1

	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	echo "wait and check task running"
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

	run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker3 worker4" \
		"\"result\": true" 3

	echo "restart dm-worker3"
	kill_process dm-worker3
	check_port_offline $WORKER3_PORT 20
	rm -rf $WORK_DIR/worker3/relay_log

	echo "wait and check task running"
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/test '"stage": "Running"' 10

	run_dm_worker $WORK_DIR/worker3 $WORKER3_PORT $cur/conf/dm-worker3.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER3_PORT

	echo "isolate dm-worker4"
	isolate_worker 4 "isolate"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

	echo "isolate dm-worker3"
	isolate_worker 3 "isolate"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"stage\": \"Running\"" 1 \
		"\"result\": false" 1

	echo "disable isolate dm-worker4"
	isolate_worker 4 "disable_isolate"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT1" \
		"query-status test" \
		"\"stage\": \"Running\"" 3

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

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 3

	echo "restart worker4"
	kill_process dm-worker4
	check_port_offline $WORKER4_PORT 20
	rm -rf $WORK_DIR/worker4/relay_log
	run_dm_worker $WORK_DIR/worker4 $WORKER4_PORT $cur/conf/dm-worker4.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER4_PORT

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	run_sql_file_withdb $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1 $ha_test
	run_sql_file_withdb $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2 $ha_test
	sleep 2

	echo "use sync_diff_inspector to check increment2 data now!"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	echo "[$(date)] <<<<<< finish test_kill_and_isolate_worker >>>>>>"
	export GO_FAILPOINTS=""
}

function run() {
	test_kill_master             # TICASE-996, 958
	test_kill_and_isolate_worker # TICASE-968, 973, 1002, 975, 969, 972, 974, 970, 971, 976, 978, 988
}

cleanup_data $ha_test
cleanup_data $ha_test2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
