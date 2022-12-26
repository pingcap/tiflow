#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"

function complex_behaviour() {
	cleanup_data expr_filter

	run_dm_master $WORK_DIR/master $MASTER_PORT1 $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_sql_file $cur/data/db1.prepare2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# test no permission
	chmod -w $WORK_DIR/worker1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task2.yaml"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"dumpling runs with error" 1 \
		"permission denied" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test"
	chmod +w $WORK_DIR/worker1

	dmctl_start_task_standalone $cur/conf/dm-task2.yaml

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"synced\": true" 1

	run_sql_tidb "select count(0) from expr_filter.t2"
	check_contains "count(0): 6"
	run_sql_tidb "select count(1) from expr_filter.t2 where should_skip = 0"
	check_contains "count(1): 6"
	run_sql_tidb "select count(2) from expr_filter.t2 where should_skip = 1"
	check_contains "count(2): 0"

	run_sql_tidb "select count(3) from expr_filter.t3"
	check_contains "count(3): 1"
	run_sql_tidb "select count(4) from expr_filter.t3 where should_skip = 1"
	check_contains "count(4): 0"

	run_sql_tidb "select count(5) from expr_filter.t4"
	check_contains "count(5): 1"
	run_sql_tidb "select count(6) from expr_filter.t4 where should_skip = 1"
	check_contains "count(6): 0"

	run_sql_tidb "select count(7) from expr_filter.t5"
	check_contains "count(7): 4"
	run_sql_tidb "select count(8) from expr_filter.t5 where should_skip = 1"
	check_contains "count(8): 0"

	run_sql_tidb "select count(9) from expr_filter.t6 where name = 'Müller' and msg = 'Müller' and name2 = 'Müller'"
	check_contains "count(9): 2"
	run_sql_tidb "select count(10) from expr_filter.t6 where name != 'Müller'"
	check_contains "count(10): 0"

	run_sql_tidb "select count(11) from expr_filter.t7 where r = 'a' and s = 2"
	check_contains "count(11): 1"
	run_sql_tidb "select count(12) from expr_filter.t7 where r = 'a' and s = 3"
	check_contains "count(12): 0"

	insert_num=$(grep -o '"number of filtered insert"=[0-9]\+' $WORK_DIR/worker1/log/dm-worker.log | grep -o '[0-9]\+' | awk '{n += $1}; END{print n}')
	[ $insert_num -eq 6 ]
	update_num=$(grep -o '"number of filtered update"=[0-9]\+' $WORK_DIR/worker1/log/dm-worker.log | grep -o '[0-9]\+' | awk '{n += $1}; END{print n}')
	[ $update_num -eq 4 ]

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test"

	ls $WORK_DIR/worker1/schema-tracker* && exit 1 || echo "schema tracker path has been cleaned"

	cleanup_process $*
	cleanup_data expr_filter
}

function run() {
	complex_behaviour

	run_dm_master $WORK_DIR/master $MASTER_PORT1 $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	dmctl_start_task_standalone $cur/conf/dm-task.yaml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# too many expression filter will slow the sync
	sleep 5
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"synced\": true" 1

	run_sql_source1 "select count(9) from expr_filter.t1"
	check_contains "count(9): 60"

	run_sql_tidb "select count(10) from expr_filter.t1"
	check_contains "count(10): 30"

	run_sql_tidb "select count(11) from expr_filter.t1 where should_skip = 1"
	check_contains "count(11): 0"

	insert_num=$(grep -o '"number of filtered insert"=[0-9]\+' $WORK_DIR/worker1/log/dm-worker.log | grep -o '[0-9]\+' | awk '{n += $1}; END{print n}')
	[ $insert_num -eq 30 ]
}

cleanup_data expr_filter
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
