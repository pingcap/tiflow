#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
db1="downstream_more_column1"
tb1="t1"
db="downstream_more_column"
tb="t"

# pk/uk added using alter stmt in tidb will be appended to the table
# unlike mysql which will put pk/uk at the beginning
# this test make sure we can handle this case
# may panic previously since tracker uses the wrong index to get pk
# see https://github.com/pingcap/tiflow/issues/5159
function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	# start a task
	cat $cur/conf/dm-task.yaml >$WORK_DIR/dm-task.yaml
	dmctl_start_task_standalone $WORK_DIR/dm-task.yaml "--remove-meta"

	# make sure we're in sync unit
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"result\": true" 2 \
		"\"unit\": \"Sync\"" 1

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_sql "select * from dm_syncer_tracker.t" $TIDB_PORT $TIDB_PASSWORD
	check_not_contains "dm_syncer_ignore_db"
}

cleanup_data downstream_more_column
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
