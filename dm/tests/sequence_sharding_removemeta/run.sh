#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

function run() {
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	check_metric_not_contains $MASTER_PORT 'dm_master_ddl_state_number' 3

	# start DM task only
	dmctl_start_task

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	sleep 3
	# check task's ddl unsynced locks
	task_name="sequence_sharding_removemeta"
	lock_id="$task_name-\`sharding_target3\`.\`t_target\`"
	ddl="ALTER TABLE \`sharding_target3\`.\`t_target\` ADD COLUMN \`d\` INT"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"ID\": \"$lock_id\"" 1 \
		"$ddl" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $TEST_NAME" \
		"this DM-worker doesn't receive any shard DDL of this group" 0 \
		"\"masterBinlog\": \"\"" 0
	check_metric $MASTER_PORT 'dm_master_ddl_state_number{task="sequence_sharding_removemeta",type="Un-synced"}' 3 0 2

	dmctl_stop_task $task_name

	# clean downstream data
	run_sql "drop database if exists sharding_target3" $TIDB_PORT $TIDB_PASSWORD
	# run all the data
	run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	# start again with remove-meta
	dmctl_start_task "$cur/conf/dm-task.yaml" "--remove-meta"
	sleep 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"no DDL lock exists" 1
	check_metric_not_contains $MASTER_PORT 'dm_master_ddl_state_number' 3
	# use sync_diff_inspector to check full data
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	# test unlock-ddl-lock could work after stop-task
	ddl="ALTER TABLE \`sharding_target3\`.\`t_target\` ADD COLUMN \`f\` INT"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"ID\": \"$lock_id\"" 1 \
		"$ddl" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $TEST_NAME" \
		"this DM-worker doesn't receive any shard DDL of this group" 1

	check_metric $MASTER_PORT 'dm_master_ddl_state_number{task="sequence_sharding_removemeta",type="Un-synced"}' 3 0 2
	dmctl_stop_task $task_name

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock unlock $lock_id" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"no DDL lock exists" 1
	check_metric_not_contains $MASTER_PORT 'dm_master_ddl_state_number' 3
}

cleanup_data sharding_target3
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
