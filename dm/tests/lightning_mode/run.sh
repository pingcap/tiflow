#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
API_VERSION="v1alpha1"
ILLEGAL_CHAR_NAME='t-Ë!s`t'

function run() {
	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO'"

	inject_points=(
		"github.com/pingcap/tiflow/dm/dm/worker/TaskCheckInterval=return(\"500ms\")"
		"github.com/pingcap/tiflow/dm/relay/NewUpstreamServer=return(true)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	# start DM worker and master
	run_dm_master_info_log $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	check_metric $MASTER_PORT 'start_leader_counter' 3 0 2
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml
	# make sure source1 is bound to worker1
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# start DM task only
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/name: test/name: $ILLEGAL_CHAR_NAME/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task "$WORK_DIR/dm-task.yaml" "--remove-meta"
	# check task has started
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 10 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 10 1 3

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# check default session config
	check_log_contain_with_retry '\\"tidb_txn_mode\\":\\"optimistic\\"' $WORK_DIR/worker1/log/dm-worker.log
	check_log_contain_with_retry '\\"tidb_txn_mode\\":\\"optimistic\\"' $WORK_DIR/worker2/log/dm-worker.log

	# restart dm-worker1
	pkill -hup -f dm-worker1.toml 2>/dev/null || true
	wait_pattern_exit dm-worker1.toml
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	# make sure worker1 have bound a source, and the source should same with bound before
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"worker1" 1

	# restart dm-worker2
	pkill -hup -f dm-worker2.toml 2>/dev/null || true
	wait_pattern_exit dm-worker2.toml
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	check_metric $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 10 1 3
	check_metric $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 10 1 3

	sleep 10
	echo "after restart dm-worker, task should resume automatically"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/dm-task.yaml" \
		"\"result\": false" 1 \
		"subtasks with name $ILLEGAL_CHAR_NAME for sources \[mysql-replica-01 mysql-replica-02\] already exist" 1
	sleep 2

	# wait for task running
	check_http_alive 127.0.0.1:$MASTER_PORT/apis/${API_VERSION}/status/$ILLEGAL_CHAR_NAME '"stage": "Running"' 10
	sleep 2 # still wait for subtask running on other dm-workers

	# kill tidb
	pkill -hup tidb-server 2>/dev/null || true
	wait_process_exit tidb-server

	# dm-worker execute sql failed, and will try auto resume task
	run_sql_file $cur/data/db2.increment0.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	sleep 2
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "dispatch auto resume task"
	check_log_contains $WORK_DIR/worker2/log/dm-worker.log "lightning"

	# restart tidb, and task will recover success
	run_tidb_server 4000 $TIDB_PASSWORD
	sleep 2

	# test after pause and resume relay, relay could continue from syncer's checkpoint
	run_sql_source1 "flush logs"
	run_sql_file $cur/data/db1.increment0.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1

	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay -s mysql-replica-01" \
		"\"result\": true" 2
	# we used failpoint to imitate an upstream switching, which purged whole relay dir
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-relay -s mysql-replica-01" \
		"\"result\": true" 2

	sleep 2
	# relay should continue pulling from syncer's checkpoint, so only pull the latest binlog
	server_uuid=$(tail -n 1 $WORK_DIR/worker1/relay_log/server-uuid.index)
	echo "relay logs $(ls $WORK_DIR/worker1/relay_log/$server_uuid)"
	relay_log_num=$(ls $WORK_DIR/worker1/relay_log/$server_uuid | grep -v 'relay.meta' | wc -l)
	[ $relay_log_num -eq 1 ]

	# use sync_diff_inspector to check data now!
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	# test block-allow-list by the way
	run_sql "show databases;" $TIDB_PORT $TIDB_PASSWORD
	check_not_contains "ignore_db"
	check_contains "lightning_mode"

	echo "check dump files have been cleaned"
	ls $WORK_DIR/worker1/dumped_data.$ILLEGAL_CHAR_NAME && exit 1 || echo "worker1 auto removed dump files"
	ls $WORK_DIR/worker2/dumped_data.$ILLEGAL_CHAR_NAME && exit 1 || echo "worker2 auto removed dump files"

	echo "check no password in log"
	check_log_not_contains $WORK_DIR/master/log/dm-master.log "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="
	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="
	check_log_not_contains $WORK_DIR/worker2/log/dm-worker.log "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="
	check_log_not_contains $WORK_DIR/master/log/dm-master.log "123456"
	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log "123456"
	check_log_not_contains $WORK_DIR/worker2/log/dm-worker.log "123456"

	# test drop table if exists
	run_sql_source1 "drop table if exists \`lightning_mode\`.\`tb1\`;"
	run_sql_source1 "drop table if exists \`lightning_mode\`.\`tb1\`;"
	run_sql_source2 "drop table if exists \`lightning_mode\`.\`tb2\`;"
	run_sql_source2 "drop table if exists \`lightning_mode\`.\`tb2\`;"
	check_log_not_contains $WORK_DIR/worker1/log/dm-worker.log "Error .* Table .* doesn't exist"
	check_log_not_contains $WORK_DIR/worker2/log/dm-worker.log "Error .* Table .* doesn't exist"

	# test Db not exists should be reported

	run_sql_tidb "drop database lightning_mode"
	run_sql_source1 "create table lightning_mode.db_error (c int primary key);"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $ILLEGAL_CHAR_NAME" \
		"Error 1049: Unknown database" 1

	# stop task, task state should be cleaned
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $ILLEGAL_CHAR_NAME" \
		"\"result\": true" 3
	check_metric_not_contains $WORKER1_PORT "dm_worker_task_state{source_id=\"mysql-replica-01\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker1\"}" 3
	check_metric_not_contains $WORKER2_PORT "dm_worker_task_state{source_id=\"mysql-replica-02\",task=\"$ILLEGAL_CHAR_NAME\",worker=\"worker2\"}" 3

	# all unit without error.
	check_metric_not_contains $WORKER1_PORT "dm_mydumper_exit_with_error_count" 3
	check_metric_not_contains $WORKER1_PORT "dm_loader_exit_with_error_count" 3
	check_metric_not_contains $WORKER1_PORT "dm_syncer_exit_with_error_count" 3

	# check syncer metrics
	check_two_metric_equal $WORKER1_PORT 'dm_syncer_binlog_file{node="master"' 'dm_syncer_binlog_file{node="syncer"' 3
	check_two_metric_equal $WORKER2_PORT 'dm_syncer_binlog_file{node="master"' 'dm_syncer_binlog_file{node="syncer"' 3
	export GO_FAILPOINTS=''

	run_sql_both_source "SET @@GLOBAL.SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
}

cleanup_data lightning_mode
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
