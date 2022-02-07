#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
TASK_CONF=$cur/conf/dm-task.yaml
TASK_NAME="test"
MYSQL1_CONF=$cur/conf/source1.yaml
SQL_RESULT_FILE="$TEST_DIR/sql_res.$TEST_NAME.txt"

# used to coverage wrong usage of dmctl command
function usage_and_arg_test() {
	echo "check_task_wrong_arg"
	check_task_wrong_arg
	check_task_wrong_config_file

	echo "pause_relay_wrong_arg"
	pause_relay_wrong_arg
	pause_relay_without_worker

	echo "resume_relay_wrong_arg"
	resume_relay_wrong_arg
	resume_relay_wihout_worker

	echo "pause_task_wrong_arg"
	pause_task_wrong_arg

	echo "resume_task_wrong_arg"
	resume_task_wrong_arg

	echo "query_status_wrong_arg"
	query_status_wrong_arg
	query_status_wrong_params

	echo "start_task_wrong_arg"
	start_task_wrong_arg
	start_task_wrong_config_file

	echo "stop_task_wrong_arg"
	stop_task_wrong_arg

	echo "show_ddl_locks_wrong_arg"
	show_ddl_locks_wrong_arg

	#    echo "update_relay_wrong_arg"
	#    update_relay_wrong_arg
	#    update_relay_wrong_config_file
	#    update_relay_should_specify_one_dm_worker $MYSQL1_CONF

	# echo "update_task_wrong_arg"
	# update_task_wrong_arg
	# update_task_wrong_config_file

	#    echo "update_master_config_wrong_arg"
	#    update_master_config_wrong_arg
	#    update_master_config_wrong_config_file
	#
	echo "purge_relay_wrong_arg"
	purge_relay_wrong_arg
	purge_relay_without_worker
	purge_relay_filename_with_multi_workers

	echo "operate_source_empty_arg"
	operate_source_empty_arg
	operate_source_wrong_config_file
	operate_source_invalid_op $MYSQL1_CONF
	operate_source_stop_not_created_config $MYSQL1_CONF

	echo "transfer_source_empty_arg"
	transfer_source_empty_arg
	transfer_source_less_arg
	transfer_source_more_arg

	echo "start_relay_empty_arg"
	start_relay_empty_arg
	start_relay_wrong_arg

	echo "stop_relay_empty_arg"
	stop_relay_empty_arg
	stop_relay_wrong_arg
}

function recover_max_binlog_size() {
	run_sql "set @@global.max_binlog_size = $1" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "set @@global.max_binlog_size = $2" $MYSQL_PORT2 $MYSQL_PASSWORD2
}

function test_operate_task_bound_to_a_source() {
	echo "############################################################"
	echo "test case for operate tasks bound to a source"
	echo "############################################################"
	#
	# both foo-task and bar-task have 2 sources
	#
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "start-task $cur/conf/foo-task.yaml" "\"result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "start-task $cur/conf/bar-task.yaml" "\"result\": true" 3

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "pause-task -s xxx" "haven't been added" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task -s xxx" "haven't been added" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "stop-task -s xxx" "haven't been added" 1

	# batch pause mysql-replica-01
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "pause-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Paused\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 2
	# batch pause mysql-replica-02
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "pause-task -s mysql-replica-02" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Paused\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Paused\"" 2

	# batch resume mysql-replica-01
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Paused\"" 2
	# batch resume mysql-replica-02
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task -s mysql-replica-02" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 2
	# stop
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "stop-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": " 0
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "stop-task -s mysql-replica-02" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": " 0
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": " 0

	#
	# bar-task has 2 sources, baz-task has 1 sources
	#
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "start-task $cur/conf/bar-task.yaml" "\"result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "start-task $cur/conf/baz-task.yaml" "\"result\": true" 2

	# batch pause mysql-replica-01
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "pause-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Paused\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 1
	# batch pause mysql-replica-02
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "pause-task -s mysql-replica-02" "\"result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Paused\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Paused\"" 1

	# batch resume mysql-replica-01
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Paused\"" 1
	# batch resume mysql-replica-02
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "resume-task -s mysql-replica-02" "\"result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 1
	# stop
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "stop-task -s mysql-replica-01" "\"result\": true" 5
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": " 0
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": \"Running\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "stop-task -s mysql-replica-02" "\"result\": true" 3
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-01" "\"stage\": " 0
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "query-status -s mysql-replica-02" "\"stage\": " 0
}

function run() {
	inject_points=(
		"github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval=return(1)"
	)
	export GO_FAILPOINTS="$(join_string \; ${inject_points[@]})"

	run_sql "show variables like 'max_binlog_size'\G" $MYSQL_PORT1 $MYSQL_PASSWORD1
	max_binlog_size1=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $NF}')
	run_sql "show variables like 'max_binlog_size'\G" $MYSQL_PORT2 $MYSQL_PASSWORD2
	max_binlog_size2=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $NF}')
	run_sql "set @@global.max_binlog_size = 12288" $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql "set @@global.max_binlog_size = 12288" $MYSQL_PORT2 $MYSQL_PASSWORD2
	trap "recover_max_binlog_size $max_binlog_size1 $max_binlog_size2" EXIT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cd $cur
	for file in "check_list"/*; do
		source $file
	done
	cd -

	mkdir -p $WORK_DIR/master $WORK_DIR/worker1 $WORK_DIR/worker2
	dm_master_conf="$WORK_DIR/master/dm-master.toml"
	dm_worker1_conf="$WORK_DIR/worker1/dm-worker.toml"
	dm_worker2_conf="$WORK_DIR/worker2/dm-worker.toml"
	cp $cur/conf/dm-worker1.toml $dm_worker1_conf
	cp $cur/conf/dm-worker2.toml $dm_worker2_conf
	cp $cur/conf/dm-master.toml $dm_master_conf

	# start dmctl when master have not started
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" "any command" \
		"can't connect to 127.0.0.1:8261" 1 \
		"Please check your network connection\." 1
	run_dm_ctl_cmd_mode $WORK_DIR "127.0.0.1:$MASTER_PORT" "any command" \
		"unknown command \"any\" for \"dmctl\"" 1 \
		"Run 'dmctl --help' for usage\." 1

	run_dm_master $WORK_DIR/master $MASTER_PORT $dm_master_conf
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# only one master, check it should be leader and etcd metrics works
	check_metric $MASTER_PORT 'etcd_server_has_leader' 3 0 2

	usage_and_arg_test

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker2/relay_log" $WORK_DIR/source2.yaml

	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# test stop source will not check connectivity to upstream
	cp $cur/conf/source1.yaml $WORK_DIR/source1-wrong.yaml
	sed '/password/d' $WORK_DIR/source1-wrong.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source stop $WORK_DIR/source1-wrong.yaml $SOURCE_ID2" \
		"\"result\": true" 3

	# ensure source1 is bound to worker1
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $dm_worker1_conf
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source show" \
		"\"result\": true" 3 \
		'msg": "source is added but there is no free worker to bound"' 1 \
		'"worker": "worker1"' 1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $dm_worker2_conf
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source show" \
		"\"result\": true" 3 \
		'"worker": "worker1"' 1 \
		'"worker": "worker2"' 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source show -s $SOURCE_ID1" \
		"\"result\": true" 2 \
		'"worker": "worker1"' 1

	test_operate_task_bound_to_a_source

	transfer_source_valid $SOURCE_ID1 worker1 # transfer to self
	transfer_source_invalid $SOURCE_ID1 worker2

	# test for start relay
	echo "kill worker2"
	ps aux | grep worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	stop_relay_on_offline_worker
	start_relay_on_offline_worker

	echo "restart worker2"
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $dm_worker2_conf
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	start_relay_success
	start_relay_diff_worker_fail
	start_relay_without_worker_name_fail
	stop_relay_without_worker_name_fail

	echo "pause_relay_success"
	pause_relay_success
	query_status_stopped_relay
	# pause twice won't receive an error now
	# pause_relay_fail
	resume_relay_success
	query_status_with_no_tasks

	echo "dmctl_check_task"
	check_task_pass $TASK_CONF
	check_task_wrong_start_time_format $cur/conf/dm-task3.yaml
	check_task_not_pass $cur/conf/dm-task2.yaml
	check_task_error_count $cur/conf/dm-task3.yaml

	echo "check_task_only_warning"
	check_task_only_warning $cur/conf/only_warning.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/only_warning.yaml" \
		"\"state\": \"warn\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $cur/conf/only_warning.yaml" \
		"\"result\": true" 2

	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task-error-database-config.yaml
	sed -i "s/password: \"\"/password: \"wrond password\"/g" $WORK_DIR/dm-task-error-database-config.yaml
	check_task_error_database_config $WORK_DIR/dm-task-error-database-config.yaml

	echo "dmctl_start_task"
	start_task_wrong_start_time_format $cur/conf/dm-task3.yaml
	dmctl_start_task
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1,$SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1 \
		"\"stage\": \"Running\"" 4
	# test whether put --master-addr to the end works
	run_dm_ctl_cmd_mode $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status -s $SOURCE_ID1,$SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1 \
		"\"stage\": \"Running\"" 4
	# update_task_not_paused $TASK_CONF

	# stop relay because get_config_to_file will stop source
	stop_relay_fail
	stop_relay_success

	# stop worker to test query-status works well when no worker
	ps aux | grep dmctl_basic/worker1 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER1_PORT 20
	ps aux | grep dmctl_basic/worker2 | awk '{print $2}' | xargs kill || true
	check_port_offline $WORKER2_PORT 20

	query_status_with_offline_worker

	# start worker again
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $dm_worker1_conf
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $dm_worker2_conf
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	echo "config"
	config_wrong_arg
	config_to_file

	# retry to wait for recovered from etcd ready
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl_cmd_mode $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"stage\": \"Running\"" 2

	echo "show_ddl_locks_no_locks"
	show_ddl_locks_no_locks $TASK_NAME
	query_status_with_tasks
	pause_task_success $TASK_NAME
	query_status_paused_tasks

	echo "dmctl master_addr"
	dmctl_multiple_addrs
	dmctl_unwrap_schema
	dmctl_wrong_addrs
	dmctl_no_addr

	# echo "update_task_worker_not_found"
	# update_task_worker_not_found $TASK_CONF 127.0.0.1:9999
	# update_task_success_single_worker $TASK_CONF $SOURCE_ID1
	# update_task_success $TASK_CONF

	start_relay_without_worker_name_success
	start_relay_with_worker_name_fail
	stop_relay_with_worker_name_fail
	stop_relay_with_worker_name_success

	start_relay_success

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	resume_task_success $TASK_NAME
	query_status_running_tasks
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 20

	# test use task file instead of task name
	pause_task_success "$cur/conf/dm-task.yaml"
	resume_task_success "$cur/conf/dm-task.yaml"

	#    update_relay_success $cur/conf/source1.yaml $SOURCE_ID1
	#    update_relay_success $cur/conf/source2.yaml $SOURCE_ID2
	# check worker config backup file is correct
	[ -f $WORK_DIR/worker1/dm-worker-config.bak ] && cmp $WORK_DIR/worker1/dm-worker-config.bak $cur/conf/dm-worker1.toml
	[ -f $WORK_DIR/worker2/dm-worker-config.bak ] && cmp $WORK_DIR/worker2/dm-worker-config.bak $cur/conf/dm-worker2.toml
	# check worker config has been changed
	md5_new_worker1=$(md5sum $dm_worker1_conf | awk '{print $1}')
	md5_new_worker2=$(md5sum $dm_worker2_conf | awk '{print $1}')
	md5_old_worker1=$(md5sum $cur/conf/dm-worker1.toml | awk '{print $1}')
	md5_old_worker2=$(md5sum $cur/conf/dm-worker2.toml | awk '{print $1}')
	[ "md5_new_worker1" != "md5_old_worker1" ]
	[ "md5_new_worker2" != "md5_old_worker2" ]

	#    update_master_config_success $dm_master_conf
	#    cmp $dm_master_conf $cur/conf/dm-master.toml

	#   TODO: The ddl sharding part for DM-HA still has some problem. This should be uncommented when it's fixed.
	#    run_sql_file $cur/data/db1.increment2.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	#    set +e
	#    i=0
	#    while [ $i -lt 10 ]
	#    do
	#        show_ddl_locks_with_locks "$TASK_NAME-\`dmctl\`.\`t_target\`" "ALTER TABLE \`dmctl\`.\`t_target\` DROP COLUMN \`d\`"
	#        ((i++))
	#        if [ "$?" != 0 ]; then
	#            echo "wait 1s and check for the $i-th time"
	#            sleep 1
	#        else
	#            break
	#        fi
	#    done
	#    set -e
	#    if [ $i -ge 10 ]; then
	#        echo "show_ddl_locks_with_locks check timeout"
	#        exit 1
	#    fi
	#    run_sql_file $cur/data/db2.increment2.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	#    check_sync_diff $WORK_DIR $cur/conf/diff_config.toml 10
	#    show_ddl_locks_no_locks $TASK_NAME

	# make sure every shard table in source 1 has be forwarded to newer binlog, so older relay log could be purged
	run_sql_source1 "flush logs"
	run_sql_source1 "update dmctl.t_1 set d = '' where id = 13"
	run_sql_source1 "update dmctl.t_2 set d = '' where id = 12"

	# sleep 2*1s to ensure syncer unit has flushed global checkpoint and updates
	# updated ActiveRelayLog
	sleep 2
	server_uuid=$(tail -n 1 $WORK_DIR/worker1/relay_log/server-uuid.index)
	run_sql_source1 "show binary logs\G"
	max_binlog_name=$(grep Log_name "$SQL_RESULT_FILE" | tail -n 1 | awk -F":" '{print $NF}')
	binlog_count=$(grep Log_name "$SQL_RESULT_FILE" | wc -l)
	relay_log_count=$(($(ls $WORK_DIR/worker1/relay_log/$server_uuid | wc -l) - 1))
	[ "$binlog_count" -eq "$relay_log_count" ]
	[ "$relay_log_count" -ne 1 ]
	purge_relay_success $max_binlog_name $SOURCE_ID1
	new_relay_log_count=$(($(ls $WORK_DIR/worker1/relay_log/$server_uuid | wc -l) - 1))
	[ "$new_relay_log_count" -eq 1 ]

	# not be skipped by source configure's sql-pattern because binlog-filter's table name
	run_sql_source1 "alter table dmctl.t_1 add column aaa int"
	# skipped by binlog-filter's table name
	# ps: maybe original reason is to test the task config's filter is prior than source's
	# ps: but due to binlog-filter's logic, the unmatched table name will be skipped directly.
	run_sql_source1 "alter table dmctl.t_2 add column aaa int"
	# skipped by sql-pattern
	run_sql_source2 "alter table dmctl.t_1 add column aaa int"
	# skipped by sql-pattern
	run_sql_source2 "alter table dmctl.t_2 add column aaa int"

	# all t_1 synced, all t_2 unsynced
	run_sql_source1 "alter table dmctl.t_2 add column bbb int"
	run_sql_source2 "alter table dmctl.t_2 add column bbb int"

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"detect inconsistent DDL sequence" 2
}

cleanup_data dmctl
# also cleanup dm processes in case of last run failed
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
