#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME

db_name=$TEST_NAME

help_cnt=46

function run() {
	# check dmctl output with help flag
	# it should usage for root command
	$PWD/bin/dmctl.test DEVEL --help >$WORK_DIR/help.log
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | wc -l | xargs)
	if [ "$help_msg_cnt" != $help_cnt ]; then
		echo "dmctl case 1 help failed: $help_msg"
		exit 1
	fi

	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# check dmctl command start-task output with master-addr
	# it should usage for start-task
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT start-task >$WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
	help_msg=$(cat $WORK_DIR/help.log)

	echo $help_msg | grep -q "dmctl start-task"
	if [ $? -ne 0 ]; then
		echo "dmctl case 2 help failed: $help_msg"
		exit 1
	fi

	# check dmctl command start-task output with master-addr and unknown flag
	# it should print unknown command xxxx
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT xxxx start-task >$WORK_DIR/help.log 2>&1 && exit 1 || echo "exit code should be not zero"
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | wc -l | xargs)
	if [ "$help_msg_cnt" -lt 1 ]; then
		echo "dmctl case 3 help failed: $help_msg"
		exit 1
	fi
	echo $help_msg | grep -q "unknown command \"xxxx\" for \"dmctl\""
	if [ $? -ne 0 ]; then
		echo "dmctl case 3 help failed: $help_msg"
		exit 1
	fi

	# check dmctl command start-task --help
	# it should print the usage for start-task
	$PWD/bin/dmctl.test DEVEL start-task --help >$WORK_DIR/help.log
	help_msg=$(cat $WORK_DIR/help.log)
	help_msg_cnt=$(echo "${help_msg}" | grep "Usage:" | wc -l)
	if [ "$help_msg_cnt" -ne 1 ]; then
		echo "dmctl case 4 help failed: $help_msg"
		exit 1
	fi

	# run normal task with command
	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	check_contains 'Query OK, 2 rows affected'
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_contains 'Query OK, 3 rows affected'

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	# check wrong backoff-max
	cp $cur/conf/source1.yaml $WORK_DIR/wrong-source.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/wrong-source.yaml
	sed -i "s/backoff-max: 5m/backoff-max: 0.1s/g" $WORK_DIR/wrong-source.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/wrong-source.yaml" \
		"\"result\": false" 1 \
		"Please increase \`backoff-max\`" 1

	# operate mysql config to worker
	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	sed -i "/relay-binlog-name/i\relay-dir: $WORK_DIR/worker1/relay_log" $WORK_DIR/source1.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source1.yaml -w $WORKER1_NAME" \
		"\"result\": true" 2 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"list-member --name worker1" \
		"\"stage\": \"bound\"" 1 \
		"\"source\": \"mysql-replica-01\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source2.yaml -w wrong-worker" \
		"\"result\": false" 1 \
		"not exists" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create $WORK_DIR/source2.yaml -w worker1" \
		"\"result\": false" 1 \
		"not free" 1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	# check wrong do-tables
	cp $cur/conf/dm-task.yaml $WORK_DIR/wrong-dm-task.yaml
	sed -i "/do-dbs:/a\    do-tables:\n    - db-name: \"dmctl_command\"" $WORK_DIR/wrong-dm-task.yaml
	sed -i "/do-dbs:/d" $WORK_DIR/wrong-dm-task.yaml
	echo "ignore-checking-items: [\"all\"]" >>$WORK_DIR/wrong-dm-task.yaml

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/wrong-dm-task.yaml" \
		"Table string cannot be empty" 1

	# check wrong chunk-filesize
	cp $cur/conf/dm-task.yaml $WORK_DIR/wrong-dm-task.yaml
	sed -i "s/chunk-filesize: 64/chunk-filesize: 6qwe4/g" $WORK_DIR/wrong-dm-task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/wrong-dm-task.yaml" \
		"invalid \`chunk-filesize\` 6qwe4" 1

	# start DM task with command mode
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $cur/conf/dm-task.yaml" \
		"\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead" 1
	check_log_contains $WORK_DIR/master/log/dm-master.log "\`remove-meta\` in task config is deprecated, please use \`start-task ... --remove-meta\` instead"

	# use sync_diff_inspector to check full dump loader
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 2\/1\/1\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 2

	# query status with command mode
	$PWD/bin/dmctl.test DEVEL --master-addr=:$MASTER_PORT query-status >$WORK_DIR/query-status.log

	# use DM_MASTER_ADDR env
	export DM_MASTER_ADDR="127.0.0.1:$MASTER_PORT"
	run_dm_ctl_with_retry $WORK_DIR "" \
		"query-status test" \
		"worker1" 1

	running_task=$(grep -r Running $WORK_DIR/query-status.log | wc -l | xargs)

	if [ "$running_task" != 1 ]; then
		echo "query status failed with command: $running_task task"
		exit 1
	fi
}

function checktask_full_mode_conn() {
	# full mode
	# dumpers: (2 + 2) for each
	# loaders: 5 + 1 = 6
	run_sql_source1 "set @@GLOBAL.max_connections=3;"
	check_task_not_pass $cur/conf/dm-task3.yaml # dumper threads too few
	run_sql_source1 "set @@GLOBAL.max_connections=4;"
	check_task_pass $cur/conf/dm-task3.yaml

	run_sql_tidb "set @@GLOBAL.max_connections=5;" # loader threads too few
	check_task_not_pass $cur/conf/dm-task3.yaml
	run_sql_tidb "set @@GLOBAL.max_connections=6;"
	check_task_pass $cur/conf/dm-task3.yaml

	# test no enough privilege
	cp $cur/conf/dm-task3.yaml $WORK_DIR/temp.yaml
	sed -i "s/  user: \"root\"/  user: \"test1\"/g" $WORK_DIR/temp.yaml
	run_sql_tidb "drop user if exists test1"
	run_sql_tidb "create user test1;"
	run_sql_tidb "grant select, create, insert, delete, alter, drop, index on *.* to test1;" # no super privilege
	run_sql_tidb "flush privileges;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $WORK_DIR/temp.yaml" \
		"lack of Super global" 1
	run_sql_tidb "drop user test1;"

	run_sql_source1 "set @@GLOBAL.max_connections=151;"
	run_sql_source2 "set @@GLOBAL.max_connections=151;"
	run_sql_tidb "set @@GLOBAL.max_connections=151;"
}

function check_task_lightning() {
	# unlimited connections: no need to warn
	run_sql_tidb "set @@GLOBAL.max_connections=0;"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task2.yaml" \
		"\"passed\": true" 1 \
		"task precheck cannot accurately check the number of connection needed for Lightning" 0
	run_sql_tidb "set @@GLOBAL.max_connections=5;"
	# fail but give warning, because it's using Lightining
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task2.yaml" \
		"task precheck cannot accurately check the number of connection needed for Lightning" 1
}

function check_full_mode_conn() {
	# TODO: currently, pool-size are not efficacious for Lightning
	# which simply determines the concurrency by hardware conditions.
	# This should be solved in the future.
	run_sql_tidb "set @@GLOBAL.max_connections=151;"
	run_sql_source1 "set @@GLOBAL.max_connections=151;"
	run_sql_source2 "set @@GLOBAL.max_connections=151;"
	run_sql_tidb "drop database if exists dmctl_conn"
	run_sql_both_source "drop database if exists dmctl_conn"
	run_sql_both_source "create database dmctl_conn"
	# ref: many_tables/run.sh
	for ((i = 0; i <= 1000; ++i)); do
		run_sql_source1 "create table dmctl_conn.test_$i(id int primary key)"
		run_sql_source1 "insert into dmctl_conn.test_$i values (1),(2),(3),(4),(5)"
	done
	dmctl_start_task_standalone "$cur/conf/dm-task3.yaml" --remove-meta
	run_sql_source1 'SHOW PROCESSLIST;'
	check_rows_equal 5 # 4 + 1 for SHOWPROCESSLIST

	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Load" 1
	run_sql_tidb 'SHOW PROCESSLIST;'
	check_rows_equal 7 # (5 + 1) + 1 for SHOW PROCESSLIST= 7

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 2
	run_sql_tidb "drop database if exists dm_meta" # cleanup checkpoint
	run_sql_tidb "drop database if exists dmctl_conn"
}

function run_validation_start_stop_cmd {
	cleanup_data dmctl_command
	cleanup_data_upstream dmctl_command
	cleanup_process $*

	export GO_FAILPOINTS=""
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2
	dmctl_start_task $cur/conf/dm-task-no-validator.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"unit\": \"Sync\"" 2

	echo "--> (fail) validation start: missing mode value"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --mode" \
		"Error: flag needs an argument: --mode" 1

	echo "--> (fail) validation start: invalid mode"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --mode xxx" \
		"Error: mode should be either \`full\` or \`fast\`" 1

	echo "--> (fail) validation start: missing start-time value"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time" \
		"Error: flag needs an argument: --start-time" 1

	echo "--> (fail) validation start: invalid start-time"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --start-time xx" \
		"Error: start-time should be in the format like '2006-01-02 15:04:05' or '2006-01-02T15:04:05'" 1

	echo "--> (fail) validation start: without all-task and task-name"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --mode full" \
		"Error: either \`task-name\` or \`all-task\` should be set" 1

	echo "--> (fail) validation start: with both all-task and task-name"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --all-task test" \
		"Error: either \`task-name\` or \`all-task\` should be set" 1

	echo "--> (fail) validation start: too many arguments"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test test2" \
		"Error: too many arguments are specified" 1

	echo "--> (fail) validation start: non-existed subtask"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start xxxxx" \
		"\"result\": false" 1 \
		"cannot get subtask by task name \`xxxxx\` and sources" 1

	echo "--> (fail) validation start: non-exist source"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start -s xxxxx --all-task" \
		"\"result\": false" 1 \
		"cannot get subtask by sources" 1

	echo "--> (fail) validation stop: without all-task and task-name"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop" \
		"Error: either \`task-name\` or \`all-task\` should be set" 1

	echo "--> (fail) validation stop: with both all-task and task-name"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop --all-task test" \
		"Error: either \`task-name\` or \`all-task\` should be set" 1

	echo "--> (fail) validation stop: too many arguments"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test test2" \
		"Error: too many arguments are specified" 1

	echo "--> (fail) validation stop: non-existed subtask"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop xxxxx" \
		"\"result\": false" 1 \
		"cannot get subtask by task name \`xxxxx\` and sources" 1

	echo "--> (fail) validation stop: non-exist source"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop -s xxxxx --all-task" \
		"\"result\": false" 1 \
		"cannot get subtask by sources" 1

	echo "--> (fail) validation stop: stop not-enabled validator"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop -s $SOURCE_ID1 test" \
		"\"result\": false" 1 \
		"some target validator(test with source $SOURCE_ID1) is not enabled" 1

	echo "--> (success) validation start: start validation without explicit mode for source 1"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start -s $SOURCE_ID1 test" \
		"\"result\": true" 1
	# right now validation status cannot check status of subtask.
	# in the following case, it will be checked.

	echo "--> (fail) validation start: start all validator with explicit mode, but 1 subtask already enabled"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --mode full test" \
		"\"result\": false" 1 \
		"some of target validator(test with source $SOURCE_ID1) has already enabled" 1

	echo "--> (fail) validation start: start validation with explicit mode for source 1 again"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start -s $SOURCE_ID1 --mode full test" \
		"\"result\": false" 1 \
		"all target validator has enabled, cannot do 'validation start' with explicit mode or start-time" 1

	echo "--> (fail) validation start: start all validator without explicit mode"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": false" 1 \
		"some of target validator(test with source $SOURCE_ID1) has already enabled" 1

	echo "--> (fail) validation stop: stop all but 1 subtask is not enabled"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test" \
		"\"result\": false" 1 \
		"some target validator(test with source $SOURCE_ID2) is not enabled" 1

	echo "--> (success) validation start: start validation with fast mode and start-time for source 2"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start -s $SOURCE_ID2 --mode fast --start-time '2020-01-01 00:01:00' test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 0

	echo "--> (success) validation start: start all validator of the task without explicit param again, i.e. resuming"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 0

	echo "--> (success) validation stop: stop validation of source 1"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop -s $SOURCE_ID1 test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1

	echo "--> (success) validation stop: stop all of test"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 2

	echo "--> (success) validation stop: stop all of test again"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 2

	echo "--> (success) validation start: start all of test"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 0

	echo "--> (success) validation stop: multiple source"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop -s $SOURCE_ID1 -s $SOURCE_ID2 test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 2

	echo "--> (success) validation start: multiple source"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start -s $SOURCE_ID1 -s $SOURCE_ID2 test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 1 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 0

	dmctl_stop_task "test"
	dmctl_start_task $cur/conf/dm-task-no-validator.yaml --remove-meta
	dmctl_start_task_standalone $cur/conf/dm-task2-no-validator.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"unit\": \"Sync\"" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test2" \
		"\"unit\": \"Sync\"" 1

	echo "--> (success) validation start: start all tasks"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation start --all-task --mode fast" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 0 \
		"\"mode\": \"fast\"" 2 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test2" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 0 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 0

	echo "--> (success) validation stop: stop all tasks"
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop --all-task" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 0 \
		"\"mode\": \"fast\"" 2 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test2" \
		"\"result\": true" 1 \
		"\"mode\": \"full\"" 0 \
		"\"mode\": \"fast\"" 1 \
		"\"stage\": \"Running\"" 0 \
		"\"stage\": \"Stopped\"" 1
}
function query_status_disable_validator() {
	dmctl_start_task $cur/conf/dm-task2.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test2" \
		"\"validation\": null"
	dmctl_stop_task "test2"
}

function trigger_checkpoint_flush() {
	sleep 1.5
	run_sql_source1 "alter table $db_name.t1 comment 'a';" # force flush checkpoint
}

function prepare_for_validator_cmd {
	cleanup_process $*
	cleanup_data $db_name
	cleanup_data_upstream $db_name
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	# skip incremental rows with id <= 2
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/syncer/SkipDML=return(2)'
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2

	query_status_disable_validator

	dmctl_start_task $cur/conf/dm-task.yaml --remove-meta
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml

	run_sql_source1 "insert into dmctl_command.t1 values(0,'ignore-row')"
	run_sql_source1 "insert into dmctl_command.t1 values(-1,'ignore-row')"
	run_sql_file $cur/data/db1.increment.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.increment.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2
	# do not do sync diff this time, will fail
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 4\/1\/1\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 4\/0\/0" 1 \
		"\"stage\": \"Running\"" 4 \
		"\"stage\": \"Stopped\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"insert\/update\/delete: 4\/1\/1\"" 1 \
		"insert\/update\/delete: 0\/0\/1\"" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 4\/0\/0" 1
	trigger_checkpoint_flush
}

function run_validator_cmd {
	prepare_for_validator_cmd
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"stage\": \"Running\"" 4 \
		"\"stage\": \"Stopped\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage running test" \
		"\"stage\": \"Running\"" 4 \
		"\"stage\": \"Stopped\"" 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage stopped test" \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 1 \
		"no primary key" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error all test" \
		"\"id\": \"1\"" 1 \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	run_validator_cmd_error
	# resolve error 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test 1"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 3\/0\/1" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error unprocessed test" \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	# default we show unprocessed
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error test" \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error all test" \
		"\"id\": \"1\"" 1 \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	# ignore error 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test 2"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 2\/1\/1" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error ignored test" \
		"\"id\": \"2\"" 1
	# clear error 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test 1"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 2\/1\/0" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error ignored test" \
		"\"id\": \"2\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test 2"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 2\/0\/0" 1
	# test we can get validation status even when it's stopped
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation stop test" \
		"\"result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"\"processedRowsStatus\": \"insert\/update\/delete: 4\/1\/1\"" 1 \
		"\"processedRowsStatus\": \"insert\/update\/delete: 0\/0\/1\"" 1 \
		"pendingRowsStatus\": \"insert\/update\/delete: 0\/0\/0" 2 \
		"new\/ignored\/resolved: 0\/0\/0" 1 \
		"new\/ignored\/resolved: 2\/0\/0" 1 \
		"\"stage\": \"Running\"" 2 \
		"\"stage\": \"Stopped\"" 3
	# check the validator is stopped but syncer remains active
	curl 127.0.0.1:$WORKER1_PORT/debug/pprof/goroutine?debug=2 >$WORK_DIR/goroutine.worker1
	check_log_not_contains $WORK_DIR/goroutine.worker1 "validator"
	check_log_contains $WORK_DIR/goroutine.worker1 "syncer"
	curl 127.0.0.1:$WORKER2_PORT/debug/pprof/goroutine?debug=2 >$WORK_DIR/goroutine.worker2
	check_log_not_contains $WORK_DIR/goroutine.worker2 "validator"
	check_log_contains $WORK_DIR/goroutine.worker2 "syncer"
	# still able to query validation error
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error all test" \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	# still able to operate validation error
	# ignore error 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test 3"
	# resolve error 4
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test 4"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 0\/1\/1" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test --all"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 0\/0\/0" 2
	run_sql_source1 "insert into dmctl_command.t1 values(-10,'ignore-row')"
	run_sql_source1 "create table dmctl_command.t_trigger_flush110(id int primary key)" # trigger flush
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"\"synced\": true" 2 \
		"\"stage\": \"Stopped\"" 2
	run_sql_tidb "select * from dmctl_command.t1"
	check_not_contains "id: -10" # sync failed due to GO_FAILPOINTS
	# no validation error, because validator has been stopped
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 0\/0\/0" 2
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"insert\/update\/delete: 4\/1\/1\"" 1 \
		"insert\/update\/delete: 0\/0\/1\"" 1 \
		"new\/ignored\/resolved: 0\/0\/0" 2

	dmctl_stop_task "test"
	echo "clean up data" # pre-check will not pass, since there is a table without pk
	cleanup_data_upstream dmctl_command
	cp $cur/conf/dm-task.yaml $WORK_DIR/dm-task.yaml
	sed -i "s/    mode: full/    mode: none/g" $WORK_DIR/dm-task.yaml
	dmctl_start_task $WORK_DIR/dm-task.yaml --remove-meta
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"validator not found for task" 2

	echo "--> validation resolve-error --all"
	prepare_for_validator_cmd
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error test" \
		"\"id\": \"1\"" 1 \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test --all"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error test" \
		"\"id\": " 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 0\/0\/4" 1

	echo "--> validation ignore-error --all"
	prepare_for_validator_cmd
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error test" \
		"\"id\": \"1\"" 1 \
		"\"id\": \"2\"" 1 \
		"\"id\": \"3\"" 1 \
		"\"id\": \"4\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test --all"
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error test" \
		"\"id\": " 0
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status test" \
		"new\/ignored\/resolved: 0\/4\/0" 1
}

function run_check_task() {
	source $cur/../dmctl_basic/check_list/check_task.sh
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT
	export GO_FAILPOINTS='github.com/pingcap/tiflow/dm/loader/longLoadProcess=return(20)'

	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT

	run_sql_file $cur/data/db1.prepare.sql $MYSQL_HOST1 $MYSQL_PORT1 $MYSQL_PASSWORD1
	run_sql_file $cur/data/db2.prepare.sql $MYSQL_HOST2 $MYSQL_PORT2 $MYSQL_PASSWORD2

	cp $cur/conf/source1.yaml $WORK_DIR/source1.yaml
	cp $cur/conf/source2.yaml $WORK_DIR/source2.yaml
	dmctl_operate_source create $WORK_DIR/source1.yaml $SOURCE_ID1
	dmctl_operate_source create $WORK_DIR/source2.yaml $SOURCE_ID2
	run_sql_source1 "set @@GLOBAL.max_connections=151;"
	run_sql_source2 "set @@GLOBAL.max_connections=151;"
	check_task_lightning
	check_full_mode_conn
	checktask_full_mode_conn
	run_sql_source1 "set @@GLOBAL.max_connections=151;"
	run_sql_source2 "set @@GLOBAL.max_connections=151;"
	run_sql_tidb "set @@GLOBAL.max_connections=0;" # set default (unlimited), or other tests will fail
}

function run_validator_cmd_error() {
	# status: without taskname
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage stopped" \
		"Error: task name should be specified" 1
	# status: invalid stage
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation status --table-stage start test" \
		"Error: stage should be either" 1

	# show errors: resolved error is illegal
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error resolved test" \
		"Error: error flag should be either" 1
	# show errors: no task name
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation show-error --error all" \
		"Error: task name should be specified" 1

	# operate error: conflict id and --all flag
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test 100 --all" \
		"Error: either \`--all\` or \`error-id\` should be set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test 100 --all" \
		"Error: either \`--all\` or \`error-id\` should be set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test 100 --all" \
		"Error: either \`--all\` or \`error-id\` should be set" 1

	# operate error: more than one arguments
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test 100 101" \
		"Error: too many arguments are specified" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test 100 101" \
		"Error: too many arguments are specified" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test 100 101" \
		"Error: too many arguments are specified" 1

	# operate error: NaN id
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test error-id" \
		"Error: \`error-id\` should be integer when \`--all\` is not set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test error-id" \
		"Error: \`error-id\` should be integer when \`--all\` is not set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test error-id" \
		"Error: \`error-id\` should be integer when \`--all\` is not set" 1

	# operate error: neither all nor id
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error test" \
		"Error: either \`--all\` or \`error-id\` should be set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error test" \
		"Error: either \`--all\` or \`error-id\` should be set" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error test" \
		"Error: either \`--all\` or \`error-id\` should be set" 1

	# operate error: no task name
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error" \
		"Error: task name should be specified" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error" \
		"Error: task name should be specified" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error" \
		"Error: task name should be specified" 1

	# operate error: invalid task name
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation ignore-error non-exist-task-name 1" \
		"cannot get subtask by task name" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation resolve-error non-exist-task-name 1" \
		"cannot get subtask by task name" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"validation clear-error non-exist-task-name 1" \
		"cannot get subtask by task name" 1
}

cleanup_data dmctl_command
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

# run validator commands
cleanup_data dmctl_command
run_validator_cmd $*
run_validation_start_stop_cmd
cleanup_process $*

# run check task
cleanup_data dmctl_command
run_check_task
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
