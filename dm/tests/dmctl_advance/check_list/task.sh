#!/bin/bash
function task_op() {
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT
	dmctl_operate_source create $cur/conf/source1.yaml $SOURCE_ID1

	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $cur/conf/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT
	dmctl_operate_source create $cur/conf/source2.yaml $SOURCE_ID2

	invalid_task_op
	available_task_op
}

function invalid_task_op() {
	invalid_task_create_op
	invalid_task_start_op
	invalid_task_stop_op
	invalid_task_delete_op
	invalid_task_update_op
	invalid_task_status_op
	invalid_task_list_op
}

function available_task_op() {
	task_create_op
	task_start_op
	task_stop_op
	task_delete_op
	task_update_op
	task_status_op
	task_list_op
	other_op
	https_availability
}

function invalid_task_create_op() {
	echo "run run invalid_task_create_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create" \
		"dmctl task create <config-file> \[flags\]" 1
	# no file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create not_exist_file" \
		"error in get file content" 1
	# no source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task_no_source.yaml" \
		"source mysql-replica-03 in deployment configuration not found" 1
}

function task_create_op() {
	echo "run task_create_op"
	# has source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	# status is Stopped
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Stopped\"" 2
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes" \
		"Delete task test success" 1
}

function invalid_task_start_op() {
	echo "run invalid_task_start_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start" \
		"dmctl task start <task-name | config-file> \[-s source ...\] \[--remove-meta\] \[--start-time\] \[--safe-mode-duration\] \[flags\]" 1
	# no task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test" \
		"task with name test not exist" 1
	# no task file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start not_exist_file.yaml" \
		"error in get file content" 1
	# has file, but no source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task_no_source.yaml" \
		"source mysql-replica-03 in deployment configuration not found" 1
	# create task success
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	# invalid start-time
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test --start-time '2022/01/11 12:12:12.345'" \
		"error while parse start-time" 1
	# invalid safe-mode-time
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test --safe-mode-duration '100'" \
		"error while parse safe-mode-duration" 1
	# start by error source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml -s mysql-replica-03" \
		"source config with ID mysql-replica-03 not exists" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes" \
		"Delete task test success" 1
}

function task_start_op() {
	echo "run task_start_op"
	# start by file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes -f" \
		"Delete task test success" 1
	# start at one source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml -s mysql-replica-01" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Stopped\"" 1
	# start at two source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test -s mysql-replica-01,mysql-replica-02" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test" \
		"Stop task test success" 1
	# start by remove-meta
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test --remove-meta" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes -f" \
		"Delete task test success" 1
	# start by start-time
	run_sql_source1 'DROP DATABASE if exists task_advance;'
	run_sql_source1 'CREATE DATABASE task_advance;'
	run_sql_source2 'DROP DATABASE if exists task_advance;'
	run_sql_source2 'CREATE DATABASE task_advance;'
	sleep 2
	start_time=$(TZ='UTC8' date '+%Y-%m-%d %T')
	sleep 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml --start-time '$start_time'" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test" \
		"Stop task test success" 1
	# start by safe-mode-duration
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start test --safe-mode-duration '100s'" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test" \
		"Stop task test success" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes" \
		"Delete task test success" 1
	run_sql_source1 'DROP DATABASE if exists task_advance;'
	run_sql_source2 'DROP DATABASE if exists task_advance;'
}

function invalid_task_stop_op() {
	echo "run invalid_task_stop_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop" \
		"dmctl task stop \[task-name | config-file\] \[-s source ...\] \[--timeout duration\] \[--batch-size 5\] \[flags\]" 1
	# no task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test" \
		"task with name test not exist" 1
	# no task file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop not_exist_file.yaml" \
		"task with name not_exist_file.yaml not exist" 1
	# no task but multi source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop -s abc,bcd" \
		"can give only one source-name when task-name\/task-conf is not specifie" 1
	# invalid batch-size
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop -s a --batch-size abc" \
		"invalid argument \"abc\" for \"--batch-size\"" 1
	# no task about source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop -s mysql-replica-01" \
		"there is no tasks of source: \[mysql-replica-01\]" 1
	# create task success
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	# invalid timeout
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test --timeout 101" \
		"error while parse stop_wait_timeout_duration" 1
	# stop by error source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test -s mysql-replica-03" \
		"sources \[mysql-replica-03\] need to be operate not exist" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes -f" \
		"Delete task test success" 1
}

function task_stop_op() {
	echo "run task_stop_op"
	# stop by test
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"unit\": \"Sync\"" 2 \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test" \
		"Stop task test success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Paused\"" 2
	# stop by one source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test -s mysql-replica-01" \
		"Stop task test in source \[mysql-replica-01\] success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Paused\"" 1
	# stop by two source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test -s mysql-replica-01,mysql-replica-02" \
		"Stop task test in source \[mysql-replica-01 mysql-replica-02\] success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Paused\"" 2
	# stop by timeout
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop test --timeout '5s'" \
		"Stop task test success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Paused\"" 2
	# stop source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task1.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop -s mysql-replica-01 --batch-size 10" \
		"Stop task test in source \[mysql-replica-01\] success" 1 \
		"Stop task test1 in source \[mysql-replica-01\] success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Paused\"" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test1" \
		"\"stage\": \"Running\"" 1 \
		"\"stage\": \"Paused\"" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test --yes -f" \
		"Delete task test success" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test1 --yes -f" \
		"Delete task test1 success" 1
}

function invalid_task_delete_op() {
	echo "run invalid_task_delete_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete" \
		"dmctl task delete <task-name | config-file> \[--force\] \[--yes\] \[flags\]" 1
	# no task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -y" \
		"task with name test not exist" 1
	# no task file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete not_exist_file.yaml -y" \
		"task with name not_exist_file.yaml not exist" 1
	# no -y
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test" \
		"Do you want to continue" 1
}

function task_delete_op() {
	echo "run task_delete_op"
	# delete stopped task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -y" \
		"Delete task test success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"task with name test not exist" 1
	# delete running task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -y" \
		"task test have running subtasks" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -f -y" \
		"Delete task test success" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"task with name test not exist" 1
}

function invalid_task_update_op() {
	echo "run invalid_task_update_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update" \
		"update <config-file> \[flags\]" 1
	# no task file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update not_exist_file.yaml" \
		"error in get file content" 1
	# error task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update $cur/conf/dm-task_no_source.yaml" \
		"source mysql-replica-03 in deployment configuration not found" 1
	# no need to be updated task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update $cur/conf/dm-task.yaml" \
		"task with name test not exist" 1
}

function task_update_op() {
	echo "run task_update_op"
	# running task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update $cur/conf/dm-task.yaml" \
		"can only update when no running tasks for now" 1
	# stop and update
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task stop $cur/conf/dm-task.yaml" \
		"Stop task test success" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task update $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -f -y" \
		"Delete task test success" 1
}

function invalid_task_status_op() {
	echo "run invalid_task_status_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status" \
		"dmctl task status <task-name | config-file> \[-s source ...\] \[flags\]" 1
	# no task
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"task with name test not exist" 1
	# no task file
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status not_exist_file.yaml" \
		"task with name not_exist_file.yaml not exist" 1
}

function task_status_op() {
	echo "run task_status_op"
	# has task but error source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test -s mysql-replica-03" \
		"\"total\": 0" 1
	# two sources
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test -s mysql-replica-01,mysql-replica-03" \
		"\"total\": 1" 1
	# by file
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status  $cur/conf/dm-task.yaml" \
		"\"total\": 2" 1
	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -f -y" \
		"Delete task test success" 1
}

function invalid_task_list_op() {
	echo "run invalid_task_list_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list -h" \
		"dmctl task list \[\[--filter status=Running | source=...\]..\]\[--more\] \[flags\]" 1
	# no task and param
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list" \
		"\"total\": 0" 1
	# invalid status
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list --filter status=abc" \
		"value is not one of the allowed values" 1
	# invalid source
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list --filter source=abc" \
		"\"total\": 0" 1
	# too many filters
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list --filter source=abc --filter status=abc --filter source=bcd" \
		"too many filters" 1
}

function task_list_op() {
	echo "run task_list_op"
	# filter
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task create $cur/conf/dm-task1.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list --filter source=mysql-replica-01 --filter status=Stopped" \
		"\"total\": 1" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task list --filter source=mysql-replica-01 --filter status=Running --more" \
		"\"stage\": \"Running\"" 2 \
		"\"total\": 1" 1

	# TODO need to change status to be Stopped
	# run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	# 	"task stop test" \
	# 	"Stop task test success" 1
	# run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
	# 	"task list --filter source=mysql-replica-01 --filter status=Stopped" \
	# 	"\"total\": 1" 1

	# delete
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -f -y" \
		"Delete task test success" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test1 -f -y" \
		"Delete task test1 success" 1
}

# check,pause,resume still use grpc interface, just Simply test availability
function other_op() {
	echo "run other_op"
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task check" \
		"dmctl task check <config-file> \[--error count\] \[--warn count\] \[--start-time\] \[flags\]" 1
	# availability
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task check $cur/conf/dm-task.yaml" \
		"pre-check is passed" 1
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task pause" \
		"dmctl task pause \[task-name | config-file\] \[-s source ...\] \[flags\]" 1
	# availability
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task start $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2 \
		"\"unit\": \"Sync\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task pause test" \
		"\"result\": true" 3
	# command tips
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task resume" \
		"dmctl task resume \[task-name | config-file\] \[-s source ...\] \[flags\]" 1
	# availability
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task resume test" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"task delete test -f -y" \
		"Delete task test success" 1
}

function https_availability() {
	echo "run https_availability"
	cleanup_data task_advance
	cleanup_process

	cp $cur/tls_conf/dm-master.toml $WORK_DIR/
	cp $cur/tls_conf/dm-worker1.toml $WORK_DIR/
	cp $cur/tls_conf/dm-worker2.toml $WORK_DIR/
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-master.toml
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-worker1.toml
	sed -i "s%dir-placeholer%$cur\/tls_conf%g" $WORK_DIR/dm-worker2.toml

	# run dm-master
	run_dm_master $WORK_DIR/master $MASTER_PORT1 $WORK_DIR/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1 "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	# run dm-worker1
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $WORK_DIR/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"operate-source create $cur/conf/source1.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1
	# run dm-worker2
	run_dm_worker $WORK_DIR/worker2 $WORKER2_PORT $WORK_DIR/dm-worker2.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER2_PORT "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key"
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"operate-source create $cur/conf/source2.yaml" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID2\"" 1

	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"task create $cur/conf/dm-task.yaml" \
		"\"Result\": true" 1
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"task start test" \
		"\"Result\": true" 1
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"task status test" \
		"\"stage\": \"Running\"" 2
	run_dm_ctl_with_tls_and_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" "$cur/tls_conf/ca.pem" "$cur/tls_conf/dm.pem" "$cur/tls_conf/dm.key" \
		"task delete test -f -y" \
		"Delete task test success" 1

	cleanup_data task_advance
	cleanup_process

	# run dm-master
	run_dm_master $WORK_DIR/master1 $MASTER_PORT1 $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT1
}
