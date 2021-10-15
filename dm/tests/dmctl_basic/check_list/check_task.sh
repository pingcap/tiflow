#!/bin/bash

function check_task_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task" \
		"check-task <config-file> \[--error count\] \[--warn count\] \[flags\]" 1
}

function check_task_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task not_exists_config_file" \
		"error in get file content" 1
}

function check_task_pass() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"\"msg\": \"check pass!!!\"" 1 \
		"\"result\": true" 1
}

function check_task_not_pass() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"\"result\": false" 1
}

function check_task_error_database_config() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"Access denied for user" 1 \
		"Please check the database connection and the database config in configuration file" 1
}

function check_task_error_count() {
	# 10 errors
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task3.yaml" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 2

	# 1 error
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task3.yaml -e 1" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 1

	# 100 errors
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task3.yaml -e 100 -w 1" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 2

	# 0 error
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $cur/conf/dm-task3.yaml -e 0" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 0
}
