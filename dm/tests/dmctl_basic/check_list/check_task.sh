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
		"\"result\": true" 1
}

function check_task_not_pass() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"\"result\": false" 1
}

function check_task_not_pass_with_message() {
	task_conf=$1
	error_message=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"$error_message" 1
}

function check_task_error_database_config() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"Access denied for user" 1 \
		"Please check the database connection and the database config in configuration file" 1
}

function check_task_wrong_start_time_format() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf --start-time '20060102 150405'" \
		"error while parse start-time" 1
}

function check_task_error_count() {
	task_conf=$1
	# 10 errors
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 2

	# 1 error
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf -e 1" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 1

	# 100 errors
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf -e 100 -w 1" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 2

	# 0 error
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf -e 0" \
		"\"result\": false" 1 \
		"\"failed\": 2" 1 \
		"\"state\": \"fail\"" 0
}

function check_task_only_warning() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"\"state\": \"warn\"" 1
}

function check_task_empty_dump_config() {
	sed "/threads/d" $1 >/tmp/empty-dump.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task /tmp/empty-dump.yaml" \
		"pre-check is passed" 1
}

function check_task_empty_load_config() {
	sed "/pool-size/d" $1 >/tmp/empty-load.yaml
	cat /tmp/empty-load.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task /tmp/empty-load.yaml" \
		"pre-check is passed" 1
}

function check_task_empty_sync_config() {
	sed "/worker-count/d" $1 >/tmp/empty-sync.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task /tmp/empty-sync.yaml" \
		"pre-check is passed" 1
}

function check_task_empty_config() {
	check_task_empty_dump_config $1
	check_task_empty_load_config $1
	check_task_empty_sync_config $1
}

function check_task_wrong_no_source_meta() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf" \
		"must specify \`binlog-name\` without GTID enabled for the source or specify \`binlog-gtid\` with GTID enabled for the source" 1
}

function check_task_no_source_meta_but_start_time() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"check-task $task_conf --start-time '2006-01-02 15:04:05'" \
		"pre-check is passed" 1
}
