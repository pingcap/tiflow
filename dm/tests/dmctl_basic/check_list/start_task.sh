#!/bin/bash

function start_task_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task" \
		"start-task \[-s source ...\] \[--remove-meta\] <config-file> \[flags\]" 1
}

function start_task_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task not_exists_config_file" \
		"error in get file content" 1
}

function start_task_wrong_start_time_format() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $task_conf --start-time '20060102 150405'" \
		"error while parse start-time" 1
}

function start_task_not_pass_with_message() {
	task_conf=$1
	error_message=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $task_conf" \
		"$2" 1
}

function start_task_empty_config() {
	start_task_empty_dump_config $1
	start_task_empty_load_config $1
	start_task_empty_sync_config $1
}

function start_task_empty_dump_config() {
	sed "/threads/d" $1 >/tmp/empty-dump.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task /tmp/empty-dump.yaml" \
		"The configurations as following .* are set in global configuration but instances don't use them" 1
}

function start_task_empty_load_config() {
	sed "/pool-size/d" $1 >/tmp/empty-load.yaml
	cat /tmp/empty-load.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task /tmp/empty-load.yaml" \
		"The configurations as following .* are set in global configuration but instances don't use them" 1
}

function start_task_empty_sync_config() {
	sed "/worker-count/d" $1 >/tmp/empty-sync.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task /tmp/empty-sync.yaml" \
		"The configurations as following .* are set in global configuration but instances don't use them" 1
}
