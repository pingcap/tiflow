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
	cp $1 /tmp/empty-cfg.yaml
	sed -i "/threads/d" /tmp/empty-cfg.yaml
	sed -i "/pool-size/d" /tmp/empty-cfg.yaml
	sed -i "/worker-count/d" /tmp/empty-cfg.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task /tmp/empty-cfg.yaml" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"config task empty-unit-task" \
		"threads: 4" 1 \
		"pool-size: 16" 1 \
		"worker-count: 16" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task $1" \
		"\"result\": true" 2
}

function start_task_wrong_no_source_meta() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $task_conf" \
		"must set meta for task-mode incremental" 1
}

function start_task_no_source_meta_but_start_time() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $task_conf --start-time '2006-01-02 15:04:05'" \
		"\"result\": true" 3
	run_dm_ctl_with_retry $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status $task_conf" \
		"\"result\": true" 3
}
