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
