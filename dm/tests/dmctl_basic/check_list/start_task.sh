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
