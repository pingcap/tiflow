#!/bin/bash

function pause_task_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task" \
		"pause-task \[-s source ...\] \[task-name | task-file\] \[flags\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task -s a -s b" \
		"pause-task \[-s source ...\] \[task-name | task-file\] \[flags\]" 1
}

function pause_task_success() {
	task_name=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task $task_name" \
		"\"result\": true" 3 \
		"\"op\": \"Pause\"" 1 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1
}
