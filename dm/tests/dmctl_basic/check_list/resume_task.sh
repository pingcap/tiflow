#!/bin/bash

function resume_task_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task" \
		"resume-task \[-s source ...\] \[task-name | task-file\] \[flags\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task -s a -s b" \
		"resume-task \[-s source ...\] \[task-name | task-file\] \[flags\]" 1
}

function resume_task_success() {
	task_name=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task $task_name" \
		"\"result\": true" 3 \
		"\"op\": \"Resume\"" 1 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1
}
