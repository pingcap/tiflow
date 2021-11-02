#!/bin/bash

function update_task_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task" \
		"update-task \[-s source ...\] <config-file> \[flags\]" 1
}

function update_task_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task not_exists_config_file" \
		"error in get file content" 1
}

function update_task_worker_not_found() {
	task_conf=$1
	not_found_source_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task $task_conf -s $not_found_source_id " \
		"\"result\": true" 1 \
		"\"result\": false" 1 \
		"\"source\": \"$not_found_source_id\"" 1 \
		"\"msg\": \"source not found in task's config\"" 1
}

function update_task_not_paused() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task $task_conf" \
		"\"result\": true" 1 \
		"\"result\": false" 2 \
		"can only update task on Paused stage, but current stage is Running" 2
}

function update_task_success_single_worker() {
	task_conf=$1
	source_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task $task_conf -s $source_id" \
		"\"result\": true" 2 \
		"\"source\": \"$source_id\"" 1
}

function update_task_success() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-task $task_conf" \
		"\"result\": true" 3
}
