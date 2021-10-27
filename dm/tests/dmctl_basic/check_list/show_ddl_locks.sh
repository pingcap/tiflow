#!/bin/bash

function show_ddl_locks_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock a b" \
		"dmctl shard-ddl-lock \[task\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks a b" \
		"show-ddl-locks \[-s source ...\] \[task-name | task-file\] \[flags\]" 1
}

function show_ddl_locks_no_locks() {
	task_name=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock $task_name" \
		"\"msg\": \"no DDL lock exists\"" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks $task_name" \
		"\"msg\": \"no DDL lock exists\"" 1
}

function show_ddl_locks_with_locks() {
	lock_id=$1
	ddl=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"shard-ddl-lock" \
		"\"ID\": \"$lock_id\"" 1 \
		"$ddl" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"show-ddl-locks" \
		"\"ID\": \"$lock_id\"" 1 \
		"$ddl" 1
}
