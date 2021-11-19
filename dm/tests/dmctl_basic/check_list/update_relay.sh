#!/bin/bash

function update_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-relay" \
		"update-relay \[-s source ...\] <config-file> \[flags\]" 1
}

function update_relay_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-relay not_exists_config_file" \
		"error in get file content" 1
}

function update_relay_should_specify_one_dm_worker() {
	task_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-relay $task_conf" \
		"must specify one source (\`-s\` \/ \`--source\`)" 1
}

function update_relay_success() {
	task_conf=$1
	source_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-relay $task_conf -s $source_id" \
		"\"result\": true" 1
}
