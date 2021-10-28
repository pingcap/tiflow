#!/bin/bash

function operate_source_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source" \
		"operate-source <operate-type> \[config-file ...\] \[--print-sample-config\] \[flags\]" 1
}

function operate_source_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source create not_exists_config_file" \
		"error in get file content" 1
}

function operate_source_stop_not_created_config() {
	source_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source stop $source_conf" \
		"source config with ID mysql-replica-01 not exists" 1
}

function operate_source_invalid_op() {
	source_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-source invalid $source_conf" \
		"invalid operate" 1
}
