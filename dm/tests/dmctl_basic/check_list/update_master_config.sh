#!/bin/bash

function update_master_config_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-master-config" \
		"update-master-config <config-file> \[flags\]" 1
}

function update_master_config_wrong_config_file() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-master-config not_exists_config_file" \
		"error in get file content" 1
}

function update_master_config_success() {
	master_conf=$1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"update-master-config $master_conf" \
		"\"result\": true" 1
}
