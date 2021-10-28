#!/bin/bash

function dmctl_multiple_addrs() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT,127.0.0.1:1234" \
		"list-member" \
		"\"result\": true" 1
}

function dmctl_unwrap_schema() {
	run_dm_ctl $WORK_DIR "http://127.0.0.1:1234,127.0.0.1:4322,https://127.0.0.1:$MASTER_PORT" \
		"list-member" \
		"\"result\": true" 1
}

function dmctl_wrong_addrs() {
	run_dm_ctl $WORK_DIR "https://127.0.0.1:1234,127.0.0.2:1234" \
		"list-member" \
		"can't connect to https:\/\/127.0.0.1:1234,127.0.0.2:1234" 1
}

function dmctl_no_addr() {
	run_dm_ctl_cmd_mode $WORK_DIR "" \
		"list-member" \
		"master-addr not provided" 1
}
