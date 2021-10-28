#!/bin/bash

function transfer_source_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source" \
		"transfer-source <source-id> <worker-id>" 1
}

function transfer_source_less_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source source-id" \
		"transfer-source <source-id> <worker-id>" 1
}

function transfer_source_more_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source source-id worker-id another-worker" \
		"transfer-source <source-id> <worker-id>" 1
}

function transfer_source_valid() {
	source_id=$1
	worker_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source $1 $2" \
		"\"result\": true" 1
}

function transfer_source_invalid() {
	source_id=$1
	worker_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"transfer-source $1 $2" \
		"invalid stage transformation for dm-worker $2" 1
}
