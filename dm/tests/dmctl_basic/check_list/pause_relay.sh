#!/bin/bash

function pause_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay wrong_arg" \
		"pause-relay <-s source ...> \[flags\]" 1
}

function pause_relay_without_worker() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay" \
		"must specify at least one source" 1
}

function pause_relay_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay -s $SOURCE_ID1 -s $SOURCE_ID2" \
		"\"result\": true" 3 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1
}

function pause_relay_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-relay -s $SOURCE_ID1 -s $SOURCE_ID2" \
		"\"result\": true" 1 \
		"\"result\": false" 2 \
		"\"msg\": \".*current stage is Paused, Running required" 2
}
