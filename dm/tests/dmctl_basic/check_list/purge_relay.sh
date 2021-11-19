#!/bin/bash

function purge_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay wrong_arg" \
		"purge-relay <-s source> <-f filename> \[--sub-dir directory\] \[flags\]" 1
}

function purge_relay_without_worker() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay" \
		"must specify at least one source (\`-s\` \/ \`--source\`)" 1
}

function purge_relay_filename_with_multi_workers() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay --filename bin-000001 -s $SOURCE_ID1 -s $SOURCE_ID2" \
		"for --filename, can only specify one source per time" 1
}

function purge_relay_success() {
	binlog_file=$1
	source_id=$2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"purge-relay --filename $binlog_file -s $source_id" \
		"\"result\": true" 2
}
