#!/bin/bash

function start_relay_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay" \
		"start-relay <-s source-id>" 1
}

function start_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay wrong_arg" \
		"must specify one source (\`-s\` \/ \`--source\`)" 1
}

function start_relay_success() {
  # TODO: not sure source1's worker now, revert this after weight is supported
  source1worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test -s $SOURCE_ID1 |
                    grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)
  source2worker=$($PWD/bin/dmctl.test DEVEL --master-addr "127.0.0.1:$MASTER_PORT" query-status test -s $SOURCE_ID2 |
                    grep 'worker' | awk -F: '{print $2}' | cut -d'"' -f 2)

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 $source1worker" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID1\"" 1 \
		"\"worker\": \"$source1worker\"" 1

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 $source2worker" \
		"\"result\": true" 2 \
		"\"source\": \"$SOURCE_ID2\"" 1 \
		"\"worker\": \"$source2worker\"" 1
}

function start_relay_without_worker_name_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1" \
		"\"result\": true" 1
}

function start_relay_diff_worker_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker2" \
		"\"result\": true" 2
	# revert this change for the following test
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
  	"stop-relay -s $SOURCE_ID1 worker2" \
  		"\"result\": true" 2
}

function start_relay_with_worker_name_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1 worker1" \
		"can't \`start-relay\` with worker name now" 1
}

function start_relay_without_worker_name_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID1" \
		"can't \`start-relay\` without worker name now" 1
}

function start_relay_on_offline_worker() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2 \
		"\"msg\": \"source relay is operated but the bounded worker is offline\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1 \
		"\"worker\": \"worker2\"" 1
}
