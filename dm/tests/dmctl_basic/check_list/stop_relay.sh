#!/bin/bash

function stop_relay_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay" \
		"stop-relay <-s source-id>" 1
}

function stop_relay_wrong_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay wrong_arg" \
		"must specify one source (\`-s\` \/ \`--source\`)" 1
}

function stop_relay_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1 worker1" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2
}

function stop_relay_with_worker_name_success() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1" \
		"\"result\": true" 1
}

function stop_relay_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1 worker2" \
		"these workers \[worker2\] have started relay for another sources \[$SOURCE_ID2\] respectively" 1
}

function stop_relay_with_worker_name_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1 worker1" \
		"can't \`stop-relay\` with worker name now" 1
}

function stop_relay_without_worker_name_fail() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID1" \
		"can't \`stop-relay\` without worker name now" 1
}

function stop_relay_on_offline_worker() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-relay -s $SOURCE_ID2 worker2" \
		"\"result\": true" 2 \
		"\"msg\": \"source relay is operated but the bound worker is offline\"" 1 \
		"\"source\": \"$SOURCE_ID2\"" 1 \
		"\"worker\": \"worker2\"" 1
}
