#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

MAX_RETRIES=50

function sql_check() {
	# run check in sequence and short circuit principle, if error hanppens,
	# the following statement will be not executed

	# check table availability.
	echo "run sql_check", ${DOWN_TIDB_HOST}
	run_sql "SELECT id, val FROM test.availability1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "id: 1" &&
		check_contains "val: 1" &&
		check_contains "id: 2" &&
		check_contains "val: 22" &&
		check_not_contains "id: 3"
}
export -f sql_check

function check_result() {
	ensure $MAX_RETRIES sql_check
}

function empty() {
	sql=$*
	run_sql "$sql" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_not_contains "id:"
}

function nonempty() {
	sql=$*
	run_sql "$sql" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_contains "id:"
}

export -f empty
export -f nonempty

function test_capture_ha() {
	test_kill_capture
	test_hang_up_capture
	test_expire_capture
	check_result
}

# test_kill_capture starts two servers and kills the working one
# We expect the task is rebalanced to the live capture and the data
# continues to replicate.
function test_kill_capture() {
	echo "run test case test_kill_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_kill_capture.server1

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# wait for the tables to appear
	check_table_exists test.availability1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 20

	run_sql "INSERT INTO test.availability1(id, val) VALUES (1, 1);"
	ensure $MAX_RETRIES nonempty 'select id, val from test.availability1 where id=1 and val=1'

	# start the second capture
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301" --logsuffix test_kill_capture.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")

	# kill the owner
	kill -9 $owner_pid

	run_sql "INSERT INTO test.availability1(id, val) VALUES (2, 2);"
	ensure $MAX_RETRIES nonempty 'select id, val from test.availability1 where id=2 and val=2'

	cleanup_process $CDC_BINARY
}

# test_hang_up_caputre starts two captures and hang up the working one by
# send SIGSTOP signal to the process.
# We expect the task is rebalanced to the live capture and the data continues
# to replicate.
function test_hang_up_capture() {
	echo "run test case test_hang_up_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_hang_up_capture.server1

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# start the second capture
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301" --logsuffix test_hang_up_capture.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")

	kill -STOP $owner_pid
	run_sql "INSERT INTO test.availability1(id, val) VALUES (3, 3);"
	ensure $MAX_RETRIES nonempty 'select id, val from test.availability1 where id=3 and val=3'
	kill -CONT $owner_pid
	cleanup_process $CDC_BINARY
}

# test_expire_capture start one server and then stop it unitl
# the session expires, and then resume the server.
# We expect the capture suicides itself and then recovers. The data
# should be replicated after recovering.
function test_expire_capture() {
	echo "run test case test_expire_capture"
	# start one server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# stop the owner
	kill -SIGSTOP $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")

	# ensure the session has expired
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\[\]'"

	# resume the owner
	kill -SIGCONT $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")

	run_sql "UPDATE test.availability1 set val = 22 where id = 2;"
	run_sql "DELETE from test.availability1 where id = 3;"
	ensure $MAX_RETRIES nonempty 'select id, val from test.availability1 where id=2 and val=22'
	cleanup_process $CDC_BINARY
}
