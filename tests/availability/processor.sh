#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

MAX_RETRIES=20

function test_processor_ha() {
	test_stop_processor
}

# test_stop_processor stops the working processor
# and then resume it.
# We expect the data after resuming is replicated.
function test_stop_processor() {
	echo "run test case test_stop_processor"
	# start a capture server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_stop_processor
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --disable-version-check 2>&1 | grep '\"is-owner\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id

	# get the change feed id
	changefeed=$($CDC_BINARY cli changefeed list --disable-version-check 2>&1 | awk -F '"' '/id/{print $4}')
	echo "changefeed id:" $changefeed

	# stop the change feed job
	# use ensure to wait for the change feed loading into memory from etcd
	ensure $MAX_RETRIES "curl -s -d \"cf-id=$changefeed&admin-job=1\" http://127.0.0.1:8300/capture/owner/admin | grep true"

	run_sql "INSERT INTO test.availability1(id, val) VALUES (4, 4);"

	# resume the change feed job
	curl -d "cf-id=$changefeed&admin-job=2" http://127.0.0.1:8300/capture/owner/admin
	ensure $MAX_RETRIES nonempty 'select id, val from test.availability1 where id=4 and val=4'

	echo "test_stop_processor pass"
	cleanup_process $CDC_BINARY
}
