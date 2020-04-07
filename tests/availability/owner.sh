#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

MAX_RETRIES=10

function test_owner_ha() {
    test_kill_owner
    test_hang_up_owner
    test_expire_owner
}
# test_kill_owner starts two captures and kill the owner
# we expect the live capture will be elected as the new
# owner
function test_kill_owner() {
    echo "run test case test_kill_owner"
    # start a capture server
    run_cdc_server $WORK_DIR $CDC_BINARY
    # ensure the server become the owner
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is-owner\": true'"
    owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}')
    echo "owner pid:" $owner_pid
    echo "owner id" $owner_id

    # run another server
    run_cdc_server $WORK_DIR $CDC_BINARY
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep -v \"$owner_id\" | grep id"
    capture_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")
    echo "capture_id:" $capture_id

    # kill the server
    kill $owner_pid

    # check that the new owner is elected
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 |grep $capture_id -A1 | grep '\"is-owner\": true'"
    echo "test_kill_owner: pass"

    cleanup_process $CDC_BINARY
}

# test_hang_up_owner starts two captures and stops the owner
# by sending a SIGSTOP signal.
# We expect another capture will be elected as the new owner
function test_hang_up_owner() {
    echo "run test case test_hang_up_owner"

    run_cdc_server $WORK_DIR $CDC_BINARY
    # ensure the server become the owner
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is-owner\": true'"

    owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}')
    echo "owner pid:" $owner_pid
    echo "owner id" $owner_id

    # run another server
    run_cdc_server $WORK_DIR $CDC_BINARY
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep -v \"$owner_id\" | grep id"
    capture_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")
    echo "capture_id:" $capture_id

    # stop the owner
    kill -SIGSTOP $owner_pid

    # check that the new owner is elected
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 |grep $capture_id -A1 | grep '\"is-owner\": true'"
    # resume the original process
    kill -SIGCONT $owner_pid

    echo "test_hang_up_owner: pass"

    cleanup_process $CDC_BINARY
}

# test_expire_owner stops the owner by sending
# the SIGSTOP signal and wait unitl its session
# expires.
# We expect when the owner process resumes, it suicides
# itself and recovers from the death.
function test_expire_owner() {
    echo "run test case test_expire_owner"

    run_cdc_server $WORK_DIR $CDC_BINARY
    # ensure the server become the owner
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is-owner\": true'"

    owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}')
    echo "owner pid:" $owner_pid
    echo "owner id" $owner_id

    # stop the owner
    kill -SIGSTOP $owner_pid
    echo "process status:" $(ps -h -p $owner_pid -o "s")

    # ensure the session has expired
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\[\]'"

    # resume the owner
    kill -SIGCONT $owner_pid
    echo "process status:" $(ps -h -p $owner_pid -o "s")
    # ensure the owner has recovered
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is-owner\": true'"
    echo "test_expire_owner pass"

    cleanup_process $CDC_BINARY
}
