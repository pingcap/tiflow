#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare

WORK_DIR=$TEST_DIR/$TEST_NAME

function test_dm() {
	env_name=$1
	env_val=$2
	export $env_name=$env_val

	expected_str="\[\"using proxy config\"\] \[$env_name=$env_val\]"

	# run dm master
	run_dm_master $WORK_DIR/master $MASTER_PORT $cur/conf/dm-master.toml
	check_rpc_alive $cur/../bin/check_master_online 127.0.0.1:$MASTER_PORT

	# check dm master log
	check_log_contains "$WORK_DIR/master/log/dm-master.log" $expected_str 1

	# rum dm worker
	run_dm_worker $WORK_DIR/worker1 $WORKER1_PORT $cur/conf/dm-worker1.toml
	check_rpc_alive $cur/../bin/check_worker_online 127.0.0.1:$WORKER1_PORT

	# check dm worker log
	check_log_contains "$WORK_DIR/worker1/log/dm-worker.log" $expected_str 1

	# replace url forward slash with backward and forward slash
	env_val=$(echo "$env_val" | sed "s/\/\//\\\\\/\\\\\//")

	# check dm ctl output
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" "$env_name=$env_val" 1 \
		'"result": false' 1

	unset $env_name

	kill_dm_master
	kill_dm_worker
}

function run() {
	echo "test dm grpc proxy env setting checking for http_proxy=http://127.0.0.1:8080"
	test_dm "http_proxy" "http://127.0.0.1:8080"

	echo "test dm grpc proxy env setting checking for https_proxy=https://127.0.0.1:8080"
	test_dm "https_proxy" "https://127.0.0.1:8080"

	echo "test dm grpc proxy env setting checking for no_proxy=localhost,127.0.0.1"
	test_dm "no_proxy" "localhost,127.0.0.1"
}

cleanup_data $TEST_NAME
cleanup_process
run
cleanup_process

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"
