#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export DOCKER_COMPOSE_DIR=$(cd $CUR_DIR/../../../deployments/engine/docker-compose/ && pwd)
export PATH=$PATH:$CUR_DIR/../utils
export OUT_DIR=/tmp/tiflow_engine_test
mkdir -p $OUT_DIR || true

if [ "${1-}" = 'debug' ]; then
	shift
	if [[ $# -gt 0 ]]; then
		cnf=$*
	else
		cnf="$DOCKER_COMPOSE_DIR/1m1e.yaml"
		echo "got empty file, use default config: ${cnf}"
	fi

	TEST_NAME=debug
	WORK_DIR=$OUT_DIR/$TEST_NAME
	CONFIG=$(adjust_config $OUT_DIR $TEST_NAME $cnf)
	trap "stop_engine_cluster $WORK_DIR $CONFIG" EXIT
	start_engine_cluster $CONFIG

	echo 'You may now debug from another terminal. Press [ENTER] to exit.'
	read line
	exit 0
fi

run_case() {
	# cleanup test binaries and data, preserve logs, if we debug one case,
	# these files will be preserved since no more case will be run.
	find /tmp/tiflow_engine_test/*/* -type d | xargs rm -rf || true
	local case=$1
	local script=$2

	# validate the case script
	check_case.sh

	echo "=================>> Running test $script... <<================="
	PATH="$PATH:$CUR_DIR/../utils" \
		OUT_DIR=$OUT_DIR \
		TEST_NAME=$case \
		DOCKER_COMPOSE_DIR=$DOCKER_COMPOSE_DIR \
		bash "$script"
}

set +eu

test_case=$1
if [ -z "$test_case" ]; then
	test_case="*"
fi

start_at=$2
run_test="no"
if [ -z "$start_at" ]; then
	run_test="yes"
else
	test_case="*"
fi

set -eu
if [ "$test_case" == "*" ]; then
	for script in $CUR_DIR/*/run.sh; do
		test_name="$(basename "$(dirname "$script")")"
		if [ "$run_test" == "yes" ] || [ "$start_at" == "$test_name" ]; then
			run_test="yes"
			run_case $test_name $script
		fi
	done
else
	for name in $test_case; do
		script="$CUR_DIR/$name/run.sh"
		run_case $name $script
	done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
