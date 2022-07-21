#!/bin/bash

set -eu

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK_DIR=$OUT_DIR/$TEST_NAME
CONFIG="$DOCKER_COMPOSE_DIR/3m3e.yaml $DOCKER_COMPOSE_DIR/demo.yaml"

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_engine_cluster $CONFIG
	go test -count=1 -v -run ^TestSubmitTest$ github.com/pingcap/tiflow/engine/test/e2e
}

trap "stop_engine_cluster $CONFIG" EXIT
run $*
# TODO: handle log properly
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
