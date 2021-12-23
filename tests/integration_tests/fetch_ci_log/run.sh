#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1


function run() {
	i=0
	check_time=1200
	set +e
	while [ $i -lt $check_time ]; do
		((i++))
		echo "sleep 60s for $i-th time, retry later"
		sleep 60
	done
	set -e

	echo "exit now"
	exit 1

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
