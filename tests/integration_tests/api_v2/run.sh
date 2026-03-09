#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# create table to upstream.
	run_sql "CREATE DATABASE api_v2" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE api_v2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE table test.t1(id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	cdc cli changefeed create -c="cf-blackhole" --sink-uri="blackhole://"
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-blackhole" "normal" "null" ""
}

trap stop_tidb_cluster EXIT
# kafka and storage is not supported yet.
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	cd "$(dirname "$0")"
	set -euxo pipefail

	GO111MODULE=on go run main.go model.go request.go cases.go

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
