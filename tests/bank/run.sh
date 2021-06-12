#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # create table to upstream.
    run_sql "CREATE DATABASE bank" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE DATABASE bank" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    run_cdc_cli changefeed create --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
}

trap stop_tidb_cluster EXIT
# kafka is not supported yet.
if [ "$SINK_TYPE" != "kafka" ]; then
    prepare $*

    cd "$(dirname "$0")"
    set -euxo pipefail

    GO111MODULE=on go run bank.go case.go -u "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/bank" \
        -d "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/bank" --test-round=20000

    cleanup_process $CDC_BINARY
    echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi




