#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
export PATH=$PATH:$CUR:$CUR/../bin
echo $PATH
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
DOWN_TIDB_HOST=127.0.0.1
UP_TIDB_HOST=127.0.0.1
DOWN_TIDB_PORT=4000
UP_TIDB_PORT=3306

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster_local --workdir $WORK_DIR

    cd $WORK_DIR
    run_sql_file $CUR/data/database.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql_file $CUR/data/database.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    sleep 1000000
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
