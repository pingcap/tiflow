#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/owner.sh
source $CUR/capture.sh
source $CUR/processor.sh
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

export DOWN_TIDB_HOST
export DOWN_TIDB_PORT

function prepare() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    run_sql "CREATE table test.availability1(id int primary key, val int);"
    run_sql "CREATE table test.availability2(id int primary key, val int);"
    run_sql "CREATE table test.availability3(id int primary key, val int);"

    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="mysql://root@127.0.0.1:3306/"
}

trap stop_tidb_cluster EXIT
prepare $*
test_owner_ha $*
test_capture_ha $*
test_processor_ha $*
check_cdc_state_log $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
