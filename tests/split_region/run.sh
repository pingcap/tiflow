#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    run_cdc_server $WORK_DIR $CDC_BINARY
    cdc cli changefeed create --start-ts=$start_ts --sink-uri="mysql://root@127.0.0.1:3306/"

    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    check_table_exists split_region.test1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists split_region.test2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # split table into 5 regions, run some other DMLs and check data is synchronized to downstream
    run_sql "split table split_region.test1 between (1) and (100000) regions 50;"
    run_sql "split table split_region.test2 between (1) and (100000) regions 50;"
    run_sql_file $CUR/data/increment.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
