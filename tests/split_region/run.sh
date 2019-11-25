#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    # set max-replicas to 1 as we have only one tikv in test, or the region scatter will be refused by PD
    pd-ctl config set max-replicas 1

    cd $WORK_DIR

    # record tso before we create tables for two reasons
    # 1. skip the system table DDL
    # 2. currently we support providing table IDs only when we create a changefeed, so we have to create tables before creating a changefeed.
    start_ts=$(($(date +%s%N | cut -b1-13)<<18))

    run_sql_file $CUR/data/prepare.sql ${US_TIDB_HOST} ${US_TIDB_PORT}

    cdc server --log-file $WORK_DIR/cdc.log --log-level info > $WORK_DIR/stdout.log 2>&1 &
    cdc cli --start-ts=$start_ts --databases=split_region

    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    check_table_exists split_region.test1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists split_region.test2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # split table into 5 regions, run some other DMLs and check data is synchronized to downstream
    run_sql "split table split_region.test1 between (1) and (10000) regions 5;"
    run_sql "split table split_region.test2 between (1) and (10000) regions 5;"
    run_sql_file $CUR/data/increment.sql ${US_TIDB_HOST} ${US_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    killall cdc || true
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
