#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME

CDC_COUNT=3
DB_COUNT=4

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables for two reasons
    # 1. skip the system table DDL
    # 2. currently we support providing table IDs only when we create a changefeed, so we have to create tables before creating a changefeed.
    start_ts=$(($(date +%s%N | cut -b1-13)<<18))
    cdc_cli_params=""

    for i in $(seq $DB_COUNT); do
        db="multi_capture_$i"
        run_sql "CREATE DATABASE $db;"
        go-ycsb load mysql -P $CUR/conf/workload1 -p mysql.host=${US_TIDB_HOST} -p mysql.port=${US_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
        cdc_cli_params="$cdc_cli_params --databases=multi_capture_$i"
    done

    for i in $(seq $CDC_COUNT); do
        cdc server --log-file $WORK_DIR/cdc${i}.log --log-level info > $WORK_DIR/stdout${i}.log 2>&1 &
    done
    cdc cli --start-ts=$start_ts $cdc_cli_params

    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    for i in $(seq $DB_COUNT); do
        check_table_exists "multi_capture_$i.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    for i in $(seq $DB_COUNT); do
        db="multi_capture_$i"
        go-ycsb load mysql -P $CUR/conf/workload2 -p mysql.host=${US_TIDB_HOST} -p mysql.port=${US_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    killall cdc || true
}

# trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
