#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE DATABASE file_sort;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=file_sort
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "info"

    TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    sort_dir="$WORK_DIR/file_sort_cache"
    mkdir $sort_dir
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --sort-engine="file" --sort-dir="$sort_dir"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    # Add a check table to reduce check time, or if we check data with sync diff
    # directly, there maybe a lot of diff data at first because of the incremental scan
    run_sql "CREATE table file_sort.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "file_sort.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists "file_sort.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "truncate table file_sort.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_sql "CREATE table file_sort.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "file_sort.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=file_sort
    run_sql "CREATE table file_sort.check3(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "file_sort.check3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "create table file_sort.USERTABLE2 like file_sort.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "insert into file_sort.USERTABLE2 select * from file_sort.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table file_sort.check4(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "file_sort.USERTABLE2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists "file_sort.check4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
