#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-split-region-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

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
