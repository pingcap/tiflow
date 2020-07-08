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

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    # create $DB_COUNT databases and import initial workload
    for i in $(seq $DB_COUNT); do
        db="multi_capture_$i"
        run_sql "CREATE DATABASE $db;"
        go-ycsb load mysql -P $CUR/conf/workload1 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
    done

    # start $CDC_COUNT cdc servers, and create a changefeed
    for i in $(seq $CDC_COUNT); do
        run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$i" --addr "127.0.0.1:830${i}"
    done

    TOPIC_NAME="ticdc-multi-capture-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    # check tables are created and data is synchronized
    for i in $(seq $DB_COUNT); do
        check_table_exists "multi_capture_$i.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # add more data in upstream and check again
    for i in $(seq $DB_COUNT); do
        db="multi_capture_$i"
        go-ycsb load mysql -P $CUR/conf/workload2 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
