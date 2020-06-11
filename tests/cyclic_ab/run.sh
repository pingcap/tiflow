#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    # kafka is not supported yet.
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    cd $WORK_DIR


    # create table to upstream.
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    # create table to downsteam.
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/"

    # record tso after we create tables to not block on waiting mark tables DDLs.
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server \
        --workdir $WORK_DIR \
        --binary $CDC_BINARY \
        --logsuffix "_${TEST_NAME}_upsteam" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}" \
        --addr "127.0.0.1:8300"

    run_cdc_server \
        --workdir $WORK_DIR \
        --binary $CDC_BINARY \
        --logsuffix "_${TEST_NAME}_downsteam" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
        --addr "127.0.0.1:8301"

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}" \
        --cyclic-replica-id 1 \
        --cyclic-filter-replica-ids 2 \
        --cyclic-sync-ddl true

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
        --cyclic-replica-id 2 \
        --cyclic-filter-replica-ids 1 \
        --cyclic-sync-ddl false

    for i in $(seq 11 20); do {
        sqlup="START TRANSACTION;"
        sqldown="START TRANSACTION;"
        for j in $(seq 21 24); do {
            if [ $((j%2)) -eq 0 ]; then
                sqldown+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 2);"
            else
                sqlup+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 1);"
            fi
        } done;
        sqlup+="COMMIT;"
        sqldown+="COMMIT;"

        echo $sqlup
        echo $sqldown
        run_sql "${sqlup}" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        run_sql "${sqldown}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    } done;

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # Sleep a while to make sure no more events will be created in cyclic.
    sleep 5

    # Why 30? 20 insert + 10 mark table insert.
    expect=30
    uuid=$(run_cdc_cli changefeed list --pd=http://$UP_PD_HOST:$UP_PD_PORT 2>&1 | grep -oE "[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}")
    count=$(curl -sSf 127.0.0.1:8300/metrics | grep txn_batch_size_sum | grep ${uuid} | awk -F ' ' '{print $2}')
    if [[ $count != $expect ]]; then
        echo "[$(date)] <<<<< found extra mysql events! expect to ${expect} got ${count} >>>>>"
        exit 1
    fi

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
