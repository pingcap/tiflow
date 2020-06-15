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

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR


    # create table to upstream.
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"

    # make sure create-marktables does not create mark table for mark table.
    for c in $(seq 1 10); do {
        # must not cause an error table name too long.
        run_cdc_cli changefeed cyclic create-marktables \
            --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"
    } done

    # record tso after we create tables to not block on waiting mark tables DDLs.
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "_${TEST_NAME}_upsteam" --pd "http://${UP_PD_HOST}:${UP_PD_PORT}"

    # Loop back to self.
    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}" \
        --cyclic-replica-id 1 \
        --cyclic-filter-replica-ids 2 \
        --cyclic-sync-ddl true

    for i in $(seq 1 10); do {
        sqlup="START TRANSACTION;"
        for j in $(seq 1 4); do {
            sqlup+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 1);"
        } done;
        sqlup+="COMMIT;"

        echo $sqlup
        run_sql "${sqlup}" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    } done;

    # Sleep a while to make sure no more events will be created in cyclic.
    sleep 5
    uuid=$(run_cdc_cli changefeed list --pd=http://$UP_PD_HOST:$UP_PD_PORT 2>&1 | grep -oE "[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}")

    # Why 50? 40 insert + 10 mark table insert.
    expect=50
    for i in $(seq 1 10); do {
        count=$(curl -sSf 127.0.0.1:8300/metrics | grep txn_batch_size_sum | grep ${uuid} | awk -F ' ' '{print $2}')
        if [[ $count == ${expect} ]]; then
            break
        fi
        sleep 2
    } done;

    if [[ $count != $expect ]]; then
        echo "[$(date)] <<<<< found extra mysql events! expect to ${expect} got ${count} >>>>>"
        exit 1
    fi

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
