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
    # create an ineligible table to make sure cyclic replication works fine even with some ineligible tables.
    run_sql "CREATE table test.ineligible(id int, val int);"

    # create table to downsteam.
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/"

    # make sure create-marktables does not create mark table for mark table.
    for c in $(seq 1 10); do {
        # must not cause an error table name too long.
        run_cdc_cli changefeed cyclic create-marktables \
            --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/"
    } done

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

    # Echo y to ignore ineligible tables
    echo "y" | run_cdc_cli changefeed create --start-ts=$start_ts \
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
        --cyclic-sync-ddl false \
        --config $CUR/conf/only_test_simple.toml

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

    # sync-diff creates table which may block cyclic replication.
    # Sleep a while to make sure all changes has been replicated.
    sleep 10

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # At the time of writing, tso is at least 18 digits long in decimal format.
    # $ pd-ctl tso 418252158551982113
    # system:  2020-07-23 19:56:05.57 +0800 CST
    # logic:  33
    run_sql "SELECT start_timestamp FROM tidb_cdc.repl_mark_test_simple LIMIT 1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && \
    check_contains "start_timestamp: [0-9]{18,}"

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
