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
    start_third_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR


    # create table in all cluters.
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2)) \
            partition by range (id1) ( \
                partition p0 values less than (10), \
                partition p1 values less than (20), \
                partition p3 values less than (30) \
            );" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2)) \
            partition by range (id1) ( \
                partition p0 values less than (10), \
                partition p1 values less than (20), \
                partition p3 values less than (30) \
            );" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE table test.simple(id1 int, id2 int, source int, primary key (id1, id2)) \
            partition by range (id1) ( \
                partition p0 values less than (10), \
                partition p1 values less than (20), \
                partition p3 values less than (30) \
            );" ${THIRD_TIDB_HOST} ${THIRD_TIDB_PORT}

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${THIRD_TIDB_HOST}:${THIRD_TIDB_PORT})/" \
        --pd "http://${THIRD_PD_HOST}:${THIRD_PD_PORT}"

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

    run_cdc_server \
        --workdir $WORK_DIR \
        --binary $CDC_BINARY \
        --logsuffix "_${TEST_NAME}_third" \
        --pd "http://${THIRD_PD_HOST}:${THIRD_PD_PORT}" \
        --addr "127.0.0.1:8302"

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}" \
        --cyclic-replica-id 1 \
        --cyclic-filter-replica-ids 2 \
        --cyclic-sync-ddl true

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${THIRD_TIDB_HOST}:${THIRD_TIDB_PORT}/" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
        --cyclic-replica-id 2 \
        --cyclic-filter-replica-ids 3 \
        --cyclic-sync-ddl false

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/" \
        --pd "http://${THIRD_PD_HOST}:${THIRD_PD_PORT}" \
        --cyclic-replica-id 3 \
        --cyclic-filter-replica-ids 1 \
        --cyclic-sync-ddl false

    for i in $(seq 6 15); do {
        sqlup="START TRANSACTION;"
        sqldown="START TRANSACTION;"
        sqlthird="START TRANSACTION;"
        for j in $(seq 21 24); do {
            if [ $((j%3)) -eq 0 ]; then # 2 rows for 3
                sqlthird+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 3);"
            elif [ $((j%3)) -eq 1 ]; then # 1 row for 1
                sqlup+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 1);"
            else # 1 row for 2
                sqldown+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 2);"
            fi
        } done;
        sqlup+="COMMIT;"
        sqldown+="COMMIT;"
        sqlthird+="COMMIT;"

        echo $sqlup
        echo $sqldown
        echo $sqlthird
        run_sql "${sqlup}" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        run_sql "${sqldown}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
        run_sql "${sqlthird}" ${THIRD_TIDB_HOST} ${THIRD_TIDB_PORT}
    } done;

    check_sync_diff $WORK_DIR $CUR/conf/diff_config_up_down.toml
    check_sync_diff $WORK_DIR $CUR/conf/diff_config_down_third.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
