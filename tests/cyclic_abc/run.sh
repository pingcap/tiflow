#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$( cd $CUR/../_certificates && pwd )

function run() {
    # kafka is not supported yet.
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR
    start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

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
            );" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
            --ssl-ca=$TLS_DIR/ca.pem \
            --ssl-cert=$TLS_DIR/server.pem \
            --ssl-key=$TLS_DIR/server-key.pem

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}"

    run_cdc_cli changefeed cyclic create-marktables \
        --cyclic-upstream-dsn="root@tcp(${TLS_TIDB_HOST}:${TLS_TIDB_PORT})/" \
        --pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
        --ca=$TLS_DIR/ca.pem \
        --cert=$TLS_DIR/client.pem \
        --key=$TLS_DIR/client-key.pem \
        --cyclic-upstream-ssl-ca=$TLS_DIR/ca.pem \
        --cyclic-upstream-ssl-cert=$TLS_DIR/server.pem \
        --cyclic-upstream-ssl-key=$TLS_DIR/server-key.pem

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
        --logsuffix "_${TEST_NAME}_tls" \
        --pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
        --addr "127.0.0.1:8302" \
        --tlsdir $TLS_DIR \
        --cert-allowed-cn "client" # The common name of client.pem

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
        --pd "http://${UP_PD_HOST}:${UP_PD_PORT}" \
        --cyclic-replica-id 1 \
        --cyclic-filter-replica-ids 2 \
        --cyclic-sync-ddl true

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${TLS_TIDB_HOST}:${TLS_TIDB_PORT}/?ssl-ca=${TLS_DIR}/ca.pem&ssl-cert=${TLS_DIR}/server.pem?ssl-key=${TLS_DIR}/server-key.pem" \
        --pd "http://${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
        --cyclic-replica-id 2 \
        --cyclic-filter-replica-ids 3 \
        --cyclic-sync-ddl true

    run_cdc_cli changefeed create --start-ts=$start_ts \
        --sink-uri="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/" \
        --pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
        --ca=$TLS_DIR/ca.pem \
        --cert=$TLS_DIR/client.pem \
        --key=$TLS_DIR/client-key.pem \
        --cyclic-replica-id 3 \
        --cyclic-filter-replica-ids 1 \
        --cyclic-sync-ddl false

    for i in $(seq 6 15); do {
        sqlup="START TRANSACTION;"
        sqldown="START TRANSACTION;"
        sqltls="START TRANSACTION;"
        for j in $(seq 21 24); do {
            if [ $((j%3)) -eq 0 ]; then # 2 rows for 3
                sqltls+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 3);"
            elif [ $((j%3)) -eq 1 ]; then # 1 row for 1
                sqlup+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 1);"
            else # 1 row for 2
                sqldown+="INSERT INTO test.simple(id1, id2, source) VALUES (${i}, ${j}, 2);"
            fi
        } done;
        sqlup+="COMMIT;"
        sqldown+="COMMIT;"
        sqltls+="COMMIT;"

        echo $sqlup
        echo $sqldown
        echo $sqltls
        run_sql "${sqlup}" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        run_sql "${sqldown}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
        run_sql "${sqltls}" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT} \
            --ssl-ca=$TLS_DIR/ca.pem \
            --ssl-cert=$TLS_DIR/server.pem \
            --ssl-key=$TLS_DIR/server-key.pem
    } done;

    # sync-diff creates table which may block cyclic replication.
    # Sleep a while to make sure all changes has been replicated.
    sleep 10

    check_sync_diff $WORK_DIR $CUR/conf/diff_config_up_down.toml
    check_sync_diff $WORK_DIR $CUR/conf/diff_config_down_tls.toml

    # Check cert Common Name.
    curl --cacert $TLS_DIR/ca.pem \
        --cert $TLS_DIR/client.pem \
        --key $TLS_DIR/client-key.pem \
        https://127.0.0.1:8302/status

    if curl --cacert $TLS_DIR/ca.pem \
        --cert $TLS_DIR/server.pem \
        --key $TLS_DIR/server-key.pem \
        -sf --show-error \
        https://127.0.0.1:8302/status ; then
        echo "must not connect successfully"
        exit 1
    fi

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
