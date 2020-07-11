#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
TLS_DIR=$CUR/certificates
CDC_BINARY=cdc.test

function prepare() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR
    echo $WORK_DIR
    set -x
    start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=https://${TLS_PD_HOST}:${TLS_PD_PORT} --ca=$TLS_DIR/ca.pem --cert=$TLS_DIR/client.pem --key=$TLS_DIR/client-key.pem)

    run_sql "CREATE table test.simple1(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "CREATE table test.simple2(id int primary key, val int);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}

    run_cdc_server \
        --workdir $WORK_DIR \
        --binary $CDC_BINARY \
        --logsuffix "_${TEST_NAME}_tls" \
        --pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
        --addr "127.0.0.1:8300" \
        --tlsdir $TLS_DIR

    run_cdc_cli changefeed create \
        --pd=https://${TLS_PD_HOST}:${TLS_PD_PORT} \
        --start-ts=$start_ts \
        --sink-uri="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/" \
        --ca=$TLS_DIR/ca.pem \
        --cert=$TLS_DIR/client.pem \
        --key=$TLS_DIR/client-key.pem
}

function sql_check() {
    # run check in sequence and short circuit principle, if error hanppens,
    # the following statement will be not executed

    # check table simple1.
    run_sql "SELECT id, val FROM test.simple1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT} && \
    check_contains "id: 1" && \
    check_contains "val: 1" && \
    check_contains "id: 2" && \
    check_contains "val: 22" && \
    check_not_contains "id: 3" && \

    # check table simple2.
    run_sql "SELECT id, val FROM test.simple2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT} && \
    check_contains "id: 1" && \
    check_contains "val: 1" && \
    check_contains "id: 2" && \
    check_contains "val: 22" && \
    check_not_contains "id: 3"
}

function sql_test() {
    # test insert/update/delete for two table in the same way.
    run_sql "INSERT INTO test.simple1(id, val) VALUES (1, 1);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "INSERT INTO test.simple1(id, val) VALUES (2, 2);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "INSERT INTO test.simple1(id, val) VALUES (3, 3);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}

    # update id = 2 and delete id = 3
    run_sql "UPDATE test.simple1 set val = 22 where id = 2;" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "DELETE from test.simple1 where id = 3;" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}

    # same dml for table simple2
    run_sql "INSERT INTO test.simple2(id, val) VALUES (1, 1);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "INSERT INTO test.simple2(id, val) VALUES (2, 2);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "INSERT INTO test.simple2(id, val) VALUES (3, 3);" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}

    run_sql "UPDATE test.simple2 set val = 22 where id = 2;" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}
    run_sql "DELETE from test.simple2 where id = 3;" ${TLS_TIDB_HOST} ${TLS_TIDB_PORT}

    i=0
    check_time=50
    set +e
    while [ $i -lt $check_time ]
    do
        sql_check
        ret=$?
        if [ "$ret" == 0 ]; then
            echo "check data successfully"
            break
        fi
        ((i++))
        echo "check data failed $i-th time, retry later"
        sleep 2
    done
    set -e

    if [ $i -ge $check_time ]; then
        echo "check data failed at last"
        exit 1
    fi

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
prepare $*
sql_test $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
