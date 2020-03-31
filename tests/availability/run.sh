#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/owner.sh
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test

export DOWN_TIDB_HOST
export DOWN_TIDB_PORT

function prepare() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_sql "CREATE table test.availability(id int primary key, val int);"

    cdc cli changefeed create --start-ts=$start_ts --sink-uri="mysql://root@127.0.0.1:3306/"
}

function sql_check() {
    # run check in sequence and short circuit principle, if error hanppens,
    # the following statement will be not executed

    # check table availability.
    echo "run sql_check", ${DOWN_TIDB_HOST}
    run_sql "SELECT id, val FROM test.availability;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
        check_contains "id: 1" &&
        check_contains "val: 1" &&
        check_contains "id: 2" &&
        check_contains "val: 22" &&
        check_not_contains "id: 3"
}
export -f sql_check

function check_result() {
    ensure 50 sql_check
}

function nonempty() {
    sql=$*
    run_sql "$sql" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
        check_contains "id:"
}
export -f nonempty

function availability_test() {
    # start one server
    run_cdc_server $WORK_DIR $CDC_BINARY

    # wait for the tables to appear
    check_table_exists test.availability ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 20

    run_sql "INSERT INTO test.availability(id, val) VALUES (1, 1);"
    ensure 20 nonempty 'select id, val from test.availability where id=1 and val=1'

    # kill the server, then start another one
    echo "restart the $CDC_BINARY"
    cleanup_process $CDC_BINARY
    run_cdc_server $WORK_DIR $CDC_BINARY
    run_sql "INSERT INTO test.availability(id, val) VALUES (2, 2);"
    ensure 20 nonempty 'select id, val from test.availability where id=2 and val=2'

    # run two other servers, three servers in total
    run_cdc_server $WORK_DIR $CDC_BINARY
    run_cdc_server $WORK_DIR $CDC_BINARY
    # randomly kill a server, TODO: kill the owner instead
    random_kill_process $CDC_BINARY
    run_sql "INSERT INTO test.availability(id, val) VALUES (3, 3);"
    ensure 20 nonempty 'select id, val from test.availability where id=3 and val=3'

    random_kill_process $CDC_BINARY
    run_sql "UPDATE test.availability set val = 22 where id = 2;"
    run_sql "DELETE from test.availability where id = 3;"

    check_result
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
prepare $*
#availability_test $*
test_owner_ha $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
