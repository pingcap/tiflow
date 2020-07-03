#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_sql "CREATE table test.simple1(id int primary key, val int);"
    run_sql "CREATE table test.simple2(id int primary key, val int);"

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-simple-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-client-id=cdc_test_simple";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi
}

function sql_check() {
    # run check in sequence and short circuit principle, if error hanppens,
    # the following statement will be not executed

    # check table simple1.
    run_sql "SELECT id, val FROM test.simple1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && \
    check_contains "id: 1" && \
    check_contains "val: 1" && \
    check_contains "id: 2" && \
    check_contains "val: 22" && \
    check_not_contains "id: 3" && \

    # check table simple2.
    run_sql "SELECT id, val FROM test.simple2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && \
    check_contains "id: 1" && \
    check_contains "val: 1" && \
    check_contains "id: 2" && \
    check_contains "val: 22" && \
    check_not_contains "id: 3"
}

function sql_test() {
    # test insert/update/delete for two table in the same way.
    run_sql "INSERT INTO test.simple1(id, val) VALUES (1, 1);"
    run_sql "INSERT INTO test.simple1(id, val) VALUES (2, 2);"
    run_sql "INSERT INTO test.simple1(id, val) VALUES (3, 3);"

    # update id = 2 and delete id = 3
    run_sql "UPDATE test.simple1 set val = 22 where id = 2;"
    run_sql "DELETE from test.simple1 where id = 3;"


    # same dml for table simple2
    run_sql "INSERT INTO test.simple2(id, val) VALUES (1, 1);"
    run_sql "INSERT INTO test.simple2(id, val) VALUES (2, 2);"
    run_sql "INSERT INTO test.simple2(id, val) VALUES (3, 3);"

    run_sql "UPDATE test.simple2 set val = 22 where id = 2;"
    run_sql "DELETE from test.simple2 where id = 3;"

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
