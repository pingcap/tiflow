#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run_length_limit() {
    # test kafka sink only in this case
    if [ "$SINK_TYPE" == "mysql" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    run_sql "DROP DATABASE if exists kafka_message;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE DATABASE kafka_message;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=kafka_message
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "info"

    TOPIC_NAME="ticdc-kafka-message-test-$RANDOM"
    SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&max-message-bytes=4096&kafka-version=${KAFKA_VERSION}"
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&max-message-bytes=4096&version=${KAFKA_VERSION}"
    fi

    # Add a check table to reduce check time, or if we check data with sync diff
    # directly, there maybe a lot of diff data at first because of the incremental scan
    run_sql "CREATE table kafka_message.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_table_exists "kafka_message.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "truncate table kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_sql "CREATE table kafka_message.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=kafka_message
    run_sql "CREATE table kafka_message.check3(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.check3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "create table kafka_message.USERTABLE2 like kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "insert into kafka_message.USERTABLE2 select * from kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table kafka_message.check4(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.USERTABLE2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_table_exists "kafka_message.check4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
    stop_tidb_cluster
}

function run_batch_size_limit() {
      # test kafka sink only in this case
    if [ "$SINK_TYPE" == "mysql" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    run_sql "DROP DATABASE if exists kafka_message;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE DATABASE kafka_message;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=kafka_message
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --loglevel "info"

    TOPIC_NAME="ticdc-kafka-message-test-$RANDOM"
    SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&max-batch-size=3&kafka-version=${KAFKA_VERSION}"
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&max-batch-size=3&version=${KAFKA_VERSION}"
    fi

    # Add a check table to reduce check time, or if we check data with sync diff
    # directly, there maybe a lot of diff data at first because of the incremental scan
    run_sql "CREATE table kafka_message.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_table_exists "kafka_message.check1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "truncate table kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_sql "CREATE table kafka_message.check2(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.check2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=kafka_message
    run_sql "CREATE table kafka_message.check3(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.check3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "create table kafka_message.USERTABLE2 like kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "insert into kafka_message.USERTABLE2 select * from kafka_message.USERTABLE" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table kafka_message.check4(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists "kafka_message.USERTABLE2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90
    check_table_exists "kafka_message.check4" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 90

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
    stop_tidb_cluster
}

trap stop_tidb_cluster EXIT
run_length_limit $*
run_batch_size_limit $*
check_cdc_state_log $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
