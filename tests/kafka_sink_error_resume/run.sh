#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4
MAX_RETRIES=20

function check_changefeed_state() {
    pd_addr=$1
    changefeed_id=$2
    expected=$3
    state=$(cdc cli --pd=$pd_addr changefeed query -s -c $changefeed_id|jq -r ".state")
    if [[ "$state" != "$expected" ]];then
        echo "unexpected state $state, expected $expected"
        exit 1
    fi
}

export -f check_changefeed_state

function run() {
    # test kafka sink only in this case
    if [ "$SINK_TYPE" == "mysql" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
    TOPIC_NAME="ticdc-kafka-sink-error-resume-test-$RANDOM"
    SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}"
    run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"

    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/sink/producer/kafka/KafkaSinkAsyncSendError=4*return(true)'
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    run_sql "CREATE DATABASE kafka_sink_error_resume;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table kafka_sink_error_resume.t1(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table kafka_sink_error_resume.t2(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO kafka_sink_error_resume.t1 VALUES ();"

    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "error"
    cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "normal"

    check_table_exists "kafka_sink_error_resume.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists "kafka_sink_error_resume.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    run_sql "INSERT INTO kafka_sink_error_resume.t1 VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO kafka_sink_error_resume.t2 VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "UPDATE kafka_sink_error_resume.t2 SET val = 100;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
