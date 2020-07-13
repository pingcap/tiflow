#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

function check_processor_table_count() {
    pd=$1
    changefeed=$2
    capture=$3
    expected=$4
    count=$(cdc cli processor query --pd=$pd -c $changefeed -p $capture 2>&1|jq '.status."tables"|length')
    if [[ ! "$count" -eq "$expected" ]]; then
        echo "table count $count does equal to expected count $expected"
        exit 1
    fi
}

function check_no_capture() {
    pd=$1
    count=$(cdc cli capture list --pd=$pd 2>&1|jq '.|length')
    if [[ ! "$count" -eq "0" ]]; then
        exit 1
    fi
}

export -f check_processor_table_count
export -f check_no_capture

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST:$UP_PD_PORT"
    TOPIC_NAME="ticdc-changefeed-reconstruct-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --logsuffix server1 --pd $pd_addr
    owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    run_sql "CREATE DATABASE changefeed_reconstruct;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_reconstruct
    check_table_exists "changefeed_reconstruct.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    # kill capture
    kill $owner_pid
    ensure $MAX_RETRIES check_no_capture $pd_addr

    # run another cdc server
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --logsuffix server2
    ensure $MAX_RETRIES "$CDC_BINARY cli capture list --pd=$pd_addr 2>&1 | grep id"
    capture_id=$($CDC_BINARY cli --pd=$pd_addr capture list 2>&1 | awk -F '"' '/id/{print $4}')
    echo "capture_id:" $capture_id

    # check table has been dispatched to new capture
    ensure $MAX_RETRIES check_processor_table_count $pd_addr $changefeed_id $capture_id 1
    run_sql "DROP DATABASE changefeed_reconstruct;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    # check table has been removed from processor
    ensure $MAX_RETRIES check_processor_table_count $pd_addr $changefeed_id $capture_id 0

    run_sql "CREATE DATABASE changefeed_reconstruct;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_reconstruct
    check_table_exists "changefeed_reconstruct.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
