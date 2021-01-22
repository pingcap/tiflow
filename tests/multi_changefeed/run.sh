#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=10

function check_capture_count() {
    pd=$1
    expected=$2
    count=$(cdc cli capture list --pd=$pd 2>&1|jq '.|length')
    if [[ ! "$count" -eq "$expected" ]]; then
        echo "count: $count expected: $expected"
        exit 1
    fi
}

function kill_cdc_and_restart() {
    pd_addr=$1
    work_dir=$2
    cdc_binary=$3
    MAX_RETRIES=10
    cdc_pid=$(curl -s http://127.0.0.1:8300/status|jq '.pid')
    kill $cdc_pid
    ensure $MAX_RETRIES check_capture_count $pd_addr 0
    run_cdc_server --workdir $work_dir --binary $cdc_binary --addr "127.0.0.1:8300" --pd $pd_addr
    ensure $MAX_RETRIES check_capture_count $pd_addr 1
}

export -f check_capture_count
export -f kill_cdc_and_restart

function run() {
    # kafka is not supported yet.
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
    SINK_URI="blackhole://"

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" -c "old-value-cf" --config="$CUR/conf/changefeed1.toml"
    cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" -c "no-old-value-cf" --config="$CUR/conf/changefeed2.toml"
    echo "c"
    run_sql "CREATE DATABASE multi_changefeed;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table multi_changefeed.t1 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO multi_changefeed.t1 VALUES (1,1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "UPDATE multi_changefeed.t1 SET val = 2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "DELETE FROM multi_changefeed.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    echo "a"
    ls -l $WORK_DIR
    grep "BlockHoleSink: FlushRowChangedEvents" $WORK_DIR/cdc.log > $WORK_DIR/output.log
    echo "b"
    cat aabbccdd

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
