#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=10

function check_old_value_enabled() {
    echo "check_old_value_enabled"
    row_logs=$(grep "EmitRowChangedEvents" "$WORK_DIR/cdc.log" || true)
    echo $row_logs
    exit 1
#    if [[ ! "$count" -eq "$expected" ]]; then
#        echo "count: $count expected: $expected"
#        exit 1
#    fi
}


export -f check_old_value_enabled

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

    ensure $MAX_RETRIES check_old_value_enabled
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
