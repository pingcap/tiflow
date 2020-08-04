#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=5

function get_safepoint() {
    pd_addr=$1
    pd_cluster_id=$2
    safe_point=$(etcdctl get --endpoints=$pd_addr /pd/$pd_cluster_id/gc/safe_point/service/ticdc|grep -oE "SafePoint\":[0-9]+"|grep -oE "[0-9]+")
    echo $safe_point
}

function check_safepoint_cleared() {
    pd_addr=$1
    pd_cluster_id=$2
    query=$(etcdctl get --endpoints=$pd_addr /pd/$pd_cluster_id/gc/safe_point/service/ticdc)
    if [ ! -z "$query" ]; then
        echo "gc safepoint is not cleared: $query"
    fi
}

function check_safepoint_forward() {
    pd_addr=$1
    pd_cluster_id=$2
    safe_point1=$(get_safepoint $pd_addr $pd_cluster_id)
    sleep 1
    safe_point2=$(get_safepoint $pd_addr $pd_cluster_id)
    if [[ "$safe_point1" == "$safe_point2" ]]; then
        echo "safepoint $safe_point1 is not forward"
        exit 1
    fi
}

function check_safepoint_equal() {
    pd_addr=$1
    pd_cluster_id=$2
    safe_point1=$(get_safepoint $pd_addr $pd_cluster_id)
    for i in $(seq 1 3); do
        sleep 1
        safe_point2=$(get_safepoint $pd_addr $pd_cluster_id)
        if [[ "$safe_point1" != "$safe_point2" ]]; then
            echo "safepoint is unexpected forward: $safe_point1 -> $safe_point2"
            exit 1
        fi
    done
}

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

export -f get_safepoint
export -f check_safepoint_forward
export -f check_safepoint_cleared
export -f check_safepoint_equal
export -f check_changefeed_state

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST:$UP_PD_PORT"
    TOPIC_NAME="ticdc-gc-safepoint-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    run_sql "CREATE DATABASE gc_safepoint;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table gc_safepoint.simple(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO gc_safepoint.simple VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    pd_cluster_id=$(curl -s $pd_addr/pd/api/v1/cluster|grep -oE "id\":\s[0-9]+"|grep -oE "[0-9]+")
    ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id

    # after the changefeed is paused, the safe_point will be not updated
    cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "stopped"
    ensure $MAX_RETRIES check_safepoint_equal $pd_addr $pd_cluster_id

    # resume changefeed will recover the safe_point forward
    cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "normal"
    ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id

    cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "stopped"
    # create another changefeed, because there exists a paused changefeed,
    # the safe_point still does not forward
    changefeed_id2=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id2 "normal"
    ensure $MAX_RETRIES check_safepoint_equal $pd_addr $pd_cluster_id

    # remove paused changefeed, the safe_point forward will recover
    cdc cli changefeed remove --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id "removed"
    ensure $MAX_RETRIES check_safepoint_forward $pd_addr $pd_cluster_id

    # remove all changefeeds, the safe_point will be cleared
    cdc cli changefeed remove --changefeed-id=$changefeed_id2 --pd=$pd_addr
    ensure $MAX_RETRIES check_changefeed_state $pd_addr $changefeed_id2 "removed"
    ensure $MAX_RETRIES check_safepoint_cleared $pd_addr $pd_cluster_id

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
