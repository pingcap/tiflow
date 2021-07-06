#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=10

function check_changefeed_mark_failed() {
    endpoints=$1
    changefeedid=$2
    error_msg=$3
    info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
    echo "$info"
    state=$(echo $info|jq -r '.state')
    if [[ ! "$state" == "failed" ]]; then
        echo "changefeed state $state does not equal to failed"
        exit 1
    fi
    message=$(echo $info|jq -r '.error.message')
    if [[ ! "$message" =~ "$error_msg" ]]; then
        echo "error message '$message' is not as expected '$error_msg'"
        exit 1
    fi
}

function check_changefeed_mark_failed_regex() {
    endpoints=$1
    changefeedid=$2
    error_msg=$3
    info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
    echo "$info"
    state=$(echo $info|jq -r '.state')
    if [[ ! "$state" == "failed" ]]; then
        echo "changefeed state $state does not equal to failed"
        exit 1
    fi
    message=$(echo $info|jq -r '.error.message')
    if [[ ! "$message" =~ $error_msg ]]; then
        echo "error message '$message' does not match '$error_msg'"
        exit 1
    fi
}

function check_changefeed_mark_stopped_regex() {
    endpoints=$1
    changefeedid=$2
    error_msg=$3
    info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
    echo "$info"
    state=$(echo $info|jq -r '.state')
    if [[ ! "$state" == "stopped" ]]; then
        echo "changefeed state $state does not equal to stopped"
        exit 1
    fi
    message=$(echo $info|jq -r '.error.message')
    if [[ ! "$message" =~ $error_msg ]]; then
        echo "error message '$message' does not match '$error_msg'"
        exit 1
    fi
}

function check_changefeed_mark_stopped() {
    endpoints=$1
    changefeedid=$2
    error_msg=$3
    info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
    echo "$info"
    state=$(echo $info|jq -r '.state')
    if [[ ! "$state" == "stopped" ]]; then
        echo "changefeed state $state does not equal to stopped"
        exit 1
    fi
    message=$(echo $info|jq -r '.error.message')
    if [[ ! "$message" =~ "$error_msg" ]]; then
        echo "error message '$message' is not as expected '$error_msg'"
        exit 1
    fi
}

function check_no_changefeed() {
    pd=$1
    count=$(cdc cli changefeed list --pd=$pd 2>&1|jq '.|length')
    if [[ ! "$count" -eq "0" ]]; then
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

export -f check_changefeed_mark_failed
export -f check_changefeed_mark_failed_regex
export -f check_changefeed_mark_stopped_regex
export -f check_changefeed_mark_stopped
export -f check_no_changefeed
export -f check_no_capture

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)
    run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/NewChangefeedNoRetryError=1*return(true)'
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

    TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1";;
    esac
    changefeedid=$(cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi

    ensure $MAX_RETRIES check_changefeed_mark_failed_regex http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} ".*CDC:ErrStartTsBeforeGC.*"
    changefeed_info=$(ETCDCTL_API=3 etcdctl --endpoints=${UP_PD_HOST_1}:${UP_PD_PORT_1} get /tidb/cdc/changefeed/info/${changefeedid}|tail -n 1)
    new_info=$(echo $changefeed_info|sed 's/"state":"failed"/"state":"normal"/g')
    ETCDCTL_API=3 etcdctl --endpoints=${UP_PD_HOST_1}:${UP_PD_PORT_1} put /tidb/cdc/changefeed/info/${changefeedid} "$new_info"

    check_table_exists "changefeed_error.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/NewChangefeedRetryError=return(true)'
    kill $capture_pid
    ensure $MAX_RETRIES check_no_capture http://${UP_PD_HOST_1}:${UP_PD_PORT_1}
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    ensure $MAX_RETRIES check_changefeed_mark_failed http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "failpoint injected retriable error"

    cdc cli changefeed remove -c $changefeedid
    ensure $MAX_RETRIES check_no_changefeed ${UP_PD_HOST_1}:${UP_PD_PORT_1}

    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY

    # owner DDL error case
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/InjectChangefeedDDLError=return(true)'
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    changefeedid_1=$(cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    run_sql "CREATE table changefeed_error.DDLERROR(id int primary key, val int);"
    ensure $MAX_RETRIES check_changefeed_mark_stopped http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_1} "[CDC:ErrExecDDLFailed]exec DDL failed"

    cdc cli changefeed remove -c $changefeedid_1
    cleanup_process $CDC_BINARY

    # updating GC safepoint failure case
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/InjectActualGCSafePoint=return(9223372036854775807)'
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    changefeedid_2=$(cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    ensure $MAX_RETRIES check_changefeed_mark_stopped_regex http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_2} "service safepoint lost"

    cdc cli changefeed remove -c $changefeedid_2
    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
