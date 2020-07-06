#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function check_changefeed_mark_failed() {
    endpoints=$1
    changefeedid=$2
    ETCDCTL_API=3 etcdctl --endpoints=$endpoints get /tidb/cdc/changefeed/info/${changefeedid}
    changefeed_info=$(ETCDCTL_API=3 etcdctl --endpoints=$endpoints get /tidb/cdc/changefeed/info/${changefeedid}|tail -n 1)
    if [[ ! $changefeed_info == *"\"state\":\"failed\""* ]]; then
        echo "changefeed is not marked as failed: $changefeed_info"
        exit 1
    fi
}

export -f check_changefeed_mark_failed

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)
    run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/NewChangefeedError=1*return(true)'
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    changefeedid=$(cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    ensure 5 check_changefeed_mark_failed ${UP_PD_HOST}:${UP_PD_PORT} ${changefeedid}
    changefeed_info=$(ETCDCTL_API=3 etcdctl --endpoints=${UP_PD_HOST}:${UP_PD_PORT} get /tidb/cdc/changefeed/info/${changefeedid}|tail -n 1)
    new_info=$(echo $changefeed_info|sed 's/"state":"failed"/"state":"normal"/g')
    ETCDCTL_API=3 etcdctl --endpoints=${UP_PD_HOST}:${UP_PD_PORT} put /tidb/cdc/changefeed/info/${changefeedid} "$new_info"

    check_table_exists "changefeed_error.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
