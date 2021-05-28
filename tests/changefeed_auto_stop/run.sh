#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_changefeed_state() {
    endpoints=$1
    changefeedid=$2
    expected=$3
    output=$(cdc cli changefeed query --simple --changefeed-id $changefeedid --pd=http://$endpoints 2>&1)
    state=$(echo $output | grep -oE "\"state\": \"[a-z]+\""|tr -d '" '|awk -F':' '{print $(NF)}')
    if [ "$state" != "$expected" ]; then
        echo "unexpected state $output, expected $expected"
        exit 1
    fi
}

export -f check_changefeed_state

function run() {
    DB_COUNT=4

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR
    start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    for i in $(seq $DB_COUNT); do
        db="changefeed_auto_stop_$i"
        run_sql "CREATE DATABASE $db;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
    done

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
    # export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/ProcessorSyncResolvedError=1*return(true);github.com/pingcap/ticdc/cdc/ProcessorUpdatePositionDelaying=return(true)' # old processor
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/processor/pipeline/ProcessorSyncResolvedError=1*return(true);github.com/pingcap/ticdc/cdc/processor/ProcessorUpdatePositionDelaying=sleep(1000)' # new processor
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
    export GO_FAILPOINTS=''

    TOPIC_NAME="ticdc-changefeed-auto-stop-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://normal:123456@127.0.0.1:3306/";;
    esac
    changefeedid=$(cdc cli changefeed create --pd="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" --start-ts=$start_ts --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi

    ensure 10 check_changefeed_state  ${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "stopped"

    cdc cli changefeed resume --changefeed-id=${changefeedid} --pd="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
    for i in $(seq $DB_COUNT); do
        check_table_exists "changefeed_auto_stop_$i.USERTABLE" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
