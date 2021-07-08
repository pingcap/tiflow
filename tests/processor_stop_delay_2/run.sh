#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=3

function check_position_removed() {
    pd=$1
    position=$(cdc cli unsafe show-metadata --pd=$pd | grep "position")
    cdc cli unsafe show-metadata --pd=$pd
    echo "AAposition: $position"
    echo "AAposition: ${#position}"
    if [[ ! "${#position}" -eq "0" ]]; then
        echo "position: $position"
        exit 1
    fi
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

export -f check_position_removed
export -f check_changefeed_state

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
    TOPIC_NAME="ticdc-processor-stop-delay-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/ProcessorDDLPullerExitDelaying=sleep(2000);github.com/pingcap/ticdc/cdc/ProcessorUpdatePositionDelaying=sleep(2000)' # old processor
    # this case should be skipped when new processor enabled

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    run_sql "CREATE DATABASE processor_stop_delay_2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table processor_stop_delay_2.t (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO processor_stop_delay_2.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    check_table_exists "processor_stop_delay_2.t" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    # pause changefeed first, and then resume the changefeed. The processor stop
    # logic will be delayed by 10s, which is controlled by failpoint injection.
    # The changefeed should be resumed and no data loss.
    cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr
    ensure 10 check_position_removed $pd_addr
    ensure 10 check_changefeed_state $pd_addr $changefeed_id "stopped"
    run_sql "INSERT INTO processor_stop_delay_2.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr
    run_sql "INSERT INTO processor_stop_delay_2.t values (),(),(),(),(),(),()" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    export GO_FAILPOINTS=''
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
