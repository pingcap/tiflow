#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    stop_tidb_cluster

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-resolve-lock-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/tidb-txn-mode=pessimistic";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
        run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi
}

trap stop_tidb_cluster EXIT
prepare $*

cd "$(dirname "$0")"
set -o pipefail
GO111MODULE=on go run main.go -config ./config.toml 2>&1 | tee $WORK_DIR/tester.log
check_table_exists test.t2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
check_sync_diff $WORK_DIR $CUR/diff_config.toml
cleanup_process $CDC_BINARY
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
