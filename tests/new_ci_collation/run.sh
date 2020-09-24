#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR --tidb-config $CUR/conf/tidb_config.toml

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-new_ci_collation-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?safe-mode=true";;
    esac
    cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config $CUR/conf/changefeed.toml
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    run_sql_file $CUR/data/test1.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    for i in $(seq 1 5); do
        table="new_ci_collation_test.t$i"
        check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    done
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    run_sql_file $CUR/data/test2.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
