#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function split_and_random_merge() {
    pd_addr=$1
    scale=$2
    echo "split_and_random_merge scale: $scale"
    run_sql "SPLIT TABLE region_merge.t1 BETWEEN (-9223372036854775808) AND (9223372036854775807) REGIONS $scale;" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
    run_sql "SELECT count(distinct region_id) from information_schema.tikv_region_status where db_name = 'region_merge' and table_name = 't1';" \
        && cat $OUT_DIR/sql_res.region_merge.txt
    run_sql "insert into region_merge.t1 values (-9223372036854775808),(0),(1),(9223372036854775807);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "delete from region_merge.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    # sleep 5s to wait some region merge
    sleep 5
}

large_scale=(100 200 400 800 1600 3200 6400 12800 25600 51200)
small_scale=(100 200 400)
# in CI, we use the small data set
test_scale=( "${small_scale[@]}" )

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true --pd-config $CUR/conf/pd_config.toml

    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    TOPIC_NAME="ticdc-region-merge-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi

    # set max_execution_time to 15s, because split region could block even region has been split.
    run_sql "SET @@global.MAX_EXECUTION_TIME = 15000;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE DATABASE region_merge;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE TABLE region_merge.t1 (id bigint primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    for scale in "${test_scale[@]}"; do
        split_and_random_merge $pd_addr $scale
    done

    run_sql "insert into region_merge.t1 values (-9223372036854775808),(0),(1),(9223372036854775807);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    check_table_exists region_merge.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
