#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$( cd $CUR/../_certificates && pwd )

function run() {
    sudo pip install requests

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR --multiple-upstream-pd true

    cd $WORK_DIR
    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-http-api-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://normal:123456@127.0.0.1:3306/";;
    esac

    python $CUR/util/test_case.py check_health
    python $CUR/util/test_case.py get_status

    python $CUR/util/test_case.py create_changefeed "$SINK_URI"

    run_sql "CREATE table test.simple(id int primary key, val int);"
    run_sql "CREATE table test.\`simple-dash\`(id int primary key, val int);"
    sleep 1

    python $CUR/util/test_case.py list_changefeed
    python $CUR/util/test_case.py get_changefeed
    python $CUR/util/test_case.py pause_changefeed
    python $CUR/util/test_case.py update_changefeed
    python $CUR/util/test_case.py resume_changefeed
    python $CUR/util/test_case.py rebalance_table
    python $CUR/util/test_case.py move_table
    python $CUR/util/test_case.py get_processor
    python $CUR/util/test_case.py list_processor
    python $CUR/util/test_case.py set_log_level
    python $CUR/util/test_case.py remove_changefeed
    python $CUR/util/test_case.py resign_owner

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
