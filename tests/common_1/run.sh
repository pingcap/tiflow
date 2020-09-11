#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-common-1-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    tidb_build_branch=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -e \
        "select tidb_version()\G"|grep "Git Branch"|awk -F: '{print $(NF)}'|tr -d " ")
    if [[ ! $tidb_build_branch =~ ^(master)$ ]]; then
        echo "skip some SQLs in tidb v4.0.x"
    else
        run_sql_file $CUR/data/test_v5.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    fi
    run_sql_file $CUR/data/test_finish.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    check_table_exists common_1.v1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists common_1.recover_and_insert ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_table_exists common_1.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
