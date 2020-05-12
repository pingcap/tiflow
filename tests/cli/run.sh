#!/bin/bash

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(cdc cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    TOPIC_NAME="ticdc-cli-test-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        mysql) ;&
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac
    cdc cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi

    run_sql "CREATE database tidb_cdc;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table tidb_cdc.repl_mark_test_simple (
        bucket INT NOT NULL, \
        replica_id BIGINT UNSIGNED NOT NULL, \
        val BIGINT DEFAULT 0, \
        PRIMARY KEY (bucket, replica_id) \
    );" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    # Make sure changefeed is created.
    check_table_exists tidb_cdc.repl_mark_test_simple ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    uuid=$(cdc cli changefeed list 2>&1 | grep -oE "[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}")

    # Pause changefeed
    cdc cli changefeed --changefeed-id $uuid pause && sleep 3
    jobtype=$(cdc cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 1 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 1 got ${jobtype} >>>>>"
        exit 1
    fi

    # Resume changefeed
    cdc cli changefeed --changefeed-id $uuid resume && sleep 3
    jobtype=$(cdc cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 2 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 2 got ${jobtype} >>>>>"
        exit 1
    fi

    # Remove changefeed
    cdc cli changefeed --changefeed-id $uuid remove && sleep 3
    jobtype=$(cdc cli changefeed --changefeed-id $uuid query 2>&1 | grep 'admin-job-type' | grep -oE '[0-9]' | head -1)
    if [[ $jobtype != 3 ]]; then
        echo "[$(date)] <<<<< unexpect admin job type! expect 3 got ${jobtype} >>>>>"
        exit 1
    fi

    # Make sure bad sink url fails at creating changefeed.
    badsink=$(cdc cli changefeed create --start-ts=$start_ts --sink-uri="mysql://badsink" | grep -oE 'fail')
    if [[ -z $badsink ]]; then
        echo "[$(date)] <<<<< unexpect output got ${badsink} >>>>>"
        exit 1
    fi

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
