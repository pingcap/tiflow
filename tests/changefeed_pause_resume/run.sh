#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=3

function run() {
    rm -rf $WORK_DIR && mkdir -p $WORK_DIR
    start_tidb_cluster --workdir $WORK_DIR
    cd $WORK_DIR

    pd_addr="http://$UP_PD_HOST:$UP_PD_PORT"
    TOPIC_NAME="ticdc-changefeed-pause-resume-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4"
    fi
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')

    run_sql "CREATE DATABASE changefeed_pause_resume;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    for i in $(seq 1 $TABLE_COUNT); do
        stmt="CREATE table changefeed_pause_resume.t$i (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)"
        run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    done

    for i in $(seq 1 $TABLE_COUNT); do
        table="changefeed_pause_resume.t$i"
        check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    done

    for i in $(seq 1 10); do
        cdc cli changefeed pause --changefeed-id=$changefeed_id --pd=$pd_addr

        for j in $(seq 1 $TABLE_COUNT); do
            stmt="drop table changefeed_pause_resume.t$j"
            run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        done

        for j in $(seq 1 $TABLE_COUNT); do
            stmt="CREATE table changefeed_pause_resume.t$j (id int primary key auto_increment, t datetime DEFAULT CURRENT_TIMESTAMP)"
            run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        done

        for j in $(seq 1 $TABLE_COUNT); do
            stmt="insert into changefeed_pause_resume.t$j values (),(),(),(),()"
            run_sql "$stmt" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
        done

        cdc cli changefeed resume --changefeed-id=$changefeed_id --pd=$pd_addr

        check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
    done

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
