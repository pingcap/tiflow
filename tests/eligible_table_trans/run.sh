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

    pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
    TOPIC_NAME="eligible-table-trans-$RANDOM"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&kafka-version=${KAFKA_VERSION}";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1";;
    esac
    if [ "$SINK_TYPE" == "kafka" ]; then
      run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?partition-num=4&version=${KAFKA_VERSION}"
    fi

    # These DDLs can't be replicated to downstream, so we run it in both upstream and downstream
    run_sql "CREATE DATABASE eligible_table_trans;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE DATABASE eligible_table_trans;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t1 (id int, unique uniq_id(id))" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t1 (id int, unique uniq_id(id))" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t2 (id int not null)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t2 (id int not null)" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
    cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" --no-confirm

    # These DDLs can't be replicated to downstream, so we run it in both upstream and downstream
    run_sql "CREATE table eligible_table_trans.t3 (id int, unique uniq_id(id))" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t3 (id int, unique uniq_id(id))" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t4 (id int not null)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "CREATE table eligible_table_trans.t4 (id int not null)" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    # run DDLs which can make some tables change from ineligible to eligible
    run_sql "ALTER table eligible_table_trans.t1 modify column id int not null" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "ALTER table eligible_table_trans.t2 add unique uniq_id(id)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "ALTER table eligible_table_trans.t3 modify column id int not null" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "ALTER table eligible_table_trans.t4 add unique uniq_id(id)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    run_sql "INSERT INTO eligible_table_trans.t1 values (1),(2),(4),(8),(1024),(2048),(4096),(2147483647)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO eligible_table_trans.t2 values (1),(2),(4),(8),(1024),(2048),(4096),(2147483647)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO eligible_table_trans.t3 values (1),(2),(4),(8),(1024),(2048),(4096),(2147483647)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "INSERT INTO eligible_table_trans.t4 values (1),(2),(4),(8),(1024),(2048),(4096),(2147483647)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
