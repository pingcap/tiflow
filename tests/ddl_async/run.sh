#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

SINK_URI="mysql://root@127.0.0.1:3306/"

function check_ts_forward() {
    changefeedid=$1
    rts1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    sleep 1
    rts2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    if [[ "$rts1" != "null" ]] && [[ "$rts1" != "0" ]]; then
        if [[  "$rts1" -ne "$rts2" ]] || [[ "$checkpoint1" -ne "$checkpoint2" ]]; then
            echo "changefeed is working normally rts: ${rts1}->${rts2} checkpoint: ${checkpoint1}->${checkpoint2}"
            return
        fi
    fi
    exit 1
}

function check_ts_block() {
    changefeedid=$1
    rts1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint1=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    sleep 1
    rts2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."resolved-ts"')
    checkpoint2=$(cdc cli changefeed query --changefeed-id=${changefeedid} 2>&1|jq '.status."checkpoint-ts"')
    if [[ "$rts1" != "null" ]] && [[ "$rts1" != "0" ]]; then
        if [[  "$rts1" -eq "$rts2" ]] || [[ "$checkpoint1" -eq "$checkpoint2" ]]; then
            echo "changefeed is blocking rts: ${rts1}->${rts2} checkpoint: ${checkpoint1}->${checkpoint2}"
            return
        fi
    fi
    exit 1
}

export -f check_ts_forward
export -f check_ts_block

function run() {
      # don't test kafka in this case
    if [ "$SINK_TYPE" == "kafka" ]; then
      return
    fi

    rm -rf $WORK_DIR && mkdir -p $WORK_DIR

    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR

    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
    # normal changefeed
    changefeedid=$(run_cdc_cli changefeed create --sink-uri="$SINK_URI" 2>&1|tail -n2|head -n1|awk '{print $2}')
    # ddl blocked changefeed
    export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/InjectChangefeedDDLBlock=return(true)'
    run_cdc_cli changefeed create -c="changefeed-ddl-block" --sink-uri="blackhole://"
    # write ddl
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    ensure 3 check_ts_block "changefeed-ddl-block"
    ensure 4 check_ts_forward $changefeedid
    ensure 3 check_ts_block "changefeed-ddl-block"

    check_table_exists ddl_async.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
