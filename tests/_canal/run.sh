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
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST:$UP_PD_PORT)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
cat - >"$WORK_DIR/changefeed.toml" <<EOF
enable-old-value = true
[sink]
protocol = "canal"
EOF
    TOPIC_NAME="example"
    case $SINK_TYPE in
        kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal";;
        *) SINK_URI="mysql://root@127.0.0.1:3306/";;
    esac

    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$WORK_DIR/changefeed.toml"
    run_sql_file $CUR/data/database.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
    run_sql_file $CUR/data/database.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    sleep 1000000
    # sync_diff can't check non-exist table, so we check expected tables are created in downstream first
    cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
