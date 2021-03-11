#!/bin/bash

set -e

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
TEST_HOST_LIST=(UP_TIDB_HOST UP_PD_HOST_{1..3} UP_TIKV_HOST_{1..3})

function start_proxy() {
    tools/bin/grpc-dump --port=8080 > $OUR_DIR/packets.dump 2>/dev/null &
    pid=$!
    export http_proxy=http://127.0.0.1:8080 \
        https_proxy=$http_proxy
}

function prepare() {
    for host in $TEST_HOST_LIST; do
        check_is_loopback='import ipaddress; import os; exit(0) if ipaddress.ip_address(os.getenv(u"%")).is_loopback else exit(1)'
        if echo $host | xargs -I% python3 -c "$check_is_loopback" ; then
            echo "[WARNING] http_proxies: the host of component $host is loopback, hence proxies would be ignored, skipping this test..."
            exit 0
        fi
    done

    # firstly, let's build grpc-dump...
    make tools/bin/grpc-dump
    start_proxy

    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    stop_tidb_cluster
    start_tidb_cluster --workdir $WORK_DIR

    cd $WORK_DIR
    # record tso before we create tables to skip the system table DDLs
    start_ts=$(run_cdc_cli tso query --pd=http://$UP_PD_HOST_1:$UP_PD_PORT_1)

    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

    SINK_URI="blackhole:///"
    run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
}

function check() {
    service_type_count=$(cat $OUR_DIR/packets.dump | jq '.service' | sort | uniq | wc -l)
    # at least two types: 
    #   "pdpb.PD"
    #   "tikvpb.TiKV"
    [ $service_type_count -ge 2 ]
}

trap stop_tidb_cluster EXIT
trap "killall grpc-dump" EXIT

prepare
check

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
