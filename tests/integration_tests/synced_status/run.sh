#!/bin/bash

# [DISCRIPTION]:
#   This test is related to 
#   It will test the sync status request of cdc server in the following scenarios:(including both enable redo and disable redo)
#   1. The sync status request of cdc server when the upstream cluster is available
#      1.1 pdNow - lastSyncedTs > threshold, pdNow - checkpointTs < threshold
#      1.2 pdNow - lastSyncedTs < threshold
#   2. The sync status request of cdc server when the upstream pd is unavailable
#      2.1 resolvedTs - checkpointTs < threshold
#   3. The sync status request of cdc server when the upstream tikv is unavailable
#      3.1 pdNow - lastSyncedTs > threshold, pdNow - checkpointTs > threshold, resolvedTs - checkpointTs < threshold
#      3.2 pdNow - lastSyncedTs < threshold
#   4. The sync status request of cdc server when the downstream tidb is available
#      4.1 pdNow - lastSyncedTs > threshold, pdNow - checkpointTs < threshold
#      4.2 pdNow - lastSyncedTs < threshold
# [STEP]:
#   1. Create changefeed with synced-time-config = xx
#   2. insert data to upstream cluster, and do the related actions for each scenarios
#   3. do the query of synced status of cdc server
#   4. check the info and status of query

set -xeu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function kill_pd(){
    info=`ps aux | grep pd-server | grep $WORK_DIR` || true
    `ps aux | grep pd-server | grep $WORK_DIR | awk '{print $2}' | xargs kill -9 &>/dev/null ` || true
}

function kill_tikv(){
    info=`ps aux | grep tikv-server | grep $WORK_DIR` || true
    `ps aux | grep tikv-server  | grep $WORK_DIR | awk '{print $2}' | xargs kill -9 &>/dev/null ` || true
}

function kill_tidb(){
    info=`ps aux | grep tidb-server | grep $WORK_DIR` || true
    `ps aux | grep tidb-server  | grep $WORK_DIR | awk '{print $2}' | xargs kill -9 &>/dev/null ` || true
}

function run_normal_case_and_unavailable_pd() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1" --config="$CUR/conf/changefeed.toml"

    # case 1: test in available cluster
	synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`

    status=$(echo $synced_status | jq '.synced')
    sink_checkpoint_ts=$(echo $synced_status | jq -r '.sink_checkpoint_ts')
    puller_resolved_ts=$(echo $synced_status | jq -r '.puller_resolved_ts')
    last_synced_ts=$(echo $synced_status | jq -r '.last_synced_ts')
    if [ $status != true ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    # the timestamp for puller_resolved_ts is 0 when do data insert
    if [ "$puller_resolved_ts" != "1970-01-01 08:00:00" ]; then 
        echo "puller_resolved_ts is not 1970-01-01 08:00:00"
        exit 1
    fi
    # the timestamp for last_synced_ts is 0 when do data insert
    if [ "$last_synced_ts" != "1970-01-01 08:00:00" ]; then 
        echo "last_synced_ts is not 1970-01-01 08:00:00"
        exit 1
    fi

    # compare sink_checkpoint_ts with current time
    current=$(date +"%Y-%m-%d %H:%M:%S")
    echo "sink_checkpoint_ts is "$sink_checkpoint_ts
    checkpoint_timestamp=$(date -d "$sink_checkpoint_ts" +%s)
    current_timestamp=$(date -d "$current" +%s)
    if [ $(($current_timestamp-$checkpoint_timestamp)) -gt 300 ]; then # give a soft check
        echo "sink_checkpoint_ts is not correct"
        exit 1
    fi

    run_sql "USE TEST;Create table t1(a int primary key, b int);insert into t1 values(1,2);insert into t1 values(2,3);"
    check_table_exists "test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    sleep 5 # wait data insert
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != false ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    if [ "$info" != "The data syncing is not finished, please wait" ]; then
        echo "synced status info is not correct"
        exit 1
    fi
    
    sleep 130 # wait enough time for pass synced-check-interval
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != true ]; then
        echo "synced status isn't correct"
        exit 1
    fi


    #========== 
    # case 2: test with unavailable pd 
    kill_pd

    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != false ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    target_message="[CDC:ErrAPIGetPDClientFailed]failed to get PDClient to connect PD, please recheck: [pd] failed to get cluster id. \
You can check the pd first, and if pd is available, means we don't finish sync data. \
If pd is not available, please check the whether we satisfy the condition that \
the time difference from lastSyncedTs to the current time from the time zone of pd is greater than 120 secs. \
If it's satisfied, means the data syncing is totally finished"

    if [ "$info" != "$target_message" ]; then
        echo "synced status info is not correct"
        exit 1
    fi

	cleanup_process $CDC_BINARY
    stop_tidb_cluster
}

function run_case_with_unavailable_tikv() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1" --config="$CUR/conf/changefeed.toml"

    # case 3: test in unavailable tikv cluster
    run_sql "USE TEST;Create table t1(a int primary key, b int);insert into t1 values(1,2);insert into t1 values(2,3);"
    check_table_exists "test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    sleep 5 # make data inserted into downstream
    kill_tikv

    # test the case when pdNow - lastSyncedTs < threshold
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != false ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    target_message="The data syncing is not finished, please wait"

    if [ "$info" != "$target_message" ]; then
        echo "synced status info is not correct"
        exit 1
    fi

    sleep 130 # wait enough time for pass synced-check-interval
    # test the case when pdNow - lastSyncedTs > threshold
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != false ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    target_message="Please check whether pd is health and tikv region is all available. \
If pd is not health or tikv region is not available, the data syncing is finished. \
Otherwise the data syncing is not finished, please wait"

    if [ "$info" != "$target_message" ]; then
        echo "synced status info is not correct"
        exit 1
    fi

	cleanup_process $CDC_BINARY
    stop_tidb_cluster
}

function run_case_with_unavailable_tidb() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1" --config="$CUR/conf/changefeed.toml"

    # case 3: test in unavailable tikv cluster
    run_sql "USE TEST;Create table t1(a int primary key, b int);insert into t1 values(1,2);insert into t1 values(2,3);"
    check_table_exists "test.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    sleep 5 # make data inserted into downstream
    kill_tidb

    # test the case when pdNow - lastSyncedTs < threshold
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != false ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    target_message="The data syncing is not finished, please wait"

    if [ "$info" != "$target_message" ]; then
        echo "synced status info is not correct"
        exit 1
    fi

    sleep 130 # wait enough time for pass synced-check-interval
    # test the case when pdNow - lastSyncedTs > threshold
    synced_status=`curl -X GET http://127.0.0.1:8300/api/v2/changefeeds/test-1/synced`
    status=$(echo $synced_status | jq '.synced')
    if [ $status != true ]; then
        echo "synced status isn't correct"
        exit 1
    fi
    info=$(echo $synced_status | jq -r '.info')
    target_message="Data syncing is finished"

    if [ "$info" != "$target_message" ]; then
        echo "synced status info is not correct"
        exit 1
    fi

	cleanup_process $CDC_BINARY
    stop_tidb_cluster
}



trap stop_tidb_cluster EXIT
run_normal_case_and_unavailable_pd $*
run_case_with_unavailable_tikv $*
run_case_with_unavailable_tidb $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"