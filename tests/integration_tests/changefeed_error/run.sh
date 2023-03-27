#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function check_no_changefeed() {
	pd=$1
	count=$(cdc cli changefeed list --pd=$pd 2>&1 | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

function check_no_capture() {
	pd=$1
	count=$(cdc cli capture list --pd=$pd 2>&1 | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

export -f check_no_changefeed
export -f check_no_capture

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/NewChangefeedNoRetryError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	changefeedid="changefeed-error"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid
	if [ "$SINK_TYPE" == "kafka" ]; then
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"
	fi

	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "failed" "[CDC:ErrStartTsBeforeGC]" ""
	run_cdc_cli changefeed resume -c $changefeedid

	check_table_exists "changefeed_error.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/NewChangefeedRetryError=return(true)'
	kill $capture_pid
	ensure $MAX_RETRIES check_no_capture http://${UP_PD_HOST_1}:${UP_PD_PORT_1}
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "error" "failpoint injected retriable error" ""

	run_cdc_cli changefeed remove -c $changefeedid
	ensure $MAX_RETRIES check_no_changefeed ${UP_PD_HOST_1}:${UP_PD_PORT_1}

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY

	# owner DDL error case
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/InjectChangefeedDDLError=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	changefeedid_1="changefeed-error-1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid_1

	run_sql "CREATE table changefeed_error.DDLERROR(id int primary key, val int);"
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_1} "error" "[CDC:ErrExecDDLFailed]exec DDL failed" ""

	run_cdc_cli changefeed remove -c $changefeedid_1
	cleanup_process $CDC_BINARY

	# updating GC safepoint failure case
	export GO_FAILPOINTS='github.com/pingcap/tiflow/pkg/txnutil/gc/InjectActualGCSafePoint=return(9223372036854775807)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	changefeedid_2="changefeed-error-2"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid_2
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_2} "failed" "[CDC:ErrSnapshotLostByGC]" ""

	run_cdc_cli changefeed remove -c $changefeedid_2
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY

	# make sure initialize changefeed error will not stuck the owner
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/ChangefeedNewRedoManagerError=2*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	changefeedid_3="changefeed-initialize-error"
	run_cdc_cli changefeed create --start-ts=0 --sink-uri="$SINK_URI" -c $changefeedid_3
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_3} "normal" "null" ""
	run_cdc_cli changefeed pause -c $changefeedid_3
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_3} "stopped" "changefeed new redo manager injected error" ""
	run_cdc_cli changefeed resume -c $changefeedid_3
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_3} "normal" "null" ""
	run_cdc_cli changefeed remove -c $changefeedid_3
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
