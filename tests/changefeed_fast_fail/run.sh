#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function check_changefeed_mark_error() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "error" ]]; then
		echo "changefeed state $state does not equal to error"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ "$error_msg" ]]; then
		echo "error message '$message' is not as expected '$error_msg'"
		exit 1
	fi
}

function check_changefeed_mark_failed_regex() {
	endpoints=$1
	changefeedid=$2
	error_msg=$3
	info=$(cdc cli changefeed query --pd=$endpoints -c $changefeedid -s)
	echo "$info"
	state=$(echo $info | jq -r '.state')
	if [[ ! "$state" == "failed" ]]; then
		echo "changefeed state $state does not equal to failed"
		exit 1
	fi
	message=$(echo $info | jq -r '.error.message')
	if [[ ! "$message" =~ $error_msg ]]; then
		echo "error message '$message' does not match '$error_msg'"
		exit 1
	fi
}

export -f check_changefeed_mark_failed_regex

function run() {
	# it is no need to test kafka
	# the logic are all the same
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	export GO_FAILPOINTS='github.com/pingcap/ticdc/cdc/owner/InjectChangefeedDDLError=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"

	changefeedid="changefeed-fast-fail"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid

	ensure $MAX_RETRIES check_changefeed_mark_failed_regex http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_2} "ErrGCTTLExceeded"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
