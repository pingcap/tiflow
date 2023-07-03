#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function run() {
	# No need to test kafka and storage sink, since the logic are all the same.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/InjectChangefeedFastFailError=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"

	changefeedid="changefeed-fast-fail"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid

	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "failed" "ErrStartTsBeforeGC" ""

	# test changefeed remove
	result=$(cdc cli changefeed remove -c $changefeedid)
	if [[ $result != *"Changefeed remove successfully"* ]]; then
		echo "changefeed remove result is expected to contains 'Changefeed remove successfully', \
              but actually got $result"
		exit 1
	fi

	# test changefeed remove twice
	result=$(cdc cli changefeed remove -c $changefeedid)
	if [[ $result != *"Changefeed not found"* ]]; then
		echo "changefeeed remove result is expected to contains 'Changefeed not found', \
            but actually got $result"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
