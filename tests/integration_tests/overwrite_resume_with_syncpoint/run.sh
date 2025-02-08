#!/bin/bash
# the script test when we enable syncpoint, and pause the changefeed, 
# then resume with a forward checkpoint, to ensure the changefeed can be sync correctly.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# No need to test kafka and storage sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR --tidb-config $CUR/conf/tidb_config.toml

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY 

    SINK_URI="mysql://root@127.0.0.1:3306/"
    run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml --changefeed-id="test4"

	check_ts_forward "test4"

    run_cdc_cli changefeed pause --changefeed-id="test4"

    # resume a forward checkpointTs
    run_cdc_cli changefeed resume --changefeed-id="test4" --no-confirm --overwrite-checkpoint-ts=999999999999999999 

    check_ts_forward "test4"

	cleanup_process $CDC_BINARY
}


trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
