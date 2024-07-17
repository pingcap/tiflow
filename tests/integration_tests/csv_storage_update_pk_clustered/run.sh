#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run_changefeed() {
	local changefeed_id=$1
	local start_ts=$2
	local expected_split_count=$3
	SINK_URI="file://$WORK_DIR/storage_test/$changefeed_id?flush-interval=5s"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$CUR/conf/$changefeed_id.toml -c "$changefeed_id"

	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/$changefeed_id.toml $changefeed_id
	sleep 8
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	real_split_count=$(grep "split update event" $WORK_DIR/cdc.log | wc -l)
	if [[ $real_split_count -ne $expected_split_count ]]; then
		echo "expected split count $expected_split_count, real split count $real_split_count"
		exit 1
	fi
	run_sql "drop database if exists test" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
}

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/run.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_changefeed "changefeed1" $start_ts 5
	run_changefeed "changefeed2" $start_ts 5
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
