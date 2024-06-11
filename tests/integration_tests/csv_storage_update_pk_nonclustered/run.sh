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
	local should_pass_check=$4
	SINK_URI="file://$WORK_DIR/storage_test/$changefeed_id?flush-interval=5s"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$CUR/conf/$changefeed_id.toml -c "$changefeed_id"

	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/$changefeed_id.toml $changefeed_id
	sleep 8

	cp $CUR/conf/diff_config.toml $WORK_DIR/diff_config.toml
	sed -i "s/<suffix>/$changefeed_id/" $WORK_DIR/diff_config.toml
	if [[ $should_pass_check == true ]]; then
		check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 100
	else
		check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 30 && exit 1 || echo "check_sync_diff failed as expected for $changefeed_id"
	fi

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

	run_changefeed "changefeed1" $start_ts 10 true
	# changefeed2 fail since delete events are not sorted
	run_changefeed "changefeed2" $start_ts 10 false
	# changefeed3 fail since update pk/uk events are not split
	run_changefeed "changefeed3" $start_ts 10 false
	run_changefeed "changefeed4" $start_ts 20 true
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
