#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This integration test is used to test the following scenario:
# 1. cdc works well in batch mode
# 2. cdc works well in no-batch mode
# 3. cdc can switch from batch mode to no-batch mode and vice versa and works well
function run() {
	# batch mode only supports mysql sink
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_sql "set global tidb_enable_change_multi_schema = on" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# This must be set before cdc server starts
	run_sql "set global tidb_enable_change_multi_schema = on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# TiDB global variables cache 2 seconds at most
	sleep 2

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# this test contains `recover table`, which requires super privilege, so we
	# can't use the normal user
	SINK_URI="mysql://root@127.0.0.1:3306/?batch-dml-enable=true"

	changefeed_id="test"
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c ${changefeed_id}

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# pause changefeed
	run_cdc_cli changefeed pause -c ${changefeed_id}
	# update changefeed to no batch dml mode
	run_cdc_cli changefeed update -c ${changefeed_id} --sink-uri="mysql://root@127.0.0.1:3306/?batch-dml-enable=true" --no-confirm
	# resume changefeed
	run_cdc_cli changefeed resume -c ${changefeed_id}

	run_sql_file $CUR/data/test_v5.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# pause changefeed
	run_cdc_cli changefeed pause -c ${changefeed_id}
	# update changefeed to no batch dml mode
	run_cdc_cli changefeed update -c ${changefeed_id} --sink-uri="mysql://root@127.0.0.1:3306/?batch-dml-enable=false" --no-confirm
	# resume changefeed
	run_cdc_cli changefeed resume -c ${changefeed_id}

	run_sql_file $CUR/data/test_finish.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists batch_update_to_no_batch.v1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists batch_update_to_no_batch.recover_and_insert ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists batch_update_to_no_batch.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
