#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=10

function check_old_value_enabled() {
	row_logs=$(grep "EmitRowChangedEvents" "$1/cdc.log")
	echo $row_logs

	# check update rows
	# check if exist a row include `column` and `pre-column`
	# When old value is turned on, we will have both column and pre-column in the update.
	# So here we have 2 (pre val) and 3 (new val).
	update_with_old_value_count=$(grep "EmitRowChangedEvents" "$1/cdc.log" | grep 'pre\-columns\\\":\[' | grep '\"columns\\\":\[' | grep 'value\\\":2' | grep -c 'value\\\":3')
	if [[ "$update_with_old_value_count" -ne 1 ]]; then
		echo "can't found update row with old value"
		exit 1
	fi

	# check if exist a update row without `pre-column`
	# When old value is turned off, we only have the column in the update.
	# So here we only have 3 (new val).
	update_without_old_value_count=$(grep "EmitRowChangedEvents" "$1/cdc.log" | grep 'pre\-columns\\\":null' | grep -c 'value\\\":3')
	if [[ "$update_without_old_value_count" -ne 1 ]]; then
		echo "can't found update row without old value"
		exit 1
	fi

	# check delete rows
	# check if exist a delete row with a complete `pre-column`
	# When old value is turned on, the pre-column in our delete will include all the columns.
	# So here we have 1 (id) and 3 (val).
	delete_with_old_value_count=$(grep "EmitRowChangedEvents" "$1/cdc.log" | grep 'pre\-columns\\\":\[' | grep 'columns\\\":null' | grep 'value\\\":1' | grep -c 'value\\\":3')
	if [[ "$delete_with_old_value_count" -ne 1 ]]; then
		echo "can't found delete row with old value"
		exit 1
	fi

	# check if exist a delete row without a complete `pre-column`
	# When old value is turned off, the pre-column in our delete will only include the handle columns.
	# So here we only have 1 (id).
	delete_without_old_value_count=$(grep "EmitRowChangedEvents" "$1/cdc.log" | grep 'pre\-columns\\\":\[' | grep 'columns\\\":null' | grep -c 'value\\\":1,\\\"default\\\":null},null')
	if [[ "$delete_without_old_value_count" -ne 1 ]]; then
		echo "can't found delete row without old value"
		exit 1
	fi
}

export -f check_old_value_enabled

function run() {
	# kafka is not supported yet.
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="blackhole://"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" -c "old-value-cf" --config="$CUR/conf/changefeed1.toml"
	cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" -c "no-old-value-cf" --config="$CUR/conf/changefeed2.toml"
	run_sql "CREATE DATABASE multi_changefeed;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table multi_changefeed.t1 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO multi_changefeed.t1 VALUES (1,2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE multi_changefeed.t1 SET val = 3;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DELETE FROM multi_changefeed.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	ensure $MAX_RETRIES check_old_value_enabled $WORK_DIR
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
