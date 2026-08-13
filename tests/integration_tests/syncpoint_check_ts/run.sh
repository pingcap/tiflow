#!/bin/bash

# [DISCRIPTION]:
#   The test is used to check the first sync point ts value when we set different SyncPointInterval
# [STEP]:
#   1. Create changefeed with --sync-point --sync-interval=30s
#   2. After test, get fisrt syncpoints from tidb_cdc.syncpoint_v1
#   3. check the first syncpoint ts value

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "kafka downstream isn't support syncpoint record"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_sql "SET GLOBAL tidb_enable_external_ts_read = on;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# this test contains `set global tidb_external_ts = ?` , which requires super privilege, so we
	# can't use the normal user
	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql "SET GLOBAL tidb_enable_external_ts_read = off;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	sleep 60

	run_sql "SELECT primary_ts, secondary_ts FROM tidb_cdc.syncpoint_v1 order by primary_ts limit 1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "____________________________________"
	cat "$OUT_DIR/sql_res.$TEST_NAME.txt"
	primary_ts=($(grep primary_ts $OUT_DIR/sql_res.$TEST_NAME.txt | awk -F ": " '{print $2}'))
	echo "primary_ts is " $primary_ts "start_ts is " $start_ts

	shifted_primary_ts=$(($primary_ts >> 18))
	if [ $(($shifted_primary_ts % 30000)) -eq 0 ]; then
		pre_primary_ts=$(($primary_ts >> 18 - 30000))
		pre_primary_ts=$(($pre_primary_ts << 18))
		if [ $pre_primary_ts -lt $start_ts ]; then
			echo "check successfully"
		else
			echo "primary ts is not correct, there should be a smaller primary ts as the first sync point ts"
			exit 1
		fi
	else
		echo "primary ts is not correct, the difference between it and the time.Unix(0,0) should be the integer multiples of the syncPointInterval "
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
