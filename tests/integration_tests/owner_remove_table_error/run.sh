#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4
MAX_RETRIES=20

function run() {
	# kafka is not supported yet.
	if [ "$SINK_TYPE" == "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"

	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/owner/OwnerRemoveTableError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE owner_remove_table_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table owner_remove_table_error.t1(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO owner_remove_table_error.t1 VALUES (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DROP table owner_remove_table_error.t1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table owner_remove_table_error.t2(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO owner_remove_table_error.t2 VALUES (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table owner_remove_table_error.finished_mark(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "owner_remove_table_error.finished_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
