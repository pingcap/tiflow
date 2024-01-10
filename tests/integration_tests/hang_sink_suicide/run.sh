#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# test with mysql sink only
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql/MySQLSinkHangLongTime=2*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 1 --addr "127.0.0.1:8300"

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"
	changefeed_id=test
	cdc cli changefeed create -c $changefeed_id --config $CUR/conf/changefeed.toml --pd=$pd_addr --sink-uri="$SINK_URI"

	run_sql "CREATE DATABASE hang_sink_suicide;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table hang_sink_suicide.t1 (id int primary key auto_increment)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table hang_sink_suicide.t2 (id int primary key auto_increment)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "hang_sink_suicide.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "hang_sink_suicide.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "insert into hang_sink_suicide.t1 values (),(),(),(),()"
	run_sql "insert into hang_sink_suicide.t2 values (),(),(),(),()"

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
# TODO: update the case to use kafka sink instead of mysql sink.
# run $*
# check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
