#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

stop() {
	# to distinguish whether the test failed in the DML synchronization phase or the DDL synchronization phase
	echo $(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -e "show databases;")
	stop_tidb_cluster
}

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
	run_sql "set @@global.tidb_enable_exchange_partition=on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix partition_table.server1

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql_file $CUR/data/create.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "partition_table2.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	sleep 5
	cleanup_process $CDC_BINARY
	# Inject the failpoint to prevent sink execution, but the global resolved can be moved forward.
	# Then we can apply redo log to reach an eventual consistent state in downstream.
	export GO_FAILPOINTS='github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql/MySQLSinkHangLongTime=return(true);github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql/MySQLSinkExecDDLDelay=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix partition_table.server2

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# to ensure row changed events have been replicated to TiCDC
	sleep 120
	cleanup_process $CDC_BINARY

	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	export GO_FAILPOINTS=''

	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')
	sed "s/<placeholder>/$rts/g" $CUR/conf/diff_config.toml >$WORK_DIR/diff_config.toml

	cat $WORK_DIR/diff_config.toml
	cdc redo apply --tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/"

	# check_table_exists "partition_table.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
