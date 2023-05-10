#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

stop() {
	# to distinguish whether the test failed in the DML synchronization phase or the DDL synchronization phase
	echo $(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -e "select count(*) from consistent_replicate_nfs.usertable;")
	stop_tidb_cluster
}

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc cli changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" 2>&1 | tail -n2 | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE consistent_replicate_nfs;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_replicate_nfs
	check_table_exists "consistent_replicate_nfs.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cleanup_process $CDC_BINARY
	# to ensure row changed events have been replicated to TiCDC
	sleep 10

	storage_path="nfs://$WORK_DIR/nfs/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')

	sed "s/<placeholder>/$rts/g" $CUR/conf/diff_config.toml >$WORK_DIR/diff_config.toml

	cat $WORK_DIR/diff_config.toml
	cdc redo apply --tmp-dir="$tmp_download_path/apply" --storage="$storage_path" --sink-uri="mysql://normal:123456@127.0.0.1:3306/"
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
